/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/execution/execute/ops/batch/batch_update_edge.h"

#include <set>
#include <utility>
#include <vector>

#include "neug/common/columns/edge_columns.h"
#include "neug/execution/execute/ops/edge_column_rebuild.h"
#include "neug/execution/expression/expr.h"
#include "neug/utils/pb_utils.h"

namespace neug {

namespace execution {
namespace ops {

/**
 * @brief UpdateEdgeOpr is used to update edge properties in batch.
 */
class UpdateEdgeOpr : public IOperator {
 public:
  using edge_data_t =
      std::tuple<int32_t, std::string,
                 std::unique_ptr<ExprBase>>;  // tag_id, property_name, value
  using edge_data_vec_t = std::vector<edge_data_t>;

  explicit UpdateEdgeOpr(edge_data_vec_t&& edge_data)
      : edge_data_(std::move(edge_data)) {}

  std::string get_operator_name() const override { return "UpdateEdgeOpr"; }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override;

 private:
  edge_data_vec_t edge_data_;
};

neug::result<Context> UpdateEdgeOpr::Eval(IStorageInterface& graph_interface,
                                          const ParamsMap& params,
                                          Context&& ctx, OprTimer* timer) {
  auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
  VLOG(10) << "Executing UpdateEdgeOpr with " << edge_data_.size()
           << " entries.";

  struct PendingUpdate {
    EdgeRecord record;
    std::pair<int32_t, int32_t> offsets;
    int32_t property_id;
    Value value;
  };

  std::set<int32_t> edge_tags;
  for (const auto& entry : edge_data_) {
    edge_tags.insert(std::get<0>(entry));
  }

  std::set<LabelTriplet> affected_labels;
  for (const auto tag_id : edge_tags) {
    for (auto& chunk : ctx.chunks()) {
      auto column = chunk.get(tag_id);
      auto edge_column = std::dynamic_pointer_cast<IEdgeColumn>(column);
      if (!edge_column) {
        continue;
      }
      for (const auto& label : edge_column->get_labels()) {
        const auto edge_schema = graph.schema().get_edge_schema(
            label.src_label, label.dst_label, label.edge_label);
        if (edge_schema->is_bundled()) {
          affected_labels.insert(label);
        }
      }
    }
  }

  auto snapshots = CaptureEdgeColumnsForRefresh(graph, ctx, affected_labels);
  for (const auto& [tag_id, property_name, expression] : edge_data_) {
    auto bound_expression = expression->bind(&graph, params);
    const auto& record_expression = bound_expression->Cast<RecordExprBase>();
    std::vector<PendingUpdate> updates;
    bool refresh_columns = false;
    for (auto& chunk : ctx.chunks()) {
      auto column = chunk.get(tag_id);
      if (!column) {
        THROW_RUNTIME_ERROR("Column " + std::to_string(tag_id) +
                            " not found in context.");
      }
      auto edge_column = std::dynamic_pointer_cast<IEdgeColumn>(column);
      if (!edge_column) {
        THROW_RUNTIME_ERROR("Column " + std::to_string(tag_id) +
                            " is not an edge column.");
      }

      for (size_t row = 0; row < edge_column->size(); ++row) {
        if (!edge_column->has_value(row)) {
          continue;
        }

        const auto record = edge_column->get_edge(row);
        const auto edge_schema = graph.schema().get_edge_schema(
            record.label.src_label, record.label.dst_label,
            record.label.edge_label);
        const auto property_id = edge_schema->get_property_index(property_name);
        if (property_id < 0) {
          THROW_RUNTIME_ERROR(
              "Property " + property_name + " does not exist for edge label: " +
              std::to_string(static_cast<int>(record.label.edge_label)));
        }

        auto value = record_expression.eval_record(chunk.chunk(), row);
        if (value.IsNull()) {
          THROW_NOT_SUPPORTED_EXCEPTION("Setting NULL for property " +
                                        property_name);
        }
        if (edge_schema->properties[property_id] != value.type()) {
          THROW_RUNTIME_ERROR("Property type mismatch for property " +
                              property_name);
        }
        refresh_columns = refresh_columns || edge_schema->is_bundled();
        updates.push_back(PendingUpdate{record,
                                        ResolveEdgeOffsets(graph, record),
                                        property_id, std::move(value)});
      }
    }
    for (const auto& update : updates) {
      RETURN_STATUS_ERROR_IF_NOT_OK(graph.UpdateEdgeProperty(
          update.record.label.src_label, update.record.src,
          update.record.label.dst_label, update.record.dst,
          update.record.label.edge_label, update.offsets.first,
          update.offsets.second, update.property_id, update.value));
    }
    if (refresh_columns) {
      RefreshEdgeColumns(graph, snapshots);
    }
  }
  return std::move(ctx);
}

neug::result<OpBuildResultT> UpdateEdgeOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta meta = ctx_meta;
  const auto& opr = plan.plan(op_idx).opr().set_edge();
  typename UpdateEdgeOpr::edge_data_vec_t edge_data_vec;
  for (const auto& entry : opr.entries()) {
    auto& edge_binding = entry.edge_binding();
    if (!edge_binding.has_tag()) {
      LOG(ERROR) << "Edge binding must have a tag.";
      THROW_RUNTIME_ERROR("Edge binding must have a tag.");
    }
    CHECK(edge_binding.tag().item_case() == common::NameOrId::ItemCase::kId)
        << "Edge binding tag must be an ID.";
    auto tag_id = edge_binding.tag().id();
    const auto& prop_mapping = entry.property_mapping();
    if (!prop_mapping.property().has_key()) {
      THROW_RUNTIME_ERROR(
          "Setting edge property without key is not supported.");
    }
    auto expr =
        parse_expression(prop_mapping.data(), ctx_meta, VarType::kRecord);
    edge_data_vec.emplace_back(tag_id, prop_mapping.property().key().name(),
                               std::move(expr));
  }
  return std::make_pair(
      std::make_unique<UpdateEdgeOpr>(std::move(edge_data_vec)), meta);
}

}  // namespace ops

}  // namespace execution

}  // namespace neug
