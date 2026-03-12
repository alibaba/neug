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
#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/expression/expr.h"
#include "neug/storages/csr/generic_view_utils.h"
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
  for (const auto& entry : edge_data_) {
    auto tag_id = std::get<0>(entry);
    const auto& prop_name = std::get<1>(entry);
    const auto& expression = std::get<2>(entry);

    auto col = ctx.get(tag_id);
    if (!col) {
      LOG(ERROR) << "Column " << tag_id << " not found in context.";
      THROW_RUNTIME_ERROR("Column " + std::to_string(tag_id) +
                          " not found in context.");
    }
    auto edge_col = std::dynamic_pointer_cast<IEdgeColumn>(col);
    if (!edge_col) {
      LOG(ERROR) << "Column " << tag_id << " is not an edge column.";
      THROW_RUNTIME_ERROR("Column " + std::to_string(tag_id) +
                          " is not an edge column.");
    }

    auto expr = expression->bind(&graph, params);
    const auto& expr_ref = expr->Cast<RecordExprBase>();
    for (size_t ind = 0; ind < edge_col->size(); ++ind) {
      auto value = expr_ref.eval_record(ctx, ind);
      auto er = edge_col->get_edge(ind);
      auto label_id = er.label.edge_label;
      auto src_label = er.label.src_label;
      auto dst_label = er.label.dst_label;
      auto property_names = graph.schema().get_edge_property_names(
          src_label, dst_label, label_id);
      int col_id = -1;
      for (size_t i = 0; i < property_names.size(); ++i) {
        if (property_names[i] == prop_name) {
          col_id = static_cast<int>(i);
          break;
        }
      }
      if (col_id == -1) {
        LOG(ERROR) << "Property " << prop_name
                   << " does not exist for edge label "
                   << static_cast<int>(label_id);
        THROW_RUNTIME_ERROR(
            "Property " + prop_name +
            " does not exist for edge label: " + std::to_string(label_id));
      }
      Property prop;
      auto val_type = value.type();
      if (val_type.id() == DataTypeId::kEmpty) {
      } else if (val_type.id() == DataTypeId::kInt32) {
        prop.set_int32(value.GetValue<int32_t>());
      } else if (val_type.id() == DataTypeId::kInt64) {
        prop.set_int64(value.GetValue<int64_t>());
      } else if (val_type.id() == DataTypeId::kVarchar) {
        prop.set_string_view(StringValue::Get(value));
      } else if (val_type.id() == DataTypeId::kDouble) {
        prop.set_double(value.GetValue<double>());
      } else {
        THROW_RUNTIME_ERROR("Unsupported property type: " +
                            std::to_string(static_cast<int>(val_type.id())));
      }
      auto oe_view =
          graph.GetGenericOutgoingGraphView(src_label, dst_label, label_id);
      auto ie_view =
          graph.GetGenericIncomingGraphView(dst_label, src_label, label_id);
      auto prop_types =
          graph.schema().get_edge_properties(src_label, dst_label, label_id);
      auto offset_pair =
          record_to_csr_offset_pair(oe_view, ie_view, er, prop_types);
      graph.UpdateEdgeProperty(src_label, er.src, dst_label, er.dst, label_id,
                               offset_pair.first, offset_pair.second, col_id,
                               prop);
    }
  }
  return neug::result<Context>(std::move(ctx));
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