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

#include "neug/execution/execute/ops/batch/batch_update_vertex.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/expression/expr.h"
#include "neug/utils/pb_utils.h"

#include <glog/logging.h>

namespace neug {
namespace execution {
namespace ops {

/**
 * @brief UpdateVertexOpr is used to update vertex properties in batch.
 */
class UpdateVertexOpr : public IOperator {
 public:
  using vertex_prop_vec_t =
      std::vector<std::tuple<int32_t, std::string, std::unique_ptr<ExprBase>>>;
  UpdateVertexOpr(vertex_prop_vec_t&& vertex_data)
      : vertex_data_(std::move(vertex_data)) {}

  std::string get_operator_name() const override { return "UpdateVertexOpr"; }

  neug::result<Context> eval_impl(StorageUpdateInterface& graph,
                                  const ParamsMap& params, Context&& ctx,
                                  OprTimer* timer);

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override;

 private:
  // No alias is produced in this operator.
  vertex_prop_vec_t vertex_data_;
};

neug::result<Context> UpdateVertexOpr::eval_impl(StorageUpdateInterface& graph,
                                                 const ParamsMap& params,
                                                 Context&& ctx,
                                                 OprTimer* timer) {
  for (const auto& entry : vertex_data_) {
    auto tag_id = std::get<0>(entry);
    const auto& prop_name = std::get<1>(entry);
    const auto& expression = std::get<2>(entry);
    auto col = ctx.get(tag_id);
    if (!col) {
      LOG(ERROR) << "Column " << tag_id << " not found in context.";
    }
    auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
    if (!vertex_col) {
      LOG(ERROR) << "Column " << tag_id << " is not a vertex column.";
      THROW_RUNTIME_ERROR("Column " + std::to_string(tag_id) +
                          " is not a vertex column");
    }

    auto expr = expression->bind(&graph, params);
    const auto& expr_ref = expr->Cast<RecordExprBase>();

    for (size_t ind = 0; ind < vertex_col->size(); ++ind) {
      auto value = value_to_property(expr_ref.eval_record(ctx, ind));
      auto vr = vertex_col->get_vertex(ind);
      auto label_id = vr.label();
      // Restricts: 0. Could not set primary key; 1. Could not set empty
      // value. 2. check the property exists
      const auto& property_names =
          graph.schema().get_vertex_property_names(label_id);
      const auto& property_types =
          graph.schema().get_vertex_properties(label_id);
      const auto& pks = graph.schema().get_vertex_primary_key(label_id);
      if (std::get<1>(pks[0]) == prop_name) {
        LOG(ERROR) << "Cannot set primary key property: " << prop_name
                   << " for vertex label: " << static_cast<int>(label_id);
        THROW_RUNTIME_ERROR("Cannot set primary key property: " + prop_name +
                            " for vertex label: " + std::to_string(label_id));
      }
      if (property_names.empty() ||
          std::find(property_names.begin(), property_names.end(), prop_name) ==
              property_names.end()) {
        LOG(ERROR) << "Property " << prop_name
                   << " does not exist for vertex label "
                   << static_cast<int>(label_id);
        THROW_RUNTIME_ERROR(
            "Property " + prop_name +
            " does not exist for vertex label: " + std::to_string(label_id));
      }
      auto pos =
          std::find(property_names.begin(), property_names.end(), prop_name);
      int32_t col_id = std::distance(property_names.begin(), pos);
      assert(col_id >= 0 &&
             col_id < static_cast<int32_t>(property_names.size()));
      if (property_types[col_id].id() != value.type()) {
        LOG(ERROR) << "Property type mismatch for property " << prop_name
                   << ": expected " << property_types[col_id].ToString()
                   << ", got " << std::to_string(value.type());
        THROW_RUNTIME_ERROR("Property type mismatch for property " + prop_name +
                            ": expected " + property_types[col_id].ToString() +
                            ", got " + std::to_string(value.type()));
      }
      graph.UpdateVertexProperty(vr.label(), vr.vid(), col_id, value);
    }
  }
  return neug::result<Context>(std::move(ctx));
}

neug::result<Context> UpdateVertexOpr::Eval(IStorageInterface& graph_interface,
                                            const ParamsMap& params,
                                            Context&& ctx, OprTimer* timer) {
  auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
  return eval_impl(graph, params, std::move(ctx), timer);
}

neug::result<OpBuildResultT> UpdateVertexOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta = ctx_meta;
  const auto& opr = plan.plan(op_idx).opr().set_vertex();
  typename UpdateVertexOpr::vertex_prop_vec_t vertex_data;
  for (auto& entry : opr.entries()) {
    auto& vertex_binding = entry.vertex_binding();
    if (!vertex_binding.has_tag()) {
      LOG(ERROR) << "Vertex binding must have a tag.";
      THROW_RUNTIME_ERROR("Vertex binding must have a tag.");
    }
    CHECK(vertex_binding.tag().item_case() == common::NameOrId::ItemCase::kId)
        << "Vertex binding tag must be an ID.";
    auto tag_id = vertex_binding.tag().id();
    const auto& prop_mapping = entry.property_mapping();
    if (!prop_mapping.property().has_key()) {
      THROW_RUNTIME_ERROR(
          "Setting vertex property without key is not supported.");
    }
    auto expression = neug::execution::parse_expression(
        prop_mapping.data(), ctx_meta, neug::execution::VarType::kRecord);
    vertex_data.emplace_back(tag_id, prop_mapping.property().key().name(),
                             std::move(expression));
  }
  return std::make_pair(
      std::make_unique<UpdateVertexOpr>(std::move(vertex_data)), ret_meta);
}
}  // namespace ops
}  // namespace execution
}  // namespace neug