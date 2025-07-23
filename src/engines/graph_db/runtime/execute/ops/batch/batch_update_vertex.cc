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

#include "src/engines/graph_db/runtime/execute/ops/batch/batch_update_vertex.h"

#include "src/utils/pb_utils.h"

namespace gs {
namespace runtime {
namespace ops {

template <typename GraphInterface>
bl::result<Context> UpdateVertexOpr::eval_impl(
    GraphInterface& graph, const std::map<std::string, std::string>& params,
    Context&& ctx, OprTimer& timer) {
  for (const auto& entry : vertex_data_) {
    auto tag_id = std::get<0>(entry);
    const auto& prop_name = std::get<1>(entry);
    const auto& value = std::get<2>(entry);

    auto col = ctx.get(tag_id);
    if (!col) {
      LOG(ERROR) << "Column " << tag_id << " not found in context.";
    }
    auto vertex_col = std::dynamic_pointer_cast<IVertexColumn>(col);
    if (!vertex_col) {
      LOG(ERROR) << "Column " << tag_id << " is not a vertex column.";
    }

    for (size_t ind = 0; ind < vertex_col->size(); ++ind) {
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
        throw std::runtime_error(
            "Cannot set primary key property: " + prop_name +
            " for vertex label: " + std::to_string(label_id));
      }
      if (property_names.empty() ||
          std::find(property_names.begin(), property_names.end(), prop_name) ==
              property_names.end()) {
        LOG(ERROR) << "Property " << prop_name
                   << " does not exist for vertex label "
                   << static_cast<int>(label_id);
        throw std::runtime_error(
            "Property " + prop_name +
            " does not exist for vertex label: " + std::to_string(label_id));
      }
      auto pos =
          std::find(property_names.begin(), property_names.end(), prop_name);
      int32_t col_id = std::distance(property_names.begin(), pos);
      assert(col_id >= 0 &&
             col_id < static_cast<int32_t>(property_names.size()));
      if (property_types[col_id] != value.type) {
        LOG(ERROR) << "Property type mismatch for property " << prop_name
                   << ": expected " << property_types[col_id].ToString()
                   << ", got " << value.type.ToString();
        throw std::runtime_error("Property type mismatch for property " +
                                 prop_name + ": expected " +
                                 property_types[col_id].ToString() + ", got " +
                                 value.type.ToString());
      }
      graph.SetVertexField(vr.label(), vr.vid(), col_id, value);
    }
  }
  return bl::result<Context>(std::move(ctx));
}

bl::result<Context> UpdateVertexOpr::Eval(
    GraphUpdateInterface& graph,
    const std::map<std::string, std::string>& params, Context&& ctx,
    OprTimer& timer) {
  return eval_impl(graph, params, std::move(ctx), timer);
}

std::unique_ptr<IUpdateOperator> UpdateVertexOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.query_plan().plan(op_idx).opr().set_vertex();
  typename UpdateVertexOpr::vertex_prop_vec_t vertex_data;
  for (auto& entry : opr.entries()) {
    auto& vertex_binding = entry.vertex_binding();
    if (!vertex_binding.has_tag()) {
      LOG(ERROR) << "Vertex binding must have a tag.";
      throw std::runtime_error("Vertex binding must have a tag.");
    }
    CHECK(vertex_binding.tag().item_case() == common::NameOrId::ItemCase::kId)
        << "Vertex binding tag must be an ID.";
    auto tag_id = vertex_binding.tag().id();
    const auto& prop_mapping = entry.property_mapping();
    if (!prop_mapping.property().has_key()) {
      throw std::runtime_error(
          "Setting vertex property without key is not supported.");
    }
    if (prop_mapping.data().operators_size() != 1) {
      throw std::runtime_error(
          "Setting vertex property with multiple operators is not "
          "supported.");
    }
    if (prop_mapping.data().operators(0).item_case() !=
        common::ExprOpr::ItemCase::kConst) {
      throw std::runtime_error(
          "Setting vertex property with non-constant value is not "
          "supported.");
    }
    vertex_data.emplace_back(
        tag_id, prop_mapping.property().key().name(),
        expr_opr_value_to_any(prop_mapping.data().operators(0)));
  }
  return std::make_unique<UpdateVertexOpr>(std::move(vertex_data));
}
}  // namespace ops
}  // namespace runtime
}  // namespace gs