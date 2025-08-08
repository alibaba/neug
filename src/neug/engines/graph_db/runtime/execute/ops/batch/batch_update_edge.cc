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

#include "neug/engines/graph_db/runtime/execute/ops/batch/batch_update_edge.h"
#include "neug/engines/graph_db/runtime/common/columns/edge_columns.h"
#include "neug/utils/pb_utils.h"

namespace gs {

namespace runtime {
namespace ops {

bl::result<Context> UpdateEdgeOpr::Eval(
    GraphUpdateInterface& graph,
    const std::map<std::string, std::string>& params, Context&& ctx,
    OprTimer& timer) {
  VLOG(10) << "Executing UpdateEdgeOpr with " << edge_data_.size()
           << " entries.";
  for (const auto& entry : edge_data_) {
    auto tag_id = std::get<0>(entry);
    const auto& prop_name = std::get<1>(entry);
    const auto& value = std::get<2>(entry);

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
    LOG(INFO) << "edge column info: " << edge_col->column_info();

    for (size_t ind = 0; ind < edge_col->size(); ++ind) {
      auto er = edge_col->get_edge(ind);
      auto label_id = er.label_triplet().edge_label;
      auto src_label = er.label_triplet().src_label;
      auto dst_label = er.label_triplet().dst_label;
      const auto& property_names = graph.schema().get_edge_property_names(
          src_label, dst_label, label_id);
      const auto& property_types =
          graph.schema().get_edge_properties(src_label, dst_label, label_id);
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
      if (property_types[col_id] != value.type) {
        LOG(ERROR) << "Property type mismatch for property " << prop_name
                   << ": expected " << property_types[col_id].ToString()
                   << ", got " << value.type.ToString();
        THROW_RUNTIME_ERROR("Property type mismatch for property " + prop_name +
                            ": expected " + property_types[col_id].ToString() +
                            ", got " + value.type.ToString());
      }
      if (er.dir() == Direction::kOut) {
        graph.SetEdgeData(true, src_label, er.src(), dst_label, er.dst(),
                          label_id, value, col_id);
        graph.SetEdgeData(false, dst_label, er.dst(), src_label, er.src(),
                          label_id, value, col_id);
      } else {
        graph.SetEdgeData(false, dst_label, er.dst(), src_label, er.src(),
                          label_id, value, col_id);
        graph.SetEdgeData(true, src_label, er.src(), dst_label, er.dst(),
                          label_id, value, col_id);
      }

      // We also need to update the edge data stored in previous columns in
      // context, for consistency.
      edge_col->set_edge_data(ind, col_id, value);
      LOG(INFO) << "After insert " << value.to_string();
    }
  }
  return bl::result<Context>(std::move(ctx));
}

std::unique_ptr<IUpdateOperator> UpdateEdgeOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.query_plan().plan(op_idx).opr().set_edge();
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
    if (prop_mapping.data().operators_size() != 1) {
      THROW_RUNTIME_ERROR(
          "Setting edge property with multiple operators is not "
          "supported.");
    }
    if (prop_mapping.data().operators(0).item_case() !=
        common::ExprOpr::ItemCase::kConst) {
      THROW_RUNTIME_ERROR(
          "Setting edge property with non-constant value is not "
          "supported.");
    }
    edge_data_vec.emplace_back(
        tag_id, prop_mapping.property().key().name(),
        expr_opr_value_to_any(prop_mapping.data().operators(0)));
  }
  return std::make_unique<UpdateEdgeOpr>(std::move(edge_data_vec));
}

}  // namespace ops

}  // namespace runtime

}  // namespace gs
