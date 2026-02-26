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

#include "neug/execution/common/operators/insert/create_edge.h"
#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/expression/expr.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace execution {
namespace ops {
neug::result<Context> CreateEdge::insert_edge(
    StorageInsertInterface& graph, Context&& ctx,
    std::vector<LabelTriplet> labels,
    const std::vector<std::pair<int32_t, int32_t>>& src_dst_tags,
    std::vector<
        std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>>&&
        props,
    const std::vector<int>& alias) {
  const auto& schema = graph.schema();
  for (size_t i = 0; i < labels.size(); ++i) {
    label_t src_label = labels[i].src_label;
    label_t dst_label = labels[i].dst_label;
    label_t edge_label = labels[i].edge_label;
    SDSLEdgeColumnBuilder builder(Direction::kOut, labels[i]);
    auto& properties = props[i];
    auto properties_name =
        schema.get_edge_property_names(src_label, dst_label, edge_label);
    auto properties_type =
        schema.get_edge_properties(src_label, dst_label, edge_label);
    const auto& default_values = schema.get_edge_default_property_values(
        src_label, dst_label, edge_label);
    assert(properties_name.size() == properties_type.size() &&
           properties_name.size() == default_values.size());
    if (properties.size() != properties_name.size()) {
      THROW_RUNTIME_ERROR("Provided properties size " +
                          std::to_string(properties.size()) +
                          " does not match schema size: " +
                          std::to_string(properties_name.size()));
    }
    const auto& src_vertex_col = dynamic_cast<const IVertexColumn&>(
        *ctx.get(src_dst_tags[i].first).get());
    const auto& dst_vertex_col = dynamic_cast<const IVertexColumn&>(
        *ctx.get(src_dst_tags[i].second).get());
    for (size_t i = 0; i < ctx.row_num(); ++i) {
      auto v1 = src_vertex_col.get_vertex(i);
      if (v1.label_ != src_label) {
        THROW_RUNTIME_ERROR("Source vertex label mismatch: expected " +
                            std::to_string(src_label) + ", got " +
                            std::to_string(v1.label_));
      }
      auto v2 = dst_vertex_col.get_vertex(i);
      if (v2.label_ != dst_label) {
        THROW_RUNTIME_ERROR("Destination vertex label mismatch: expected " +
                            std::to_string(dst_label) + ", got " +
                            std::to_string(v2.label_));
      }
      std::vector<Property> property_values(properties.size());
      for (size_t j = 0; j < properties.size(); ++j) {
        const auto& [prop_name, prop_expr] = properties[j];
        Value value = prop_expr->Cast<RecordExprBase>().eval_record(ctx, i);
        auto it = std::find(properties_name.begin(), properties_name.end(),
                            prop_name);
        if (it == properties_name.end()) {
          THROW_RUNTIME_ERROR(
              "Property " + prop_name + " not found in schema for edge (" +
              std::to_string(src_label) + "," + std::to_string(edge_label) +
              "," + std::to_string(dst_label) + ")");
        } else {
          size_t index = std::distance(properties_name.begin(), it);
          if (value.IsNull()) {
            property_values[index] = default_values[index];
          } else {
            property_values[index] = value_to_property(value);
          }
        }
      }
      if (!graph.AddEdge(src_label, v1.vid_, dst_label, v2.vid_, edge_label,
                         property_values)) {
        THROW_RUNTIME_ERROR("Failed to add edge (" + std::to_string(src_label) +
                            "," + std::to_string(edge_label) + "," +
                            std::to_string(dst_label) + ")");
      }
      // TODO: store edge properties
      builder.push_back_opt(v1.vid_, v2.vid_, nullptr);
    }
    ctx.set(alias[i], builder.finish());
  }
  return ctx;
}
}  // namespace ops
}  // namespace execution
}  // namespace neug