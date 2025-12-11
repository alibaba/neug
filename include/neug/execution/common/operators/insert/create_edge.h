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
#pragma once

#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/utils/expr.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/result.h"

namespace gs {
namespace runtime {
namespace ops {
class CreateEdge {
 public:
  static gs::result<Context> insert_edge(
      StorageInsertInterface& graph, Context&& ctx,
      std::vector<LabelTriplet> labels,
      const std::vector<std::pair<int32_t, int32_t>>& src_dst_tags,
      const std::vector<std::vector<std::pair<std::string, Expr>>>& props,
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
      if (properties.size() != properties_name.size()) {
        THROW_RUNTIME_ERROR("Provided properties size " +
                            std::to_string(properties.size()) +
                            " does not match schema size: " +
                            std::to_string(properties_name.size()));
      }
      std::shared_ptr<Arena> arena = std::make_shared<Arena>();
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
          RTAny value = prop_expr.eval_path(i, *arena);
          auto it = std::find(properties_name.begin(), properties_name.end(),
                              prop_name);
          if (it == properties_name.end()) {
            THROW_RUNTIME_ERROR(
                "Property " + prop_name + " not found in schema for edge (" +
                std::to_string(src_label) + "," + std::to_string(edge_label) +
                "," + std::to_string(dst_label) + ")");
          } else {
            size_t index = std::distance(properties_name.begin(), it);
            property_values[index] = value.to_any();
          }
        }
        if (!graph.AddEdge(src_label, v1.vid_, dst_label, v2.vid_, edge_label,
                           property_values)) {
          THROW_RUNTIME_ERROR("Failed to add edge (" +
                              std::to_string(src_label) + "," +
                              std::to_string(edge_label) + "," +
                              std::to_string(dst_label) + ")");
        }
        // TODO: store edge properties
        builder.push_back_opt(v1.vid_, v2.vid_, nullptr);
      }
      ctx.set(alias[i], builder.finish());
    }
    return ctx;
  }
};
}  // namespace ops
}  // namespace runtime
}  // namespace gs