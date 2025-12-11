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

#include "neug/execution/common/context.h"
#include "neug/execution/utils/expr.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/result.h"
namespace gs {
namespace runtime {
namespace ops {
class CreateVertex {
 public:
  static gs::result<Context> insert_vertex(
      StorageInsertInterface& graph, Context&& ctx,
      const std::vector<label_t>& labels,
      const std::vector<std::vector<std::pair<std::string, Expr>>>& props,
      const std::vector<int>& alias) {
    const auto& schema = graph.schema();
    for (size_t i = 0; i < labels.size(); ++i) {
      MSVertexColumnBuilder builder(labels[i]);
      label_t label = labels[i];
      const auto& properties = props[i];
      auto properties_name = schema.get_vertex_property_names(label);
      auto properties_type = schema.get_vertex_properties(label);
      if (properties_name.size() != properties_type.size()) {
        THROW_RUNTIME_ERROR("Vertex label " + std::to_string(label) +
                            " has different number of properties: " +
                            std::to_string(properties_name.size()) + " vs " +
                            std::to_string(properties_type.size()));
      }
      const auto& pks = schema.get_vertex_primary_key(label);
      if (pks.size() != 1) {
        THROW_RUNTIME_ERROR("Vertex label " + std::to_string(label) +
                            " must have exactly one primary key, but found: " +
                            std::to_string(pks.size()));
      }
      const auto& pk = pks[0];
      if (properties.size() != properties_name.size() + 1) {
        THROW_RUNTIME_ERROR("Provided properties size " +
                            std::to_string(properties.size()) +
                            " does not match schema size: " +
                            std::to_string(properties_name.size() + 1));
      }

      std::shared_ptr<Arena> arena = std::make_shared<Arena>();
      Property pk_value;
      std::vector<Property> property_values(properties.size() - 1);
      for (size_t i = 0; i < ctx.row_num(); ++i) {
        for (size_t j = 0; j < properties.size(); ++j) {
          const auto& [prop_name, prop_expr] = properties[j];
          RTAny value = prop_expr.eval_path(i, *arena);
          if (prop_name == std::get<1>(pk)) {
            pk_value = value.to_any();
          } else {
            auto it = std::find(properties_name.begin(), properties_name.end(),
                                prop_name);
            if (it == properties_name.end()) {
              THROW_RUNTIME_ERROR("Property " + prop_name +
                                  " not found in vertex label " +
                                  std::to_string(label));
            }
            size_t index = std::distance(properties_name.begin(), it);
            property_values[index] = value.to_any();
          }
        }
        vid_t existing_vid;
        if (graph.GetVertexIndex(label, pk_value, existing_vid)) {
          LOG(ERROR) << "Vertex with label " << (int32_t) label
                     << " and primary key " << pk_value.to_string()
                     << " already exists.";
          RETURN_STATUS_ERROR(gs::StatusCode::ERR_INVALID_ARGUMENT,
                              "Vertex with label " + std::to_string(label) +
                                  " and primary key " + pk_value.to_string() +
                                  " already exists.");
        }
        vid_t vid;
        if (!graph.AddVertex(label, pk_value, property_values, vid)) {
          LOG(ERROR) << "Failed to add vertex with label " << (int32_t) label
                     << " and primary key " << pk_value.to_string();
          RETURN_STATUS_ERROR(gs::StatusCode::ERR_INTERNAL_ERROR,
                              "Failed to add vertex with label " +
                                  std::to_string(label) + " and primary key " +
                                  pk_value.to_string());
        }
        builder.push_back_opt(vid);
      }
      ctx.set(alias[i], builder.finish());
    }
    return ctx;
  }
};
}  // namespace ops
}  // namespace runtime
}  // namespace gs
