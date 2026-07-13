/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/storages/graph/schema_view.h"

#include <unordered_set>

#include "neug/utils/exception/exception.h"

namespace neug {

SchemaView::SchemaView(const Schema* schema, std::string namespace_name)
    : schema_(schema), namespace_(std::move(namespace_name)) {
  EnsureSchema();
}

SchemaView::SchemaView(const Schema& schema, std::string namespace_name)
    : SchemaView(&schema, std::move(namespace_name)) {}

const Schema& SchemaView::GetSchema() const {
  EnsureSchema();
  return *schema_;
}

std::vector<std::shared_ptr<const VertexSchema>> SchemaView::GetVertexSchemas()
    const {
  EnsureSchema();
  std::vector<std::shared_ptr<const VertexSchema>> result;
  for (const auto& vertex_schema : schema_->get_all_vertex_schemas()) {
    if (vertex_schema != nullptr &&
        schema_->is_vertex_label_valid(vertex_schema->label_id) &&
        IsVertexInNamespace(*vertex_schema)) {
      result.emplace_back(vertex_schema);
    }
  }
  return result;
}

std::vector<std::shared_ptr<const EdgeSchema>> SchemaView::GetEdgeSchemas()
    const {
  EnsureSchema();
  std::vector<std::shared_ptr<const EdgeSchema>> result;
  for (const auto& [_, edge_schema] : schema_->get_all_edge_schemas()) {
    if (edge_schema != nullptr &&
        schema_->is_vertex_label_valid(edge_schema->src_label_id) &&
        schema_->is_vertex_label_valid(edge_schema->dst_label_id) &&
        schema_->is_edge_triplet_valid(edge_schema->src_label_id,
                                       edge_schema->dst_label_id,
                                       edge_schema->edge_label_id) &&
        IsEdgeInNamespace(*edge_schema)) {
      result.emplace_back(edge_schema);
    }
  }
  return result;
}

result<std::shared_ptr<const VertexSchema>> SchemaView::GetVertexSchema(
    label_t label) const {
  if (!ContainsVertexLabel(label)) {
    RETURN_STATUS_ERROR(StatusCode::ERR_NOT_FOUND,
                        "Vertex label " + std::to_string(label) +
                            " not found in namespace " + namespace_ + ".");
  }
  return schema_->get_vertex_schema(label);
}

result<std::shared_ptr<const VertexSchema>> SchemaView::GetVertexSchema(
    const std::string& label) const {
  if (!ContainsVertexLabel(label)) {
    RETURN_STATUS_ERROR(StatusCode::ERR_NOT_FOUND,
                        "Vertex label " + label + " not found in namespace " +
                            namespace_ + ".");
  }
  return schema_->get_vertex_schema(schema_->get_vertex_label_id(label));
}

result<std::shared_ptr<const EdgeSchema>> SchemaView::GetEdgeSchema(
    label_t src_label, label_t dst_label, label_t edge_label) const {
  if (!ContainsEdgeTriplet(src_label, dst_label, edge_label)) {
    RETURN_STATUS_ERROR(StatusCode::ERR_NOT_FOUND,
                        "Edge triplet (" + std::to_string(src_label) + ", " +
                            std::to_string(dst_label) + ", " +
                            std::to_string(edge_label) +
                            ") not found in namespace " + namespace_ + ".");
  }
  return schema_->get_edge_schema(src_label, dst_label, edge_label);
}

result<std::shared_ptr<const EdgeSchema>> SchemaView::GetEdgeSchema(
    const std::string& src_label, const std::string& dst_label,
    const std::string& edge_label) const {
  if (!ContainsEdgeTriplet(src_label, dst_label, edge_label)) {
    RETURN_STATUS_ERROR(StatusCode::ERR_NOT_FOUND,
                        "Edge triplet (" + src_label + ", " + dst_label + ", " +
                            edge_label + ") not found in namespace " +
                            namespace_ + ".");
  }
  return schema_->get_edge_schema(schema_->get_vertex_label_id(src_label),
                                  schema_->get_vertex_label_id(dst_label),
                                  schema_->get_edge_label_id(edge_label));
}

bool SchemaView::ContainsVertexLabel(label_t label) const {
  EnsureSchema();
  return schema_->is_vertex_label_valid(label) && IsVertexInNamespace(label);
}

bool SchemaView::ContainsVertexLabel(const std::string& label) const {
  EnsureSchema();
  return schema_->is_vertex_label_valid(label) &&
         IsVertexInNamespace(schema_->get_vertex_label_id(label));
}

bool SchemaView::ContainsEdgeLabel(label_t label) const {
  EnsureSchema();
  if (!schema_->is_edge_label_valid(label)) {
    return false;
  }
  for (const auto& edge_schema : GetEdgeSchemas()) {
    if (edge_schema->edge_label_id == label) {
      return true;
    }
  }
  return false;
}

bool SchemaView::ContainsEdgeLabel(const std::string& label) const {
  EnsureSchema();
  return schema_->is_edge_label_valid(label) &&
         ContainsEdgeLabel(schema_->get_edge_label_id(label));
}

bool SchemaView::ContainsEdgeTriplet(label_t src_label, label_t dst_label,
                                     label_t edge_label) const {
  EnsureSchema();
  if (!schema_->is_vertex_label_valid(src_label) ||
      !schema_->is_vertex_label_valid(dst_label) ||
      !schema_->is_edge_label_valid(edge_label) ||
      !schema_->is_edge_triplet_valid(src_label, dst_label, edge_label)) {
    return false;
  }
  return IsVertexInNamespace(src_label) && IsVertexInNamespace(dst_label) &&
         IsEdgeInNamespace(
             *schema_->get_edge_schema(src_label, dst_label, edge_label));
}

bool SchemaView::ContainsEdgeTriplet(const std::string& src_label,
                                     const std::string& dst_label,
                                     const std::string& edge_label) const {
  EnsureSchema();
  if (!schema_->is_edge_triplet_valid(src_label, dst_label, edge_label)) {
    return false;
  }
  return ContainsEdgeTriplet(schema_->get_vertex_label_id(src_label),
                             schema_->get_vertex_label_id(dst_label),
                             schema_->get_edge_label_id(edge_label));
}

std::vector<label_t> SchemaView::GetVertexLabelIds() const {
  std::vector<label_t> result;
  for (const auto& vertex_schema : GetVertexSchemas()) {
    result.emplace_back(vertex_schema->label_id);
  }
  return result;
}

std::vector<label_t> SchemaView::GetEdgeLabelIds() const {
  std::vector<label_t> result;
  std::unordered_set<label_t> seen;
  for (const auto& edge_schema : GetEdgeSchemas()) {
    if (seen.emplace(edge_schema->edge_label_id).second) {
      result.emplace_back(edge_schema->edge_label_id);
    }
  }
  return result;
}

void SchemaView::EnsureSchema() const {
  if (schema_ == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Schema is null");
  }
}

bool SchemaView::IsVertexInNamespace(label_t label) const {
  return IsVertexInNamespace(*schema_->get_vertex_schema(label));
}

bool SchemaView::IsVertexInNamespace(const VertexSchema& schema) const {
  return schema.namespace_name == namespace_;
}

bool SchemaView::IsEdgeInNamespace(const EdgeSchema& schema) const {
  return schema.namespace_name == namespace_;
}

}  // namespace neug
