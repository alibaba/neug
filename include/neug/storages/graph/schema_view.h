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
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "neug/storages/graph/schema.h"

namespace neug {

class SchemaView {
 public:
  SchemaView(const Schema* schema, std::string namespace_name);
  SchemaView(const Schema& schema, std::string namespace_name);

  const Schema& GetSchema() const;
  const std::string& GetNamespace() const { return namespace_; }

  std::vector<std::shared_ptr<const VertexSchema>> GetVertexSchemas() const;
  std::vector<std::shared_ptr<const EdgeSchema>> GetEdgeSchemas() const;

  result<std::shared_ptr<const VertexSchema>> GetVertexSchema(
      label_t label) const;
  result<std::shared_ptr<const VertexSchema>> GetVertexSchema(
      const std::string& label) const;

  result<std::shared_ptr<const EdgeSchema>> GetEdgeSchema(
      label_t src_label, label_t dst_label, label_t edge_label) const;
  result<std::shared_ptr<const EdgeSchema>> GetEdgeSchema(
      const std::string& src_label, const std::string& dst_label,
      const std::string& edge_label) const;

  bool ContainsVertexLabel(label_t label) const;
  bool ContainsVertexLabel(const std::string& label) const;

  bool ContainsEdgeLabel(label_t label) const;
  bool ContainsEdgeLabel(const std::string& label) const;

  bool ContainsEdgeTriplet(label_t src_label, label_t dst_label,
                           label_t edge_label) const;
  bool ContainsEdgeTriplet(const std::string& src_label,
                           const std::string& dst_label,
                           const std::string& edge_label) const;

  std::vector<label_t> GetVertexLabelIds() const;
  std::vector<label_t> GetEdgeLabelIds() const;

 private:
  void EnsureSchema() const;

  bool IsVertexInNamespace(label_t label) const;
  bool IsVertexInNamespace(const VertexSchema& schema) const;
  bool IsEdgeInNamespace(const EdgeSchema& schema) const;

  const Schema* schema_;
  std::string namespace_;
};

}  // namespace neug
