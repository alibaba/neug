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

#include <string>

#include "neug/storages/graph/schema.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {
namespace execution {
namespace ops {

/// Resolve a vertex type name to label_t. Returns ERR_SCHEMA_MISMATCH if
/// absent.
inline Status ResolveVertexLabel(const Schema& schema,
                                 const std::string& vertex_type,
                                 label_t& label) {
  if (!schema.is_vertex_label_valid(vertex_type)) {
    return Status(StatusCode::ERR_SCHEMA_MISMATCH,
                  "Vertex type " + vertex_type + " does not exist.");
  }
  label = schema.get_vertex_label_id(vertex_type);
  return Status::OK();
}

/// Resolve an edge triplet name to label ids. Returns ERR_SCHEMA_MISMATCH if
/// the triplet is not valid.
inline Status ResolveEdgeTriplet(const Schema& schema,
                                 const std::string& src_type,
                                 const std::string& dst_type,
                                 const std::string& edge_type, label_t& src,
                                 label_t& dst, label_t& edge) {
  if (!schema.is_edge_triplet_valid(src_type, dst_type, edge_type)) {
    return Status(StatusCode::ERR_SCHEMA_MISMATCH,
                  "Edge type " + edge_type + " does not exist between " +
                      src_type + " and " + dst_type + ".");
  }
  src = schema.get_vertex_label_id(src_type);
  dst = schema.get_vertex_label_id(dst_type);
  edge = schema.get_edge_label_id(edge_type);
  return Status::OK();
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
