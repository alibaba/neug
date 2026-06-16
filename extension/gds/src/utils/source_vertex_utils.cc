/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#include "utils/source_vertex_utils.h"

namespace neug {
namespace gds {

vid_t parse_source_vertex(const StorageReadInterface& graph,
                          label_t vertex_label, const std::string& source_str) {
  auto pk_type =
      std::get<0>(graph.schema().get_vertex_primary_key(vertex_label)[0]);
  vid_t source;
  
  // Create Value with owned string data to avoid dangling reference
  execution::Value oid;
  switch (pk_type.id()) {
  case DataTypeId::kInt32:
    oid = execution::Value::INT32(std::stoi(source_str));
    break;
  case DataTypeId::kInt64:
    oid = execution::Value::INT64(std::atoll(source_str.c_str()));
    break;
  case DataTypeId::kUInt32:
    oid = execution::Value::UINT32(std::stoul(source_str));
    break;
  case DataTypeId::kUInt64:
    oid = execution::Value::UINT64(std::stoull(source_str));
    break;
  case DataTypeId::kVarchar:
    oid = execution::Value::CreateValue(source_str);
    break;
  default:
    throw std::runtime_error("Unsupported primary key type for source vertex lookup");
  }
  
  if (!graph.GetVertexIndex(vertex_label, oid, source)) {
    throw std::runtime_error("Source vertex not found: " + source_str);
  }
  return source;
}
}  // namespace gds
}  // namespace neug