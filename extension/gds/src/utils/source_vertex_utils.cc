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

static execution::Value get_property_from_string(const std::string& str,
                                                 const DataType& data_type) {
  switch (data_type.id()) {
  case DataTypeId::kInt32:
    return execution::Value::INT32(std::stoi(str));
  case DataTypeId::kInt64:
    return execution::Value::INT64(std::atoll(str.c_str()));
  case DataTypeId::kUInt32:
    return execution::Value::UINT32(std::stoul(str));
  case DataTypeId::kUInt64:
    return execution::Value::UINT64(std::stoull(str));
  case DataTypeId::kVarchar:
    return execution::Value::STRING(str);
  default:
    throw std::runtime_error("Unsupported data type: " + str);
  }
}
vid_t parse_source_vertex(const StorageReadInterface& graph,
                          label_t vertex_label, const std::string& source_str) {
  auto pk_type =
      std::get<0>(graph.schema().get_vertex_primary_key(vertex_label)[0]);
  vid_t source;
  auto oid = get_property_from_string(source_str, pk_type);
  if (!graph.GetVertexIndex(vertex_label, oid, source)) {
    throw std::runtime_error("Source vertex not found: " + source_str);
  }
  return source;
}
}  // namespace gds
}  // namespace neug