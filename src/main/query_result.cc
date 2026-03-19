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

#include "neug/main/query_result.h"

#include <glog/logging.h>
#include <stdint.h>
#include <cstring>
#include <memory>
#include <ostream>
#include <sstream>
#include <string_view>
#include <utility>
#include <vector>

#include "neug/utils/exception/exception.h"
#include "neug/utils/pb_utils.h"

#include <arrow/api.h>
#include <arrow/array/concatenate.h>
#include <arrow/io/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/scalar.h>
#include <arrow/type.h>

namespace neug {

static bool is_valid(const std::string& validity_map, size_t row_index) {
  return validity_map.empty() ||
         validity_map[row_index / 8] & (1 << (row_index % 8));
}

static void get_value(const neug::Array& array, size_t row_index,
                      std::stringstream& ss) {
  switch (array.typed_array_case()) {
  case neug::Array::kInt32Array: {
    if (!is_valid(array.int32_array().validity(), row_index)) {
      ss << "null";
      break;
    } else {
      ss << array.int32_array().values(row_index);
    }
    break;
  }
  case neug::Array::kUint32Array: {
    if (!is_valid(array.uint32_array().validity(), row_index)) {
      ss << "null";
      break;
    } else {
      ss << array.uint32_array().values(row_index);
    }
    break;
  }
  case neug::Array::kInt64Array: {
    if (!is_valid(array.int64_array().validity(), row_index)) {
      ss << "null";
      break;
    } else {
      ss << array.int64_array().values(row_index);
    }
    break;
  }
  case neug::Array::kUint64Array: {
    if (!is_valid(array.uint64_array().validity(), row_index)) {
      ss << "null";
      break;
    } else {
      ss << array.uint64_array().values(row_index);
    }
    break;
  }
  case neug::Array::kFloatArray: {
    if (!is_valid(array.float_array().validity(), row_index)) {
      ss << "null";
    } else {
      ss << array.float_array().values(row_index);
    }
    break;
  }
  case neug::Array::kDoubleArray: {
    if (!is_valid(array.double_array().validity(), row_index)) {
      ss << "null";
    } else {
      ss << array.double_array().values(row_index);
    }
    break;
  }
  case neug::Array::kStringArray: {
    if (!is_valid(array.string_array().validity(), row_index)) {
      ss << "null";
    } else {
      ss << array.string_array().values(row_index);
    }
    break;
  }
  case neug::Array::kBoolArray: {
    if (!is_valid(array.bool_array().validity(), row_index)) {
      ss << "null";
    } else {
      ss << (array.bool_array().values(row_index) ? "true" : "false");
    }
    break;
  }
  case neug::Array::kDateArray: {
    if (!is_valid(array.date_array().validity(), row_index)) {
      ss << "null";
    } else {
      ss << Date(array.date_array().values(row_index)).to_string();
    }
    break;
  }
  case neug::Array::kTimestampArray: {
    if (!is_valid(array.timestamp_array().validity(), row_index)) {
      ss << "null";
    } else {
      ss << DateTime(array.timestamp_array().values(row_index)).to_string();
    }
    break;
  }
  case neug::Array::kIntervalArray: {
    if (!is_valid(array.interval_array().validity(), row_index)) {
      ss << "null";
    } else {
      ss << array.interval_array().values(row_index);
    }
    break;
  }
  case neug::Array::kVertexArray: {
    if (!is_valid(array.vertex_array().validity(), row_index)) {
      ss << "null";
    } else {
      ss << array.vertex_array().values(row_index);
    }
    break;
  }
  case neug::Array::kEdgeArray: {
    if (!is_valid(array.edge_array().validity(), row_index)) {
      ss << "null";
    } else {
      ss << array.edge_array().values(row_index);
    }
    break;
  }
  case neug::Array::kPathArray: {
    if (!is_valid(array.path_array().validity(), row_index)) {
      ss << "null";
    } else {
      ss << array.path_array().values(row_index);
    }
    break;
  }
  default: {
    LOG(WARNING) << "Unsupported array type in RowView: "
                 << array.typed_array_case();
    ss << "null";
  }
  }
}

std::string RowView::ToString() const {
  if (response_ == nullptr) {
    THROW_RUNTIME_ERROR("RowView has null response");
  }
  if (row_index_ >= response_->row_count()) {
    THROW_RUNTIME_ERROR("Row index out of range");
  }
  std::stringstream ss;
  for (int i = 0; i < response_->arrays_size(); ++i) {
    const auto& array = response_->arrays(i);
    get_value(array, row_index_, ss);
    if (i < response_->arrays_size() - 1) {
      ss << ", ";
    }
  }
  return ss.str();
}

std::string QueryResult::ToString() const { return response_.DebugString(); }

QueryResult QueryResult::From(const std::string& serialized_table) {
  return From(std::string(serialized_table));
}

QueryResult QueryResult::From(std::string&& serialized_table) {
  QueryResult result;
  if (!result.response_.ParseFromString(serialized_table)) {
    LOG(ERROR) << "Failed to parse QueryResponse from string";
  }
  return result;
}

std::string QueryResult::Serialize() const {
  std::string serialized_response;
  if (!response_.SerializeToString(&serialized_response)) {
    LOG(ERROR) << "Failed to serialize QueryResponse to string";
    THROW_RUNTIME_ERROR("Failed to serialize QueryResponse to string");
  }
  return serialized_response;
}

}  // namespace neug
