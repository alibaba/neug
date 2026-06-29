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
    LOG(WARNING) << "Unsupported array type in QueryResult: "
                 << array.typed_array_case();
    ss << "null";
  }
  }
}

std::string QueryResult::ToString() const { return response_->DebugString(); }

std::string QueryResult::GetCurrentRowAsString() const {
  if (current_row_index_ >= static_cast<size_t>(response_->row_count())) {
    THROW_RUNTIME_ERROR("Cursor past end of result set (row " +
                        std::to_string(current_row_index_) +
                        " >= " + std::to_string(response_->row_count()) + ")");
  }
  std::stringstream ss;
  for (int i = 0; i < response_->arrays_size(); ++i) {
    if (i > 0) {
      ss << ", ";
    }
    get_value(response_->arrays(i), current_row_index_, ss);
  }
  return ss.str();
}

QueryResult QueryResult::From(const std::string& serialized_table) {
  return From(std::string(serialized_table));
}

QueryResult QueryResult::From(std::string&& serialized_table) {
  QueryResult result;
  if (!result.response_->ParseFromString(serialized_table)) {
    LOG(ERROR) << "Failed to parse QueryResponse from string";
  }
  return result;
}

std::string QueryResult::Serialize() const {
  std::string serialized_response;
  if (!response_->SerializeToString(&serialized_response)) {
    LOG(ERROR) << "Failed to serialize QueryResponse to string";
    THROW_RUNTIME_ERROR("Failed to serialize QueryResponse to string");
  }
  return serialized_response;
}

// ---------------------------------------------------------------------------
// Cursor-based traversal
// ---------------------------------------------------------------------------

bool QueryResult::hasNext() const {
  return current_row_index_ < static_cast<size_t>(response_->row_count());
}

void QueryResult::next() {
  if (!hasNext()) {
    THROW_RUNTIME_ERROR("No more rows available in QueryResult");
  }
  ++current_row_index_;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

void QueryResult::ValidateCursorAccess(size_t column_index) const {
  if (current_row_index_ >= static_cast<size_t>(response_->row_count())) {
    THROW_RUNTIME_ERROR("Cursor past end of result set (row " +
                        std::to_string(current_row_index_) +
                        " >= " + std::to_string(response_->row_count()) + ")");
  }
  if (column_index >= static_cast<size_t>(response_->arrays_size())) {
    THROW_RUNTIME_ERROR(
        "Column index out of range: " + std::to_string(column_index) +
        " >= " + std::to_string(response_->arrays_size()));
  }
}

size_t QueryResult::GetColumnIndex(const std::string& column_name) const {
  const auto& schema = response_->schema();
  for (int i = 0; i < schema.name_size(); ++i) {
    if (schema.name(i) == column_name) {
      return static_cast<size_t>(i);
    }
  }
  THROW_RUNTIME_ERROR("Column not found: " + column_name);
}

const neug::Array& QueryResult::GetColumn(size_t column_index) const {
  return response_->arrays(static_cast<int>(column_index));
}

size_t QueryResult::ColumnCount() const {
  return static_cast<size_t>(response_->arrays_size());
}

std::vector<std::string> QueryResult::ColumnNames() const {
  const auto& schema = response_->schema();
  std::vector<std::string> names(schema.name_size());
  for (int i = 0; i < schema.name_size(); ++i) {
    names[i] = schema.name(i);
  }
  return names;
}

// ---------------------------------------------------------------------------
// Null check
// ---------------------------------------------------------------------------

bool QueryResult::IsNull(size_t column_index) const {
  ValidateCursorAccess(column_index);
  const auto& array = GetColumn(column_index);

  const std::string* validity = nullptr;
  switch (array.typed_array_case()) {
  case neug::Array::kInt32Array:
    validity = &array.int32_array().validity();
    break;
  case neug::Array::kUint32Array:
    validity = &array.uint32_array().validity();
    break;
  case neug::Array::kInt64Array:
    validity = &array.int64_array().validity();
    break;
  case neug::Array::kUint64Array:
    validity = &array.uint64_array().validity();
    break;
  case neug::Array::kFloatArray:
    validity = &array.float_array().validity();
    break;
  case neug::Array::kDoubleArray:
    validity = &array.double_array().validity();
    break;
  case neug::Array::kStringArray:
    validity = &array.string_array().validity();
    break;
  case neug::Array::kBoolArray:
    validity = &array.bool_array().validity();
    break;
  case neug::Array::kDateArray:
    validity = &array.date_array().validity();
    break;
  case neug::Array::kTimestampArray:
    validity = &array.timestamp_array().validity();
    break;
  case neug::Array::kIntervalArray:
    validity = &array.interval_array().validity();
    break;
  case neug::Array::kVertexArray:
    validity = &array.vertex_array().validity();
    break;
  case neug::Array::kEdgeArray:
    validity = &array.edge_array().validity();
    break;
  case neug::Array::kPathArray:
    validity = &array.path_array().validity();
    break;
  default:
    return true;
  }
  return !is_valid(*validity, current_row_index_);
}

// ---------------------------------------------------------------------------
// Typed getters (with implicit widening conversions)
//
// Widening rules:
//   int32  → int32, int64, float, double
//   uint32 → uint32, int64, uint64, float, double
//   int64  → int64, double
//   uint64 → uint64, double
//   float  → float, double
//   bool   → int32, int64, uint32, uint64
//   date / timestamp → int64 (raw epoch value as stored in the protobuf)
//
// Temporal columns (DATE / TIMESTAMP / INTERVAL) are not exposed as dedicated
// typed objects to avoid leaking internal representations into the public API.
// They are accessible as a human-readable string via GetString(), and DATE /
// TIMESTAMP additionally expose their raw int64 epoch value via GetInt64().
// ---------------------------------------------------------------------------

int32_t QueryResult::GetInt32(size_t column_index) const {
  ValidateCursorAccess(column_index);
  const auto& array = GetColumn(column_index);
  int row = static_cast<int>(current_row_index_);
  switch (array.typed_array_case()) {
  case neug::Array::kInt32Array:
    return array.int32_array().values(row);
  case neug::Array::kBoolArray:
    return array.bool_array().values(row) ? 1 : 0;
  default:
    THROW_RUNTIME_ERROR("Column " + std::to_string(column_index) +
                        " cannot be converted to Int32");
  }
}

uint32_t QueryResult::GetUInt32(size_t column_index) const {
  ValidateCursorAccess(column_index);
  const auto& array = GetColumn(column_index);
  int row = static_cast<int>(current_row_index_);
  switch (array.typed_array_case()) {
  case neug::Array::kUint32Array:
    return array.uint32_array().values(row);
  case neug::Array::kBoolArray:
    return array.bool_array().values(row) ? 1 : 0;
  default:
    THROW_RUNTIME_ERROR("Column " + std::to_string(column_index) +
                        " cannot be converted to UInt32");
  }
}

int64_t QueryResult::GetInt64(size_t column_index) const {
  ValidateCursorAccess(column_index);
  const auto& array = GetColumn(column_index);
  int row = static_cast<int>(current_row_index_);
  switch (array.typed_array_case()) {
  case neug::Array::kInt64Array:
    return array.int64_array().values(row);
  case neug::Array::kInt32Array:
    return static_cast<int64_t>(array.int32_array().values(row));
  case neug::Array::kUint32Array:
    return static_cast<int64_t>(array.uint32_array().values(row));
  case neug::Array::kBoolArray:
    return array.bool_array().values(row) ? 1 : 0;
  case neug::Array::kDateArray:
    return array.date_array().values(row);
  case neug::Array::kTimestampArray:
    return array.timestamp_array().values(row);
  default:
    THROW_RUNTIME_ERROR("Column " + std::to_string(column_index) +
                        " cannot be converted to Int64");
  }
}

uint64_t QueryResult::GetUInt64(size_t column_index) const {
  ValidateCursorAccess(column_index);
  const auto& array = GetColumn(column_index);
  int row = static_cast<int>(current_row_index_);
  switch (array.typed_array_case()) {
  case neug::Array::kUint64Array:
    return array.uint64_array().values(row);
  case neug::Array::kUint32Array:
    return static_cast<uint64_t>(array.uint32_array().values(row));
  case neug::Array::kBoolArray:
    return array.bool_array().values(row) ? 1 : 0;
  default:
    THROW_RUNTIME_ERROR("Column " + std::to_string(column_index) +
                        " cannot be converted to UInt64");
  }
}

float QueryResult::GetFloat(size_t column_index) const {
  ValidateCursorAccess(column_index);
  const auto& array = GetColumn(column_index);
  int row = static_cast<int>(current_row_index_);
  switch (array.typed_array_case()) {
  case neug::Array::kFloatArray:
    return array.float_array().values(row);
  case neug::Array::kInt32Array:
    return static_cast<float>(array.int32_array().values(row));
  case neug::Array::kUint32Array:
    return static_cast<float>(array.uint32_array().values(row));
  case neug::Array::kBoolArray:
    return array.bool_array().values(row) ? 1.0f : 0.0f;
  default:
    THROW_RUNTIME_ERROR("Column " + std::to_string(column_index) +
                        " cannot be converted to Float");
  }
}

double QueryResult::GetDouble(size_t column_index) const {
  ValidateCursorAccess(column_index);
  const auto& array = GetColumn(column_index);
  int row = static_cast<int>(current_row_index_);
  switch (array.typed_array_case()) {
  case neug::Array::kDoubleArray:
    return array.double_array().values(row);
  case neug::Array::kFloatArray:
    return static_cast<double>(array.float_array().values(row));
  case neug::Array::kInt32Array:
    return static_cast<double>(array.int32_array().values(row));
  case neug::Array::kUint32Array:
    return static_cast<double>(array.uint32_array().values(row));
  case neug::Array::kInt64Array:
    return static_cast<double>(array.int64_array().values(row));
  case neug::Array::kUint64Array:
    return static_cast<double>(array.uint64_array().values(row));
  case neug::Array::kBoolArray:
    return array.bool_array().values(row) ? 1.0 : 0.0;
  default:
    THROW_RUNTIME_ERROR("Column " + std::to_string(column_index) +
                        " cannot be converted to Double");
  }
}

std::string QueryResult::GetString(size_t column_index) const {
  ValidateCursorAccess(column_index);
  const auto& array = GetColumn(column_index);
  if (array.typed_array_case() != neug::Array::kStringArray) {
    // Fall back to string representation for any type
    std::stringstream ss;
    get_value(array, current_row_index_, ss);
    return ss.str();
  }
  return array.string_array().values(static_cast<int>(current_row_index_));
}

bool QueryResult::GetBool(size_t column_index) const {
  ValidateCursorAccess(column_index);
  const auto& array = GetColumn(column_index);
  if (array.typed_array_case() != neug::Array::kBoolArray) {
    THROW_RUNTIME_ERROR("Column " + std::to_string(column_index) +
                        " cannot be converted to Bool");
  }
  return array.bool_array().values(static_cast<int>(current_row_index_));
}

// ---------------------------------------------------------------------------
// Column-name overloads (delegate to index-based versions)
// ---------------------------------------------------------------------------

bool QueryResult::IsNull(const std::string& column_name) const {
  return IsNull(GetColumnIndex(column_name));
}

int32_t QueryResult::GetInt32(const std::string& column_name) const {
  return GetInt32(GetColumnIndex(column_name));
}

uint32_t QueryResult::GetUInt32(const std::string& column_name) const {
  return GetUInt32(GetColumnIndex(column_name));
}

int64_t QueryResult::GetInt64(const std::string& column_name) const {
  return GetInt64(GetColumnIndex(column_name));
}

uint64_t QueryResult::GetUInt64(const std::string& column_name) const {
  return GetUInt64(GetColumnIndex(column_name));
}

float QueryResult::GetFloat(const std::string& column_name) const {
  return GetFloat(GetColumnIndex(column_name));
}

double QueryResult::GetDouble(const std::string& column_name) const {
  return GetDouble(GetColumnIndex(column_name));
}

std::string QueryResult::GetString(const std::string& column_name) const {
  return GetString(GetColumnIndex(column_name));
}

bool QueryResult::GetBool(const std::string& column_name) const {
  return GetBool(GetColumnIndex(column_name));
}

}  // namespace neug
