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

/**
 * This file is based on the DuckDB project
 * (https://github.com/duckdb/duckdb) Licensed under the MIT License.
 */

#include "neug/storages/index/art_tree/art_key.h"
#include "neug/storages/index/art_tree/common.h"

namespace neug::art {

//===--------------------------------------------------------------------===//
// ARTKey
//===--------------------------------------------------------------------===//

ARTKey::ARTKey() : len(0), data(nullptr) {}

ARTKey::ARTKey(const data_ptr_t data, idx_t len) : len(len), data(data) {}

ARTKey::ARTKey(ARTArenaAllocator& allocator, idx_t len) : len(len) {
  data = allocator.Allocate(len);
}

template <>
ARTKey ARTKey::CreateARTKey(ARTArenaAllocator& allocator, string_t value) {
  auto string_data = const_data_ptr_cast(value.GetData());
  auto string_len = value.GetSize();

  // We escape \00 and \01.
  idx_t escape_count = 0;
  for (idx_t i = 0; i < string_len; i++) {
    if (string_data[i] <= 1) {
      escape_count++;
    }
  }

  idx_t key_len = string_len + escape_count + 1;
  auto key_data = allocator.Allocate(key_len);

  // Copy over the data and add escapes.
  idx_t pos = 0;
  for (idx_t i = 0; i < string_len; i++) {
    if (string_data[i] <= 1) {
      // Add escape.
      key_data[pos++] = '\01';
    }
    key_data[pos++] = string_data[i];
  }

  // End with a null-terminator.
  key_data[pos] = '\0';
  return ARTKey(key_data, key_len);
}

template <>
void ARTKey::CreateARTKey(ARTArenaAllocator& allocator, ARTKey& key,
                          string_t value) {
  key = ARTKey::CreateARTKey<string_t>(allocator, value);
}

ARTKey ARTKey::CreateKey(ARTArenaAllocator& allocator,
                         const neug::Value& value) {
  if (value.IsNull())
    return ARTKey();
  switch (value.type().id()) {
  case DataTypeId::kBoolean:
    return CreateARTKey<bool>(allocator, value);
  case DataTypeId::kInt32:
    return CreateARTKey<int32_t>(allocator, value);
  case DataTypeId::kInt64:
    return CreateARTKey<int64_t>(allocator, value);
  case DataTypeId::kUInt32:
    return CreateARTKey<uint32_t>(allocator, value);
  case DataTypeId::kUInt64:
    return CreateARTKey<uint64_t>(allocator, value);
  case DataTypeId::kFloat:
    return CreateARTKey<float>(allocator, value);
  case DataTypeId::kDouble:
    return CreateARTKey<double>(allocator, value);
  case DataTypeId::kVarchar: {
    const auto& string = StringValue::Get(value);
    return CreateARTKey(allocator, string_t(string));
  }
  default:
    throw InternalException("Unsupported NeuG Value type for ART key: %u",
                            static_cast<unsigned>(value.type().id()));
  }
}

bool ARTKey::operator>(const ARTKey& key) const {
  for (idx_t i = 0; i < MinValue(len, key.len); i++) {
    if (data[i] > key.data[i]) {
      return true;
    } else if (data[i] < key.data[i]) {
      return false;
    }
  }
  return len > key.len;
}

bool ARTKey::operator>=(const ARTKey& key) const {
  for (idx_t i = 0; i < MinValue(len, key.len); i++) {
    if (data[i] > key.data[i]) {
      return true;
    } else if (data[i] < key.data[i]) {
      return false;
    }
  }
  return len >= key.len;
}

bool ARTKey::operator==(const ARTKey& key) const {
  if (len != key.len) {
    return false;
  }
  for (idx_t i = 0; i < len; i++) {
    if (data[i] != key.data[i]) {
      return false;
    }
  }
  return true;
}

row_t ARTKey::GetRowId() const {
  D_ASSERT(len == sizeof(row_t));
  return Radix::DecodeData<row_t>(data);
}

idx_t ARTKey::GetMismatchPos(const ARTKey& other, const idx_t start) const {
  D_ASSERT(len <= other.len);
  D_ASSERT(start <= len);
  for (idx_t i = start; i < other.len; i++) {
    if (data[i] != other.data[i]) {
      return i;
    }
  }
  throw FatalException(
      "Corrupted ArtTree index - likely the same row id was inserted twice "
      "into the same ArtTree");
}

}  // namespace neug::art
