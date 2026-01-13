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

/**
 * This file is based on the DuckDB project
 * (https://github.com/duckdb/duckdb) Licensed under the MIT License. Modified
 * by Liu Lexiao in 2025 to support Neug-specific features.
 */

#include <assert.h>

#include "neug/common/types.h"

#include "neug/common/extra_type_info.h"

namespace gs {

DataType::DataType() : DataType(DataTypeId::kInvalid) {}

DataType::DataType(DataTypeId id) : id_(id) {}

DataType::DataType(DataTypeId id, std::shared_ptr<ExtraTypeInfo> type_info)
    : id_(id), type_info_(std::move(type_info)) {}

DataType::DataType(const DataType& other)
    : id_(other.id_), type_info_(other.type_info_) {}

DataType::DataType(DataType&& other) noexcept
    : id_(other.id_), type_info_(std::move(other.type_info_)) {}
DataType::~DataType() {}

bool DataType::EqualTypeInfo(const DataType& rhs) const {
  if (type_info_.get() == rhs.type_info_.get()) {
    return true;
  }
  if (type_info_) {
    return type_info_->Equals(rhs.type_info_.get());
  } else {
    assert(rhs.type_info_);
    return rhs.type_info_->Equals(type_info_.get());
  }
}

bool DataType::operator==(const DataType& rhs) const {
  if (id_ != rhs.id_) {
    return false;
  }
  return EqualTypeInfo(rhs);
}

const std::vector<DataType>& StructType::GetChildTypes(const DataType& type) {
  assert(type.id() == DataTypeId::kStruct);
  auto info = type.AuxInfo();
  assert(info);
  return info->Cast<StructTypeInfo>().child_types;
}

const DataType& StructType::GetChildType(const DataType& type, size_t index) {
  const auto& children = GetChildTypes(type);
  assert(index < children.size());
  return children[index];
}

}  // namespace gs