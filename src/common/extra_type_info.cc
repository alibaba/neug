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

#include "neug/common/extra_type_info.h"
namespace neug {

ExtraTypeInfo::ExtraTypeInfo(ExtraTypeInfoType type) : type(type) {}

ExtraTypeInfo::~ExtraTypeInfo() {}

ExtraTypeInfo::ExtraTypeInfo(const ExtraTypeInfo& other) : type(other.type) {
  // copy extension_info if needed
}

ExtraTypeInfo& ExtraTypeInfo::operator=(const ExtraTypeInfo& other) {
  if (this != &other) {
    type = other.type;
    // copy extension_info if needed
  }
  return *this;
}

bool ExtraTypeInfo::Equals(ExtraTypeInfo* other_p) const {
  if (type == ExtraTypeInfoType::INVALID_TYPE_INFO ||
      type == ExtraTypeInfoType::STRING_TYPE_INFO ||
      type == ExtraTypeInfoType::GENERIC_TYPE_INFO) {
    if (!other_p) {
      // if extension_info exists, return false
      return true;
    }
    // if extension_info exists, compare them
    return true;
  }
  if (!other_p) {
    return false;
  }
  if (type != other_p->type) {
    return false;
  }
  // if extension_info exists, compare them
  return EqualsInternal(other_p);
}

bool ExtraTypeInfo::EqualsInternal(ExtraTypeInfo* other_p) const {
  return true;
}

StructTypeInfo::StructTypeInfo()
    : ExtraTypeInfo(ExtraTypeInfoType::STRUCT_TYPE_INFO) {}

StructTypeInfo::StructTypeInfo(std::vector<DataType> child_types)
    : ExtraTypeInfo(ExtraTypeInfoType::STRUCT_TYPE_INFO),
      child_types(std::move(child_types)) {}

bool StructTypeInfo::EqualsInternal(ExtraTypeInfo* other_p) const {
  auto& other = other_p->Cast<StructTypeInfo>();
  return child_types == other.child_types;
}

ListTypeInfo::ListTypeInfo(DataType child_type_)
    : ExtraTypeInfo(ExtraTypeInfoType::LIST_TYPE_INFO),
      child_type(std::move(child_type_)) {}

bool ListTypeInfo::EqualsInternal(ExtraTypeInfo* other_p) const {
  auto& other = other_p->Cast<ListTypeInfo>();
  return child_type == other.child_type;
}

bool StringTypeInfo::EqualsInternal(ExtraTypeInfo* other_p) const {
  auto& other = other_p->Cast<StringTypeInfo>();
  return max_length == other.max_length;
}

}  // namespace neug