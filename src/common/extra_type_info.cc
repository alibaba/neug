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

#include <algorithm>

namespace neug {

ExtraTypeInfo::ExtraTypeInfo(ExtraTypeInfoType type) : type(type) {}

ExtraTypeInfo::~ExtraTypeInfo() {}

ExtraTypeInfo::ExtraTypeInfo(const ExtraTypeInfo& other) : type(other.type) {}

ExtraTypeInfo& ExtraTypeInfo::operator=(const ExtraTypeInfo& other) {
  if (this != &other) {
    type = other.type;
  }
  return *this;
}

bool ExtraTypeInfo::Equals(ExtraTypeInfo* other_p) const {
  if (type == ExtraTypeInfoType::INVALID_TYPE_INFO ||
      type == ExtraTypeInfoType::STRING_TYPE_INFO ||
      type == ExtraTypeInfoType::GENERIC_TYPE_INFO) {
    if (!other_p) {
      return true;
    }
    return true;
  }
  if (!other_p) {
    return false;
  }
  if (type != other_p->type) {
    return false;
  }
  return EqualsInternal(other_p);
}

bool ExtraTypeInfo::EqualsInternal(ExtraTypeInfo* other_p) const {
  return true;
}

// --- StructTypeInfo ---

StructTypeInfo::StructTypeInfo()
    : ExtraTypeInfo(ExtraTypeInfoType::STRUCT_TYPE_INFO) {}

StructTypeInfo::StructTypeInfo(std::vector<DataType> child_types_p)
    : ExtraTypeInfo(ExtraTypeInfoType::STRUCT_TYPE_INFO),
      child_types(std::move(child_types_p)) {}

StructTypeInfo::StructTypeInfo(std::vector<std::string> field_names_p,
                               std::vector<DataType> child_types_p)
    : ExtraTypeInfo(ExtraTypeInfoType::STRUCT_TYPE_INFO),
      child_types(std::move(child_types_p)),
      field_names(std::move(field_names_p)) {
  buildFieldNameIndex();
}

void StructTypeInfo::buildFieldNameIndex() {
  field_name_to_idx.clear();
  for (size_t i = 0; i < field_names.size(); ++i) {
    // Use emplace to keep the first occurrence when there are duplicate names.
    // This matches the old compiler StructTypeInfo behavior where base fields
    // (e.g., _SRC with kVertex type) take precedence over property fields
    // (e.g., _SRC with kInternalId type) added later.
    field_name_to_idx.emplace(field_names[i], i);
  }
}

bool StructTypeInfo::hasField(const std::string& name) const {
  return field_name_to_idx.find(name) != field_name_to_idx.end();
}

size_t StructTypeInfo::getFieldIdx(const std::string& name) const {
  auto it = field_name_to_idx.find(name);
  if (it == field_name_to_idx.end()) {
    return UINT8_MAX;  // INVALID_STRUCT_FIELD_IDX
  }
  return it->second;
}

const std::string& StructTypeInfo::getFieldName(size_t idx) const {
  assert(idx < field_names.size());
  return field_names[idx];
}

bool StructTypeInfo::EqualsInternal(ExtraTypeInfo* other_p) const {
  auto& other = other_p->Cast<StructTypeInfo>();
  return child_types == other.child_types && field_names == other.field_names;
}

// --- ListTypeInfo ---

ListTypeInfo::ListTypeInfo(DataType child_type_p)
    : ExtraTypeInfo(ExtraTypeInfoType::LIST_TYPE_INFO),
      child_type(std::move(child_type_p)) {}

ListTypeInfo::ListTypeInfo(ExtraTypeInfoType info_type, DataType child_type_p)
    : ExtraTypeInfo(info_type), child_type(std::move(child_type_p)) {}

bool ListTypeInfo::EqualsInternal(ExtraTypeInfo* other_p) const {
  auto& other = other_p->Cast<ListTypeInfo>();
  return child_type == other.child_type;
}

// --- ArrayTypeInfo ---

ArrayTypeInfo::ArrayTypeInfo(DataType child_type_p, uint64_t num_elements_p)
    : ListTypeInfo(ExtraTypeInfoType::ARRAY_TYPE_INFO, std::move(child_type_p)),
      num_elements(num_elements_p) {}

bool ArrayTypeInfo::EqualsInternal(ExtraTypeInfo* other_p) const {
  auto& other = other_p->Cast<ArrayTypeInfo>();
  return child_type == other.child_type && num_elements == other.num_elements;
}

// --- MapTypeInfo ---

MapTypeInfo::MapTypeInfo(DataType key_type_p, DataType value_type_p)
    : ExtraTypeInfo(ExtraTypeInfoType::MAP_TYPE_INFO),
      key_type(std::move(key_type_p)),
      value_type(std::move(value_type_p)) {}

bool MapTypeInfo::EqualsInternal(ExtraTypeInfo* other_p) const {
  auto& other = other_p->Cast<MapTypeInfo>();
  return key_type == other.key_type && value_type == other.value_type;
}

// --- StringTypeInfo ---

bool StringTypeInfo::EqualsInternal(ExtraTypeInfo* other_p) const {
  auto& other = other_p->Cast<StringTypeInfo>();
  return max_length == other.max_length;
}

// --- GNodeTypeInfo ---

GNodeTypeInfo::GNodeTypeInfo(std::vector<std::string> field_names_p,
                             std::vector<DataType> child_types_p,
                             std::shared_ptr<gopt::GNodeType> node_type_p)
    : StructTypeInfo(std::move(field_names_p), std::move(child_types_p)),
      node_type(std::move(node_type_p)) {
  type = ExtraTypeInfoType::GNODE_TYPE_INFO;
}

GNodeTypeInfo::~GNodeTypeInfo() = default;

bool GNodeTypeInfo::EqualsInternal(ExtraTypeInfo* other_p) const {
  return StructTypeInfo::EqualsInternal(other_p);
}

// --- GRelTypeInfo ---

GRelTypeInfo::GRelTypeInfo(std::vector<std::string> field_names_p,
                           std::vector<DataType> child_types_p,
                           std::shared_ptr<gopt::GRelType> rel_type_p)
    : StructTypeInfo(std::move(field_names_p), std::move(child_types_p)),
      rel_type(std::move(rel_type_p)) {
  type = ExtraTypeInfoType::GREL_TYPE_INFO;
}

GRelTypeInfo::~GRelTypeInfo() = default;

bool GRelTypeInfo::EqualsInternal(ExtraTypeInfo* other_p) const {
  return StructTypeInfo::EqualsInternal(other_p);
}

}  // namespace neug
