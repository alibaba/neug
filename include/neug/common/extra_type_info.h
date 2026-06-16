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

#pragma once

#include <assert.h>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "neug/common/types.h"

namespace neug {

namespace gopt {
struct GNodeType;
struct GRelType;
}  // namespace gopt

enum class ExtraTypeInfoType : uint8_t {
  INVALID_TYPE_INFO = 0,
  GENERIC_TYPE_INFO = 1,
  STRING_TYPE_INFO = 3,
  LIST_TYPE_INFO = 4,
  STRUCT_TYPE_INFO = 5,
  ARRAY_TYPE_INFO = 6,
  MAP_TYPE_INFO = 7,
  GNODE_TYPE_INFO = 8,
  GREL_TYPE_INFO = 9,
};

struct ExtraTypeInfo {
  ExtraTypeInfoType type;

  explicit ExtraTypeInfo(ExtraTypeInfoType type);
  virtual ~ExtraTypeInfo();

 protected:
  // copy	constructor (protected)
  ExtraTypeInfo(const ExtraTypeInfo& other);
  ExtraTypeInfo& operator=(const ExtraTypeInfo& other);

 public:
  bool Equals(ExtraTypeInfo* other_p) const;

  template <class TARGET>
  TARGET& Cast() {
    assert(dynamic_cast<TARGET*>(this) != nullptr);
    return reinterpret_cast<TARGET&>(*this);
  }
  template <class TARGET>
  const TARGET& Cast() const {
    assert(dynamic_cast<const TARGET*>(this) != nullptr);
    return reinterpret_cast<const TARGET&>(*this);
  }

 protected:
  virtual bool EqualsInternal(ExtraTypeInfo* other_p) const;
};

struct StructTypeInfo : public ExtraTypeInfo {
  explicit StructTypeInfo(std::vector<DataType> child_types_p);
  StructTypeInfo(std::vector<std::string> field_names_p,
                 std::vector<DataType> child_types_p);

  std::vector<DataType> child_types;
  std::vector<std::string> field_names;
  std::unordered_map<std::string, size_t> field_name_to_idx;

  bool hasField(const std::string& name) const;
  size_t getFieldIdx(const std::string& name) const;
  const std::string& getFieldName(size_t idx) const;

 protected:
  StructTypeInfo();
  bool EqualsInternal(ExtraTypeInfo* other_p) const override;

 private:
  void buildFieldNameIndex();
};

struct ListTypeInfo : public ExtraTypeInfo {
  DataType child_type;
  explicit ListTypeInfo(DataType child_type_p);

 protected:
  ListTypeInfo(ExtraTypeInfoType info_type, DataType child_type_p);
  bool EqualsInternal(ExtraTypeInfo* other_p) const override;
};

struct ArrayTypeInfo : public ListTypeInfo {
  uint64_t num_elements;
  explicit ArrayTypeInfo(DataType child_type_p, uint64_t num_elements_p);

 protected:
  bool EqualsInternal(ExtraTypeInfo* other_p) const override;
};

struct MapTypeInfo : public ExtraTypeInfo {
  DataType key_type;
  DataType value_type;
  explicit MapTypeInfo(DataType key_type_p, DataType value_type_p);

 protected:
  bool EqualsInternal(ExtraTypeInfo* other_p) const override;
};

struct StringTypeInfo : public ExtraTypeInfo {
  size_t max_length;
  explicit StringTypeInfo(size_t length)
      : ExtraTypeInfo(ExtraTypeInfoType::STRING_TYPE_INFO),
        max_length(length) {}

 protected:
  bool EqualsInternal(ExtraTypeInfo* other_p) const override;
};

struct GNodeTypeInfo : public StructTypeInfo {
  explicit GNodeTypeInfo(std::vector<std::string> field_names_p,
                         std::vector<DataType> child_types_p,
                         std::shared_ptr<gopt::GNodeType> node_type_p);
  ~GNodeTypeInfo() override;

  gopt::GNodeType* getNodeType() const { return node_type.get(); }
  const std::shared_ptr<gopt::GNodeType>& getNodeTypeSPtr() const {
    return node_type;
  }

 protected:
  bool EqualsInternal(ExtraTypeInfo* other_p) const override;

 private:
  std::shared_ptr<gopt::GNodeType> node_type;
};

struct GRelTypeInfo : public StructTypeInfo {
  explicit GRelTypeInfo(std::vector<std::string> field_names_p,
                        std::vector<DataType> child_types_p,
                        std::shared_ptr<gopt::GRelType> rel_type_p);
  ~GRelTypeInfo() override;

  gopt::GRelType* getRelType() const { return rel_type.get(); }
  const std::shared_ptr<gopt::GRelType>& getRelTypeSPtr() const {
    return rel_type;
  }

 protected:
  bool EqualsInternal(ExtraTypeInfo* other_p) const override;

 private:
  std::shared_ptr<gopt::GRelType> rel_type;
};

}  // namespace neug
