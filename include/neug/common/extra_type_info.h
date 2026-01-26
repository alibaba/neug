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
#include <vector>

#include "neug/common/types.h"
namespace neug {
enum class ExtraTypeInfoType : uint8_t {
  INVALID_TYPE_INFO = 0,
  GENERIC_TYPE_INFO = 1,
  STRING_TYPE_INFO = 3,
  LIST_TYPE_INFO = 4,
  STRUCT_TYPE_INFO = 5,
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

  std::vector<DataType> child_types;

 protected:
  bool EqualsInternal(ExtraTypeInfo* other_p) const override;

 private:
  StructTypeInfo();
};

struct ListTypeInfo : public ExtraTypeInfo {
  DataType child_type;
  explicit ListTypeInfo(DataType child_type_p);

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

}  // namespace neug