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

#include <stdint.h>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

namespace common {
class DataType;
class IrDataType;
}  // namespace common

namespace neug {
class InArchive;
class OutArchive;
enum class DataTypeId : uint8_t {
  kInvalid = 0,
  kNull = 1,
  kUnknown = 2,

  kBoolean = 10,
  kInt8 = 11,
  kInt16 = 12,
  kInt32 = 13,
  kInt64 = 14,
  kDate = 15,

  kTimestampMs = 18,

  kFloat = 22,
  kDouble = 23,

  kVarchar = 25,
  // kBlob = 26,
  kInterval = 27,
  kUInt8 = 28,
  kUInt16 = 29,
  kUInt32 = 30,
  kUInt64 = 31,
  // kTimestampTZ = 32,

  // kStringLiteral = 37,

  kInternalId = 40,

  kStruct = 100,
  kList = 101,
  kMap = 102,

  kArray = 108,

  kVertex = 200,
  kEdge = 201,
  kPath = 202,
  kEmpty = 203
};

// types_mapping.h
#define NUMERIC_DATA_TYPES(M) \
  M(kInt32, int32_t)          \
  M(kInt64, int64_t)          \
  M(kUInt32, uint32_t)        \
  M(kUInt64, uint64_t)        \
  M(kFloat, float)            \
  M(kDouble, double)

#define DATA_TYPES_DATETIME(M) \
  M(kTimestampMs, DateTime)    \
  M(kDate, Date)               \
  M(kInterval, Interval)

#define FOR_EACH_NUMERIC_DATA_TYPE(M) NUMERIC_DATA_TYPES(M)

#define FOR_EACH_DATA_TYPE_PRIMITIVE(M) \
  NUMERIC_DATA_TYPES(M)                 \
  M(kBoolean, bool)

#define FOR_EACH_DATA_TYPE_NO_STRING(M) \
  FOR_EACH_DATA_TYPE_PRIMITIVE(M)       \
  DATA_TYPES_DATETIME(M)

#define FOR_EACH_DATA_TYPE(M)     \
  FOR_EACH_DATA_TYPE_PRIMITIVE(M) \
  DATA_TYPES_DATETIME(M)          \
  M(kVarchar, std::string)

struct ExtraTypeInfo;

struct DataType {
  DataType();
  DataType(DataTypeId id);
  DataType(DataTypeId id, std::shared_ptr<ExtraTypeInfo> type_info);
  DataType(const DataType& other);
  DataType(DataType&& other) noexcept;
  ~DataType();

  static DataType Struct(std::vector<DataType> children);
  static DataType Struct(std::vector<std::string> field_names,
                         std::vector<DataType> field_types);
  static DataType List(const DataType& child_type);
  static DataType Array(const DataType& child_type, uint64_t num_elements);
  static DataType Map(const DataType& key_type, const DataType& value_type);
  static DataType Varchar(size_t max_length = 256);
  static DataType InternalId();

  bool containsAny() const;

  inline DataTypeId id() const { return id_; }

  void setExtraTypeInfo(std::shared_ptr<ExtraTypeInfo> info) {
    type_info_ = std::move(info);
  }

  // Backward-compatible convenience methods (match LogicalType API).
  DataType copy() const { return *this; }

  const ExtraTypeInfo* getExtraTypeInfo() const {
    return type_info_ ? type_info_.get() : nullptr;
  }
  ExtraTypeInfo* getExtraTypeInfoRef() const {
    return type_info_ ? type_info_.get() : nullptr;
  }
  bool isInternalType() const { return true; }  // all engine types are internal

  bool EqualTypeInfo(const DataType& rhs) const;

  inline DataType& operator=(const DataType& other) {
    if (this == &other) {
      return *this;
    }
    id_ = other.id_;
    type_info_ = other.type_info_;
    return *this;
  }

  inline DataType& operator=(DataType&& other) noexcept {
    id_ = other.id_;
    std::swap(type_info_, other.type_info_);
    return *this;
  }

  bool operator==(const DataType& other) const;

  inline bool operator!=(const DataType& other) const {
    return !(*this == other);
  }

  bool is_vertex() const { return id_ == DataTypeId::kVertex; }

  bool is_edge() const { return id_ == DataTypeId::kEdge; }

  std::string ToString() const;

 private:
  DataTypeId id_;
  std::shared_ptr<ExtraTypeInfo> type_info_;

 public:
  static constexpr const DataTypeId SQLNULL = DataTypeId::kNull;
  static constexpr const DataTypeId UNKNOWN = DataTypeId::kUnknown;
  static constexpr const DataTypeId BOOLEAN = DataTypeId::kBoolean;
  static constexpr const DataTypeId INT8 = DataTypeId::kInt8;
  static constexpr const DataTypeId INT16 = DataTypeId::kInt16;
  static constexpr const DataTypeId INT32 = DataTypeId::kInt32;
  static constexpr const DataTypeId INT64 = DataTypeId::kInt64;
  static constexpr const DataTypeId DATE = DataTypeId::kDate;
  static constexpr const DataTypeId TIMESTAMP_MS = DataTypeId::kTimestampMs;
  static constexpr const DataTypeId FLOAT = DataTypeId::kFloat;
  static constexpr const DataTypeId DOUBLE = DataTypeId::kDouble;
  static constexpr const DataTypeId VARCHAR = DataTypeId::kVarchar;
  static constexpr const DataTypeId INTERVAL = DataTypeId::kInterval;
  static constexpr const DataTypeId UINT8 = DataTypeId::kUInt8;
  static constexpr const DataTypeId UINT16 = DataTypeId::kUInt16;
  static constexpr const DataTypeId UINT32 = DataTypeId::kUInt32;
  static constexpr const DataTypeId UINT64 = DataTypeId::kUInt64;
  static constexpr const DataTypeId INTERNAL_ID = DataTypeId::kInternalId;
  static constexpr const DataTypeId STRUCT = DataTypeId::kStruct;
  static constexpr const DataTypeId LIST = DataTypeId::kList;
  static constexpr const DataTypeId MAP = DataTypeId::kMap;
  static constexpr const DataTypeId ARRAY = DataTypeId::kArray;
  static constexpr const DataTypeId VERTEX = DataTypeId::kVertex;
  static constexpr const DataTypeId EDGE = DataTypeId::kEdge;
  static constexpr const DataTypeId PATH = DataTypeId::kPath;
  static constexpr const DataTypeId EMPTY = DataTypeId::kEmpty;

  std::shared_ptr<ExtraTypeInfo> getExtraTypeInfoSPtr() const {
    return type_info_;
  }
};

struct ListType {
  static const DataType& GetChildType(const DataType& type);
};
struct ArrayType {
  static const DataType& GetChildType(const DataType& type);
  static uint64_t GetNumElements(const DataType& type);
};
struct StructType {
  static const std::vector<DataType>& GetChildTypes(const DataType& type);
  static const DataType& GetChildType(const DataType& type, size_t index);
  static const std::string& GetChildName(const DataType& type, size_t index);
  static const std::vector<std::string>& GetFieldNames(const DataType& type);
  static bool HasField(const DataType& type, const std::string& name);
  static size_t GetFieldIdx(const DataType& type, const std::string& name);
  static uint64_t GetNumFields(const DataType& type);
};
struct MapType {
  static const DataType& GetKeyType(const DataType& type);
  static const DataType& GetValueType(const DataType& type);
};

DataType parse_from_data_type(const ::common::DataType& ddt);

DataType parse_from_ir_data_type(const ::common::IrDataType& dt);

InArchive& operator<<(InArchive& arc, const DataType& type);
OutArchive& operator>>(OutArchive& arc, DataType& type);

}  // namespace neug
