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
#include <vector>

namespace gs {
enum class DataTypeId : uint8_t {
  INVALID = 0,
  SQLNULL = 1,
  UNKNOWN = 2,
  // ANY = 3,
  // USER = 4,
  BOOLEAN = 10,
  TINYINT = 11,
  SMALLINT = 12,
  INTEGER = 13,
  BIGINT = 14,
  DATE = 15,
  // TIME = 16,
  // TIMESTAMP_SEC = 17,
  TIMESTAMP_MS = 18,
  // TIMESTAMP = 19,
  // TIMESTAMP_NS = 20,
  // DECIMAL = 21,
  FLOAT = 22,
  DOUBLE = 23,
  // CHAR = 24,
  VARCHAR = 25,
  // BLOB = 26,
  INTERVAL = 27,
  UTINYINT = 28,
  USMALLINT = 29,
  UINTEGER = 30,
  UBIGINT = 31,
  // TIMESTAMP_TZ = 32,
  // TIME_TZ = 34,
  // TIME_NS = 35,
  // BIT = 36,
  // STRING_LITERAL = 37,
  // INTEGER_LITERAL = 38,
  // VARINT = 39,
  // UHUGEINT = 49,
  // HUGEINT = 50,
  // POINTER = 51,
  // VALIDITY = 53,
  // UUID = 54,

  STRUCT = 100,
  LIST = 101,
  // MAP = 102,
  // TABLE = 103,
  // ENUM = 104,
  // AGGREGATE_STATE = 105,
  // LAMBDA = 106,
  // UNION = 107,
  // ARRAY = 108,

  VERTEX = 200,
  EDGE = 201,
  PATH = 202,
  EMPTY = 203
};

// types_mapping.h
#define NUMERIC_DATA_TYPES(M) \
  M(INTEGER, int32_t)         \
  M(BIGINT, int64_t)          \
  M(UINTEGER, uint32_t)       \
  M(UBIGINT, uint64_t)        \
  M(FLOAT, float)             \
  M(DOUBLE, double)

#define DATA_TYPES_DATETIME(M) \
  M(TIMESTAMP_MS, DateTime)    \
  M(DATE, Date)                \
  M(INTERVAL, Interval)

#define FOR_EACH_NUMERIC_DATA_TYPE(M) NUMERIC_DATA_TYPES(M)

#define FOR_EACH_DATA_TYPE_PRIMITIVE(M) \
  NUMERIC_DATA_TYPES(M)                 \
  M(BOOLEAN, bool)

#define FOR_EACH_DATA_TYPE_NO_STRING(M) \
  FOR_EACH_DATA_TYPE_PRIMITIVE(M)       \
  DATA_TYPES_DATETIME(M)

#define FOR_EACH_DATA_TYPE(M)     \
  FOR_EACH_DATA_TYPE_PRIMITIVE(M) \
  DATA_TYPES_DATETIME(M)          \
  M(VARCHAR, std::string_view)

struct ExtraTypeInfo;

struct DataType {
  DataType();
  DataType(DataTypeId id);
  DataType(DataTypeId id, std::shared_ptr<ExtraTypeInfo> type_info);
  DataType(const DataType& other);
  DataType(DataType&& other) noexcept;
  ~DataType();

  static DataType Struct(
      std::vector<std::pair<std::string, DataType>> children);
  static DataType List(const DataType& child_type);

  inline DataTypeId id() const { return id_; }

  const ExtraTypeInfo* AuxInfo() const {
    return type_info_ ? type_info_.get() : nullptr;
  }

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

  bool is_vertex() const { return id_ == DataTypeId::VERTEX; }

  bool is_edge() const { return id_ == DataTypeId::EDGE; }

 private:
  DataTypeId id_;
  std::shared_ptr<ExtraTypeInfo> type_info_;

 public:
  static constexpr const DataTypeId SQLNULL = DataTypeId::SQLNULL;
  static constexpr const DataTypeId UNKNOWN = DataTypeId::UNKNOWN;
  static constexpr const DataTypeId BOOLEAN = DataTypeId::BOOLEAN;
  static constexpr const DataTypeId TINYINT = DataTypeId::TINYINT;
  static constexpr const DataTypeId SMALLINT = DataTypeId::SMALLINT;
  static constexpr const DataTypeId INTEGER = DataTypeId::INTEGER;
  static constexpr const DataTypeId BIGINT = DataTypeId::BIGINT;
  static constexpr const DataTypeId TIMESTAMP_MS = DataTypeId::TIMESTAMP_MS;
  static constexpr const DataTypeId FLOAT = DataTypeId::FLOAT;
  static constexpr const DataTypeId DOUBLE = DataTypeId::DOUBLE;
  static constexpr const DataTypeId VARCHAR = DataTypeId::VARCHAR;
  static constexpr const DataTypeId INTERVAL = DataTypeId::INTERVAL;
  static constexpr const DataTypeId UTINYINT = DataTypeId::UTINYINT;
  static constexpr const DataTypeId USMALLINT = DataTypeId::USMALLINT;
  static constexpr const DataTypeId UINTEGER = DataTypeId::UINTEGER;
  static constexpr const DataTypeId UBIGINT = DataTypeId::UBIGINT;
  static constexpr const DataTypeId VERTEX = DataTypeId::VERTEX;
  static constexpr const DataTypeId EDGE = DataTypeId::EDGE;
  static constexpr const DataTypeId PATH = DataTypeId::PATH;
};

struct StructType {
  static const std::vector<DataType>& GetChildTypes(const DataType& type);
  static const DataType& GetChildType(const DataType& type, size_t index);
  // static const std::string& GetChildName(const DataType& type, size_t index);
};
}  // namespace gs