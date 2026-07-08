/**
 * Copyright 2020 Alibaba Group Holding Limited.
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
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "neug/common/extra_type_info.h"
#include "neug/common/types.h"
#include "neug/compiler/common/cast.h"
#include "neug/compiler/common/types/interval_t.h"
#include "neug/utils/api.h"

namespace neug {
namespace main {
class ClientContext;
}

namespace processor {
class ParquetReader;
}
namespace catalog {
class NodeTableCatalogEntry;
}
namespace common {

struct FileInfo;

using sel_t = uint64_t;
constexpr sel_t INVALID_SEL = UINT64_MAX;
using hash_t = uint64_t;
using page_idx_t = uint32_t;
using frame_idx_t = page_idx_t;
using page_offset_t = uint32_t;
constexpr page_idx_t INVALID_PAGE_IDX = UINT32_MAX;
using file_idx_t = uint32_t;
constexpr file_idx_t INVALID_FILE_IDX = UINT32_MAX;
using page_group_idx_t = uint32_t;
using frame_group_idx_t = page_group_idx_t;
using column_id_t = uint32_t;
using property_id_t = uint32_t;
using alias_id_t = uint32_t;
constexpr column_id_t INVALID_COLUMN_ID = UINT32_MAX;
constexpr column_id_t ROW_IDX_COLUMN_ID = INVALID_COLUMN_ID - 1;
using idx_t = uint32_t;
constexpr idx_t INVALID_IDX = UINT32_MAX;
using block_idx_t = uint64_t;
constexpr block_idx_t INVALID_BLOCK_IDX = UINT64_MAX;
using struct_field_idx_t = uint8_t;
constexpr struct_field_idx_t INVALID_STRUCT_FIELD_IDX = UINT8_MAX;
using row_idx_t = uint64_t;
constexpr row_idx_t INVALID_ROW_IDX = UINT64_MAX;
constexpr uint32_t UNDEFINED_CAST_COST = UINT32_MAX;
using node_group_idx_t = uint64_t;
constexpr node_group_idx_t INVALID_NODE_GROUP_IDX = UINT64_MAX;
using partition_idx_t = uint64_t;
constexpr partition_idx_t INVALID_PARTITION_IDX = UINT64_MAX;
using length_t = uint64_t;
constexpr length_t INVALID_LENGTH = UINT64_MAX;
using list_size_t = uint32_t;
using sequence_id_t = uint64_t;
using oid_t = uint64_t;
constexpr oid_t INVALID_OID = UINT64_MAX;

using transaction_t = uint64_t;
constexpr transaction_t INVALID_TRANSACTION = UINT64_MAX;
using executor_id_t = uint64_t;
using executor_info = std::unordered_map<executor_id_t, uint64_t>;

using table_id_t = oid_t;
using table_id_vector_t = std::vector<table_id_t>;
using table_id_set_t = std::unordered_set<table_id_t>;
template <typename T>
using table_id_map_t = std::unordered_map<table_id_t, T>;
constexpr table_id_t INVALID_TABLE_ID = INVALID_OID;
using offset_t = uint64_t;
constexpr offset_t INVALID_OFFSET = UINT64_MAX;
struct internalID_t;
using nodeID_t = internalID_t;
using relID_t = internalID_t;

using cardinality_t = uint64_t;
constexpr offset_t INVALID_LIMIT = UINT64_MAX;
using offset_vec_t = std::vector<offset_t>;
struct NEUG_API internalID_t {
  offset_t offset;
  table_id_t tableID;

  internalID_t();
  internalID_t(offset_t offset, table_id_t tableID);

  bool operator==(const internalID_t& rhs) const;
  bool operator!=(const internalID_t& rhs) const;
  bool operator>(const internalID_t& rhs) const;
  bool operator>=(const internalID_t& rhs) const;
  bool operator<(const internalID_t& rhs) const;
  bool operator<=(const internalID_t& rhs) const;
};

struct overflow_value_t {
  uint64_t numElements = 0;
  uint8_t* value = nullptr;
};

struct list_entry_t {
  offset_t offset;
  list_size_t size;

  constexpr list_entry_t() : offset{INVALID_OFFSET}, size{UINT32_MAX} {}
  constexpr list_entry_t(offset_t offset, list_size_t size)
      : offset{offset}, size{size} {}
};

struct struct_entry_t {
  int64_t pos;
};

struct map_entry_t {
  list_entry_t entry;
};

struct int128_t;
struct neug_string_t;

template <typename T>
concept SignedIntegerTypes =
    std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t> ||
    std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> ||
    std::is_same_v<T, int128_t>;

template <typename T>
concept IntegerTypes =
    SignedIntegerTypes<T> || std::is_same_v<T, uint8_t> ||
    std::is_same_v<T, uint16_t> || std::is_same_v<T, uint32_t> ||
    std::is_same_v<T, uint64_t>;

template <typename T>
concept FloatingPointTypes =
    std::is_same_v<T, float> || std::is_same_v<T, double>;

template <typename T>
concept NumericTypes = IntegerTypes<T> || std::floating_point<T>;

template <typename T>
concept ComparableTypes =
    NumericTypes<T> || std::is_same_v<T, neug_string_t> ||
    std::is_same_v<T, compiler_impl::interval_t> || std::is_same_v<T, bool>;

template <typename T>
concept HashablePrimitive = ((std::integral<T> && !std::is_same_v<T, bool>) ||
                             std::floating_point<T> ||
                             std::is_same_v<T, int128_t>);
template <typename T>
concept IndexHashable =
    ((std::integral<T> && !std::is_same_v<T, bool>) || std::floating_point<T> ||
     std::is_same_v<T, int128_t> || std::is_same_v<T, neug_string_t> ||
     std::is_same_v<T, std::string_view> || std::same_as<T, std::string>);

template <typename T>
concept HashableNonNestedTypes =
    (std::integral<T> || std::floating_point<T> ||
     std::is_same_v<T, int128_t> || std::is_same_v<T, internalID_t> ||
     std::is_same_v<T, compiler_impl::interval_t> ||
     std::is_same_v<T, neug_string_t>);

template <typename T>
concept HashableNestedTypes = (std::is_same_v<T, list_entry_t> ||
                               std::is_same_v<T, struct_entry_t>);

template <typename T>
concept HashableTypes = (HashableNestedTypes<T> || HashableNonNestedTypes<T>);

// ============================================================================
// Bring engine types into neug::common namespace.
// All compiler code uses these instead of the old DataType/DataTypeId.
// ============================================================================
using neug::ArrayType;
using neug::ArrayTypeInfo;
using neug::DataType;
using neug::DataTypeId;
using neug::ExtraTypeInfo;
using neug::ExtraTypeInfoType;
using neug::GNodeTypeInfo;
using neug::GRelTypeInfo;
using neug::ListType;
using neug::ListTypeInfo;
using neug::MapType;
using neug::MapTypeInfo;
using neug::StringTypeInfo;
using neug::StructType;
using neug::StructTypeInfo;

using logical_type_vec_t = std::vector<DataType>;

// PhysicalTypeID remains a compiler-only concept for physical storage layout.
enum class PhysicalTypeID : uint8_t {
  ANY = 0,
  BOOL = 1,
  INT64 = 2,
  INT32 = 3,
  INT16 = 4,
  INT8 = 5,
  UINT64 = 6,
  UINT32 = 7,
  UINT16 = 8,
  UINT8 = 9,
  INT128 = 10,
  DOUBLE = 11,
  FLOAT = 12,
  INTERVAL = 13,
  INTERNAL_ID = 14,
  ALP_EXCEPTION_FLOAT = 15,
  ALP_EXCEPTION_DOUBLE = 16,

  STRING = 20,
  LIST = 22,
  ARRAY = 23,
  STRUCT = 24,
};

struct PhysicalTypeUtils {
  static std::string toString(PhysicalTypeID physicalType);
  static uint32_t getFixedTypeSize(PhysicalTypeID physicalType);
};

// Maps DataTypeId to its physical storage type.
PhysicalTypeID getPhysicalType(DataTypeId typeId);

struct NEUG_API LogicalTypeUtils {
  static std::string toString(DataTypeId dataTypeID);
  static std::string toString(const std::vector<DataType>& dataTypes);
  static std::string toString(const std::vector<DataTypeId>& dataTypeIDs);
  static uint32_t getRowLayoutSize(const DataType& dataType);
  static bool isDate(const DataType& dataType);
  static bool isDate(DataTypeId dataType);
  static bool isTimestamp(const DataType& dataType);
  static bool isTimestamp(DataTypeId dataType);
  static bool isUnsigned(const DataType& dataType);
  static bool isUnsigned(DataTypeId dataType);
  static bool isIntegral(const DataType& dataType);
  static bool isIntegral(DataTypeId dataType);
  static bool isNumerical(const DataType& dataType);
  static bool isNumerical(DataTypeId dataType);
  static bool isFloatingPoint(DataTypeId dataType);
  static bool isNested(const DataType& dataType);
  static bool isNested(DataTypeId dataType);
  static std::vector<DataTypeId> getAllValidComparableLogicalTypes();
  static std::vector<DataTypeId> getNumericalDataTypeIds();
  static std::vector<DataTypeId> getIntegerTypeIDs();
  static std::vector<DataTypeId> getFloatingPointTypeIDs();
  static std::vector<DataTypeId> getAllValidLogicTypeIDs();
  static std::vector<DataType> getAllValidLogicTypes();
  static bool tryGetMaxLogicalType(const DataType& left, const DataType& right,
                                   DataType& result);
  static bool tryGetMaxLogicalType(const std::vector<DataType>& types,
                                   DataType& result);

  static DataType combineTypes(const DataType& left, const DataType& right);

  static DataType purgeAny(const DataType& type, const DataType& replacement);

 private:
  static bool tryGetMaxDataTypeId(DataTypeId left, DataTypeId right,
                                  DataTypeId& result);
};

DataType convertFromString(const std::string& str,
                           main::ClientContext* context = nullptr);
bool isBuiltInType(const std::string& str);

enum class FileVersionType : uint8_t { ORIGINAL = 0, WAL_VERSION = 1 };

}  // namespace common
}  // namespace neug
