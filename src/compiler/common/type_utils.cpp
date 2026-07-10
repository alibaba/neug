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

#include "neug/compiler/common/type_utils.h"

#include "neug/compiler/common/vector/value_vector.h"

namespace neug {
namespace common {

namespace {
int64_t normalizeTimestampMillis(compiler_impl::timestamp_ms_t val) {
  constexpr int64_t kLikelyMicrosThreshold = 100000000000000LL;
  if (val.value > kLikelyMicrosThreshold ||
      val.value < -kLikelyMicrosThreshold) {
    return compiler_impl::Timestamp::getEpochMilliSeconds(
        compiler_impl::timestamp_t(val.value));
  }
  return val.value;
}
}  // namespace

std::string TypeUtils::entryToString(const DataType& dataType,
                                     const uint8_t* value,
                                     ValueVector* vector) {
  auto valueVector = reinterpret_cast<ValueVector*>(vector);
  switch (dataType.id()) {
  case DataTypeId::kBoolean:
    return TypeUtils::toString(*reinterpret_cast<const bool*>(value));
  case DataTypeId::kInt64:
    return TypeUtils::toString(*reinterpret_cast<const int64_t*>(value));
  case DataTypeId::kInt32:
    return TypeUtils::toString(*reinterpret_cast<const int32_t*>(value));
  case DataTypeId::kInt16:
    return TypeUtils::toString(*reinterpret_cast<const int16_t*>(value));
  case DataTypeId::kInt8:
    return TypeUtils::toString(*reinterpret_cast<const int8_t*>(value));
  case DataTypeId::kUInt64:
    return TypeUtils::toString(*reinterpret_cast<const uint64_t*>(value));
  case DataTypeId::kUInt32:
    return TypeUtils::toString(*reinterpret_cast<const uint32_t*>(value));
  case DataTypeId::kUInt16:
    return TypeUtils::toString(*reinterpret_cast<const uint16_t*>(value));
  case DataTypeId::kUInt8:
    return TypeUtils::toString(*reinterpret_cast<const uint8_t*>(value));
  case DataTypeId::kDouble:
    return TypeUtils::toString(*reinterpret_cast<const double*>(value));
  case DataTypeId::kFloat:
    return TypeUtils::toString(*reinterpret_cast<const float*>(value));
  case DataTypeId::kDate:
    return TypeUtils::toString(
        *reinterpret_cast<const compiler_impl::date_t*>(value));
  case DataTypeId::kTimestampMs:
    return TypeUtils::toString(
        *reinterpret_cast<const compiler_impl::timestamp_ms_t*>(value));
  case DataTypeId::kInterval:
    return TypeUtils::toString(
        *reinterpret_cast<const compiler_impl::interval_t*>(value));
  case DataTypeId::kVarchar:
    return TypeUtils::toString(*reinterpret_cast<const neug_string_t*>(value));
  case DataTypeId::kInternalId:
    return TypeUtils::toString(*reinterpret_cast<const internalID_t*>(value));
  case DataTypeId::kArray:
  case DataTypeId::kList:
    return TypeUtils::toString(*reinterpret_cast<const list_entry_t*>(value),
                               valueVector);
  case DataTypeId::kMap:
    return TypeUtils::toString(*reinterpret_cast<const map_entry_t*>(value),
                               valueVector);
  case DataTypeId::kStruct:
    return TypeUtils::toString(*reinterpret_cast<const struct_entry_t*>(value),
                               valueVector);
  case DataTypeId::kVertex:
    return TypeUtils::nodeToString(
        *reinterpret_cast<const struct_entry_t*>(value), valueVector);
  case DataTypeId::kEdge:
    return TypeUtils::relToString(
        *reinterpret_cast<const struct_entry_t*>(value), valueVector);
  default:
    NEUG_UNREACHABLE;
  }
}

static std::string entryToStringWithPos(sel_t pos, ValueVector* vector) {
  if (vector->isNull(pos)) {
    return "";
  }
  return TypeUtils::entryToString(
      vector->dataType, vector->getData() + vector->getNumBytesPerValue() * pos,
      vector);
}

template <>
std::string TypeUtils::toString(const int128_t& val, void* /*valueVector*/) {
  return Int128_t::ToString(val);
}

template <>
std::string TypeUtils::toString(const bool& val, void* /*valueVector*/) {
  return val ? "True" : "False";
}

template <>
std::string TypeUtils::toString(const internalID_t& val,
                                void* /*valueVector*/) {
  return std::to_string(val.tableID) + ":" + std::to_string(val.offset);
}

template <>
std::string TypeUtils::toString(const compiler_impl::date_t& val,
                                void* /*valueVector*/) {
  return compiler_impl::Date::toString(val);
}

template <>
std::string TypeUtils::toString(const compiler_impl::timestamp_ms_t& val,
                                void* /*valueVector*/) {
  return toString(compiler_impl::Timestamp::fromEpochMilliSeconds(val.value));
}

template <>
std::string TypeUtils::toString(const compiler_impl::timestamp_t& val,
                                void* /*valueVector*/) {
  return compiler_impl::Timestamp::toString(val);
}

template <>
std::string TypeUtils::toString(const compiler_impl::interval_t& val,
                                void* /*valueVector*/) {
  return compiler_impl::Interval::toString(val);
}

template <>
std::string TypeUtils::toString(const neug_string_t& val,
                                void* /*valueVector*/) {
  return val.getAsString();
}

template <>
std::string TypeUtils::toString(const list_entry_t& val, void* valueVector) {
  auto listVector = (ValueVector*) valueVector;
  if (val.size == 0) {
    return "[]";
  }
  std::string result = "[";
  auto dataVector = ListVector::getDataVector(listVector);
  for (auto i = 0u; i < val.size - 1; ++i) {
    result += entryToStringWithPos(val.offset + i, dataVector);
    result += ",";
  }
  result += entryToStringWithPos(val.offset + val.size - 1, dataVector);
  result += "]";
  return result;
}

static std::string getMapEntryStr(sel_t pos, ValueVector* dataVector,
                                  ValueVector* keyVector,
                                  ValueVector* valVector) {
  if (dataVector->isNull(pos)) {
    return "";
  }
  return entryToStringWithPos(pos, keyVector) + "=" +
         entryToStringWithPos(pos, valVector);
}

template <>
std::string TypeUtils::toString(const map_entry_t& val, void* valueVector) {
  auto mapVector = (ValueVector*) valueVector;
  if (val.entry.size == 0) {
    return "{}";
  }
  std::string result = "{";
  auto dataVector = ListVector::getDataVector(mapVector);
  auto keyVector = MapVector::getKeyVector(mapVector);
  auto valVector = MapVector::getValueVector(mapVector);
  for (auto i = 0u; i < val.entry.size - 1; ++i) {
    auto pos = val.entry.offset + i;
    result += getMapEntryStr(pos, dataVector, keyVector, valVector);
    result += ", ";
  }
  auto pos = val.entry.offset + val.entry.size - 1;
  result += getMapEntryStr(pos, dataVector, keyVector, valVector);
  result += "}";
  return result;
}

template <bool SKIP_NULL_ENTRY>
static std::string structToString(const struct_entry_t& val,
                                  ValueVector* vector) {
  const auto& fieldNames = StructType::GetFieldNames(vector->dataType);
  if (fieldNames.size() == 0) {
    return "{}";
  }
  std::string result = "{";
  auto i = 0u;
  for (; i < fieldNames.size() - 1; ++i) {
    auto fieldVector = StructVector::getFieldVector(vector, i);
    if constexpr (SKIP_NULL_ENTRY) {
      if (fieldVector->isNull(val.pos)) {
        continue;
      }
    }
    if (i != 0) {
      result += ", ";
    }
    result += StructType::GetChildName(vector->dataType, i);
    result += ": ";
    result += entryToStringWithPos(val.pos, fieldVector.get());
  }
  auto fieldVector = StructVector::getFieldVector(vector, i);
  if constexpr (SKIP_NULL_ENTRY) {
    if (fieldVector->isNull(val.pos)) {
      result += "}";
      return result;
    }
  }
  if (i != 0) {
    result += ", ";
  }
  result += StructType::GetChildName(vector->dataType, i);
  result += ": ";
  result += entryToStringWithPos(val.pos, fieldVector.get());
  result += "}";
  return result;
}

std::string TypeUtils::nodeToString(const struct_entry_t& val,
                                    ValueVector* vector) {
  // Internal ID vector is the first field vector.
  if (StructVector::getFieldVector(vector, 0)->isNull(val.pos)) {
    return "";
  }
  return structToString<true>(val, vector);
}

std::string TypeUtils::relToString(const struct_entry_t& val,
                                   ValueVector* vector) {
  // Internal ID vector is the third field vector.
  if (StructVector::getFieldVector(vector, 3)->isNull(val.pos)) {
    return "";
  }
  return structToString<true>(val, vector);
}

template <>
std::string TypeUtils::toString(const struct_entry_t& val, void* valVector) {
  return structToString<false>(val, (ValueVector*) valVector);
}

}  // namespace common
}  // namespace neug
