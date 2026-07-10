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

#include <utility>

#include "neug/compiler/common/types/date_t.h"
#include "neug/compiler/common/types/int128_t.h"
#include "neug/compiler/common/types/interval_t.h"
#include "neug/compiler/common/types/neug_list.h"
#include "neug/compiler/common/types/timestamp_t.h"
#include "neug/utils/api.h"

namespace neug {

namespace common {
class Deserializer;
class Serializer;
class NestedVal;
class NodeVal;
class RecursiveRelVal;
class RelVal;
class ValueVector;
}  // namespace common

namespace compiler_impl {

using common::getPhysicalType;
using common::int128_t;
using common::internalID_t;
using common::list_entry_t;
using common::neug_list_t;
using common::nodeID_t;
using common::PhysicalTypeID;
using common::struct_entry_t;

struct FileInfo;
class ArrowRowBatch;

class Value {
  friend class common::NodeVal;
  friend class common::RelVal;
  friend class common::NestedVal;
  friend class common::RecursiveRelVal;
  friend class ArrowRowBatch;
  friend class common::ValueVector;

 public:
  /**
   * @return a NULL value of ANY type.
   */
  NEUG_API static Value createNullValue();
  /**
   * @param dataType the type of the NULL value.
   * @return a NULL value of the given type.
   */
  NEUG_API static Value createNullValue(const DataType& dataType);
  /**
   * @param dataType the type of the non-NULL value.
   * @return a default non-NULL value of the given type.
   */
  NEUG_API static Value createDefaultValue(const DataType& dataType);
  /**
   * @param val_ the boolean value to set.
   */
  NEUG_API explicit Value(bool val_);
  /**
   * @param val_ the int8_t value to set.
   */
  NEUG_API explicit Value(int8_t val_);
  /**
   * @param val_ the int16_t value to set.
   */
  NEUG_API explicit Value(int16_t val_);
  /**
   * @param val_ the int32_t value to set.
   */
  NEUG_API explicit Value(int32_t val_);
  /**
   * @param val_ the int64_t value to set.
   */
  NEUG_API explicit Value(int64_t val_);
  /**
   * @param val_ the uint8_t value to set.
   */
  NEUG_API explicit Value(uint8_t val_);
  /**
   * @param val_ the uint16_t value to set.
   */
  NEUG_API explicit Value(uint16_t val_);
  /**
   * @param val_ the uint32_t value to set.
   */
  NEUG_API explicit Value(uint32_t val_);
  /**
   * @param val_ the uint64_t value to set.
   */
  NEUG_API explicit Value(uint64_t val_);
  /**
   * @param val_ the int128_t value to set.
   */
  NEUG_API explicit Value(int128_t val_);
  /**
   * @param val_ the double value to set.
   */
  NEUG_API explicit Value(double val_);
  /**
   * @param val_ the float value to set.
   */
  NEUG_API explicit Value(float val_);
  /**
   * @param val_ the date value to set.
   */
  NEUG_API explicit Value(date_t val_);
  /**
   * @param val_ the timestamp_ms value to set.
   */
  NEUG_API explicit Value(timestamp_ms_t val_);
  /**
   * @param val_ the timestamp value to set.
   */
  NEUG_API explicit Value(timestamp_t val_);
  /**
   * @param val_ the interval value to set.
   */
  NEUG_API explicit Value(interval_t val_);
  /**
   * @param val_ the internalID value to set.
   */
  NEUG_API explicit Value(internalID_t val_);
  /**
   * @param val_ the string value to set.
   */
  NEUG_API explicit Value(const char* val_);
  /**
   * @param val_ the string value to set.
   */
  NEUG_API explicit Value(const std::string& val_);
  /**
   * @param type the logical type of the value.
   * @param val_ the string value to set.
   */
  NEUG_API explicit Value(DataType type, std::string val_);
  /**
   * @param dataType the logical type of the value.
   * @param children a vector of children values.
   */
  NEUG_API explicit Value(DataType dataType,
                          std::vector<std::unique_ptr<Value>> children);
  /**
   * @param other the value to copy from.
   */
  NEUG_API Value(const Value& other);

  /**
   * @param other the value to move from.
   */
  NEUG_API Value(Value&& other) = default;
  NEUG_API Value& operator=(Value&& other) = default;
  NEUG_API bool operator==(const Value& rhs) const;

  /**
   * @brief Sets the data type of the Value.
   * @param dataType_ the data type to set to.
   */
  NEUG_API void setDataType(const DataType& dataType_);
  /**
   * @return the dataType of the value.
   */
  NEUG_API const DataType& getDataType() const;
  /**
   * @brief Sets the null flag of the Value.
   * @param flag null value flag to set.
   */
  NEUG_API void setNull(bool flag);
  /**
   * @brief Sets the null flag of the Value to true.
   */
  NEUG_API void setNull();
  /**
   * @return whether the Value is null or not.
   */
  NEUG_API bool isNull() const;
  /**
   * @brief Copies from the row layout value.
   * @param value value to copy from.
   */
  NEUG_API void copyFromRowLayout(const uint8_t* value);
  /**
   * @brief Copies from the col layout value.
   * @param value value to copy from.
   */
  NEUG_API void copyFromColLayout(const uint8_t* value,
                                  common::ValueVector* vec = nullptr);
  /**
   * @brief Copies from the other.
   * @param other value to copy from.
   */
  NEUG_API void copyValueFrom(const Value& other);
  /**
   * @return the value of the given type.
   */
  template <class T>
  T getValue() const {
    THROW_RUNTIME_ERROR("Unimplemented template for Value::getValue()");
  }
  /**
   * @return a reference to the value of the given type.
   */
  template <class T>
  T& getValueReference() {
    THROW_RUNTIME_ERROR(
        "Unimplemented template for Value::getValueReference()");
  }
  /**
   * @return a Value object based on value.
   */
  template <class T>
  static Value createValue(T /*value*/) {
    THROW_RUNTIME_ERROR("Unimplemented template for Value::createValue()");
  }

  /**
   * @return a copy of the current value.
   */
  NEUG_API std::unique_ptr<Value> copy() const;
  /**
   * @return the current value in string format.
   */
  NEUG_API std::string toString() const;

  NEUG_API void serialize(common::Serializer& serializer) const;

  NEUG_API static std::unique_ptr<Value> deserialize(
      common::Deserializer& deserializer);

  NEUG_API void validateType(common::DataTypeId targetTypeID) const;

  bool hasNoneNullChildren() const;
  bool allowTypeChange() const;

  uint64_t computeHash() const;

  NEUG_API uint32_t getChildrenSize() const { return childrenSize; }

 private:
  Value();
  explicit Value(const DataType& dataType);

  void resizeChildrenVector(uint64_t size, const DataType& childType);
  void copyFromRowLayoutList(const neug_list_t& list,
                             const DataType& childType);
  void copyFromColLayoutList(const list_entry_t& list,
                             common::ValueVector* vec);
  void copyFromRowLayoutStruct(const uint8_t* kuStruct);
  void copyFromColLayoutStruct(const struct_entry_t& structEntry,
                               common::ValueVector* vec);
  std::string mapToString() const;
  std::string listToString() const;
  std::string structToString() const;
  std::string nodeToString() const;
  std::string relToString() const;

 public:
  union Val {
    constexpr Val() : booleanVal{false} {}
    bool booleanVal;
    int128_t int128Val;
    int64_t int64Val;
    int32_t int32Val;
    int16_t int16Val;
    int8_t int8Val;
    uint64_t uint64Val;
    uint32_t uint32Val;
    uint16_t uint16Val;
    uint8_t uint8Val;
    double doubleVal;
    float floatVal;
    interval_t intervalVal;
    internalID_t internalIDVal;
  } val;
  std::string strVal;

 private:
  DataType dataType;
  bool isNull_;

 public:
  // Note: ALWAYS use childrenSize over children.size(). We do NOT resize
  // children when iterating with nested value. So children.size() reflects the
  // capacity() rather the actual size.
  std::vector<std::unique_ptr<Value>> children;
  uint32_t childrenSize;
};

/**
 * @return boolean value.
 */
template <>
NEUG_API inline bool Value::getValue() const {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::BOOL);
  return val.booleanVal;
}

/**
 * @return int8 value.
 */
template <>
NEUG_API inline int8_t Value::getValue() const {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::INT8);
  return val.int8Val;
}

/**
 * @return int16 value.
 */
template <>
NEUG_API inline int16_t Value::getValue() const {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::INT16);
  return val.int16Val;
}

/**
 * @return int32 value.
 */
template <>
NEUG_API inline int32_t Value::getValue() const {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::INT32);
  return val.int32Val;
}

/**
 * @return int64 value.
 */
template <>
NEUG_API inline int64_t Value::getValue() const {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::INT64);
  return val.int64Val;
}

/**
 * @return uint64 value.
 */
template <>
NEUG_API inline uint64_t Value::getValue() const {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::UINT64);
  return val.uint64Val;
}

/**
 * @return uint32 value.
 */
template <>
NEUG_API inline uint32_t Value::getValue() const {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::UINT32);
  return val.uint32Val;
}

/**
 * @return uint16 value.
 */
template <>
NEUG_API inline uint16_t Value::getValue() const {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::UINT16);
  return val.uint16Val;
}

/**
 * @return uint8 value.
 */
template <>
NEUG_API inline uint8_t Value::getValue() const {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::UINT8);
  return val.uint8Val;
}

/**
 * @return int128 value.
 */
template <>
NEUG_API inline int128_t Value::getValue() const {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::INT128);
  return val.int128Val;
}

/**
 * @return float value.
 */
template <>
NEUG_API inline float Value::getValue() const {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::FLOAT);
  return val.floatVal;
}

/**
 * @return double value.
 */
template <>
NEUG_API inline double Value::getValue() const {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::DOUBLE);
  return val.doubleVal;
}

/**
 * @return date_t value.
 */
template <>
NEUG_API inline date_t Value::getValue() const {
  NEUG_ASSERT(dataType.id() == DataTypeId::kDate);
  return date_t{val.int32Val};
}

/**
 * @return timestamp_t value.
 */
template <>
NEUG_API inline timestamp_t Value::getValue() const {
  NEUG_ASSERT(dataType.id() == DataTypeId::kTimestampMs);
  return timestamp_t{val.int64Val};
}

/**
 * @return timestamp_ms_t value.
 */
template <>
NEUG_API inline timestamp_ms_t Value::getValue() const {
  NEUG_ASSERT(dataType.id() == DataTypeId::kTimestampMs);
  return timestamp_ms_t{val.int64Val};
}

/**
 * @return interval_t value.
 */
template <>
NEUG_API inline interval_t Value::getValue() const {
  NEUG_ASSERT(dataType.id() == DataTypeId::kInterval);
  return val.intervalVal;
}

/**
 * @return internal_t value.
 */
template <>
NEUG_API inline internalID_t Value::getValue() const {
  NEUG_ASSERT(dataType.id() == DataTypeId::kInternalId);
  return val.internalIDVal;
}

/**
 * @return string value.
 */
template <>
NEUG_API inline std::string Value::getValue() const {
  NEUG_ASSERT(dataType.id() == DataTypeId::kVarchar);
  return strVal;
}

/**
 * @return the reference to the boolean value.
 */
template <>
NEUG_API inline bool& Value::getValueReference() {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::BOOL);
  return val.booleanVal;
}

/**
 * @return the reference to the int8 value.
 */
template <>
NEUG_API inline int8_t& Value::getValueReference() {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::INT8);
  return val.int8Val;
}

/**
 * @return the reference to the int16 value.
 */
template <>
NEUG_API inline int16_t& Value::getValueReference() {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::INT16);
  return val.int16Val;
}

/**
 * @return the reference to the int32 value.
 */
template <>
NEUG_API inline int32_t& Value::getValueReference() {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::INT32);
  return val.int32Val;
}

/**
 * @return the reference to the int64 value.
 */
template <>
NEUG_API inline int64_t& Value::getValueReference() {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::INT64);
  return val.int64Val;
}

/**
 * @return the reference to the uint8 value.
 */
template <>
NEUG_API inline uint8_t& Value::getValueReference() {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::UINT8);
  return val.uint8Val;
}

/**
 * @return the reference to the uint16 value.
 */
template <>
NEUG_API inline uint16_t& Value::getValueReference() {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::UINT16);
  return val.uint16Val;
}

/**
 * @return the reference to the uint32 value.
 */
template <>
NEUG_API inline uint32_t& Value::getValueReference() {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::UINT32);
  return val.uint32Val;
}

/**
 * @return the reference to the uint64 value.
 */
template <>
NEUG_API inline uint64_t& Value::getValueReference() {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::UINT64);
  return val.uint64Val;
}

/**
 * @return the reference to the int128 value.
 */
template <>
NEUG_API inline int128_t& Value::getValueReference() {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::INT128);
  return val.int128Val;
}

/**
 * @return the reference to the float value.
 */
template <>
NEUG_API inline float& Value::getValueReference() {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::FLOAT);
  return val.floatVal;
}

/**
 * @return the reference to the double value.
 */
template <>
NEUG_API inline double& Value::getValueReference() {
  NEUG_ASSERT(common::getPhysicalType(dataType.id()) ==
              common::PhysicalTypeID::DOUBLE);
  return val.doubleVal;
}

/**
 * @return the reference to the date value.
 */
template <>
NEUG_API inline date_t& Value::getValueReference() {
  NEUG_ASSERT(dataType.id() == DataTypeId::kDate);
  return *reinterpret_cast<date_t*>(&val.int32Val);
}

/**
 * @return the reference to the timestamp value.
 */
template <>
NEUG_API inline timestamp_t& Value::getValueReference() {
  NEUG_ASSERT(dataType.id() == DataTypeId::kTimestampMs);
  return *reinterpret_cast<timestamp_t*>(&val.int64Val);
}

/**
 * @return the reference to the timestamp_ms value.
 */
template <>
NEUG_API inline timestamp_ms_t& Value::getValueReference() {
  NEUG_ASSERT(dataType.id() == DataTypeId::kTimestampMs);
  return *reinterpret_cast<timestamp_ms_t*>(&val.int64Val);
}

/**
 * @return the reference to the interval value.
 */
template <>
NEUG_API inline interval_t& Value::getValueReference() {
  NEUG_ASSERT(dataType.id() == DataTypeId::kInterval);
  return val.intervalVal;
}

/**
 * @return the reference to the internal_id value.
 */
template <>
NEUG_API inline nodeID_t& Value::getValueReference() {
  NEUG_ASSERT(dataType.id() == DataTypeId::kInternalId);
  return val.internalIDVal;
}

/**
 * @return the reference to the string value.
 */
template <>
NEUG_API inline std::string& Value::getValueReference() {
  NEUG_ASSERT(dataType.id() == DataTypeId::kVarchar);
  return strVal;
}

/**
 * @param val the boolean value
 * @return a Value with BOOL type and val value.
 */
template <>
NEUG_API inline Value Value::createValue(bool val) {
  return Value(val);
}

template <>
NEUG_API inline Value Value::createValue(int8_t val) {
  return Value(val);
}

/**
 * @param val the int16 value
 * @return a Value with INT16 type and val value.
 */
template <>
NEUG_API inline Value Value::createValue(int16_t val) {
  return Value(val);
}

/**
 * @param val the int32 value
 * @return a Value with INT32 type and val value.
 */
template <>
NEUG_API inline Value Value::createValue(int32_t val) {
  return Value(val);
}

/**
 * @param val the int64 value
 * @return a Value with INT64 type and val value.
 */
template <>
NEUG_API inline Value Value::createValue(int64_t val) {
  return Value(val);
}

/**
 * @param val the uint8 value
 * @return a Value with UINT8 type and val value.
 */
template <>
NEUG_API inline Value Value::createValue(uint8_t val) {
  return Value(val);
}

/**
 * @param val the uint16 value
 * @return a Value with UINT16 type and val value.
 */
template <>
NEUG_API inline Value Value::createValue(uint16_t val) {
  return Value(val);
}

/**
 * @param val the uint32 value
 * @return a Value with UINT32 type and val value.
 */
template <>
NEUG_API inline Value Value::createValue(uint32_t val) {
  return Value(val);
}

/**
 * @param val the uint64 value
 * @return a Value with UINT64 type and val value.
 */
template <>
NEUG_API inline Value Value::createValue(uint64_t val) {
  return Value(val);
}

/**
 * @param val the int128_t value
 * @return a Value with INT128 type and val value.
 */
template <>
NEUG_API inline Value Value::createValue(int128_t val) {
  return Value(val);
}

/**
 * @param val the double value
 * @return a Value with DOUBLE type and val value.
 */
template <>
NEUG_API inline Value Value::createValue(double val) {
  return Value(val);
}

/**
 * @param val the date_t value
 * @return a Value with DATE type and val value.
 */
template <>
NEUG_API inline Value Value::createValue(date_t val) {
  return Value(val);
}

/**
 * @param val the timestamp_t value
 * @return a Value with TIMESTAMP type and val value.
 */
template <>
NEUG_API inline Value Value::createValue(timestamp_t val) {
  return Value(val);
}

/**
 * @param val the interval_t value
 * @return a Value with INTERVAL type and val value.
 */
template <>
NEUG_API inline Value Value::createValue(interval_t val) {
  return Value(val);
}

/**
 * @param val the nodeID_t value
 * @return a Value with NODE_ID type and val value.
 */
template <>
NEUG_API inline Value Value::createValue(nodeID_t val) {
  return Value(val);
}

/**
 * @param val the string value
 * @return a Value with type and val value.
 */
template <>
NEUG_API inline Value Value::createValue(std::string val) {
  return Value(DataType::Varchar(), std::move(val));
}

/**
 * @param value the string value
 * @return a Value with STRING type and val value.
 */
template <>
NEUG_API inline Value Value::createValue(const char* value) {
  return Value(DataType::Varchar(), std::string(value));
}

}  // namespace compiler_impl

}  // namespace neug
