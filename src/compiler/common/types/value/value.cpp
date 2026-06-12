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

#include "neug/compiler/common/types/value/value.h"

#include <utility>

#include "neug/compiler/common/null_buffer.h"
#include "neug/compiler/common/serializer/deserializer.h"
#include "neug/compiler/common/serializer/serializer.h"
#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/common/types/neug_string.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace common {

bool Value::operator==(const Value& rhs) const {
  if (dataType != rhs.dataType || isNull_ != rhs.isNull_) {
    return false;
  }
  switch (getPhysicalType(dataType.id())) {
  case PhysicalTypeID::BOOL:
    return val.booleanVal == rhs.val.booleanVal;
  case PhysicalTypeID::INT128:
    return val.int128Val == rhs.val.int128Val;
  case PhysicalTypeID::INT64:
    return val.int64Val == rhs.val.int64Val;
  case PhysicalTypeID::INT32:
    return val.int32Val == rhs.val.int32Val;
  case PhysicalTypeID::INT16:
    return val.int16Val == rhs.val.int16Val;
  case PhysicalTypeID::INT8:
    return val.int8Val == rhs.val.int8Val;
  case PhysicalTypeID::UINT64:
    return val.uint64Val == rhs.val.uint64Val;
  case PhysicalTypeID::UINT32:
    return val.uint32Val == rhs.val.uint32Val;
  case PhysicalTypeID::UINT16:
    return val.uint16Val == rhs.val.uint16Val;
  case PhysicalTypeID::UINT8:
    return val.uint8Val == rhs.val.uint8Val;
  case PhysicalTypeID::DOUBLE:
    return val.doubleVal == rhs.val.doubleVal;
  case PhysicalTypeID::FLOAT:
    return val.floatVal == rhs.val.floatVal;
  case PhysicalTypeID::INTERVAL:
    return val.intervalVal == rhs.val.intervalVal;
  case PhysicalTypeID::INTERNAL_ID:
    return val.internalIDVal == rhs.val.internalIDVal;
  case PhysicalTypeID::STRING:
    return strVal == rhs.strVal;
  case PhysicalTypeID::ARRAY:
  case PhysicalTypeID::LIST:
  case PhysicalTypeID::STRUCT: {
    if (childrenSize != rhs.childrenSize) {
      return false;
    }
    for (auto i = 0u; i < childrenSize; ++i) {
      if (*children[i] != *rhs.children[i]) {
        return false;
      }
    }
    return true;
  }
  default:
    NEUG_UNREACHABLE;
  }
}

void Value::setDataType(const DataType& dataType_) {
  NEUG_ASSERT(allowTypeChange());
  dataType = dataType_.copy();
}

const DataType& Value::getDataType() const { return dataType; }

void Value::setNull(bool flag) { isNull_ = flag; }

void Value::setNull() { isNull_ = true; }

bool Value::isNull() const { return isNull_; }

std::unique_ptr<Value> Value::copy() const {
  return std::make_unique<Value>(*this);
}

Value Value::createNullValue() { return {}; }

Value Value::createNullValue(const DataType& dataType) {
  return Value(dataType);
}

Value Value::createDefaultValue(const DataType& dataType) {
  switch (dataType.id()) {
  case DataTypeId::kInt64:
    return Value((int64_t) 0);
  case DataTypeId::kInt32:
    return Value((int32_t) 0);
  case DataTypeId::kInt16:
    return Value((int16_t) 0);
  case DataTypeId::kInt8:
    return Value((int8_t) 0);
  case DataTypeId::kUInt64:
    return Value((uint64_t) 0);
  case DataTypeId::kUInt32:
    return Value((uint32_t) 0);
  case DataTypeId::kUInt16:
    return Value((uint16_t) 0);
  case DataTypeId::kUInt8:
    return Value((uint8_t) 0);
  // INT128 removed — no engine equivalent
  case DataTypeId::kBoolean:
    return Value(true);
  case DataTypeId::kDouble:
    return Value((double) 0);
  case DataTypeId::kDate:
    return Value(date_t());
  case DataTypeId::kTimestampMs:
    return Value(timestamp_ms_t());
  case DataTypeId::kInterval:
    return Value(interval_t());
  case DataTypeId::kInternalId:
    return Value(nodeID_t());
  case DataTypeId::kVarchar:
    return Value(DataType::Varchar(), std::string(""));
  case DataTypeId::kFloat:
    return Value((float) 0);
  case DataTypeId::kArray: {
    std::vector<std::unique_ptr<Value>> children;
    const auto& childType = ArrayType::GetChildType(dataType);
    auto arraySize = ArrayType::GetNumElements(dataType);
    children.reserve(arraySize);
    for (auto i = 0u; i < arraySize; ++i) {
      children.push_back(
          std::make_unique<Value>(createDefaultValue(childType)));
    }
    return Value(dataType.copy(), std::move(children));
  }
  case DataTypeId::kMap:
  case DataTypeId::kList: {
    return Value(dataType.copy(), std::vector<std::unique_ptr<Value>>{});
  }
  case DataTypeId::kVertex:
  case DataTypeId::kEdge:
  case DataTypeId::kPath:
  case DataTypeId::kStruct: {
    std::vector<std::unique_ptr<Value>> children;
    const auto& childTypes = StructType::GetChildTypes(dataType);
    for (auto i = 0u; i < childTypes.size(); ++i) {
      children.push_back(
          std::make_unique<Value>(createDefaultValue(childTypes[i])));
    }
    return Value(dataType.copy(), std::move(children));
  }
  case DataTypeId::kUnknown: {
    return createNullValue();
  }
  default:
    NEUG_UNREACHABLE;
  }
}

Value::Value(bool val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kBoolean);
  val.booleanVal = val_;
}

Value::Value(int8_t val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kInt8);
  val.int8Val = val_;
}

Value::Value(int16_t val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kInt16);
  val.int16Val = val_;
}

Value::Value(int32_t val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kInt32);
  val.int32Val = val_;
}

Value::Value(int64_t val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kInt64);
  val.int64Val = val_;
}

Value::Value(uint8_t val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kUInt8);
  val.uint8Val = val_;
}

Value::Value(uint16_t val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kUInt16);
  val.uint16Val = val_;
}

Value::Value(uint32_t val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kUInt32);
  val.uint32Val = val_;
}

Value::Value(uint64_t val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kUInt64);
  val.uint64Val = val_;
}

Value::Value(int128_t val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kInt64);
  val.int128Val = val_;
}

Value::Value(float val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kFloat);
  val.floatVal = val_;
}

Value::Value(double val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kDouble);
  val.doubleVal = val_;
}

Value::Value(date_t val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kDate);
  val.int32Val = val_.days;
}

Value::Value(timestamp_ms_t val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kTimestampMs);
  val.int64Val = val_.value;
}

Value::Value(timestamp_t val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kTimestampMs);
  val.int64Val = val_.value;
}

Value::Value(interval_t val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kInterval);
  val.intervalVal = val_;
}

Value::Value(internalID_t val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType(DataTypeId::kInternalId);
  val.internalIDVal = val_;
}

Value::Value(const char* val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType::Varchar();
  strVal = std::string(val_);
}

Value::Value(const std::string& val_) : isNull_{false}, childrenSize{0} {
  dataType = DataType::Varchar();
  strVal = val_;
}

Value::Value(DataType type, std::string val_)
    : dataType{std::move(type)}, isNull_{false}, childrenSize{0} {
  strVal = std::move(val_);
}

Value::Value(DataType dataType_, std::vector<std::unique_ptr<Value>> children)
    : dataType{std::move(dataType_)}, isNull_{false} {
  this->children = std::move(children);
  childrenSize = this->children.size();
}

Value::Value(const Value& other) : isNull_{other.isNull_} {
  dataType = other.dataType.copy();
  copyValueFrom(other);
  childrenSize = other.childrenSize;
}

void Value::copyFromRowLayout(const uint8_t* value) {
  switch (dataType.id()) {
  case DataTypeId::kTimestampMs:
  case DataTypeId::kInt64: {
    val.int64Val = *((int64_t*) value);
  } break;
  case DataTypeId::kDate:
  case DataTypeId::kInt32: {
    val.int32Val = *((int32_t*) value);
  } break;
  case DataTypeId::kInt16: {
    val.int16Val = *((int16_t*) value);
  } break;
  case DataTypeId::kInt8: {
    val.int8Val = *((int8_t*) value);
  } break;
  case DataTypeId::kUInt64: {
    val.uint64Val = *((uint64_t*) value);
  } break;
  case DataTypeId::kUInt32: {
    val.uint32Val = *((uint32_t*) value);
  } break;
  case DataTypeId::kUInt16: {
    val.uint16Val = *((uint16_t*) value);
  } break;
  case DataTypeId::kUInt8: {
    val.uint8Val = *((uint8_t*) value);
  } break;
  case DataTypeId::kBoolean: {
    val.booleanVal = *((bool*) value);
  } break;
  case DataTypeId::kDouble: {
    val.doubleVal = *((double*) value);
  } break;
  case DataTypeId::kFloat: {
    val.floatVal = *((float*) value);
  } break;
  case DataTypeId::kInterval: {
    val.intervalVal = *((interval_t*) value);
  } break;
  case DataTypeId::kInternalId: {
    val.internalIDVal = *((nodeID_t*) value);
  } break;
  case DataTypeId::kVarchar: {
    strVal = ((neug_string_t*) value)->getAsString();
  } break;
  case DataTypeId::kMap:
  case DataTypeId::kList: {
    copyFromRowLayoutList(*(neug_list_t*) value,
                          ListType::GetChildType(dataType));
  } break;
  case DataTypeId::kArray: {
    copyFromRowLayoutList(*(neug_list_t*) value,
                          ArrayType::GetChildType(dataType));
  } break;
  case DataTypeId::kVertex:
  case DataTypeId::kEdge:
  case DataTypeId::kPath:
  case DataTypeId::kStruct: {
    copyFromRowLayoutStruct(value);
  } break;
  default:
    NEUG_UNREACHABLE;
  }
}

void Value::copyFromColLayout(const uint8_t* value, ValueVector* vector) {
  switch (getPhysicalType(dataType.id())) {
  case PhysicalTypeID::INT64: {
    val.int64Val = *((int64_t*) value);
  } break;
  case PhysicalTypeID::INT32: {
    val.int32Val = *((int32_t*) value);
  } break;
  case PhysicalTypeID::INT16: {
    val.int16Val = *((int16_t*) value);
  } break;
  case PhysicalTypeID::INT8: {
    val.int8Val = *((int8_t*) value);
  } break;
  case PhysicalTypeID::UINT64: {
    val.uint64Val = *((uint64_t*) value);
  } break;
  case PhysicalTypeID::UINT32: {
    val.uint32Val = *((uint32_t*) value);
  } break;
  case PhysicalTypeID::UINT16: {
    val.uint16Val = *((uint16_t*) value);
  } break;
  case PhysicalTypeID::UINT8: {
    val.uint8Val = *((uint8_t*) value);
  } break;
  case PhysicalTypeID::INT128: {
    val.int128Val = *((int128_t*) value);
  } break;
  case PhysicalTypeID::BOOL: {
    val.booleanVal = *((bool*) value);
  } break;
  case PhysicalTypeID::DOUBLE: {
    val.doubleVal = *((double*) value);
  } break;
  case PhysicalTypeID::FLOAT: {
    val.floatVal = *((float*) value);
  } break;
  case PhysicalTypeID::INTERVAL: {
    val.intervalVal = *((interval_t*) value);
  } break;
  case PhysicalTypeID::STRING: {
    strVal = ((neug_string_t*) value)->getAsString();
  } break;
  case PhysicalTypeID::ARRAY:
  case PhysicalTypeID::LIST: {
    copyFromColLayoutList(*(list_entry_t*) value, vector);
  } break;
  case PhysicalTypeID::STRUCT: {
    copyFromColLayoutStruct(*(struct_entry_t*) value, vector);
  } break;
  case PhysicalTypeID::INTERNAL_ID: {
    val.internalIDVal = *((nodeID_t*) value);
  } break;
  default:
    NEUG_UNREACHABLE;
  }
}

void Value::copyValueFrom(const Value& other) {
  if (other.isNull()) {
    isNull_ = true;
    return;
  }
  isNull_ = false;
  NEUG_ASSERT(dataType == other.dataType);
  switch (getPhysicalType(dataType.id())) {
  case PhysicalTypeID::BOOL: {
    val.booleanVal = other.val.booleanVal;
  } break;
  case PhysicalTypeID::INT64: {
    val.int64Val = other.val.int64Val;
  } break;
  case PhysicalTypeID::INT32: {
    val.int32Val = other.val.int32Val;
  } break;
  case PhysicalTypeID::INT16: {
    val.int16Val = other.val.int16Val;
  } break;
  case PhysicalTypeID::INT8: {
    val.int8Val = other.val.int8Val;
  } break;
  case PhysicalTypeID::UINT64: {
    val.uint64Val = other.val.uint64Val;
  } break;
  case PhysicalTypeID::UINT32: {
    val.uint32Val = other.val.uint32Val;
  } break;
  case PhysicalTypeID::UINT16: {
    val.uint16Val = other.val.uint16Val;
  } break;
  case PhysicalTypeID::UINT8: {
    val.uint8Val = other.val.uint8Val;
  } break;
  case PhysicalTypeID::INT128: {
    val.int128Val = other.val.int128Val;
  } break;
  case PhysicalTypeID::DOUBLE: {
    val.doubleVal = other.val.doubleVal;
  } break;
  case PhysicalTypeID::FLOAT: {
    val.floatVal = other.val.floatVal;
  } break;
  case PhysicalTypeID::INTERVAL: {
    val.intervalVal = other.val.intervalVal;
  } break;
  case PhysicalTypeID::INTERNAL_ID: {
    val.internalIDVal = other.val.internalIDVal;
  } break;
  case PhysicalTypeID::STRING: {
    strVal = other.strVal;
  } break;
  case PhysicalTypeID::ARRAY:
  case PhysicalTypeID::LIST:
  case PhysicalTypeID::STRUCT: {
    for (auto& child : other.children) {
      children.push_back(child->copy());
    }
  } break;
  default:
    NEUG_UNREACHABLE;
  }
}

std::string Value::toString() const {
  if (isNull_) {
    return "";
  }
  switch (dataType.id()) {
  case DataTypeId::kBoolean:
    return TypeUtils::toString(val.booleanVal);
  case DataTypeId::kInt64:
    return TypeUtils::toString(val.int64Val);
  case DataTypeId::kInt32:
    return TypeUtils::toString(val.int32Val);
  case DataTypeId::kInt16:
    return TypeUtils::toString(val.int16Val);
  case DataTypeId::kInt8:
    return TypeUtils::toString(val.int8Val);
  case DataTypeId::kUInt64:
    return TypeUtils::toString(val.uint64Val);
  case DataTypeId::kUInt32:
    return TypeUtils::toString(val.uint32Val);
  case DataTypeId::kUInt16:
    return TypeUtils::toString(val.uint16Val);
  case DataTypeId::kUInt8:
    return TypeUtils::toString(val.uint8Val);
  case DataTypeId::kDouble:
    return TypeUtils::toString(val.doubleVal);
  case DataTypeId::kFloat:
    return TypeUtils::toString(val.floatVal);
  case DataTypeId::kDate:
    return TypeUtils::toString(date_t{val.int32Val});
  case DataTypeId::kTimestampMs:
    return TypeUtils::toString(timestamp_ms_t{val.int64Val});
  case DataTypeId::kInterval:
    return TypeUtils::toString(val.intervalVal);
  case DataTypeId::kInternalId:
    return TypeUtils::toString(val.internalIDVal);
  case DataTypeId::kVarchar:
    return strVal;
  case DataTypeId::kMap: {
    return mapToString();
  }
  case DataTypeId::kList:
  case DataTypeId::kArray: {
    return listToString();
  }
  case DataTypeId::kPath:
  case DataTypeId::kStruct: {
    return structToString();
  }
  case DataTypeId::kVertex: {
    return nodeToString();
  }
  case DataTypeId::kEdge: {
    return relToString();
  }
  default:
    NEUG_UNREACHABLE;
  }
}

Value::Value() : isNull_{true}, childrenSize{0} {
  dataType = DataType(DataTypeId::kUnknown);
}

Value::Value(const DataType& dataType_) : isNull_{true}, childrenSize{0} {
  dataType = dataType_.copy();
}

void Value::resizeChildrenVector(uint64_t size, const DataType& childType) {
  if (size > children.size()) {
    children.reserve(size);
    for (auto i = children.size(); i < size; ++i) {
      children.push_back(
          std::make_unique<Value>(createDefaultValue(childType)));
    }
  }
  childrenSize = size;
}

void Value::copyFromRowLayoutList(const neug_list_t& list,
                                  const DataType& childType) {}

void Value::copyFromColLayoutList(const list_entry_t& listEntry,
                                  ValueVector* vec) {
  auto dataVec = ListVector::getDataVector(vec);
  resizeChildrenVector(listEntry.size, dataVec->dataType);
  for (auto i = 0u; i < listEntry.size; i++) {
    auto childValue = children[i].get();
    childValue->setNull(dataVec->isNull(listEntry.offset + i));
    if (!childValue->isNull()) {
      childValue->copyFromColLayout(
          ListVector::getListValuesWithOffset(vec, listEntry, i), dataVec);
    }
  }
}

void Value::copyFromRowLayoutStruct(const uint8_t* kuStruct) {}

void Value::copyFromColLayoutStruct(const struct_entry_t& structEntry,
                                    ValueVector* vec) {
  for (auto i = 0u; i < childrenSize; i++) {
    children[i]->setNull(
        StructVector::getFieldVector(vec, i)->isNull(structEntry.pos));
    if (!children[i]->isNull()) {
      auto fieldVector = StructVector::getFieldVector(vec, i);
      children[i]->copyFromColLayout(
          fieldVector->getData() +
              fieldVector->getNumBytesPerValue() * structEntry.pos,
          fieldVector.get());
    }
  }
}

void Value::serialize(Serializer& serializer) const {
  serializer.serializeValue(static_cast<uint8_t>(dataType.id()));
  serializer.serializeValue(isNull_);
  serializer.serializeValue(childrenSize);

  switch (getPhysicalType(dataType.id())) {
  case PhysicalTypeID::BOOL: {
    serializer.serializeValue(val.booleanVal);
  } break;
  case PhysicalTypeID::INT64: {
    serializer.serializeValue(val.int64Val);
  } break;
  case PhysicalTypeID::INT32: {
    serializer.serializeValue(val.int32Val);
  } break;
  case PhysicalTypeID::INT16: {
    serializer.serializeValue(val.int16Val);
  } break;
  case PhysicalTypeID::INT8: {
    serializer.serializeValue(val.int8Val);
  } break;
  case PhysicalTypeID::UINT64: {
    serializer.serializeValue(val.uint64Val);
  } break;
  case PhysicalTypeID::UINT32: {
    serializer.serializeValue(val.uint32Val);
  } break;
  case PhysicalTypeID::UINT16: {
    serializer.serializeValue(val.uint16Val);
  } break;
  case PhysicalTypeID::UINT8: {
    serializer.serializeValue(val.uint8Val);
  } break;
  case PhysicalTypeID::INT128: {
    serializer.serializeValue(val.int128Val);
  } break;
  case PhysicalTypeID::DOUBLE: {
    serializer.serializeValue(val.doubleVal);
  } break;
  case PhysicalTypeID::FLOAT: {
    serializer.serializeValue(val.floatVal);
  } break;
  case PhysicalTypeID::INTERVAL: {
    serializer.serializeValue(val.intervalVal);
  } break;
  case PhysicalTypeID::INTERNAL_ID: {
    serializer.serializeValue(val.internalIDVal);
  } break;
  case PhysicalTypeID::STRING: {
    serializer.serializeValue(strVal);
  } break;
  case PhysicalTypeID::ARRAY:
  case PhysicalTypeID::LIST:
  case PhysicalTypeID::STRUCT: {
    for (auto i = 0u; i < childrenSize; ++i) {
      children[i]->serialize(serializer);
    }
  } break;
  case PhysicalTypeID::ANY: {
    if (!isNull_) {
      NEUG_UNREACHABLE;
    }
  } break;
  default: {
    NEUG_UNREACHABLE;
  }
  }
}

std::unique_ptr<Value> Value::deserialize(Deserializer& deserializer) {
  uint8_t typeIdVal;
  deserializer.deserializeValue(typeIdVal);
  DataType dataType(static_cast<DataTypeId>(typeIdVal));
  std::unique_ptr<Value> val =
      std::make_unique<Value>(createDefaultValue(dataType));
  deserializer.deserializeValue(val->isNull_);
  deserializer.deserializeValue(val->childrenSize);
  switch (getPhysicalType(dataType.id())) {
  case PhysicalTypeID::BOOL: {
    deserializer.deserializeValue(val->val.booleanVal);
  } break;
  case PhysicalTypeID::INT64: {
    deserializer.deserializeValue(val->val.int64Val);
  } break;
  case PhysicalTypeID::INT32: {
    deserializer.deserializeValue(val->val.int32Val);
  } break;
  case PhysicalTypeID::INT16: {
    deserializer.deserializeValue(val->val.int16Val);
  } break;
  case PhysicalTypeID::INT8: {
    deserializer.deserializeValue(val->val.int8Val);
  } break;
  case PhysicalTypeID::UINT64: {
    deserializer.deserializeValue(val->val.uint64Val);
  } break;
  case PhysicalTypeID::UINT32: {
    deserializer.deserializeValue(val->val.uint32Val);
  } break;
  case PhysicalTypeID::UINT16: {
    deserializer.deserializeValue(val->val.uint16Val);
  } break;
  case PhysicalTypeID::UINT8: {
    deserializer.deserializeValue(val->val.uint8Val);
  } break;
  case PhysicalTypeID::INT128: {
    deserializer.deserializeValue(val->val.int128Val);
  } break;
  case PhysicalTypeID::DOUBLE: {
    deserializer.deserializeValue(val->val.doubleVal);
  } break;
  case PhysicalTypeID::FLOAT: {
    deserializer.deserializeValue(val->val.floatVal);
  } break;
  case PhysicalTypeID::INTERVAL: {
    deserializer.deserializeValue(val->val.intervalVal);
  } break;
  case PhysicalTypeID::INTERNAL_ID: {
    deserializer.deserializeValue(val->val.internalIDVal);
  } break;
  case PhysicalTypeID::STRING: {
    deserializer.deserializeValue(val->strVal);
  } break;
  case PhysicalTypeID::ARRAY:
  case PhysicalTypeID::LIST:
  case PhysicalTypeID::STRUCT: {
    val->children.resize(val->childrenSize);
    for (auto i = 0u; i < val->childrenSize; i++) {
      val->children[i] = deserialize(deserializer);
    }
  } break;
  case PhysicalTypeID::ANY: {
    if (!val->isNull_) {
      NEUG_UNREACHABLE;
    }
  } break;
  default: {
    NEUG_UNREACHABLE;
  }
  }
  return val;
}

void Value::validateType(DataTypeId targetTypeID) const {
  if (dataType.id() == targetTypeID) {
    return;
  }
  THROW_BINDER_EXCEPTION(stringFormat(
      "{} has data type {} but {} was expected.", toString(),
      dataType.ToString(), LogicalTypeUtils::toString(targetTypeID)));
}

bool Value::hasNoneNullChildren() const {
  for (auto i = 0u; i < childrenSize; ++i) {
    if (!children[i]->isNull()) {
      return true;
    }
  }
  return false;
}

bool Value::allowTypeChange() const {
  if (isNull_ || !dataType.isInternalType()) {
    return true;
  }
  switch (dataType.id()) {
  case DataTypeId::kUnknown:
    return true;
  case DataTypeId::kList:
  case DataTypeId::kArray: {
    if (childrenSize == 0) {
      return true;
    }
    for (auto i = 0u; i < childrenSize; ++i) {
      if (children[i]->allowTypeChange()) {
        return true;
      }
    }
    return false;
  }
  case DataTypeId::kMap: {
    if (childrenSize == 0) {
      return true;
    }
    for (auto i = 0u; i < childrenSize; ++i) {
      auto k = children[i]->children[0].get();
      auto v = children[i]->children[1].get();
      if (k->allowTypeChange() || v->allowTypeChange()) {
        return true;
      }
    }
    return false;
  }
  default:
    return false;
  }
}

uint64_t Value::computeHash() const { return 0; }

std::string Value::mapToString() const {
  std::string result = "{";
  for (auto i = 0u; i < childrenSize; ++i) {
    auto structVal = children[i].get();
    result += structVal->children[0]->toString();
    result += "=";
    result += structVal->children[1]->toString();
    result += (i == childrenSize - 1 ? "" : ", ");
  }
  result += "}";
  return result;
}

std::string Value::listToString() const {
  std::string result = "[";
  for (auto i = 0u; i < childrenSize; ++i) {
    result += children[i]->toString();
    if (i != childrenSize - 1) {
      result += ",";
    }
  }
  result += "]";
  return result;
}

std::string Value::structToString() const {
  std::string result = "{";
  auto fieldNames = StructType::GetFieldNames(dataType);
  for (auto i = 0u; i < childrenSize; ++i) {
    result += fieldNames[i] + ": ";
    result += children[i]->toString();
    if (i != childrenSize - 1) {
      result += ", ";
    }
  }
  result += "}";
  return result;
}

std::string Value::nodeToString() const {
  if (children[0]->isNull_) {
    return "";
  }
  std::string result = "{";
  auto fieldNames = StructType::GetFieldNames(dataType);
  for (auto i = 0u; i < childrenSize; ++i) {
    if (children[i]->isNull_) {
      continue;
    }
    if (i != 0) {
      result += ", ";
    }
    result += fieldNames[i] + ": " + children[i]->toString();
  }
  result += "}";
  return result;
}

std::string Value::relToString() const {
  if (children[3]->isNull_) {
    return "";
  }
  std::string result = "(" + children[0]->toString() + ")-{";
  auto fieldNames = StructType::GetFieldNames(dataType);
  for (auto i = 2u; i < childrenSize; ++i) {
    if (children[i]->isNull_) {
      continue;
    }
    if (i != 2) {
      result += ", ";
    }
    result += fieldNames[i] + ": " + children[i]->toString();
  }
  result += "}->(" + children[1]->toString() + ")";
  return result;
}

}  // namespace common
}  // namespace neug
