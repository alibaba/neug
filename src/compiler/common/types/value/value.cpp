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
#include "neug/compiler/common/types/blob.h"
#include "neug/compiler/common/types/neug_string.h"
#include "neug/compiler/common/types/uuid.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace common {

bool Value::operator==(const Value& rhs) const {
  if (dataType != rhs.dataType || isNull_ != rhs.isNull_) {
    return false;
  }
  switch (dataType.getPhysicalType()) {
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
  case PhysicalTypeID::POINTER:
    return val.pointer == rhs.val.pointer;
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

void Value::setDataType(const LogicalType& dataType_) {
  NEUG_ASSERT(allowTypeChange());
  dataType = dataType_.copy();
}

const LogicalType& Value::getDataType() const { return dataType; }

void Value::setNull(bool flag) { isNull_ = flag; }

void Value::setNull() { isNull_ = true; }

bool Value::isNull() const { return isNull_; }

std::unique_ptr<Value> Value::copy() const {
  return std::make_unique<Value>(*this);
}

Value Value::createNullValue() { return {}; }

Value Value::createNullValue(const LogicalType& dataType) {
  return Value(dataType);
}

Value Value::createDefaultValue(const LogicalType& dataType) {
  switch (dataType.getLogicalTypeID()) {
  case LogicalTypeID::SERIAL:
  case LogicalTypeID::INT64:
    return Value((int64_t) 0);
  case LogicalTypeID::INT32:
    return Value((int32_t) 0);
  case LogicalTypeID::INT16:
    return Value((int16_t) 0);
  case LogicalTypeID::INT8:
    return Value((int8_t) 0);
  case LogicalTypeID::UINT64:
    return Value((uint64_t) 0);
  case LogicalTypeID::UINT32:
    return Value((uint32_t) 0);
  case LogicalTypeID::UINT16:
    return Value((uint16_t) 0);
  case LogicalTypeID::UINT8:
    return Value((uint8_t) 0);
  case LogicalTypeID::INT128:
    return Value(int128_t((int32_t) 0));
  case LogicalTypeID::BOOL:
    return Value(true);
  case LogicalTypeID::DOUBLE:
    return Value((double) 0);
  case LogicalTypeID::DATE:
    return Value(date_t());
  case LogicalTypeID::TIMESTAMP_NS:
    return Value(timestamp_ns_t());
  case LogicalTypeID::TIMESTAMP_MS:
    return Value(timestamp_ms_t());
  case LogicalTypeID::TIMESTAMP_SEC:
    return Value(timestamp_sec_t());
  case LogicalTypeID::TIMESTAMP_TZ:
    return Value(timestamp_tz_t());
  case LogicalTypeID::TIMESTAMP:
    return Value(timestamp_t());
  case LogicalTypeID::INTERVAL:
    return Value(interval_t());
  case LogicalTypeID::INTERNAL_ID:
    return Value(nodeID_t());
  case LogicalTypeID::BLOB:
    return Value(LogicalType::BLOB(), std::string(""));
  case LogicalTypeID::UUID:
    return Value(LogicalType::UUID(), std::string(""));
  case LogicalTypeID::STRING:
    return Value(LogicalType::STRING(), std::string(""));
  case LogicalTypeID::FLOAT:
    return Value((float) 0);
  case LogicalTypeID::DECIMAL: {
    Value ret(dataType.copy());
    ret.val.int128Val = 0;
    ret.isNull_ = false;
    ret.childrenSize = 0;
    return ret;
  }
  case LogicalTypeID::ARRAY: {
    std::vector<std::unique_ptr<Value>> children;
    const auto& childType = ArrayType::getChildType(dataType);
    auto arraySize = ArrayType::getNumElements(dataType);
    children.reserve(arraySize);
    for (auto i = 0u; i < arraySize; ++i) {
      children.push_back(
          std::make_unique<Value>(createDefaultValue(childType)));
    }
    return Value(dataType.copy(), std::move(children));
  }
  case LogicalTypeID::MAP:
  case LogicalTypeID::LIST: {
    return Value(dataType.copy(), std::vector<std::unique_ptr<Value>>{});
  }
  case LogicalTypeID::UNION: {
    std::vector<std::unique_ptr<Value>> children;
    children.push_back(std::make_unique<Value>(createNullValue()));
    return Value(dataType.copy(), std::move(children));
  }
  case LogicalTypeID::NODE:
  case LogicalTypeID::REL:
  case LogicalTypeID::RECURSIVE_REL:
  case LogicalTypeID::STRUCT: {
    std::vector<std::unique_ptr<Value>> children;
    for (auto& field : StructType::getFields(dataType)) {
      children.push_back(
          std::make_unique<Value>(createDefaultValue(field.getType())));
    }
    return Value(dataType.copy(), std::move(children));
  }
  case LogicalTypeID::ANY: {
    return createNullValue();
  }
  default:
    NEUG_UNREACHABLE;
  }
}

Value::Value(bool val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::BOOL();
  val.booleanVal = val_;
}

Value::Value(int8_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::INT8();
  val.int8Val = val_;
}

Value::Value(int16_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::INT16();
  val.int16Val = val_;
}

Value::Value(int32_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::INT32();
  val.int32Val = val_;
}

Value::Value(int64_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::INT64();
  val.int64Val = val_;
}

Value::Value(uint8_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::UINT8();
  val.uint8Val = val_;
}

Value::Value(uint16_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::UINT16();
  val.uint16Val = val_;
}

Value::Value(uint32_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::UINT32();
  val.uint32Val = val_;
}

Value::Value(uint64_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::UINT64();
  val.uint64Val = val_;
}

Value::Value(int128_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::INT128();
  val.int128Val = val_;
}

Value::Value(neug_uuid_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::UUID();
  val.int128Val = val_.value;
}

Value::Value(float val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::FLOAT();
  val.floatVal = val_;
}

Value::Value(double val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::DOUBLE();
  val.doubleVal = val_;
}

Value::Value(date_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::DATE();
  val.int32Val = val_.days;
}

Value::Value(timestamp_ns_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::TIMESTAMP_NS();
  val.int64Val = val_.value;
}

Value::Value(timestamp_ms_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::TIMESTAMP_MS();
  val.int64Val = val_.value;
}

Value::Value(timestamp_sec_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::TIMESTAMP_SEC();
  val.int64Val = val_.value;
}

Value::Value(timestamp_tz_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::TIMESTAMP_TZ();
  val.int64Val = val_.value;
}

Value::Value(timestamp_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::TIMESTAMP();
  val.int64Val = val_.value;
}

Value::Value(interval_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::INTERVAL();
  val.intervalVal = val_;
}

Value::Value(internalID_t val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::INTERNAL_ID();
  val.internalIDVal = val_;
}

Value::Value(const char* val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::STRING();
  strVal = std::string(val_);
}

Value::Value(const std::string& val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::STRING();
  strVal = val_;
}

Value::Value(uint8_t* val_) : isNull_{false}, childrenSize{0} {
  dataType = LogicalType::POINTER();
  val.pointer = val_;
}

Value::Value(LogicalType type, std::string val_)
    : dataType{std::move(type)}, isNull_{false}, childrenSize{0} {
  strVal = std::move(val_);
}

Value::Value(LogicalType dataType_,
             std::vector<std::unique_ptr<Value>> children)
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
  switch (dataType.getLogicalTypeID()) {
  case LogicalTypeID::SERIAL:
  case LogicalTypeID::TIMESTAMP_NS:
  case LogicalTypeID::TIMESTAMP_MS:
  case LogicalTypeID::TIMESTAMP_SEC:
  case LogicalTypeID::TIMESTAMP_TZ:
  case LogicalTypeID::TIMESTAMP:
  case LogicalTypeID::INT64: {
    val.int64Val = *((int64_t*) value);
  } break;
  case LogicalTypeID::DATE:
  case LogicalTypeID::INT32: {
    val.int32Val = *((int32_t*) value);
  } break;
  case LogicalTypeID::INT16: {
    val.int16Val = *((int16_t*) value);
  } break;
  case LogicalTypeID::INT8: {
    val.int8Val = *((int8_t*) value);
  } break;
  case LogicalTypeID::UINT64: {
    val.uint64Val = *((uint64_t*) value);
  } break;
  case LogicalTypeID::UINT32: {
    val.uint32Val = *((uint32_t*) value);
  } break;
  case LogicalTypeID::UINT16: {
    val.uint16Val = *((uint16_t*) value);
  } break;
  case LogicalTypeID::UINT8: {
    val.uint8Val = *((uint8_t*) value);
  } break;
  case LogicalTypeID::INT128: {
    val.int128Val = *((int128_t*) value);
  } break;
  case LogicalTypeID::BOOL: {
    val.booleanVal = *((bool*) value);
  } break;
  case LogicalTypeID::DOUBLE: {
    val.doubleVal = *((double*) value);
  } break;
  case LogicalTypeID::FLOAT: {
    val.floatVal = *((float*) value);
  } break;
  case LogicalTypeID::DECIMAL: {
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::INT16:
      val.int16Val = (*(int16_t*) value);
      break;
    case PhysicalTypeID::INT32:
      val.int32Val = (*(int32_t*) value);
      break;
    case PhysicalTypeID::INT64:
      val.int64Val = (*(int64_t*) value);
      break;
    case PhysicalTypeID::INT128:
      val.int128Val = (*(int128_t*) value);
      break;
    default:
      NEUG_UNREACHABLE;
    }
  } break;
  case LogicalTypeID::INTERVAL: {
    val.intervalVal = *((interval_t*) value);
  } break;
  case LogicalTypeID::INTERNAL_ID: {
    val.internalIDVal = *((nodeID_t*) value);
  } break;
  case LogicalTypeID::BLOB: {
    strVal = ((blob_t*) value)->value.getAsString();
  } break;
  case LogicalTypeID::UUID: {
    val.int128Val = ((neug_uuid_t*) value)->value;
    strVal = UUID::toString(*((neug_uuid_t*) value));
  } break;
  case LogicalTypeID::STRING: {
    strVal = ((neug_string_t*) value)->getAsString();
  } break;
  case LogicalTypeID::MAP:
  case LogicalTypeID::LIST: {
    copyFromRowLayoutList(*(neug_list_t*) value,
                          ListType::getChildType(dataType));
  } break;
  case LogicalTypeID::ARRAY: {
    copyFromRowLayoutList(*(neug_list_t*) value,
                          ArrayType::getChildType(dataType));
  } break;
  case LogicalTypeID::UNION: {
    copyFromUnion(value);
  } break;
  case LogicalTypeID::NODE:
  case LogicalTypeID::REL:
  case LogicalTypeID::RECURSIVE_REL:
  case LogicalTypeID::STRUCT: {
    copyFromRowLayoutStruct(value);
  } break;
  case LogicalTypeID::POINTER: {
    val.pointer = *((uint8_t**) value);
  } break;
  default:
    NEUG_UNREACHABLE;
  }
}

void Value::copyFromColLayout(const uint8_t* value, ValueVector* vector) {
  switch (dataType.getPhysicalType()) {
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
  switch (dataType.getPhysicalType()) {
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
  case PhysicalTypeID::POINTER: {
    val.pointer = other.val.pointer;
  } break;
  default:
    NEUG_UNREACHABLE;
  }
}

std::string Value::toString() const {
  if (isNull_) {
    return "";
  }
  switch (dataType.getLogicalTypeID()) {
  case LogicalTypeID::BOOL:
    return TypeUtils::toString(val.booleanVal);
  case LogicalTypeID::SERIAL:
  case LogicalTypeID::INT64:
    return TypeUtils::toString(val.int64Val);
  case LogicalTypeID::INT32:
    return TypeUtils::toString(val.int32Val);
  case LogicalTypeID::INT16:
    return TypeUtils::toString(val.int16Val);
  case LogicalTypeID::INT8:
    return TypeUtils::toString(val.int8Val);
  case LogicalTypeID::UINT64:
    return TypeUtils::toString(val.uint64Val);
  case LogicalTypeID::UINT32:
    return TypeUtils::toString(val.uint32Val);
  case LogicalTypeID::UINT16:
    return TypeUtils::toString(val.uint16Val);
  case LogicalTypeID::UINT8:
    return TypeUtils::toString(val.uint8Val);
  case LogicalTypeID::INT128:
    return TypeUtils::toString(val.int128Val);
  case LogicalTypeID::DOUBLE:
    return TypeUtils::toString(val.doubleVal);
  case LogicalTypeID::FLOAT:
    return TypeUtils::toString(val.floatVal);
  case LogicalTypeID::DECIMAL:
    return decimalToString();
  case LogicalTypeID::POINTER:
    return TypeUtils::toString((uint64_t) val.pointer);
  case LogicalTypeID::DATE:
    return TypeUtils::toString(date_t{val.int32Val});
  case LogicalTypeID::TIMESTAMP_NS:
    return TypeUtils::toString(timestamp_ns_t{val.int64Val});
  case LogicalTypeID::TIMESTAMP_MS:
    return TypeUtils::toString(timestamp_ms_t{val.int64Val});
  case LogicalTypeID::TIMESTAMP_SEC:
    return TypeUtils::toString(timestamp_sec_t{val.int64Val});
  case LogicalTypeID::TIMESTAMP_TZ:
    return TypeUtils::toString(timestamp_tz_t{val.int64Val});
  case LogicalTypeID::TIMESTAMP:
    return TypeUtils::toString(timestamp_t{val.int64Val});
  case LogicalTypeID::INTERVAL:
    return TypeUtils::toString(val.intervalVal);
  case LogicalTypeID::INTERNAL_ID:
    return TypeUtils::toString(val.internalIDVal);
  case LogicalTypeID::BLOB:
    return Blob::toString(reinterpret_cast<const uint8_t*>(strVal.c_str()),
                          strVal.length());
  case LogicalTypeID::UUID:
    return UUID::toString(val.int128Val);
  case LogicalTypeID::STRING:
    return strVal;
  case LogicalTypeID::MAP: {
    return mapToString();
  }
  case LogicalTypeID::LIST:
  case LogicalTypeID::ARRAY: {
    return listToString();
  }
  case LogicalTypeID::UNION: {
    return children[0]->toString();
  }
  case LogicalTypeID::RECURSIVE_REL:
  case LogicalTypeID::STRUCT: {
    return structToString();
  }
  case LogicalTypeID::NODE: {
    return nodeToString();
  }
  case LogicalTypeID::REL: {
    return relToString();
  }
  default:
    NEUG_UNREACHABLE;
  }
}

Value::Value() : isNull_{true}, childrenSize{0} {
  dataType = LogicalType(LogicalTypeID::ANY);
}

Value::Value(const LogicalType& dataType_) : isNull_{true}, childrenSize{0} {
  dataType = dataType_.copy();
}

void Value::resizeChildrenVector(uint64_t size, const LogicalType& childType) {
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
                                  const LogicalType& childType) {}

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

void Value::copyFromUnion(const uint8_t* kuUnion) {}

void Value::serialize(Serializer& serializer) const {
  dataType.serialize(serializer);
  serializer.serializeValue(isNull_);
  serializer.serializeValue(childrenSize);

  switch (dataType.getPhysicalType()) {
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
  LogicalType dataType = LogicalType::deserialize(deserializer);
  std::unique_ptr<Value> val =
      std::make_unique<Value>(createDefaultValue(dataType));
  deserializer.deserializeValue(val->isNull_);
  deserializer.deserializeValue(val->childrenSize);
  switch (dataType.getPhysicalType()) {
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

void Value::validateType(LogicalTypeID targetTypeID) const {
  if (dataType.getLogicalTypeID() == targetTypeID) {
    return;
  }
  THROW_BINDER_EXCEPTION(stringFormat(
      "{} has data type {} but {} was expected.", toString(),
      dataType.toString(), LogicalTypeUtils::toString(targetTypeID)));
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
  switch (dataType.getLogicalTypeID()) {
  case LogicalTypeID::ANY:
    return true;
  case LogicalTypeID::LIST:
  case LogicalTypeID::ARRAY: {
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
  case LogicalTypeID::MAP: {
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
  auto fieldNames = StructType::getFieldNames(dataType);
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
  auto fieldNames = StructType::getFieldNames(dataType);
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
  auto fieldNames = StructType::getFieldNames(dataType);
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

std::string Value::decimalToString() const {
  switch (dataType.getPhysicalType()) {
  case PhysicalTypeID::INT16:
    return DecimalType::insertDecimalPoint(TypeUtils::toString(val.int16Val),
                                           DecimalType::getScale(dataType));
  case PhysicalTypeID::INT32:
    return DecimalType::insertDecimalPoint(TypeUtils::toString(val.int32Val),
                                           DecimalType::getScale(dataType));
  case PhysicalTypeID::INT64:
    return DecimalType::insertDecimalPoint(TypeUtils::toString(val.int64Val),
                                           DecimalType::getScale(dataType));
  case PhysicalTypeID::INT128:
    return DecimalType::insertDecimalPoint(TypeUtils::toString(val.int128Val),
                                           DecimalType::getScale(dataType));
  default:
    NEUG_UNREACHABLE;
  }
}

}  // namespace common
}  // namespace neug
