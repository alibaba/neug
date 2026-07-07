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

#include "neug/compiler/common/arrow/arrow_row_batch.h"

#include <cstring>

#include "neug/compiler/common/types/value/node.h"
#include "neug/compiler/common/types/value/rel.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace common {

static void resizeVector(ArrowVector* vector, const DataType& type,
                         int64_t capacity);

ArrowRowBatch::ArrowRowBatch(std::vector<DataType> types, std::int64_t capacity)
    : types{std::move(types)}, numTuples{0} {
  auto numVectors = this->types.size();
  vectors.resize(numVectors);
  for (auto i = 0u; i < numVectors; i++) {
    vectors[i] = std::make_unique<ArrowVector>();
    resizeVector(vectors[i].get(), this->types[i], capacity);
  }
}

static uint64_t getArrowMainBufferSize(const DataType& type,
                                       uint64_t capacity) {
  switch (type.id()) {
  case DataTypeId::kBoolean:
    return getNumBytesForBits(capacity);
  case DataTypeId::kTimestampMs:
  case DataTypeId::kInterval:
  case DataTypeId::kUInt64:
  case DataTypeId::kInt64:
    return sizeof(int64_t) * capacity;
  case DataTypeId::kDate:
  case DataTypeId::kUInt32:
  case DataTypeId::kInt32:
    return sizeof(int32_t) * capacity;
  case DataTypeId::kUInt16:
  case DataTypeId::kInt16:
    return sizeof(int16_t) * capacity;
  case DataTypeId::kUInt8:
  case DataTypeId::kInt8:
    return sizeof(int8_t) * capacity;
  case DataTypeId::kDouble:
    return sizeof(double) * capacity;
  case DataTypeId::kFloat:
    return sizeof(float) * capacity;
  case DataTypeId::kVarchar:
  case DataTypeId::kList:
  case DataTypeId::kMap:
    return sizeof(int32_t) * (capacity + 1);
  case DataTypeId::kArray:
  case DataTypeId::kStruct:
  case DataTypeId::kInternalId:
  case DataTypeId::kPath:
  case DataTypeId::kVertex:
  case DataTypeId::kEdge:
    return 0;
  default:
    NEUG_UNREACHABLE;
  }
}

static void resizeValidityBuffer(ArrowVector* vector, int64_t capacity) {
  vector->validity.resize(getNumBytesForBits(capacity), 0xFF);
}

static void resizeMainBuffer(ArrowVector* vector, const DataType& type,
                             int64_t capacity) {
  vector->data.resize(getArrowMainBufferSize(type, capacity));
}

static void resizeBLOBOverflow(ArrowVector* vector, int64_t capacity) {
  vector->overflow.resize(capacity);
}

static void resizeChildVectors(ArrowVector* vector,
                               const std::vector<DataType>& childTypes,
                               int64_t childCapacity) {
  for (auto i = 0u; i < childTypes.size(); i++) {
    if (i >= vector->childData.size()) {
      vector->childData.push_back(std::make_unique<ArrowVector>());
    }
    resizeVector(vector->childData[i].get(), childTypes[i], childCapacity);
  }
}

static void resizeGeneric(ArrowVector* vector, const DataType& type,
                          int64_t capacity) {
  if (vector->capacity >= capacity) {
    return;
  }
  while (vector->capacity < capacity) {
    if (vector->capacity == 0) {
      vector->capacity = 1;
    } else {
      vector->capacity *= 2;
    }
  }
  resizeValidityBuffer(vector, vector->capacity);
  resizeMainBuffer(vector, type, vector->capacity);
}

static void resizeBLOBVector(ArrowVector* vector, const DataType& type,
                             int64_t capacity, int64_t overflowCapacity) {
  resizeGeneric(vector, type, capacity);
  resizeBLOBOverflow(vector, overflowCapacity);
}

static void resizeFixedListVector(ArrowVector* vector, const DataType& type,
                                  int64_t capacity) {
  resizeGeneric(vector, type, capacity);
  std::vector<DataType> typeVec;
  typeVec.push_back(ArrayType::GetChildType(type).copy());
  resizeChildVectors(vector, typeVec,
                     vector->capacity * ArrayType::GetNumElements(type));
}

static void resizeListVector(ArrowVector* vector, const DataType& type,
                             int64_t capacity, int64_t childCapacity) {
  resizeGeneric(vector, type, capacity);
  std::vector<DataType> typeVec;
  typeVec.push_back(ListType::GetChildType(type).copy());
  resizeChildVectors(vector, typeVec, childCapacity);
}

static void resizeStructVector(ArrowVector* vector, const DataType& type,
                               int64_t capacity) {
  resizeGeneric(vector, type, capacity);
  std::vector<DataType> typeVec;
  for (const auto& i : StructType::GetChildTypes(type)) {
    typeVec.push_back(i.copy());
  }
  resizeChildVectors(vector, typeVec, vector->capacity);
}

static void resizeInternalIDVector(ArrowVector* vector, const DataType& type,
                                   int64_t capacity) {
  resizeGeneric(vector, type, capacity);
  std::vector<DataType> typeVec;
  typeVec.push_back(DataType(DataTypeId::kInt64));
  typeVec.push_back(DataType(DataTypeId::kInt64));
  resizeChildVectors(vector, typeVec, vector->capacity);
}

static void resizeVector(ArrowVector* vector, const DataType& type,
                         std::int64_t capacity) {
  auto result = std::make_unique<ArrowVector>();
  switch (type.id()) {
  case DataTypeId::kBoolean:
  case DataTypeId::kInt64:
  case DataTypeId::kInt32:
  case DataTypeId::kInt16:
  case DataTypeId::kInt8:
  case DataTypeId::kUInt64:
  case DataTypeId::kUInt32:
  case DataTypeId::kUInt16:
  case DataTypeId::kUInt8:
  case DataTypeId::kDouble:
  case DataTypeId::kFloat:
  case DataTypeId::kDate:
  case DataTypeId::kTimestampMs:
  case DataTypeId::kInterval:
    return resizeGeneric(vector, type, capacity);
  case DataTypeId::kVarchar:
    return resizeBLOBVector(vector, type, capacity, capacity);
  case DataTypeId::kList:
  case DataTypeId::kMap:
    return resizeListVector(vector, type, capacity, capacity);
  case DataTypeId::kArray:
    return resizeFixedListVector(vector, type, capacity);
  case DataTypeId::kPath:
  case DataTypeId::kVertex:
  case DataTypeId::kEdge:
  case DataTypeId::kStruct:
    return resizeStructVector(vector, type, capacity);
  case DataTypeId::kInternalId:
    return resizeInternalIDVector(vector, type, capacity);
  default: {
    THROW_RUNTIME_ERROR(common::stringFormat(
        "Unsupported type: {} for arrow conversion.", type.ToString()));
  }
  }
}

static void getBitPosition(std::int64_t pos, std::int64_t& bytePos,
                           std::int64_t& bitOffset) {
  bytePos = pos >> 3;
  bitOffset = pos - (bytePos << 3);
}

static void setBitToZero(std::uint8_t* data, std::int64_t pos) {
  std::int64_t bytePos = 0, bitOffset = 0;
  getBitPosition(pos, bytePos, bitOffset);
  data[bytePos] &= ~((std::uint64_t) 1 << bitOffset);
}

static void setBitToOne(std::uint8_t* data, std::int64_t pos) {
  std::int64_t bytePos = 0, bitOffset = 0;
  getBitPosition(pos, bytePos, bitOffset);
  data[bytePos] |= ((std::uint64_t) 1 << bitOffset);
}

void ArrowRowBatch::appendValue(ArrowVector* vector, const DataType& type,
                                Value* value) {
  if (value->isNull()) {
    copyNullValue(vector, value, vector->numValues);
  } else {
    copyNonNullValue(vector, type, value, vector->numValues);
  }
  vector->numValues++;
}

template <DataTypeId DT>
void ArrowRowBatch::templateCopyNonNullValue(ArrowVector* vector,
                                             const DataType& /*type*/,
                                             Value* value, std::int64_t pos) {}

template <>
void ArrowRowBatch::templateCopyNonNullValue<DataTypeId::kInterval>(
    ArrowVector* vector, const DataType& /*type*/, Value* value,
    std::int64_t pos) {
  auto destAddr = (int64_t*) (vector->data.data() + pos * sizeof(std::int64_t));
  auto intervalVal = value->val.intervalVal;
  *destAddr = intervalVal.micros +
              intervalVal.days * compiler_impl::Interval::MICROS_PER_DAY +
              intervalVal.months * compiler_impl::Interval::MICROS_PER_MONTH;
}

template <>
void ArrowRowBatch::templateCopyNonNullValue<DataTypeId::kBoolean>(
    ArrowVector* vector, const DataType& /*type*/, Value* value,
    std::int64_t pos) {
  if (value->val.booleanVal) {
    setBitToOne(vector->data.data(), pos);
  } else {
    setBitToZero(vector->data.data(), pos);
  }
}

template <>
void ArrowRowBatch::templateCopyNonNullValue<DataTypeId::kVarchar>(
    ArrowVector* vector, const DataType& /*type*/, Value* value,
    std::int64_t pos) {
  auto offsets = (std::uint32_t*) vector->data.data();
  auto strLength = value->strVal.length();
  if (pos == 0) {
    offsets[pos] = 0;
  }
  offsets[pos + 1] = offsets[pos] + strLength;
  vector->overflow.resize(offsets[pos + 1] + 1);
  std::memcpy(vector->overflow.data() + offsets[pos], value->strVal.data(),
              strLength);
}

template <>
void ArrowRowBatch::templateCopyNonNullValue<DataTypeId::kList>(
    ArrowVector* vector, const DataType& type, Value* value, std::int64_t pos) {
  auto offsets = (std::uint32_t*) vector->data.data();
  auto numElements = value->childrenSize;
  if (pos == 0) {
    offsets[pos] = 0;
  }
  offsets[pos + 1] = offsets[pos] + numElements;
  std::vector<DataType> typeVec;
  typeVec.push_back(ListType::GetChildType(type).copy());
  resizeChildVectors(vector, typeVec, offsets[pos + 1] + 1);
  for (auto i = 0u; i < numElements; i++) {
    appendValue(vector->childData[0].get(), ListType::GetChildType(type),
                value->children[i].get());
  }
}

template <>
void ArrowRowBatch::templateCopyNonNullValue<DataTypeId::kArray>(
    ArrowVector* vector, const DataType& type, Value* value,
    std::int64_t /*pos*/) {
  auto numElements = value->childrenSize;
  for (auto i = 0u; i < numElements; i++) {
    appendValue(vector->childData[0].get(), ArrayType::GetChildType(type),
                value->children[i].get());
  }
}

template <>
void ArrowRowBatch::templateCopyNonNullValue<DataTypeId::kMap>(
    ArrowVector* vector, const DataType& type, Value* value, std::int64_t pos) {
  return templateCopyNonNullValue<DataTypeId::kList>(vector, type, value, pos);
}

template <>
void ArrowRowBatch::templateCopyNonNullValue<DataTypeId::kStruct>(
    ArrowVector* vector, const DataType& type, Value* value,
    std::int64_t /*pos*/) {
  for (auto i = 0u; i < value->childrenSize; i++) {
    appendValue(vector->childData[i].get(), StructType::GetChildType(type, i),
                value->children[i].get());
  }
}

template <>
void ArrowRowBatch::templateCopyNonNullValue<DataTypeId::kInternalId>(
    ArrowVector* vector, const DataType& /*type*/, Value* value,
    std::int64_t /*pos*/) {
  auto nodeID = value->getValue<nodeID_t>();
  Value offsetVal((std::int64_t) nodeID.offset);
  Value tableIDVal((std::int64_t) nodeID.tableID);
  appendValue(vector->childData[0].get(), DataType(DataTypeId::kInt64),
              &offsetVal);
  appendValue(vector->childData[1].get(), DataType(DataTypeId::kInt64),
              &tableIDVal);
}

template <>
void ArrowRowBatch::templateCopyNonNullValue<DataTypeId::kVertex>(
    ArrowVector* vector, const DataType& type, Value* value,
    std::int64_t /*pos*/) {
  appendValue(vector->childData[0].get(), StructType::GetChildType(type, 0),
              NodeVal::getNodeIDVal(value));
  appendValue(vector->childData[1].get(), StructType::GetChildType(type, 1),
              NodeVal::getLabelVal(value));
  std::int64_t propertyId = 2;
  auto numProperties = NodeVal::getNumProperties(value);
  for (auto i = 0u; i < numProperties; i++) {
    auto val = NodeVal::getPropertyVal(value, i);
    appendValue(vector->childData[propertyId].get(),
                StructType::GetChildType(type, propertyId), val);
    propertyId++;
  }
}

template <>
void ArrowRowBatch::templateCopyNonNullValue<DataTypeId::kEdge>(
    ArrowVector* vector, const DataType& type, Value* value,
    std::int64_t /*pos*/) {
  appendValue(vector->childData[0].get(), StructType::GetChildType(type, 0),
              RelVal::getSrcNodeIDVal(value));
  appendValue(vector->childData[1].get(), StructType::GetChildType(type, 1),
              RelVal::getDstNodeIDVal(value));
  appendValue(vector->childData[2].get(), StructType::GetChildType(type, 2),
              RelVal::getLabelVal(value));
  appendValue(vector->childData[3].get(), StructType::GetChildType(type, 3),
              RelVal::getIDVal(value));
  common::property_id_t propertyID = 4;
  auto numProperties = RelVal::getNumProperties(value);
  for (auto i = 0u; i < numProperties; i++) {
    auto val = RelVal::getPropertyVal(value, i);
    appendValue(vector->childData[propertyID].get(),
                StructType::GetChildType(type, propertyID), val);
    propertyID++;
  }
}

void ArrowRowBatch::copyNonNullValue(ArrowVector* vector, const DataType& type,
                                     Value* value, std::int64_t pos) {
  switch (type.id()) {
  case DataTypeId::kBoolean: {
    templateCopyNonNullValue<DataTypeId::kBoolean>(vector, type, value, pos);
  } break;
  case DataTypeId::kInt64: {
    templateCopyNonNullValue<DataTypeId::kInt64>(vector, type, value, pos);
  } break;
  case DataTypeId::kInt32: {
    templateCopyNonNullValue<DataTypeId::kInt32>(vector, type, value, pos);
  } break;
  case DataTypeId::kInt16: {
    templateCopyNonNullValue<DataTypeId::kInt16>(vector, type, value, pos);
  } break;
  case DataTypeId::kInt8: {
    templateCopyNonNullValue<DataTypeId::kInt8>(vector, type, value, pos);
  } break;
  case DataTypeId::kUInt64: {
    templateCopyNonNullValue<DataTypeId::kUInt64>(vector, type, value, pos);
  } break;
  case DataTypeId::kUInt32: {
    templateCopyNonNullValue<DataTypeId::kUInt32>(vector, type, value, pos);
  } break;
  case DataTypeId::kUInt16: {
    templateCopyNonNullValue<DataTypeId::kUInt16>(vector, type, value, pos);
  } break;
  case DataTypeId::kUInt8: {
    templateCopyNonNullValue<DataTypeId::kUInt8>(vector, type, value, pos);
  } break;
  case DataTypeId::kDouble: {
    templateCopyNonNullValue<DataTypeId::kDouble>(vector, type, value, pos);
  } break;
  case DataTypeId::kFloat: {
    templateCopyNonNullValue<DataTypeId::kFloat>(vector, type, value, pos);
  } break;
  case DataTypeId::kDate: {
    templateCopyNonNullValue<DataTypeId::kDate>(vector, type, value, pos);
  } break;
  case DataTypeId::kTimestampMs: {
    templateCopyNonNullValue<DataTypeId::kTimestampMs>(vector, type, value,
                                                       pos);
  } break;
  case DataTypeId::kInterval: {
    templateCopyNonNullValue<DataTypeId::kInterval>(vector, type, value, pos);
  } break;
  case DataTypeId::kVarchar: {
    templateCopyNonNullValue<DataTypeId::kVarchar>(vector, type, value, pos);
  } break;
  case DataTypeId::kList: {
    templateCopyNonNullValue<DataTypeId::kList>(vector, type, value, pos);
  } break;
  case DataTypeId::kArray: {
    templateCopyNonNullValue<DataTypeId::kArray>(vector, type, value, pos);
  } break;
  case DataTypeId::kMap: {
    templateCopyNonNullValue<DataTypeId::kMap>(vector, type, value, pos);
  } break;
  case DataTypeId::kPath:
  case DataTypeId::kStruct: {
    templateCopyNonNullValue<DataTypeId::kStruct>(vector, type, value, pos);
  } break;
  case DataTypeId::kInternalId: {
    templateCopyNonNullValue<DataTypeId::kInternalId>(vector, type, value, pos);
  } break;
  case DataTypeId::kVertex: {
    templateCopyNonNullValue<DataTypeId::kVertex>(vector, type, value, pos);
  } break;
  case DataTypeId::kEdge: {
    templateCopyNonNullValue<DataTypeId::kEdge>(vector, type, value, pos);
  } break;
  default: {
    NEUG_UNREACHABLE;
  }
  }
}

template <DataTypeId DT>
void ArrowRowBatch::templateCopyNullValue(ArrowVector* vector,
                                          std::int64_t pos) {
  setBitToZero(vector->validity.data(), pos);
  vector->numNulls++;
}

template <>
void ArrowRowBatch::templateCopyNullValue<DataTypeId::kVarchar>(
    ArrowVector* vector, std::int64_t pos) {
  auto offsets = (std::uint32_t*) vector->data.data();
  if (pos == 0) {
    offsets[pos] = 0;
  }
  offsets[pos + 1] = offsets[pos];
  setBitToZero(vector->validity.data(), pos);
  vector->numNulls++;
}

template <>
void ArrowRowBatch::templateCopyNullValue<DataTypeId::kList>(
    ArrowVector* vector, std::int64_t pos) {
  auto offsets = (std::uint32_t*) vector->data.data();
  if (pos == 0) {
    offsets[pos] = 0;
  }
  offsets[pos + 1] = offsets[pos];
  setBitToZero(vector->validity.data(), pos);
  vector->numNulls++;
}

template <>
void ArrowRowBatch::templateCopyNullValue<DataTypeId::kMap>(ArrowVector* vector,
                                                            std::int64_t pos) {
  return templateCopyNullValue<DataTypeId::kList>(vector, pos);
}

template <>
void ArrowRowBatch::templateCopyNullValue<DataTypeId::kStruct>(
    ArrowVector* vector, std::int64_t pos) {
  setBitToZero(vector->validity.data(), pos);
  vector->numNulls++;
}

static void copyArrowArray(ArrowVector* vector, std::int64_t pos,
                           uint64_t numElements) {
  setBitToZero(vector->validity.data(), pos);
  vector->numNulls++;
  auto& child = vector->childData[0];
  child->numValues += numElements;
}

void ArrowRowBatch::copyNullValue(ArrowVector* vector, Value* value,
                                  std::int64_t pos) {
  switch (value->dataType.id()) {
  case DataTypeId::kBoolean: {
    templateCopyNullValue<DataTypeId::kBoolean>(vector, pos);
  } break;
  case DataTypeId::kInt64: {
    templateCopyNullValue<DataTypeId::kInt64>(vector, pos);
  } break;
  case DataTypeId::kInt32: {
    templateCopyNullValue<DataTypeId::kInt32>(vector, pos);
  } break;
  case DataTypeId::kInt16: {
    templateCopyNullValue<DataTypeId::kInt16>(vector, pos);
  } break;
  case DataTypeId::kInt8: {
    templateCopyNullValue<DataTypeId::kInt8>(vector, pos);
  } break;
  case DataTypeId::kUInt64: {
    templateCopyNullValue<DataTypeId::kUInt64>(vector, pos);
  } break;
  case DataTypeId::kUInt32: {
    templateCopyNullValue<DataTypeId::kUInt32>(vector, pos);
  } break;
  case DataTypeId::kUInt16: {
    templateCopyNullValue<DataTypeId::kUInt16>(vector, pos);
  } break;
  case DataTypeId::kUInt8: {
    templateCopyNullValue<DataTypeId::kUInt8>(vector, pos);
  } break;
  case DataTypeId::kDouble: {
    templateCopyNullValue<DataTypeId::kDouble>(vector, pos);
  } break;
  case DataTypeId::kFloat: {
    templateCopyNullValue<DataTypeId::kFloat>(vector, pos);
  } break;
  case DataTypeId::kDate: {
    templateCopyNullValue<DataTypeId::kDate>(vector, pos);
  } break;
  case DataTypeId::kTimestampMs: {
    templateCopyNullValue<DataTypeId::kTimestampMs>(vector, pos);
  } break;
  case DataTypeId::kInterval: {
    templateCopyNullValue<DataTypeId::kInterval>(vector, pos);
  } break;
  case DataTypeId::kVarchar: {
    templateCopyNullValue<DataTypeId::kVarchar>(vector, pos);
  } break;
  case DataTypeId::kList: {
    templateCopyNullValue<DataTypeId::kList>(vector, pos);
  } break;
  case DataTypeId::kArray: {
    copyArrowArray(vector, pos, ArrayType::GetNumElements(value->dataType));
  } break;
  case DataTypeId::kMap: {
    templateCopyNullValue<DataTypeId::kMap>(vector, pos);
  } break;
  case DataTypeId::kInternalId: {
    templateCopyNullValue<DataTypeId::kInternalId>(vector, pos);
  } break;
  case DataTypeId::kPath:
  case DataTypeId::kStruct: {
    templateCopyNullValue<DataTypeId::kStruct>(vector, pos);
  } break;
  case DataTypeId::kVertex: {
    templateCopyNullValue<DataTypeId::kVertex>(vector, pos);
  } break;
  case DataTypeId::kEdge: {
    templateCopyNullValue<DataTypeId::kEdge>(vector, pos);
  } break;
  default: {
    NEUG_UNREACHABLE;
  }
  }
}

static void releaseArrowVector(ArrowArray* array) {
  if (!array || !array->release) {
    return;
  }
  array->release = nullptr;
  auto holder = static_cast<ArrowVector*>(array->private_data);
  delete holder;
}

static std::unique_ptr<ArrowArray> createArrayFromVector(ArrowVector& vector) {
  auto result = std::make_unique<ArrowArray>();
  result->private_data = nullptr;
  result->release = releaseArrowVector;
  result->n_children = 0;
  result->offset = 0;
  result->dictionary = nullptr;
  result->buffers = vector.buffers.data();
  result->null_count = vector.numNulls;
  result->length = vector.numValues;
  result->n_buffers = 1;
  result->buffers[0] = vector.validity.data();
  if (vector.data.data() != nullptr) {
    result->n_buffers++;
    result->buffers[1] = vector.data.data();
  }
  return result;
}

template <DataTypeId DT>
ArrowArray* ArrowRowBatch::templateCreateArray(ArrowVector& vector,
                                               const DataType& /*type*/) {
  auto result = createArrayFromVector(vector);
  vector.array = std::move(result);
  return vector.array.get();
}

template <>
ArrowArray* ArrowRowBatch::templateCreateArray<DataTypeId::kVarchar>(
    ArrowVector& vector, const DataType& /*type*/) {
  auto result = createArrayFromVector(vector);
  result->n_buffers = 3;
  result->buffers[2] = vector.overflow.data();
  vector.array = std::move(result);
  return vector.array.get();
}

template <>
ArrowArray* ArrowRowBatch::templateCreateArray<DataTypeId::kList>(
    ArrowVector& vector, const DataType& type) {
  auto result = createArrayFromVector(vector);
  vector.childPointers.resize(1);
  result->children = vector.childPointers.data();
  result->n_children = 1;
  vector.childPointers[0] =
      convertVectorToArray(*vector.childData[0], ListType::GetChildType(type));
  vector.array = std::move(result);
  return vector.array.get();
}

template <>
ArrowArray* ArrowRowBatch::templateCreateArray<DataTypeId::kArray>(
    ArrowVector& vector, const DataType& type) {
  auto result = createArrayFromVector(vector);
  vector.childPointers.resize(1);
  result->n_buffers = 1;
  result->children = vector.childPointers.data();
  result->n_children = 1;
  vector.childPointers[0] =
      convertVectorToArray(*vector.childData[0], ArrayType::GetChildType(type));
  vector.array = std::move(result);
  return vector.array.get();
}

template <>
ArrowArray* ArrowRowBatch::templateCreateArray<DataTypeId::kMap>(
    ArrowVector& vector, const DataType& type) {
  return templateCreateArray<DataTypeId::kList>(vector, type);
}

template <>
ArrowArray* ArrowRowBatch::templateCreateArray<DataTypeId::kStruct>(
    ArrowVector& vector, const DataType& type) {
  return convertStructVectorToArray(vector, type);
}

ArrowArray* ArrowRowBatch::convertStructVectorToArray(ArrowVector& vector,
                                                      const DataType& type) {
  auto result = createArrayFromVector(vector);
  result->n_buffers = 1;
  vector.childPointers.resize(StructType::GetNumFields(type));
  result->children = vector.childPointers.data();
  result->n_children = (std::int64_t) StructType::GetNumFields(type);
  for (auto i = 0u; i < StructType::GetNumFields(type); i++) {
    const auto& childType = StructType::GetChildType(type, i);
    vector.childPointers[i] =
        convertVectorToArray(*vector.childData[i], childType);
  }
  vector.array = std::move(result);
  return vector.array.get();
}

ArrowArray* ArrowRowBatch::convertInternalIDVectorToArray(
    ArrowVector& vector, const DataType& /*type*/) {
  auto result = createArrayFromVector(vector);
  result->n_buffers = 1;
  vector.childPointers.resize(2);
  result->children = vector.childPointers.data();
  result->n_children = 2;
  for (auto i = 0; i < 2; i++) {
    auto childType = DataType(DataTypeId::kInt64);
    vector.childPointers[i] =
        convertVectorToArray(*vector.childData[i], childType);
  }
  vector.array = std::move(result);
  return vector.array.get();
}

template <>
ArrowArray* ArrowRowBatch::templateCreateArray<DataTypeId::kInternalId>(
    ArrowVector& vector, const DataType& type) {
  return convertInternalIDVectorToArray(vector, type);
}

template <>
ArrowArray* ArrowRowBatch::templateCreateArray<DataTypeId::kVertex>(
    ArrowVector& vector, const DataType& type) {
  return convertStructVectorToArray(vector, type);
}

template <>
ArrowArray* ArrowRowBatch::templateCreateArray<DataTypeId::kEdge>(
    ArrowVector& vector, const DataType& type) {
  return convertStructVectorToArray(vector, type);
}

ArrowArray* ArrowRowBatch::convertVectorToArray(ArrowVector& vector,
                                                const DataType& type) {
  switch (type.id()) {
  case DataTypeId::kBoolean: {
    return templateCreateArray<DataTypeId::kBoolean>(vector, type);
  }
  case DataTypeId::kInt64: {
    return templateCreateArray<DataTypeId::kInt64>(vector, type);
  }
  case DataTypeId::kInt32: {
    return templateCreateArray<DataTypeId::kInt32>(vector, type);
  }
  case DataTypeId::kInt16: {
    return templateCreateArray<DataTypeId::kInt16>(vector, type);
  }
  case DataTypeId::kInt8: {
    return templateCreateArray<DataTypeId::kInt8>(vector, type);
  }
  case DataTypeId::kUInt64: {
    return templateCreateArray<DataTypeId::kUInt64>(vector, type);
  }
  case DataTypeId::kUInt32: {
    return templateCreateArray<DataTypeId::kUInt32>(vector, type);
  }
  case DataTypeId::kUInt16: {
    return templateCreateArray<DataTypeId::kUInt16>(vector, type);
  }
  case DataTypeId::kUInt8: {
    return templateCreateArray<DataTypeId::kUInt8>(vector, type);
  }
  case DataTypeId::kDouble: {
    return templateCreateArray<DataTypeId::kDouble>(vector, type);
  }
  case DataTypeId::kFloat: {
    return templateCreateArray<DataTypeId::kFloat>(vector, type);
  }
  case DataTypeId::kDate: {
    return templateCreateArray<DataTypeId::kDate>(vector, type);
  }
  case DataTypeId::kTimestampMs: {
    return templateCreateArray<DataTypeId::kTimestampMs>(vector, type);
  }
  case DataTypeId::kInterval: {
    return templateCreateArray<DataTypeId::kInterval>(vector, type);
  }
  case DataTypeId::kVarchar: {
    return templateCreateArray<DataTypeId::kVarchar>(vector, type);
  }
  case DataTypeId::kList: {
    return templateCreateArray<DataTypeId::kList>(vector, type);
  }
  case DataTypeId::kArray: {
    return templateCreateArray<DataTypeId::kArray>(vector, type);
  }
  case DataTypeId::kMap: {
    return templateCreateArray<DataTypeId::kMap>(vector, type);
  }
  case DataTypeId::kPath:
  case DataTypeId::kStruct: {
    return templateCreateArray<DataTypeId::kStruct>(vector, type);
  }
  case DataTypeId::kInternalId: {
    return templateCreateArray<DataTypeId::kInternalId>(vector, type);
  }
  case DataTypeId::kVertex: {
    return templateCreateArray<DataTypeId::kVertex>(vector, type);
  }
  case DataTypeId::kEdge: {
    return templateCreateArray<DataTypeId::kEdge>(vector, type);
  }
  default: {
    NEUG_UNREACHABLE;
  }
  }
}

ArrowArray ArrowRowBatch::toArray() {
  auto rootHolder = std::make_unique<ArrowVector>();
  ArrowArray result{};
  rootHolder->childPointers.resize(types.size());
  result.children = rootHolder->childPointers.data();
  result.n_children = (std::int64_t) types.size();
  result.length = numTuples;
  result.n_buffers = 1;
  result.buffers = rootHolder->buffers.data();
  result.offset = 0;
  result.null_count = 0;
  result.dictionary = nullptr;
  rootHolder->childData = std::move(vectors);
  for (auto i = 0u; i < rootHolder->childData.size(); i++) {
    rootHolder->childPointers[i] =
        convertVectorToArray(*rootHolder->childData[i], types[i]);
  }
  result.private_data = rootHolder.release();
  result.release = releaseArrowVector;
  return result;
}

ArrowArray ArrowRowBatch::append(main::QueryResult& queryResult,
                                 std::int64_t chunkSize) {
  return toArray();
}

}  // namespace common
}  // namespace neug
