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

#include <optional>
#include <utility>

#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/cast.h"
#include "neug/compiler/common/copy_constructors.h"
#include "neug/compiler/common/data_chunk/data_chunk_state.h"
#include "neug/compiler/common/null_mask.h"
#include "neug/compiler/common/types/neug_string.h"
#include "neug/compiler/common/vector/auxiliary_buffer.h"

namespace neug {
namespace common {

class Value;

//! A Vector represents values of the same data type.
//! The capacity of a ValueVector is either 1 (sequence) or
//! DEFAULT_VECTOR_CAPACITY.
class NEUG_API ValueVector {
  friend class ListVector;
  friend class ListAuxiliaryBuffer;
  friend class StructVector;
  friend class StringVector;
  friend class ArrowColumnVector;

 public:
  explicit ValueVector(
      LogicalType dataType, storage::MemoryManager* memoryManager = nullptr,
      std::shared_ptr<DataChunkState> dataChunkState = nullptr);
  explicit ValueVector(LogicalTypeID dataTypeID,
                       storage::MemoryManager* memoryManager = nullptr)
      : ValueVector(LogicalType(dataTypeID), memoryManager) {
    NEUG_ASSERT(dataTypeID != LogicalTypeID::LIST);
  }

  DELETE_COPY_AND_MOVE(ValueVector);
  ~ValueVector() = default;

  template <typename T>
  std::optional<T> firstNonNull() const {
    sel_t selectedSize = state->getSelSize();
    if (selectedSize == 0) {
      return std::nullopt;
    }
    if (hasNoNullsGuarantee()) {
      return getValue<T>(state->getSelVector()[0]);
    } else {
      for (size_t i = 0; i < selectedSize; i++) {
        auto pos = state->getSelVector()[i];
        if (!isNull(pos)) {
          return std::make_optional(getValue<T>(pos));
        }
      }
    }
    return std::nullopt;
  }

  template <class Func>
  void forEachNonNull(Func&& func) const {
    if (hasNoNullsGuarantee()) {
      state->getSelVector().forEach(func);
    } else {
      state->getSelVector().forEach([&](auto i) {
        if (!isNull(i)) {
          func(i);
        }
      });
    }
  }

  uint32_t countNonNull() const;

  void setState(const std::shared_ptr<DataChunkState>& state_);

  void setAllNull() { nullMask.setAllNull(); }
  void setAllNonNull() { nullMask.setAllNonNull(); }
  // On return true, there are no null. On return false, there may or may not be
  // nulls.
  bool hasNoNullsGuarantee() const { return nullMask.hasNoNullsGuarantee(); }
  void setNullRange(uint32_t startPos, uint32_t len, bool value) {
    nullMask.setNullFromRange(startPos, len, value);
  }
  const NullMask& getNullMask() const { return nullMask; }
  void setNull(uint32_t pos, bool isNull);
  uint8_t isNull(uint32_t pos) const { return nullMask.isNull(pos); }
  void setAsSingleNullEntry() {
    state->getSelVectorUnsafe().setSelSize(1);
    setNull(state->getSelVector()[0], true);
  }

  bool setNullFromBits(const uint64_t* srcNullEntries, uint64_t srcOffset,
                       uint64_t dstOffset, uint64_t numBitsToCopy,
                       bool invert = false);

  uint32_t getNumBytesPerValue() const { return numBytesPerValue; }

  // TODO(Guodong): Rename this to getValueRef
  template <typename T>
  const T& getValue(uint32_t pos) const {
    return ((T*) valueBuffer.get())[pos];
  }
  template <typename T>
  T& getValue(uint32_t pos) {
    return ((T*) valueBuffer.get())[pos];
  }
  template <typename T>
  void setValue(uint32_t pos, T val);
  // copyFromRowData assumes rowData is non-NULL.
  void copyFromRowData(uint32_t pos, const uint8_t* rowData);
  // copyToRowData assumes srcVectorData is non-NULL.
  void copyToRowData(uint32_t pos, uint8_t* rowData,
                     InMemOverflowBuffer* rowOverflowBuffer) const;
  // copyFromVectorData assumes srcVectorData is non-NULL.
  void copyFromVectorData(uint8_t* dstData, const ValueVector* srcVector,
                          const uint8_t* srcVectorData);
  void copyFromVectorData(uint64_t dstPos, const ValueVector* srcVector,
                          uint64_t srcPos);
  void copyFromValue(uint64_t pos, const Value& value);

  std::unique_ptr<Value> getAsValue(uint64_t pos) const;

  uint8_t* getData() const { return valueBuffer.get(); }

  offset_t readNodeOffset(uint32_t pos) const {
    NEUG_ASSERT(dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID);
    return getValue<nodeID_t>(pos).offset;
  }

  void resetAuxiliaryBuffer();

  // If there is still non-null values after discarding, return true. Otherwise,
  // return false. For an unflat vector, its selection vector is also updated to
  // the resultSelVector.
  static bool discardNull(ValueVector& vector);

  void serialize(Serializer& ser) const;
  static std::unique_ptr<ValueVector> deSerialize(
      Deserializer& deSer, storage::MemoryManager* mm,
      std::shared_ptr<DataChunkState> dataChunkState);

  SelectionVector* getSelVectorPtr() const {
    return state ? &state->getSelVectorUnsafe() : nullptr;
  }

 private:
  uint32_t getDataTypeSize(const LogicalType& type);
  void initializeValueBuffer();

 public:
  LogicalType dataType;
  std::shared_ptr<DataChunkState> state;

 private:
  std::unique_ptr<uint8_t[]> valueBuffer;
  NullMask nullMask;
  uint32_t numBytesPerValue;
  std::unique_ptr<AuxiliaryBuffer> auxiliaryBuffer;
};

class NEUG_API StringVector {
 public:
  static inline InMemOverflowBuffer* getInMemOverflowBuffer(
      ValueVector* vector) {
    NEUG_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    return neug_dynamic_cast<StringAuxiliaryBuffer*>(
               vector->auxiliaryBuffer.get())
        ->getOverflowBuffer();
  }

  static void addString(ValueVector* vector, uint32_t vectorPos,
                        neug_string_t& srcStr);
  static void addString(ValueVector* vector, uint32_t vectorPos,
                        const char* srcStr, uint64_t length);
  static void addString(ValueVector* vector, uint32_t vectorPos,
                        const std::string& srcStr);
  // Add empty string with space reserved for the provided size
  // Returned value can be modified to set the string contents
  static neug_string_t& reserveString(ValueVector* vector, uint32_t vectorPos,
                                      uint64_t length);
  static void reserveString(ValueVector* vector, neug_string_t& dstStr,
                            uint64_t length);
  static void addString(ValueVector* vector, neug_string_t& dstStr,
                        neug_string_t& srcStr);
  static void addString(ValueVector* vector, neug_string_t& dstStr,
                        const char* srcStr, uint64_t length);
  static void addString(neug::common::ValueVector* vector,
                        neug_string_t& dstStr, const std::string& srcStr);
  static void copyToRowData(const ValueVector* vector, uint32_t pos,
                            uint8_t* rowData,
                            InMemOverflowBuffer* rowOverflowBuffer);
};

struct NEUG_API BlobVector {
  static void addBlob(ValueVector* vector, uint32_t pos, const char* data,
                      uint32_t length) {
    StringVector::addString(vector, pos, data, length);
  }  // namespace common
  static void addBlob(ValueVector* vector, uint32_t pos, const uint8_t* data,
                      uint64_t length) {
    StringVector::addString(vector, pos, reinterpret_cast<const char*>(data),
                            length);
  }
};  // namespace neug

// ListVector is used for both LIST and ARRAY physical type
class NEUG_API ListVector {
 public:
  static const ListAuxiliaryBuffer& getAuxBuffer(const ValueVector& vector) {
    return vector.auxiliaryBuffer->constCast<ListAuxiliaryBuffer>();
  }
  static ListAuxiliaryBuffer& getAuxBufferUnsafe(const ValueVector& vector) {
    return vector.auxiliaryBuffer->cast<ListAuxiliaryBuffer>();
  }
  // If you call setDataVector during initialize, there must be a followed up
  // copyListEntryAndBufferMetaData at runtime.
  // TODO(Xiyang): try to merge setDataVector & copyListEntryAndBufferMetaData
  static void setDataVector(const ValueVector* vector,
                            std::shared_ptr<ValueVector> dataVector) {
    NEUG_ASSERT(validateType(*vector));
    auto& listBuffer = getAuxBufferUnsafe(*vector);
    listBuffer.setDataVector(std::move(dataVector));
  }
  static void copyListEntryAndBufferMetaData(
      ValueVector& vector, const SelectionVector& selVector,
      const ValueVector& other, const SelectionVector& otherSelVector);
  static ValueVector* getDataVector(const ValueVector* vector) {
    NEUG_ASSERT(validateType(*vector));
    return getAuxBuffer(*vector).getDataVector();
  }
  static std::shared_ptr<ValueVector> getSharedDataVector(
      const ValueVector* vector) {
    NEUG_ASSERT(validateType(*vector));
    return getAuxBuffer(*vector).getSharedDataVector();
  }
  static uint64_t getDataVectorSize(const ValueVector* vector) {
    NEUG_ASSERT(validateType(*vector));
    return getAuxBuffer(*vector).getSize();
  }
  static uint8_t* getListValues(const ValueVector* vector,
                                const list_entry_t& listEntry) {
    NEUG_ASSERT(validateType(*vector));
    auto dataVector = getDataVector(vector);
    return dataVector->getData() +
           dataVector->getNumBytesPerValue() * listEntry.offset;
  }
  static uint8_t* getListValuesWithOffset(const ValueVector* vector,
                                          const list_entry_t& listEntry,
                                          offset_t elementOffsetInList) {
    NEUG_ASSERT(validateType(*vector));
    return getListValues(vector, listEntry) +
           elementOffsetInList * getDataVector(vector)->getNumBytesPerValue();
  }
  static list_entry_t addList(ValueVector* vector, uint64_t listSize) {
    NEUG_ASSERT(validateType(*vector));
    return getAuxBufferUnsafe(*vector).addList(listSize);
  }
  static void resizeDataVector(ValueVector* vector, uint64_t numValues) {
    NEUG_ASSERT(validateType(*vector));
    getAuxBufferUnsafe(*vector).resize(numValues);
  }

  static void copyFromRowData(ValueVector* vector, uint32_t pos,
                              const uint8_t* rowData);
  static void copyToRowData(const ValueVector* vector, uint32_t pos,
                            uint8_t* rowData,
                            InMemOverflowBuffer* rowOverflowBuffer);
  static void copyFromVectorData(ValueVector* dstVector, uint8_t* dstData,
                                 const ValueVector* srcVector,
                                 const uint8_t* srcData);
  static void appendDataVector(ValueVector* dstVector,
                               ValueVector* srcDataVector,
                               uint64_t numValuesToAppend);
  static void sliceDataVector(ValueVector* vectorToSlice, uint64_t offset,
                              uint64_t numValues);

 private:
  static bool validateType(const ValueVector& vector) {
    switch (vector.dataType.getPhysicalType()) {
    case PhysicalTypeID::LIST:
    case PhysicalTypeID::ARRAY:
      return true;
    default:
      return false;
    }
  }
};

class StructVector {
 public:
  static const std::vector<std::shared_ptr<ValueVector>>& getFieldVectors(
      const ValueVector* vector) {
    return neug_dynamic_cast<StructAuxiliaryBuffer*>(
               vector->auxiliaryBuffer.get())
        ->getFieldVectors();
  }

  static std::shared_ptr<ValueVector> getFieldVector(const ValueVector* vector,
                                                     struct_field_idx_t idx) {
    return neug_dynamic_cast<StructAuxiliaryBuffer*>(
               vector->auxiliaryBuffer.get())
        ->getFieldVectorShared(idx);
  }

  static ValueVector* getFieldVectorRaw(const ValueVector& vector,
                                        const std::string& fieldName) {
    auto idx = StructType::getFieldIdx(vector.dataType, fieldName);
    return neug_dynamic_cast<StructAuxiliaryBuffer*>(
               vector.auxiliaryBuffer.get())
        ->getFieldVectorPtr(idx);
  }

  static void referenceVector(ValueVector* vector, struct_field_idx_t idx,
                              std::shared_ptr<ValueVector> vectorToReference) {
    neug_dynamic_cast<StructAuxiliaryBuffer*>(vector->auxiliaryBuffer.get())
        ->referenceChildVector(idx, std::move(vectorToReference));
  }

  static void copyFromRowData(ValueVector* vector, uint32_t pos,
                              const uint8_t* rowData);
  static void copyToRowData(const ValueVector* vector, uint32_t pos,
                            uint8_t* rowData,
                            InMemOverflowBuffer* rowOverflowBuffer);
  static void copyFromVectorData(ValueVector* dstVector, const uint8_t* dstData,
                                 const ValueVector* srcVector,
                                 const uint8_t* srcData);
};

class UnionVector {
 public:
  static inline ValueVector* getTagVector(const ValueVector* vector) {
    NEUG_ASSERT(vector->dataType.getLogicalTypeID() == LogicalTypeID::UNION);
    return StructVector::getFieldVector(vector, UnionType::TAG_FIELD_IDX).get();
  }

  static inline ValueVector* getValVector(const ValueVector* vector,
                                          union_field_idx_t fieldIdx) {
    NEUG_ASSERT(vector->dataType.getLogicalTypeID() == LogicalTypeID::UNION);
    return StructVector::getFieldVector(
               vector, UnionType::getInternalFieldIdx(fieldIdx))
        .get();
  }

  static inline void referenceVector(
      ValueVector* vector, union_field_idx_t fieldIdx,
      std::shared_ptr<ValueVector> vectorToReference) {
    StructVector::referenceVector(vector,
                                  UnionType::getInternalFieldIdx(fieldIdx),
                                  std::move(vectorToReference));
  }

  static inline void setTagField(ValueVector& vector, SelectionVector& sel,
                                 union_field_idx_t tag) {
    NEUG_ASSERT(vector.dataType.getLogicalTypeID() == LogicalTypeID::UNION);
    for (auto i = 0u; i < sel.getSelSize(); i++) {
      vector.setValue<struct_field_idx_t>(sel[i], tag);
    }
  }
};

class MapVector {
 public:
  static inline ValueVector* getKeyVector(const ValueVector* vector) {
    return StructVector::getFieldVector(ListVector::getDataVector(vector),
                                        0 /* keyVectorPos */)
        .get();
  }

  static inline ValueVector* getValueVector(const ValueVector* vector) {
    return StructVector::getFieldVector(ListVector::getDataVector(vector),
                                        1 /* valVectorPos */)
        .get();
  }

  static inline uint8_t* getMapKeys(const ValueVector* vector,
                                    const list_entry_t& listEntry) {
    auto keyVector = getKeyVector(vector);
    return keyVector->getData() +
           keyVector->getNumBytesPerValue() * listEntry.offset;
  }

  static inline uint8_t* getMapValues(const ValueVector* vector,
                                      const list_entry_t& listEntry) {
    auto valueVector = getValueVector(vector);
    return valueVector->getData() +
           valueVector->getNumBytesPerValue() * listEntry.offset;
  }
};

}  // namespace common
}  // namespace neug
