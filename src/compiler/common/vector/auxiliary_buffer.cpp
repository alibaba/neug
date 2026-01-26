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

#include "neug/compiler/common/vector/auxiliary_buffer.h"

#include <numeric>

#include "neug/compiler/common/constants.h"
#include "neug/compiler/common/system_config.h"
#include "neug/compiler/common/vector/value_vector.h"

namespace neug {
namespace common {

StructAuxiliaryBuffer::StructAuxiliaryBuffer(
    const LogicalType& type, storage::MemoryManager* memoryManager) {
  auto fieldTypes = StructType::getFieldTypes(type);
  childrenVectors.reserve(fieldTypes.size());
  for (const auto& fieldType : fieldTypes) {
    childrenVectors.push_back(
        std::make_shared<ValueVector>(fieldType->copy(), memoryManager));
  }
}

ListAuxiliaryBuffer::ListAuxiliaryBuffer(const LogicalType& dataVectorType,
                                         storage::MemoryManager* memoryManager)
    : capacity{DEFAULT_VECTOR_CAPACITY},
      size{0},
      dataVector{std::make_shared<ValueVector>(dataVectorType.copy(),
                                               memoryManager)} {}

list_entry_t ListAuxiliaryBuffer::addList(list_size_t listSize) {
  auto listEntry = list_entry_t{size, listSize};
  bool needResizeDataVector = size + listSize > capacity;
  while (size + listSize > capacity) {
    capacity *= CHUNK_RESIZE_RATIO;
  }
  if (needResizeDataVector) {
    resizeDataVector(dataVector.get());
  }
  size += listSize;
  return listEntry;
}

void ListAuxiliaryBuffer::resize(uint64_t numValues) {
  if (numValues <= capacity) {
    size = numValues;
    return;
  }
  bool needResizeDataVector = numValues > capacity;
  while (numValues > capacity) {
    capacity *= 2;
    NEUG_ASSERT(capacity != 0);
  }
  if (needResizeDataVector) {
    resizeDataVector(dataVector.get());
  }
  size = numValues;
}

void ListAuxiliaryBuffer::resizeDataVector(ValueVector* dataVector) {
  auto buffer =
      std::make_unique<uint8_t[]>(capacity * dataVector->getNumBytesPerValue());
  memcpy(buffer.get(), dataVector->valueBuffer.get(),
         size * dataVector->getNumBytesPerValue());
  dataVector->valueBuffer = std::move(buffer);
  dataVector->nullMask.resize(capacity);
  // If the dataVector is a struct vector, we need to resize its field vectors.
  if (dataVector->dataType.getPhysicalType() == PhysicalTypeID::STRUCT) {
    resizeStructDataVector(dataVector);
  }
}

void ListAuxiliaryBuffer::resizeStructDataVector(ValueVector* dataVector) {
  std::iota(
      reinterpret_cast<int64_t*>(dataVector->getData() +
                                 dataVector->getNumBytesPerValue() * size),
      reinterpret_cast<int64_t*>(dataVector->getData() +
                                 dataVector->getNumBytesPerValue() * capacity),
      size);
  auto fieldVectors = StructVector::getFieldVectors(dataVector);
  for (auto& fieldVector : fieldVectors) {
    resizeDataVector(fieldVector.get());
  }
}

std::unique_ptr<AuxiliaryBuffer> AuxiliaryBufferFactory::getAuxiliaryBuffer(
    LogicalType& type, storage::MemoryManager* memoryManager) {
  switch (type.getPhysicalType()) {
  case PhysicalTypeID::STRING:
    return std::make_unique<StringAuxiliaryBuffer>(memoryManager);
  case PhysicalTypeID::STRUCT:
    return std::make_unique<StructAuxiliaryBuffer>(type, memoryManager);
  case PhysicalTypeID::LIST:
    return std::make_unique<ListAuxiliaryBuffer>(ListType::getChildType(type),
                                                 memoryManager);
  case PhysicalTypeID::ARRAY:
    return std::make_unique<ListAuxiliaryBuffer>(ArrayType::getChildType(type),
                                                 memoryManager);
  default:
    return nullptr;
  }
}

}  // namespace common
}  // namespace neug
