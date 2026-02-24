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

#include "neug/compiler/common/data_chunk/sel_vector.h"

namespace neug {
namespace common {

// F stands for Factorization
enum class FStateType : uint8_t {
  FLAT = 0,
  UNFLAT = 1,
};

class NEUG_API DataChunkState {
 public:
  DataChunkState();
  explicit DataChunkState(sel_t capacity) : fStateType{FStateType::UNFLAT} {
    selVector = std::make_shared<SelectionVector>(capacity);
  }

  // returns a dataChunkState for vectors holding a single value.
  static std::shared_ptr<DataChunkState> getSingleValueDataChunkState();

  void initOriginalAndSelectedSize(uint64_t size) {
    selVector->setSelSize(size);
  }
  bool isFlat() const { return fStateType == FStateType::FLAT; }
  void setToFlat() { fStateType = FStateType::FLAT; }
  void setToUnflat() { fStateType = FStateType::UNFLAT; }

  const SelectionVector& getSelVector() const { return *selVector; }
  sel_t getSelSize() const { return selVector->getSelSize(); }
  SelectionVector& getSelVectorUnsafe() { return *selVector; }
  std::shared_ptr<SelectionVector> getSelVectorShared() { return selVector; }
  void setSelVector(std::shared_ptr<SelectionVector> selVector_) {
    this->selVector = std::move(selVector_);
  }

  void slice(offset_t offset);

 private:
  std::shared_ptr<SelectionVector> selVector;
  // TODO: We should get rid of `fStateType` and merge DataChunkState with
  // SelectionVector.
  FStateType fStateType;
};

}  // namespace common
}  // namespace neug
