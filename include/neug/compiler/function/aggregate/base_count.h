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

#include "neug/compiler/function/aggregate_function.h"

namespace neug {
namespace function {

struct BaseCountFunction {
  struct CountState : public AggregateState {
    inline uint32_t getStateSize() const override { return sizeof(*this); }
    inline void moveResultToVector(common::ValueVector* outputVector,
                                   uint64_t pos) override {
      memcpy(
          outputVector->getData() + pos * outputVector->getNumBytesPerValue(),
          reinterpret_cast<uint8_t*>(&count),
          outputVector->getNumBytesPerValue());
    }

    uint64_t count = 0;
  };

  static std::unique_ptr<AggregateState> initialize() {
    auto state = std::make_unique<CountState>();
    state->isNull = false;
    return state;
  }

  static void combine(uint8_t* state_, uint8_t* otherState_,
                      common::InMemOverflowBuffer* /*overflowBuffer*/) {
    auto state = reinterpret_cast<CountState*>(state_);
    auto otherState = reinterpret_cast<CountState*>(otherState_);
    state->count += otherState->count;
  }

  static void finalize(uint8_t* /*state_*/) {}
};

}  // namespace function
}  // namespace neug
