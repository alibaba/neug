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
#include "neug/compiler/function/arithmetic/add.h"

namespace neug {
namespace function {

template <typename RESULT_TYPE>
struct SumState : public AggregateState {
  uint32_t getStateSize() const override { return sizeof(*this); }
  void moveResultToVector(common::ValueVector* outputVector,
                          uint64_t pos) override {
    outputVector->setValue(pos, sum);
  }

  RESULT_TYPE sum{};
};

template <typename INPUT_TYPE, typename RESULT_TYPE>
struct SumFunction {
  static std::unique_ptr<AggregateState> initialize() {
    return std::make_unique<SumState<RESULT_TYPE>>();
  }

  static void updateAll(uint8_t* state_, common::ValueVector* input,
                        uint64_t multiplicity,
                        common::InMemOverflowBuffer* /*overflowBuffer*/) {
    NEUG_ASSERT(!input->state->isFlat());
    auto* state = reinterpret_cast<SumState<RESULT_TYPE>*>(state_);
    input->forEachNonNull(
        [&](auto pos) { updateSingleValue(state, input, pos, multiplicity); });
  }

  static void updatePos(uint8_t* state_, common::ValueVector* input,
                        uint64_t multiplicity, uint32_t pos,
                        common::InMemOverflowBuffer* /*overflowBuffer*/) {
    auto* state = reinterpret_cast<SumState<RESULT_TYPE>*>(state_);
    updateSingleValue(state, input, pos, multiplicity);
  }

  static void updateSingleValue(SumState<RESULT_TYPE>* state,
                                common::ValueVector* input, uint32_t pos,
                                uint64_t multiplicity) {}

  static void combine(uint8_t* state_, uint8_t* otherState_,
                      common::InMemOverflowBuffer* /*overflowBuffer*/) {}

  static void finalize(uint8_t* /*state_*/) {}
};

}  // namespace function
}  // namespace neug
