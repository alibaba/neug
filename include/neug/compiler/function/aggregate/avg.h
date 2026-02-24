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

#include "neug/compiler/common/in_mem_overflow_buffer.h"
#include "neug/compiler/common/types/int128_t.h"
#include "neug/compiler/function/aggregate_function.h"

namespace neug {
namespace function {

template <typename T>
struct AvgState : public AggregateState {
  uint32_t getStateSize() const override { return sizeof(*this); }
  void moveResultToVector(common::ValueVector* outputVector,
                          uint64_t pos) override {
    outputVector->setValue(pos, avg);
  }

  void finalize() requires common::IntegerTypes<T> {
    if (!isNull) {
      avg = common::Int128_t::Cast<long double>(sum) /
            common::Int128_t::Cast<long double>(count);
    }
  }

  void finalize() requires common::FloatingPointTypes<T> {
    if (!isNull) {
      avg = sum / count;
    }
  }

  T sum{};
  uint64_t count = 0;
  double avg = 0;
};

template <typename INPUT_TYPE, typename RESULT_TYPE>
struct AvgFunction {
  static std::unique_ptr<AggregateState> initialize() {
    return std::make_unique<AvgState<RESULT_TYPE>>();
  }

  static void updateAll(uint8_t* state_, common::ValueVector* input,
                        uint64_t multiplicity,
                        common::InMemOverflowBuffer* /*overflowBuffer*/) {
    auto* state = reinterpret_cast<AvgState<RESULT_TYPE>*>(state_);
    NEUG_ASSERT(!input->state->isFlat());
    input->forEachNonNull(
        [&](auto pos) { updateSingleValue(state, input, pos, multiplicity); });
  }

  static void updatePos(uint8_t* state_, common::ValueVector* input,
                        uint64_t multiplicity, uint32_t pos,
                        common::InMemOverflowBuffer* /*overflowBuffer*/) {
    updateSingleValue(reinterpret_cast<AvgState<RESULT_TYPE>*>(state_), input,
                      pos, multiplicity);
  }

  static void updateSingleValue(AvgState<RESULT_TYPE>* state,
                                common::ValueVector* input, uint32_t pos,
                                uint64_t multiplicity) {}

  static void combine(uint8_t* state_, uint8_t* otherState_,
                      common::InMemOverflowBuffer* /*overflowBuffer*/) {}

  static void finalize(uint8_t* state_) {
    auto* state = reinterpret_cast<AvgState<RESULT_TYPE>*>(state_);
    state->finalize();
  }
};

}  // namespace function
}  // namespace neug
