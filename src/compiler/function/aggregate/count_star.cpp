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

#include "neug/compiler/function/aggregate/count_star.h"

using namespace neug::common;
using namespace neug::storage;

namespace neug {
namespace function {

void CountStarFunction::updateAll(uint8_t* state_, ValueVector* input,
                                  uint64_t multiplicity,
                                  InMemOverflowBuffer* /*overflowBuffer*/) {
  auto state = reinterpret_cast<CountState*>(state_);
  NEUG_ASSERT(input == nullptr);
  (void) input;
  state->count += multiplicity;
}

void CountStarFunction::updatePos(uint8_t* state_, ValueVector* input,
                                  uint64_t multiplicity, uint32_t /*pos*/,
                                  InMemOverflowBuffer* /*overflowBuffer*/) {
  auto state = reinterpret_cast<CountState*>(state_);
  NEUG_ASSERT(input == nullptr);
  (void) input;
  state->count += multiplicity;
}

function_set CountStarFunction::getFunctionSet() {
  function_set result;
  result.push_back(std::make_unique<AggregateFunction>(
      name, std::vector<LogicalTypeID>{}, LogicalTypeID::INT64, initialize,
      updateAll, updatePos, combine, finalize, false));
  return result;
}

}  // namespace function
}  // namespace neug
