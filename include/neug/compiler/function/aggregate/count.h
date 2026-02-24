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

#include "base_count.h"

namespace neug {
namespace function {

struct CountFunction : public BaseCountFunction {
  static constexpr const char* name = "COUNT";

  static void updateAll(uint8_t* state_, common::ValueVector* input,
                        uint64_t multiplicity,
                        common::InMemOverflowBuffer* overflowBuffer);

  // NOLINTNEXTLINE(readability-non-const-parameter): Would cast away
  // qualifiers.
  static inline void updatePos(
      uint8_t* state_, common::ValueVector* /*input*/, uint64_t multiplicity,
      uint32_t /*pos*/, common::InMemOverflowBuffer* /*overflowBuffer*/) {
    reinterpret_cast<CountState*>(state_)->count += multiplicity;
  }

  static void paramRewriteFunc(binder::expression_vector& arguments);

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug
