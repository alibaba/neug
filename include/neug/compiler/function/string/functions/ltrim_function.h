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

#include "base_str_function.h"
#include "neug/compiler/common/types/neug_string.h"

namespace gs {
namespace function {

struct Ltrim {
  static inline void operation(common::neug_string_t& input,
                               common::neug_string_t& result,
                               common::ValueVector& resultValueVector) {
    BaseStrOperation::operation(input, result, resultValueVector, ltrim);
  }

  static uint32_t ltrim(char* data, uint32_t len) {
    auto counter = 0u;
    for (; counter < len; counter++) {
      if (!isspace(data[counter])) {
        break;
      }
    }
    for (uint32_t i = 0; i < len - counter; i++) {
      data[i] = data[i + counter];
    }
    return len - counter;
  }
};

}  // namespace function
}  // namespace gs
