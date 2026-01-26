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

#include "neug/compiler/common/types/neug_string.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/utils/api.h"

namespace neug {
namespace function {

struct BaseLowerUpperFunction {
  NEUG_API static void operation(common::neug_string_t& input,
                                 common::neug_string_t& result,
                                 common::ValueVector& resultValueVector,
                                 bool isUpper);

  static void convertCharCase(char* result, const char* input, int32_t charPos,
                              bool toUpper, int& originalSize, int& newSize);
  static void convertCase(char* result, uint32_t len, char* input,
                          bool toUpper);
  static uint32_t getResultLen(char* inputStr, uint32_t inputLen, bool isUpper);
};

}  // namespace function
}  // namespace neug
