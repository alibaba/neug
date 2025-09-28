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

#include "ltrim_function.h"
#include "neug/compiler/common/types/neug_string.h"
#include "rtrim_function.h"

namespace gs {
namespace function {

struct Trim : BaseStrOperation {
 public:
  static inline void operation(common::neug_string_t& input,
                               common::neug_string_t& result,
                               common::ValueVector& resultValueVector) {
    BaseStrOperation::operation(input, result, resultValueVector, trim);
  }

 private:
  static uint32_t trim(char* data, uint32_t len) {
    return Rtrim::rtrim(data, Ltrim::ltrim(data, len));
  }
};

}  // namespace function
}  // namespace gs
