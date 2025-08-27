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

#include "neug/compiler/common/types/ku_string.h"
#include "neug/compiler/function/list/functions/list_len_function.h"
#include "substr_function.h"

namespace gs {
namespace function {

struct Left {
 public:
  static inline void operation(common::ku_string_t& left, int64_t& right,
                               common::ku_string_t& result,
                               common::ValueVector& resultValueVector) {
    int64_t leftLen = 0;
    ListLen::operation(left, leftLen);
    int64_t len = (right > -1) ? std::min(leftLen, right)
                               : std::max(leftLen + right, (int64_t) 0);
    SubStr::operation(left, 1, len, result, resultValueVector);
  }
};

}  // namespace function
}  // namespace gs
