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

#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/types/ku_string.h"
#include "neug/compiler/common/vector/value_vector.h"

namespace gs {
namespace function {

struct PadOperation {
 public:
  static inline void operation(
      common::ku_string_t& src, int64_t count,
      common::ku_string_t& characterToPad, common::ku_string_t& result,
      common::ValueVector& resultValueVector,
      void (*padOperation)(common::ku_string_t& result,
                           common::ku_string_t& src,
                           common::ku_string_t& characterToPad)) {
    if (count <= 0) {
      result.set("", 0);
      return;
    }
    KU_ASSERT(characterToPad.len == 1);
    padOperation(result, src, characterToPad);
    common::StringVector::addString(&resultValueVector, result,
                                    (const char*) result.getData(), count);
  }
};

}  // namespace function
}  // namespace gs
