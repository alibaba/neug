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

#include "base_pad_function.h"
#include "neug/compiler/common/types/neug_string.h"

namespace gs {
namespace function {

struct Rpad : BasePadOperation {
 public:
  static inline void operation(common::neug_string_t& src, int64_t count,
                               common::neug_string_t& characterToPad,
                               common::neug_string_t& result,
                               common::ValueVector& resultValueVector) {
    BasePadOperation::operation(src, count, characterToPad, result,
                                resultValueVector, rpadOperation);
  }

  static void rpadOperation(common::neug_string_t& src, int64_t count,
                            common::neug_string_t& characterToPad,
                            std::string& paddedResult) {
    auto srcPadInfo = BasePadOperation::padCountChars(
        count, (const char*) src.getData(), src.len);
    auto srcData = (const char*) src.getData();
    paddedResult.insert(paddedResult.end(), srcData,
                        srcData + srcPadInfo.first);
    BasePadOperation::insertPadding(count - srcPadInfo.second, characterToPad,
                                    paddedResult);
  }
};

}  // namespace function
}  // namespace gs
