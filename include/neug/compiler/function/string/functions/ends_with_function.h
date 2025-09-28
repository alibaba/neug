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

namespace gs {
namespace function {

struct EndsWith {
  static inline void operation(common::neug_string_t& left,
                               common::neug_string_t& right, uint8_t& result) {
    if (right.len > left.len) {
      result = 0;
      return;
    }
    auto lenDiff = left.len - right.len;
    auto lData = left.getData();
    auto rData = right.getData();
    for (auto i = 0u; i < right.len; i++) {
      if (rData[i] != lData[lenDiff + i]) {
        result = 0;
        return;
      }
    }
    result = 1;
  }
};

}  // namespace function
}  // namespace gs
