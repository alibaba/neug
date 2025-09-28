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

#include <cstring>

#include "neug/compiler/common/types/neug_string.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/utils/api.h"

namespace gs {
namespace function {

struct Repeat {
 public:
  NEUG_API static void operation(common::neug_string_t& left, int64_t& right,
                                 common::neug_string_t& result,
                                 common::ValueVector& resultValueVector);

 private:
  static void repeatStr(char* data, const std::string& pattern,
                        uint64_t count) {
    for (auto i = 0u; i < count; i++) {
      memcpy(data + i * pattern.length(), pattern.c_str(), pattern.length());
    }
  }
};

}  // namespace function
}  // namespace gs
