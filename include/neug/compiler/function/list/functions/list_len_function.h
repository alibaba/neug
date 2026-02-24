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
#include "utf8proc.h"

namespace neug {
namespace function {

struct ListLen {
 public:
  template <typename T>
  static void operation(T& input, int64_t& result) {
    result = input.size;
  }
};

template <>
inline void ListLen::operation(common::neug_string_t& input, int64_t& result) {
  auto totalByteLength = input.len;
  auto inputString = input.getAsString();
  for (auto i = 0u; i < totalByteLength; i++) {
    if (inputString[i] & 0x80) {
      int64_t length = 0;
      // Use grapheme iterator to identify bytes of utf8 char and increment once
      // for each char.
      utf8proc::utf8proc_grapheme_callback(
          inputString.c_str(), totalByteLength,
          [&](size_t /*start*/, size_t /*end*/) {
            length++;
            return true;
          });
      result = length;
      return;
    }
  }
  result = totalByteLength;
}

}  // namespace function
}  // namespace neug
