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

#include <cstdint>

namespace neug {
namespace function {

struct Negate {
  template <class T>
  static inline void operation(T& input, T& result) {
    result = -input;
  }
};

template <>
void Negate::operation(int8_t& input, int8_t& result);

template <>
void Negate::operation(int16_t& input, int16_t& result);

template <>
void Negate::operation(int32_t& input, int32_t& result);

template <>
void Negate::operation(int64_t& input, int64_t& result);

}  // namespace function
}  // namespace neug
