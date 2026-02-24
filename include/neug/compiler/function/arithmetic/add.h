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

struct Add {
  template <class A, class B, class R>
  static inline void operation(A& left, B& right, R& result) {
    result = left + right;
  }
};

template <>
void Add::operation(uint8_t& left, uint8_t& right, uint8_t& result);

template <>
void Add::operation(uint16_t& left, uint16_t& right, uint16_t& result);

template <>
void Add::operation(uint32_t& left, uint32_t& right, uint32_t& result);

template <>
void Add::operation(uint64_t& left, uint64_t& right, uint64_t& result);

template <>
void Add::operation(int8_t& left, int8_t& right, int8_t& result);

template <>
void Add::operation(int16_t& left, int16_t& right, int16_t& result);

template <>
void Add::operation(int32_t& left, int32_t& right, int32_t& result);

template <>
void Add::operation(int64_t& left, int64_t& right, int64_t& result);

}  // namespace function
}  // namespace neug
