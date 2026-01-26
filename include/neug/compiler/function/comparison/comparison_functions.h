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

#include "neug/compiler/common/vector/value_vector.h"

namespace neug {
namespace function {

struct Equals {
  template <class A, class B>
  static inline void operation(const A& left, const B& right, uint8_t& result,
                               common::ValueVector* /*leftVector*/,
                               common::ValueVector* /*rightVector*/) {
    result = left == right;
  }

  template <class T>
  static bool operation(const T& left, const T& right) {
    uint8_t result = 0;
    operation<T>(left, right, result, nullptr, nullptr);
    return result;
  }
};

struct NotEquals {
  template <class A, class B>
  static inline void operation(const A& left, const B& right, uint8_t& result,
                               common::ValueVector* leftVector,
                               common::ValueVector* rightVector) {
    Equals::operation(left, right, result, leftVector, rightVector);
    result = !result;
  }

  template <class T>
  static bool operation(const T& left, const T& right) {
    uint8_t result = 0;
    operation<T>(left, right, result, nullptr, nullptr);
    return result;
  }
};

struct GreaterThan {
  template <class A, class B>
  static inline void operation(const A& left, const B& right, uint8_t& result,
                               common::ValueVector* /*leftVector*/,
                               common::ValueVector* /*rightVector*/) {
    result = left > right;
  }

  template <class T>
  static bool operation(const T& left, const T& right) {
    uint8_t result = 0;
    operation<T>(left, right, result, nullptr, nullptr);
    return result;
  }
};

struct GreaterThanEquals {
  template <class A, class B>
  static inline void operation(const A& left, const B& right, uint8_t& result,
                               common::ValueVector* leftVector,
                               common::ValueVector* rightVector) {
    uint8_t isGreater = 0;
    uint8_t isEqual = 0;
    GreaterThan::operation(left, right, isGreater, leftVector, rightVector);
    Equals::operation(left, right, isEqual, leftVector, rightVector);
    result = isGreater || isEqual;
  }

  template <class T>
  static bool operation(const T& left, const T& right) {
    uint8_t result = 0;
    operation<T>(left, right, result, nullptr, nullptr);
    return result;
  }
};

struct LessThan {
  template <class A, class B>
  static inline void operation(const A& left, const B& right, uint8_t& result,
                               common::ValueVector* leftVector,
                               common::ValueVector* rightVector) {
    GreaterThanEquals::operation(left, right, result, leftVector, rightVector);
    result = !result;
  }

  template <class T>
  static bool operation(const T& left, const T& right) {
    uint8_t result = 0;
    operation<T>(left, right, result, nullptr, nullptr);
    return result;
  }
};

struct LessThanEquals {
  template <class A, class B>
  static inline void operation(const A& left, const B& right, uint8_t& result,
                               common::ValueVector* leftVector,
                               common::ValueVector* rightVector) {
    GreaterThan::operation(left, right, result, leftVector, rightVector);
    result = !result;
  }

  template <class T>
  static bool operation(const T& left, const T& right) {
    uint8_t result = 0;
    operation<T>(left, right, result, nullptr, nullptr);
    return result;
  }
};

// specialization for equal and greater than.
template <>
void Equals::operation(const common::list_entry_t& left,
                       const common::list_entry_t& right, uint8_t& result,
                       common::ValueVector* leftVector,
                       common::ValueVector* rightVector);
template <>
void Equals::operation(const common::struct_entry_t& left,
                       const common::struct_entry_t& right, uint8_t& result,
                       common::ValueVector* leftVector,
                       common::ValueVector* rightVector);
template <>
void GreaterThan::operation(const common::list_entry_t& left,
                            const common::list_entry_t& right, uint8_t& result,
                            common::ValueVector* leftVector,
                            common::ValueVector* rightVector);
template <>
void GreaterThan::operation(const common::struct_entry_t& left,
                            const common::struct_entry_t& right,
                            uint8_t& result, common::ValueVector* leftVector,
                            common::ValueVector* rightVector);

}  // namespace function
}  // namespace neug
