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

#include "neug/compiler/function/arithmetic/abs.h"

#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/function/cast/functions/numeric_limits.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace function {

// reference from duckDB arithmetic.cpp
template <class SRC_TYPE>
static inline bool AbsInPlaceWithOverflowCheck(SRC_TYPE input,
                                               SRC_TYPE& result) {
  if (input == NumericLimits<SRC_TYPE>::minimum()) {
    return false;
  }
  result = std::abs(input);
  return true;
}

struct AbsInPlace {
  template <class T>
  static inline bool operation(T& input, T& result);
};

template <>
bool inline AbsInPlace::operation(int8_t& input, int8_t& result) {
  return AbsInPlaceWithOverflowCheck<int8_t>(input, result);
}

template <>
bool inline AbsInPlace::operation(int16_t& input, int16_t& result) {
  return AbsInPlaceWithOverflowCheck<int16_t>(input, result);
}

template <>
bool inline AbsInPlace::operation(int32_t& input, int32_t& result) {
  return AbsInPlaceWithOverflowCheck<int32_t>(input, result);
}

template <>
bool AbsInPlace::operation(int64_t& input, int64_t& result) {
  return AbsInPlaceWithOverflowCheck<int64_t>(input, result);
}

template <>
void Abs::operation(int8_t& input, int8_t& result) {
  if (!AbsInPlace::operation(input, result)) {
    THROW_OVERFLOW_EXCEPTION(common::stringFormat(
        "Cannot take the absolute value of {} within INT8 range.",
        common::TypeUtils::toString(input)));
  };
}

template <>
void Abs::operation(int16_t& input, int16_t& result) {
  if (!AbsInPlace::operation(input, result)) {
    THROW_OVERFLOW_EXCEPTION(common::stringFormat(
        "Cannot take the absolute value of {} within INT16 range.",
        common::TypeUtils::toString(input)));
  };
}

template <>
void Abs::operation(int32_t& input, int32_t& result) {
  if (!AbsInPlace::operation(input, result)) {
    THROW_OVERFLOW_EXCEPTION(common::stringFormat(
        "Cannot take the absolute value of {} within INT32 range.",
        common::TypeUtils::toString(input)));
  };
}

template <>
void Abs::operation(int64_t& input, int64_t& result) {
  if (!AbsInPlace::operation(input, result)) {
    THROW_OVERFLOW_EXCEPTION(common::stringFormat(
        "Cannot take the absolute value of {} within INT64 range.",
        common::TypeUtils::toString(input)));
  };
}

template <>
void Abs::operation(common::int128_t& input, common::int128_t& result) {
  result = input < 0 ? -input : input;
}

}  // namespace function
}  // namespace neug
