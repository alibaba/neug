/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the DuckDB project
 * (https://github.com/duckdb/duckdb) Licensed under the MIT License. Modified
 * by Liu Lexiao in 2025 to support Neug-specific features.
 */

#pragma once

#include <cctype>
#include <cmath>
#include <cstdint>
#include <string_view>

#include "fast_float.h"

namespace neug {
namespace execution {

inline std::pair<const char*, size_t> removeWhiteSpaces(std::string_view sw) {
  // skip leading/trailing spaces
  const char* input = sw.data();
  size_t len = sw.size();
  while (len > 0 && isspace(input[0])) {
    input++;
    len--;
  }
  while (len > 0 && isspace(input[len - 1])) {
    len--;
  }
  return {input, len};
}

template <class T>
inline bool tryDoubleCast(std::string_view sw, T& result) {
  auto [input, len] = removeWhiteSpaces(sw);
  if (len == 0) {
    return false;
  }
  // not allow leading 0
  if (len > 1 && *input == '0') {
    if (input[1] >= '0' && input[1] <= '9') {
      return false;
    }
  }
  auto end = input + len;
  auto parse_result = kuzu_fast_float::from_chars(input, end, result);
  if (parse_result.ec != std::errc()) {
    return false;
  }
  return parse_result.ptr == end;
}

//! Note: this should not be included directly, when these methods are required
//! They should be used through the TryCast:: methods defined in
//! 'cast_operators.hpp' This file produces 'unused static method'
//! warnings/errors when included

template <class T>
struct NumericLimits {
  static constexpr T Minimum() { return std::numeric_limits<T>::lowest(); }
  static constexpr T Maximum() { return std::numeric_limits<T>::max(); }
  static constexpr bool IsSigned() { return std::is_signed<T>::value; }
  static constexpr bool IsIntegral() {
    return std::is_integral<T>::value || std::is_enum<T>::value;
  }
  static constexpr uint64_t digits();
};

template <>
constexpr uint64_t NumericLimits<int8_t>::digits() {
  return 3;
}

template <>
constexpr uint64_t NumericLimits<int16_t>::digits() {
  return 5;
}

template <>
constexpr uint64_t NumericLimits<int32_t>::digits() {
  return 10;
}

template <>
constexpr uint64_t NumericLimits<int64_t>::digits() {
  return 19;
}

template <>
constexpr uint64_t NumericLimits<uint8_t>::digits() {
  return 3;
}

template <>
constexpr uint64_t NumericLimits<uint16_t>::digits() {
  return 5;
}

template <>
constexpr uint64_t NumericLimits<uint32_t>::digits() {
  return 10;
}

template <>
constexpr uint64_t NumericLimits<uint64_t>::digits() {
  return 20;
}

template <>
constexpr uint64_t NumericLimits<float>::digits() {
  return 127;
}

template <>
constexpr uint64_t NumericLimits<double>::digits() {
  return 250;
}

inline bool FloatIsFinite(float value) {
  return !(std::isnan(value) || std::isinf(value));
}

inline bool DoubleIsFinite(double value) {
  return !(std::isnan(value) || std::isinf(value));
}

template <class T>
static bool IsFinite(T value) {
  return true;
}

template <>
bool IsFinite(float input) {
  return FloatIsFinite(input);
}

template <>
bool IsFinite(double input) {
  return DoubleIsFinite(input);
}

template <class SRC, class DST>
static bool TryCastWithOverflowCheck(SRC value, DST& result) {
  if (!IsFinite<SRC>(value)) {
    return false;
  }
  if (NumericLimits<SRC>::IsSigned() != NumericLimits<DST>::IsSigned()) {
    if (NumericLimits<SRC>::IsSigned()) {
      // signed to unsigned conversion
      if (NumericLimits<SRC>::digits() > NumericLimits<DST>::digits()) {
        if (value < 0 ||
            value > static_cast<SRC>(NumericLimits<DST>::Maximum())) {
          return false;
        }
      } else {
        if (value < 0) {
          return false;
        }
      }
      result = static_cast<DST>(value);
      return true;
    } else {
      // unsigned to signed conversion
      if (NumericLimits<SRC>::digits() >= NumericLimits<DST>::digits()) {
        if (value <= static_cast<SRC>(NumericLimits<DST>::Maximum())) {
          result = static_cast<DST>(value);
          return true;
        }
        return false;
      } else {
        result = static_cast<DST>(value);
        return true;
      }
    }
  } else {
    // same sign conversion
    if (NumericLimits<DST>::digits() >= NumericLimits<SRC>::digits()) {
      result = static_cast<DST>(value);
      return true;
    } else {
      if (value < SRC(NumericLimits<DST>::Minimum()) ||
          value > SRC(NumericLimits<DST>::Maximum())) {
        return false;
      }
      result = static_cast<DST>(value);
      return true;
    }
  }
}

template <class SRC, class T>
[[maybe_unused]] bool TryCastWithOverflowCheckFloat(SRC value, T& result,
                                                    SRC min, SRC max) {
  if (!IsFinite<SRC>(value)) {
    return false;
  }
  if (!(value >= min && value < max)) {
    return false;
  }
  // PG FLOAT => INT casts use statistical rounding.
  result = static_cast<T>(std::nearbyint(value));
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, int8_t& result) {
  return TryCastWithOverflowCheckFloat<float, int8_t>(value, result, -128.0f,
                                                      128.0f);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, int16_t& result) {
  return TryCastWithOverflowCheckFloat<float, int16_t>(value, result, -32768.0f,
                                                       32768.0f);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, int32_t& result) {
  return TryCastWithOverflowCheckFloat<float, int32_t>(
      value, result, -2147483648.0f, 2147483648.0f);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, int64_t& result) {
  return TryCastWithOverflowCheckFloat<float, int64_t>(
      value, result, -9223372036854775808.0f, 9223372036854775808.0f);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, uint8_t& result) {
  return TryCastWithOverflowCheckFloat<float, uint8_t>(value, result, 0.0f,
                                                       256.0f);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, uint16_t& result) {
  return TryCastWithOverflowCheckFloat<float, uint16_t>(value, result, 0.0f,
                                                        65536.0f);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, uint32_t& result) {
  return TryCastWithOverflowCheckFloat<float, uint32_t>(value, result, 0.0f,
                                                        4294967296.0f);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, uint64_t& result) {
  return TryCastWithOverflowCheckFloat<float, uint64_t>(
      value, result, 0.0f, 18446744073709551616.0f);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, int8_t& result) {
  return TryCastWithOverflowCheckFloat<double, int8_t>(value, result, -128.0,
                                                       128.0);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, int16_t& result) {
  return TryCastWithOverflowCheckFloat<double, int16_t>(value, result, -32768.0,
                                                        32768.0);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, int32_t& result) {
  return TryCastWithOverflowCheckFloat<double, int32_t>(
      value, result, -2147483648.0, 2147483648.0);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, int64_t& result) {
  return TryCastWithOverflowCheckFloat<double, int64_t>(
      value, result, -9223372036854775808.0, 9223372036854775808.0);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, uint8_t& result) {
  return TryCastWithOverflowCheckFloat<double, uint8_t>(value, result, 0.0,
                                                        256.0);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, uint16_t& result) {
  return TryCastWithOverflowCheckFloat<double, uint16_t>(value, result, 0.0,
                                                         65536.0);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, uint32_t& result) {
  return TryCastWithOverflowCheckFloat<double, uint32_t>(value, result, 0.0,
                                                         4294967296.0);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, uint64_t& result) {
  return TryCastWithOverflowCheckFloat<double, uint64_t>(
      value, result, 0.0, 18446744073709551615.0);
}
template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float input, float& result) {
  result = input;
  return true;
}
template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float input, double& result) {
  result = double(input);
  return true;
}
template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double input, double& result) {
  result = input;
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double input, float& result) {
  if (!IsFinite(input)) {
    result = float(input);
    return true;
  }
  auto res = float(input);
  if (!FloatIsFinite(res)) {
    return false;
  }
  result = res;
  return true;
}

//===--------------------------------------------------------------------===//
// Cast Numeric -> bool
//===--------------------------------------------------------------------===//
template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, bool& result) {
  result = bool(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(int8_t value, bool& result) {
  result = bool(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(int16_t value, bool& result) {
  result = bool(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(int32_t value, bool& result) {
  result = bool(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(int64_t value, bool& result) {
  result = bool(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(uint8_t value, bool& result) {
  result = bool(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(uint16_t value, bool& result) {
  result = bool(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(uint32_t value, bool& result) {
  result = bool(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(uint64_t value, bool& result) {
  result = bool(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, bool& result) {
  result = bool(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, bool& result) {
  result = bool(value);
  return true;
}

//===--------------------------------------------------------------------===//
// Cast bool -> Numeric
//===--------------------------------------------------------------------===//
template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, int8_t& result) {
  result = int8_t(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, int16_t& result) {
  result = int16_t(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, int32_t& result) {
  result = int32_t(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, int64_t& result) {
  result = int64_t(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, uint8_t& result) {
  result = uint8_t(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, uint16_t& result) {
  result = uint16_t(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, uint32_t& result) {
  result = uint32_t(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, uint64_t& result) {
  result = uint64_t(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, float& result) {
  result = float(value);
  return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, double& result) {
  result = double(value);
  return true;
}

struct NumericTryCast {
  template <class SRC, class DST>
  static inline bool Operation(SRC input, DST& result, bool strict = false) {
    return TryCastWithOverflowCheck(input, result);
  }
};

struct NumericCast {
  template <class SRC, class DST>
  static inline DST Operation(SRC input) {
    DST result;
    if (!NumericTryCast::Operation(input, result)) {
      throw InvalidInputException(CastExceptionText<SRC, DST>(input));
    }
    return result;
  }
};
}  // namespace execution

}  // namespace neug
