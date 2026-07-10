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
#pragma once

#include <cstddef>
#include <cstdint>

#include "neug/utils/likely.h"

namespace neug {
namespace utils {

enum class TimestampUnit { kSecond, kMilli, kMicro, kNano };

namespace detail {

/// Direct digit check — avoids std::isdigit function-call overhead.
inline bool is_digit(char c) { return c >= '0' && c <= '9'; }

/// Parse a single decimal digit (assumes caller already validated).
inline uint8_t parse_decimal_digit(char c) {
  return static_cast<uint8_t>(c - '0');
}

/// Parse exactly 2 digits — fully unrolled, no loop.
inline bool parse_2digits(const char* s, uint8_t* out) {
  if (NEUG_UNLIKELY(!is_digit(s[0]) || !is_digit(s[1]))) {
    return false;
  }
  *out = static_cast<uint8_t>(parse_decimal_digit(s[0]) * 10 +
                              parse_decimal_digit(s[1]));
  return true;
}

/// Parse exactly 4 digits — fully unrolled, no loop.
inline bool parse_4digits(const char* s, uint16_t* out) {
  if (NEUG_UNLIKELY(!is_digit(s[0]) || !is_digit(s[1]) || !is_digit(s[2]) ||
                    !is_digit(s[3]))) {
    return false;
  }
  *out = static_cast<uint16_t>(parse_decimal_digit(s[0])) * 1000 +
         static_cast<uint16_t>(parse_decimal_digit(s[1])) * 100 +
         static_cast<uint16_t>(parse_decimal_digit(s[2])) * 10 +
         static_cast<uint16_t>(parse_decimal_digit(s[3]));
  return true;
}

/// Howard Hinnant's days_from_civil — pure arithmetic, no date library
/// needed. Returns days since 1970-01-01.
inline int32_t days_from_civil(int32_t y, uint32_t m, uint32_t d) noexcept {
  y -= m <= 2;
  const int32_t era = (y >= 0 ? y : y - 399) / 400;
  const uint32_t yoe = static_cast<uint32_t>(y - era * 400);
  const uint32_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  const uint32_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + static_cast<int32_t>(doe) - 719468;
}

/// Fast date validation — replaces date::year_month_day::ok().
inline bool is_valid_date(int year, int month, int day) {
  if (NEUG_UNLIKELY(year < 0 || year > 9999))
    return false;
  if (NEUG_UNLIKELY(month < 1 || month > 12))
    return false;
  if (NEUG_UNLIKELY(day < 1 || day > 31))
    return false;
  static constexpr int8_t kDaysPerMonth[] = {31, 28, 31, 30, 31, 30,
                                             31, 31, 30, 31, 30, 31};
  int max_day = kDaysPerMonth[month - 1];
  if (month == 2) {
    bool leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (leap)
      max_day = 29;
  }
  return day <= max_day;
}

inline int64_t cast_seconds_to_unit(TimestampUnit unit, int64_t seconds) {
  switch (unit) {
  case TimestampUnit::kSecond:
    return seconds;
  case TimestampUnit::kMilli:
    return seconds * 1000;
  case TimestampUnit::kMicro:
    return seconds * 1000000;
  case TimestampUnit::kNano:
    return seconds * 1000000000;
  }
  return seconds;
}

inline bool parse_yyyy_mm_dd(const char* s, int64_t* seconds_since_epoch) {
  if (NEUG_UNLIKELY(s[4] != '-' || s[7] != '-')) {
    return false;
  }
  uint16_t year = 0;
  uint8_t month = 0, day = 0;
  if (NEUG_UNLIKELY(!parse_4digits(s, &year)))
    return false;
  if (NEUG_UNLIKELY(!parse_2digits(s + 5, &month)))
    return false;
  if (NEUG_UNLIKELY(!parse_2digits(s + 8, &day)))
    return false;
  if (NEUG_UNLIKELY(!is_valid_date(year, month, day)))
    return false;
  *seconds_since_epoch =
      static_cast<int64_t>(days_from_civil(year, month, day)) * 86400;
  return true;
}

inline bool parse_hh(const char* s, int64_t* seconds_since_midnight) {
  uint8_t hour = 0;
  if (NEUG_UNLIKELY(!parse_2digits(s, &hour)))
    return false;
  if (NEUG_UNLIKELY(hour > 23))
    return false;
  *seconds_since_midnight = static_cast<int64_t>(hour) * 3600;
  return true;
}

inline bool parse_hh_mm(const char* s, int64_t* seconds_since_midnight) {
  if (NEUG_UNLIKELY(s[2] != ':'))
    return false;
  uint8_t hour = 0, minute = 0;
  if (NEUG_UNLIKELY(!parse_2digits(s, &hour)))
    return false;
  if (NEUG_UNLIKELY(!parse_2digits(s + 3, &minute)))
    return false;
  if (NEUG_UNLIKELY(hour > 23 || minute > 59))
    return false;
  *seconds_since_midnight =
      static_cast<int64_t>(hour) * 3600 + static_cast<int64_t>(minute) * 60;
  return true;
}

inline bool parse_hhmm_compact(const char* s, int64_t* seconds_since_midnight) {
  uint8_t hour = 0, minute = 0;
  if (NEUG_UNLIKELY(!parse_2digits(s, &hour)))
    return false;
  if (NEUG_UNLIKELY(!parse_2digits(s + 2, &minute)))
    return false;
  if (NEUG_UNLIKELY(hour > 23 || minute > 59))
    return false;
  *seconds_since_midnight =
      static_cast<int64_t>(hour) * 3600 + static_cast<int64_t>(minute) * 60;
  return true;
}

inline bool parse_hh_mm_ss(const char* s, int64_t* seconds_since_midnight) {
  if (NEUG_UNLIKELY(s[2] != ':' || s[5] != ':'))
    return false;
  uint8_t hour = 0, minute = 0, second = 0;
  if (NEUG_UNLIKELY(!parse_2digits(s, &hour)))
    return false;
  if (NEUG_UNLIKELY(!parse_2digits(s + 3, &minute)))
    return false;
  if (NEUG_UNLIKELY(!parse_2digits(s + 6, &second)))
    return false;
  if (NEUG_UNLIKELY(hour > 23 || minute > 59 || second > 59))
    return false;
  *seconds_since_midnight = static_cast<int64_t>(hour) * 3600 +
                            static_cast<int64_t>(minute) * 60 +
                            static_cast<int64_t>(second);
  return true;
}

inline bool parse_subseconds(const char* s, size_t length, TimestampUnit unit,
                             int64_t* subseconds_out) {
  if (NEUG_UNLIKELY(length == 0 || length > 9))
    return false;

  size_t max_digits = 0;
  switch (unit) {
  case TimestampUnit::kSecond:
    return false;
  case TimestampUnit::kMilli:
    max_digits = 3;
    break;
  case TimestampUnit::kMicro:
    max_digits = 6;
    break;
  case TimestampUnit::kNano:
    max_digits = 9;
    break;
  }
  if (NEUG_UNLIKELY(length > max_digits))
    return false;

  int64_t raw = 0;
  for (size_t i = 0; i < length; ++i) {
    if (NEUG_UNLIKELY(!is_digit(s[i])))
      return false;
    raw = raw * 10 + (s[i] - '0');
  }
  for (size_t i = length; i < max_digits; ++i) {
    raw *= 10;
  }
  *subseconds_out = raw;
  return true;
}

inline bool parse_zone_offset(const char* s, size_t* length_io,
                              int64_t* zone_offset) {
  size_t length = *length_io;
  if (length == 0)
    return true;

  if (s[length - 1] == 'Z') {
    *length_io = length - 1;
    return true;
  }
  if (length >= 6 && (s[length - 6] == '+' || s[length - 6] == '-') &&
      s[length - 3] == ':') {
    int sign = s[length - 6] == '+' ? -1 : 1;
    int64_t offset_seconds = 0;
    if (NEUG_UNLIKELY(!parse_hh_mm(s + length - 5, &offset_seconds)))
      return false;
    *zone_offset = offset_seconds * sign;
    *length_io = length - 6;
    return true;
  }
  if (length >= 5 && (s[length - 5] == '+' || s[length - 5] == '-')) {
    int sign = s[length - 5] == '+' ? -1 : 1;
    int64_t offset_seconds = 0;
    if (NEUG_UNLIKELY(!parse_hhmm_compact(s + length - 4, &offset_seconds)))
      return false;
    *zone_offset = offset_seconds * sign;
    *length_io = length - 5;
    return true;
  }
  if (length >= 3 && (s[length - 3] == '+' || s[length - 3] == '-')) {
    int sign = s[length - 3] == '+' ? -1 : 1;
    int64_t offset_seconds = 0;
    if (NEUG_UNLIKELY(!parse_hh(s + length - 2, &offset_seconds)))
      return false;
    *zone_offset = offset_seconds * sign;
    *length_io = length - 3;
    return true;
  }
  return true;
}

}  // namespace detail

/// Parse ISO-like timestamp strings into the requested unit.
inline bool parse_timestamp(const char* s, size_t length, TimestampUnit unit,
                            int64_t* out) {
  if (NEUG_UNLIKELY(length < 10))
    return false;

  int64_t seconds_since_epoch = 0;
  if (NEUG_UNLIKELY(!detail::parse_yyyy_mm_dd(s, &seconds_since_epoch)))
    return false;

  if (length == 10) {
    *out = detail::cast_seconds_to_unit(unit, seconds_since_epoch);
    return true;
  }

  if (NEUG_UNLIKELY(s[10] != ' ' && s[10] != 'T'))
    return false;

  size_t trimmed_length = length;
  int64_t zone_offset = 0;
  if (NEUG_UNLIKELY(
          !detail::parse_zone_offset(s, &trimmed_length, &zone_offset)))
    return false;

  int64_t seconds_since_midnight = 0;
  switch (trimmed_length) {
  case 13:
    if (NEUG_UNLIKELY(!detail::parse_hh(s + 11, &seconds_since_midnight)))
      return false;
    break;
  case 16:
    if (NEUG_UNLIKELY(!detail::parse_hh_mm(s + 11, &seconds_since_midnight)))
      return false;
    break;
  case 19:
  case 21:
  case 22:
  case 23:
  case 24:
  case 25:
  case 26:
  case 27:
  case 28:
  case 29:
    if (NEUG_UNLIKELY(!detail::parse_hh_mm_ss(s + 11, &seconds_since_midnight)))
      return false;
    break;
  default:
    return false;
  }

  seconds_since_epoch += seconds_since_midnight + zone_offset;

  if (trimmed_length <= 19) {
    *out = detail::cast_seconds_to_unit(unit, seconds_since_epoch);
    return true;
  }

  if (NEUG_UNLIKELY(s[19] != '.'))
    return false;

  int64_t subseconds = 0;
  if (NEUG_UNLIKELY(!detail::parse_subseconds(s + 20, trimmed_length - 20, unit,
                                              &subseconds)))
    return false;

  *out = detail::cast_seconds_to_unit(unit, seconds_since_epoch) + subseconds;
  return true;
}

/// Parse epoch-seconds format: epoch seconds + 3-digit subseconds suffix.
inline bool parse_epoch_timestamp(const char* s, size_t length,
                                  TimestampUnit unit, int64_t* out) {
  if (NEUG_UNLIKELY(length < 4))
    return false;

  size_t sec_len = length - 3;
  int64_t seconds = 0;
  for (size_t i = 0; i < sec_len; ++i) {
    if (NEUG_UNLIKELY(!detail::is_digit(s[i])))
      return false;
    seconds = seconds * 10 + (s[i] - '0');
  }

  int64_t subseconds = 0;
  if (NEUG_UNLIKELY(
          !detail::parse_subseconds(s + sec_len, 3, unit, &subseconds)))
    return false;

  *out = detail::cast_seconds_to_unit(unit, seconds) + subseconds;
  return true;
}

inline bool parse_timestamp_ms(const char* s, size_t length, int64_t* out_ms) {
  return parse_timestamp(s, length, TimestampUnit::kMilli, out_ms);
}

inline bool parse_epoch_timestamp_ms(const char* s, size_t length,
                                     int64_t* out_ms) {
  return parse_epoch_timestamp(s, length, TimestampUnit::kMilli, out_ms);
}

}  // namespace utils
}  // namespace neug
