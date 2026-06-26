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

#include "neug/utils/datetime_parsers.h"

#include <date/date.h>
#include <cctype>
#include <chrono>

namespace neug {
namespace utils {
namespace {

using seconds_type = std::chrono::duration<int64_t>;

int64_t cast_seconds_to_unit(TimestampUnit unit, int64_t seconds) {
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

bool parse_digits(const char* s, size_t length, uint64_t* out) {
  if (length == 0) {
    return false;
  }
  uint64_t value = 0;
  for (size_t i = 0; i < length; ++i) {
    if (!std::isdigit(static_cast<unsigned char>(s[i]))) {
      return false;
    }
    value = value * 10 + static_cast<uint64_t>(s[i] - '0');
  }
  *out = value;
  return true;
}

bool parse_yyyy_mm_dd(const char* s, int64_t* seconds_since_epoch) {
  if (s[4] != '-' || s[7] != '-') {
    return false;
  }
  for (int i : {0, 1, 2, 3, 5, 6, 8, 9}) {
    if (!std::isdigit(static_cast<unsigned char>(s[i]))) {
      return false;
    }
  }
  int year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 +
             (s[3] - '0');
  int month = (s[5] - '0') * 10 + (s[6] - '0');
  int day = (s[8] - '0') * 10 + (s[9] - '0');
  date::year_month_day ymd{date::year{year},
                           date::month{static_cast<unsigned>(month)},
                           date::day{static_cast<unsigned>(day)}};
  if (!ymd.ok()) {
    return false;
  }
  auto days = date::sys_days{ymd};
  *seconds_since_epoch =
      std::chrono::duration_cast<seconds_type>(days.time_since_epoch()).count();
  return true;
}

bool parse_hh(const char* s, int64_t* seconds_since_midnight) {
  if (!std::isdigit(static_cast<unsigned char>(s[0])) ||
      !std::isdigit(static_cast<unsigned char>(s[1]))) {
    return false;
  }
  int hour = (s[0] - '0') * 10 + (s[1] - '0');
  if (hour > 23) {
    return false;
  }
  *seconds_since_midnight = hour * 3600;
  return true;
}

bool parse_hh_mm(const char* s, int64_t* seconds_since_midnight) {
  if (s[2] != ':') {
    return false;
  }
  if (!parse_hh(s, seconds_since_midnight)) {
    return false;
  }
  if (!std::isdigit(static_cast<unsigned char>(s[3])) ||
      !std::isdigit(static_cast<unsigned char>(s[4]))) {
    return false;
  }
  int minute = (s[3] - '0') * 10 + (s[4] - '0');
  if (minute > 59) {
    return false;
  }
  *seconds_since_midnight += minute * 60;
  return true;
}

bool parse_hhmm_compact(const char* s, int64_t* seconds_since_midnight) {
  if (!std::isdigit(static_cast<unsigned char>(s[0])) ||
      !std::isdigit(static_cast<unsigned char>(s[1])) ||
      !std::isdigit(static_cast<unsigned char>(s[2])) ||
      !std::isdigit(static_cast<unsigned char>(s[3]))) {
    return false;
  }
  int hour = (s[0] - '0') * 10 + (s[1] - '0');
  int minute = (s[2] - '0') * 10 + (s[3] - '0');
  if (hour > 23 || minute > 59) {
    return false;
  }
  *seconds_since_midnight = hour * 3600 + minute * 60;
  return true;
}

bool parse_hh_mm_ss(const char* s, int64_t* seconds_since_midnight) {
  if (s[5] != ':') {
    return false;
  }
  if (!parse_hh_mm(s, seconds_since_midnight)) {
    return false;
  }
  if (!std::isdigit(static_cast<unsigned char>(s[6])) ||
      !std::isdigit(static_cast<unsigned char>(s[7]))) {
    return false;
  }
  int second = (s[6] - '0') * 10 + (s[7] - '0');
  if (second > 59) {
    return false;
  }
  *seconds_since_midnight += second;
  return true;
}

bool parse_subseconds(const char* s, size_t length, TimestampUnit unit,
                      int64_t* subseconds_out) {
  if (length == 0 || length > 9) {
    return false;
  }
  uint64_t raw = 0;
  if (!parse_digits(s, length, &raw)) {
    return false;
  }
  size_t max_digits = 3;
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
  if (length > max_digits) {
    return false;
  }
  for (size_t i = length; i < max_digits; ++i) {
    raw *= 10;
  }
  *subseconds_out = static_cast<int64_t>(raw);
  return true;
}

bool parse_zone_offset(const char* s, size_t* length_io,
                       seconds_type* zone_offset) {
  size_t length = *length_io;
  if (length == 0) {
    return true;
  }
  if (s[length - 1] == 'Z') {
    *length_io = length - 1;
    return true;
  }
  if (length >= 6 && (s[length - 6] == '+' || s[length - 6] == '-') &&
      s[length - 3] == ':') {
    int sign = s[length - 6] == '+' ? -1 : 1;
    int64_t offset_seconds = 0;
    if (!parse_hh_mm(s + length - 5, &offset_seconds)) {
      return false;
    }
    *zone_offset = seconds_type(offset_seconds * sign);
    *length_io = length - 6;
    return true;
  }
  if (length >= 5 && (s[length - 5] == '+' || s[length - 5] == '-')) {
    int sign = s[length - 5] == '+' ? -1 : 1;
    int64_t offset_seconds = 0;
    if (!parse_hhmm_compact(s + length - 4, &offset_seconds)) {
      return false;
    }
    *zone_offset = seconds_type(offset_seconds * sign);
    *length_io = length - 5;
    return true;
  }
  if (length >= 3 && (s[length - 3] == '+' || s[length - 3] == '-')) {
    int sign = s[length - 3] == '+' ? -1 : 1;
    int64_t offset_seconds = 0;
    if (!parse_hh(s + length - 2, &offset_seconds)) {
      return false;
    }
    *zone_offset = seconds_type(offset_seconds * sign);
    *length_io = length - 3;
    return true;
  }
  return true;
}

}  // namespace

bool parse_epoch_timestamp(const char* s, size_t length, TimestampUnit unit,
                           int64_t* out) {
  if (length < 4) {
    return false;
  }
  uint64_t seconds = 0;
  if (!parse_digits(s, length - 3, &seconds)) {
    return false;
  }
  int64_t subseconds = 0;
  if (!parse_subseconds(s + length - 3, 3, unit, &subseconds)) {
    return false;
  }
  *out = cast_seconds_to_unit(unit, static_cast<int64_t>(seconds)) + subseconds;
  return true;
}

bool parse_timestamp(const char* s, size_t length, TimestampUnit unit,
                     int64_t* out) {
  if (length < 10) {
    return false;
  }

  int64_t seconds_since_epoch = 0;
  if (!parse_yyyy_mm_dd(s, &seconds_since_epoch)) {
    return false;
  }

  if (length == 10) {
    *out = cast_seconds_to_unit(unit, seconds_since_epoch);
    return true;
  }

  if (s[10] != ' ' && s[10] != 'T') {
    return false;
  }

  size_t trimmed_length = length;
  seconds_type zone_offset(0);
  if (!parse_zone_offset(s, &trimmed_length, &zone_offset)) {
    return false;
  }

  int64_t seconds_since_midnight = 0;
  switch (trimmed_length) {
  case 13:
    if (!parse_hh(s + 11, &seconds_since_midnight)) {
      return false;
    }
    break;
  case 16:
    if (!parse_hh_mm(s + 11, &seconds_since_midnight)) {
      return false;
    }
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
    if (!parse_hh_mm_ss(s + 11, &seconds_since_midnight)) {
      return false;
    }
    break;
  default:
    return false;
  }

  seconds_since_epoch += seconds_since_midnight + zone_offset.count();

  if (trimmed_length <= 19) {
    *out = cast_seconds_to_unit(unit, seconds_since_epoch);
    return true;
  }

  if (s[19] != '.') {
    return false;
  }

  int64_t subseconds = 0;
  if (!parse_subseconds(s + 20, trimmed_length - 20, unit, &subseconds)) {
    return false;
  }

  *out = cast_seconds_to_unit(unit, seconds_since_epoch) + subseconds;
  return true;
}

}  // namespace utils
}  // namespace neug
