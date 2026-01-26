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

#include "interval_t.h"

namespace neug {

namespace regex {
class RE2;
}

namespace common {

struct timestamp_t;

// System representation of dates as the number of days since 1970-01-01.
struct NEUG_API date_t {
  int32_t days;

  date_t();
  explicit date_t(int32_t days_p);

  // Comparison operators with date_t.
  bool operator==(const date_t& rhs) const;
  bool operator!=(const date_t& rhs) const;
  bool operator<=(const date_t& rhs) const;
  bool operator<(const date_t& rhs) const;
  bool operator>(const date_t& rhs) const;
  bool operator>=(const date_t& rhs) const;

  // Comparison operators with timestamp_t.
  bool operator==(const timestamp_t& rhs) const;
  bool operator!=(const timestamp_t& rhs) const;
  bool operator<(const timestamp_t& rhs) const;
  bool operator<=(const timestamp_t& rhs) const;
  bool operator>(const timestamp_t& rhs) const;
  bool operator>=(const timestamp_t& rhs) const;

  // arithmetic operators
  date_t operator+(const int32_t& day) const;
  date_t operator-(const int32_t& day) const;

  date_t operator+(const interval_t& interval) const;
  date_t operator-(const interval_t& interval) const;

  int64_t operator-(const date_t& rhs) const;
};

inline date_t operator+(int64_t i, const date_t date) { return date + i; }

// Note: Aside from some minor changes, this implementation is copied from
// DuckDB's source code:
// https://github.com/duckdb/duckdb/blob/master/src/include/duckdb/common/types/date.hpp.
// https://github.com/duckdb/duckdb/blob/master/src/common/types/date.cpp.
// For example, instead of using their idx_t type to refer to indices, we
// directly use uint64_t, which is the actual type of idx_t (so we say uint64_t
// len instead of idx_t len). When more functionality is needed, we should first
// consult these DuckDB links.
class Date {
 public:
  NEUG_API static const int32_t NORMAL_DAYS[13];
  NEUG_API static const int32_t CUMULATIVE_DAYS[13];
  NEUG_API static const int32_t LEAP_DAYS[13];
  NEUG_API static const int32_t CUMULATIVE_LEAP_DAYS[13];
  NEUG_API static const int32_t CUMULATIVE_YEAR_DAYS[401];
  NEUG_API static const int8_t MONTH_PER_DAY_OF_YEAR[365];
  NEUG_API static const int8_t LEAP_MONTH_PER_DAY_OF_YEAR[366];

  NEUG_API constexpr static const int32_t MIN_YEAR = -290307;
  NEUG_API constexpr static const int32_t MAX_YEAR = 294247;
  NEUG_API constexpr static const int32_t EPOCH_YEAR = 1970;

  NEUG_API constexpr static const int32_t YEAR_INTERVAL = 400;
  NEUG_API constexpr static const int32_t DAYS_PER_YEAR_INTERVAL = 146097;
  constexpr static const char* BC_SUFFIX = " (BC)";

  // Convert a string in the format "YYYY-MM-DD" to a date object
  NEUG_API static date_t fromCString(const char* str, uint64_t len);
  // Convert a date object to a string in the format "YYYY-MM-DD"
  NEUG_API static std::string toString(date_t date);
  // Try to convert text in a buffer to a date; returns true if parsing was
  // successful
  NEUG_API static bool tryConvertDate(const char* buf, uint64_t len,
                                      uint64_t& pos, date_t& result,
                                      bool allowTrailing = false);

  // private:
  // Returns true if (year) is a leap year, and false otherwise
  NEUG_API static bool isLeapYear(int32_t year);
  // Returns true if the specified (year, month, day) combination is a valid
  // date
  NEUG_API static bool isValid(int32_t year, int32_t month, int32_t day);
  // Extract the year, month and day from a given date object
  NEUG_API static void convert(date_t date, int32_t& out_year,
                               int32_t& out_month, int32_t& out_day);
  // Create a Date object from a specified (year, month, day) combination
  NEUG_API static date_t fromDate(int32_t year, int32_t month, int32_t day);

  // Helper function to parse two digits from a string (e.g. "30" -> 30, "03" ->
  // 3, "3" -> 3)
  NEUG_API static bool parseDoubleDigit(const char* buf, uint64_t len,
                                        uint64_t& pos, int32_t& result);

  NEUG_API static int32_t monthDays(int32_t year, int32_t month);

  NEUG_API static std::string getDayName(date_t date);

  NEUG_API static std::string getMonthName(date_t date);

  NEUG_API static date_t getLastDay(date_t date);

  NEUG_API static int32_t getDatePart(DatePartSpecifier specifier, date_t date);

  NEUG_API static date_t trunc(DatePartSpecifier specifier, date_t date);

  NEUG_API static int64_t getEpochNanoSeconds(const date_t& date);

  NEUG_API static const regex::RE2& regexPattern();

 private:
  static void extractYearOffset(int32_t& n, int32_t& year,
                                int32_t& year_offset);
};

}  // namespace common
}  // namespace neug
