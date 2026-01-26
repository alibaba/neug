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

#include "date_t.h"
#include "dtime_t.h"

namespace neug {
namespace common {

// Type used to represent timestamps (value is in microseconds since 1970-01-01)
struct NEUG_API timestamp_t {
  int64_t value = 0;

  timestamp_t();
  explicit timestamp_t(int64_t value_p);
  timestamp_t& operator=(int64_t value_p);

  // explicit conversion
  explicit operator int64_t() const;

  // Comparison operators with timestamp_t.
  bool operator==(const timestamp_t& rhs) const;
  bool operator!=(const timestamp_t& rhs) const;
  bool operator<=(const timestamp_t& rhs) const;
  bool operator<(const timestamp_t& rhs) const;
  bool operator>(const timestamp_t& rhs) const;
  bool operator>=(const timestamp_t& rhs) const;

  // Comparison operators with date_t.
  bool operator==(const date_t& rhs) const;
  bool operator!=(const date_t& rhs) const;
  bool operator<(const date_t& rhs) const;
  bool operator<=(const date_t& rhs) const;
  bool operator>(const date_t& rhs) const;
  bool operator>=(const date_t& rhs) const;

  // arithmetic operator
  timestamp_t operator+(const interval_t& interval) const;
  timestamp_t operator-(const interval_t& interval) const;

  interval_t operator-(const timestamp_t& rhs) const;
};

struct timestamp_tz_t : public timestamp_t {  // NO LINT
  using timestamp_t::timestamp_t;
};
struct timestamp_ns_t : public timestamp_t {  // NO LINT
  using timestamp_t::timestamp_t;
};
struct timestamp_ms_t : public timestamp_t {  // NO LINT
  using timestamp_t::timestamp_t;
};
struct timestamp_sec_t : public timestamp_t {  // NO LINT
  using timestamp_t::timestamp_t;
};

// Note: Aside from some minor changes, this implementation is copied from
// DuckDB's source code:
// https://github.com/duckdb/duckdb/blob/master/src/include/duckdb/common/types/timestamp.hpp.
// https://github.com/duckdb/duckdb/blob/master/src/common/types/timestamp.cpp.
// For example, instead of using their idx_t type to refer to indices, we
// directly use uint64_t, which is the actual type of idx_t (so we say uint64_t
// len instead of idx_t len). When more functionality is needed, we should first
// consult these DuckDB links.

// The Timestamp class is a static class that holds helper functions for the
// Timestamp type. timestamp/datetime uses 64 bits, high 32 bits for date and
// low 32 bits for time
class Timestamp {
 public:
  NEUG_API static timestamp_t fromCString(const char* str, uint64_t len);

  // Convert a timestamp object to a std::string in the format "YYYY-MM-DD
  // hh:mm:ss".
  NEUG_API static std::string toString(timestamp_t timestamp);

  NEUG_API static date_t getDate(timestamp_t timestamp);

  NEUG_API static dtime_t getTime(timestamp_t timestamp);

  // Create a Timestamp object from a specified (date, time) combination.
  NEUG_API static timestamp_t fromDateTime(date_t date, dtime_t time);

  NEUG_API static bool tryConvertTimestamp(const char* str, uint64_t len,
                                           timestamp_t& result);

  // Extract the date and time from a given timestamp object.
  NEUG_API static void convert(timestamp_t timestamp, date_t& out_date,
                               dtime_t& out_time);

  // Create a Timestamp object from the specified epochMs.
  NEUG_API static timestamp_t fromEpochMicroSeconds(int64_t epochMs);

  // Create a Timestamp object from the specified epochMs.
  NEUG_API static timestamp_t fromEpochMilliSeconds(int64_t ms);

  // Create a Timestamp object from the specified epochSec.
  NEUG_API static timestamp_t fromEpochSeconds(int64_t sec);

  // Create a Timestamp object from the specified epochNs.
  NEUG_API static timestamp_t fromEpochNanoSeconds(int64_t ns);

  NEUG_API static int32_t getTimestampPart(DatePartSpecifier specifier,
                                           timestamp_t timestamp);

  NEUG_API static timestamp_t trunc(DatePartSpecifier specifier,
                                    timestamp_t date);

  NEUG_API static int64_t getEpochNanoSeconds(const timestamp_t& timestamp);

  NEUG_API static int64_t getEpochMilliSeconds(const timestamp_t& timestamp);

  NEUG_API static int64_t getEpochSeconds(const timestamp_t& timestamp);

  NEUG_API static bool tryParseUTCOffset(const char* str, uint64_t& pos,
                                         uint64_t len, int& hour_offset,
                                         int& minute_offset);

  static std::string getTimestampConversionExceptionMsg(
      const char* str, uint64_t len, const std::string& typeID = "TIMESTAMP") {
    return "Error occurred during parsing " + typeID + ". Given: \"" +
           std::string(str, len) +
           "\". Expected format: (YYYY-MM-DD hh:mm:ss[.zzzzzz][+-TT[:tt]])";
  }

  NEUG_API static timestamp_t getCurrentTimestamp();
};

}  // namespace common
}  // namespace neug
