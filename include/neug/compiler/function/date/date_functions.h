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

#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/types/date_t.h"
#include "neug/compiler/common/types/neug_string.h"
#include "neug/compiler/common/types/timestamp_t.h"

namespace gs {
namespace function {

struct DayName {
  template <class T>
  static inline void operation(T& /*input*/,
                               common::neug_string_t& /*result*/) {
    NEUG_UNREACHABLE;
  }
};

template <>
inline void DayName::operation(common::date_t& input,
                               common::neug_string_t& result) {
  std::string dayName = common::Date::getDayName(input);
  result.set(dayName);
}

template <>
inline void DayName::operation(common::timestamp_t& input,
                               common::neug_string_t& result) {
  common::dtime_t time{};
  common::date_t date{};
  common::Timestamp::convert(input, date, time);
  std::string dayName = common::Date::getDayName(date);
  result.set(dayName);
}

struct MonthName {
  template <class T>
  static inline void operation(T& /*input*/,
                               common::neug_string_t& /*result*/) {
    NEUG_UNREACHABLE;
  }
};

template <>
inline void MonthName::operation(common::date_t& input,
                                 common::neug_string_t& result) {
  std::string monthName = common::Date::getMonthName(input);
  result.set(monthName);
}

template <>
inline void MonthName::operation(common::timestamp_t& input,
                                 common::neug_string_t& result) {
  common::dtime_t time{};
  common::date_t date{};
  common::Timestamp::convert(input, date, time);
  std::string monthName = common::Date::getMonthName(date);
  result.set(monthName);
}

struct LastDay {
  template <class T>

  static inline void operation(T& /*input*/, common::date_t& /*result*/) {
    NEUG_UNREACHABLE;
  }
};

template <>
inline void LastDay::operation(common::date_t& input, common::date_t& result) {
  result = common::Date::getLastDay(input);
}

template <>
inline void LastDay::operation(common::timestamp_t& input,
                               common::date_t& result) {
  common::date_t date{};
  common::dtime_t time{};
  common::Timestamp::convert(input, date, time);
  result = common::Date::getLastDay(date);
}

struct DatePart {
  template <class LEFT_TYPE, class RIGHT_TYPE>
  static inline void operation(LEFT_TYPE& /*partSpecifier*/,
                               RIGHT_TYPE& /*input*/, int64_t& /*result*/) {
    NEUG_UNREACHABLE;
  }
};

template <>
inline void DatePart::operation(common::neug_string_t& partSpecifier,
                                common::date_t& input, int64_t& result) {
  common::DatePartSpecifier specifier{};
  common::Interval::tryGetDatePartSpecifier(partSpecifier.getAsString(),
                                            specifier);
  result = common::Date::getDatePart(specifier, input);
}

template <>
inline void DatePart::operation(common::neug_string_t& partSpecifier,
                                common::timestamp_t& input, int64_t& result) {
  common::DatePartSpecifier specifier{};
  common::Interval::tryGetDatePartSpecifier(partSpecifier.getAsString(),
                                            specifier);
  result = common::Timestamp::getTimestampPart(specifier, input);
}

template <>
inline void DatePart::operation(common::neug_string_t& partSpecifier,
                                common::interval_t& input, int64_t& result) {
  common::DatePartSpecifier specifier{};
  common::Interval::tryGetDatePartSpecifier(partSpecifier.getAsString(),
                                            specifier);
  result = common::Interval::getIntervalPart(specifier, input);
}

struct DateTrunc {
  template <class LEFT_TYPE, class RIGHT_TYPE>
  static inline void operation(LEFT_TYPE& /*partSpecifier*/,
                               RIGHT_TYPE& /*input*/, RIGHT_TYPE& /*result*/) {
    NEUG_UNREACHABLE;
  }
};

template <>
inline void DateTrunc::operation(common::neug_string_t& partSpecifier,
                                 common::date_t& input,
                                 common::date_t& result) {
  common::DatePartSpecifier specifier{};
  common::Interval::tryGetDatePartSpecifier(partSpecifier.getAsString(),
                                            specifier);
  result = common::Date::trunc(specifier, input);
}

template <>
inline void DateTrunc::operation(common::neug_string_t& partSpecifier,
                                 common::timestamp_t& input,
                                 common::timestamp_t& result) {
  common::DatePartSpecifier specifier{};
  common::Interval::tryGetDatePartSpecifier(partSpecifier.getAsString(),
                                            specifier);
  result = common::Timestamp::trunc(specifier, input);
}

struct Greatest {
  template <class T>
  static inline void operation(T& left, T& right, T& result) {
    result = left > right ? left : right;
  }
};

struct Least {
  template <class T>
  static inline void operation(T& left, T& right, T& result) {
    result = left > right ? right : left;
  }
};

struct MakeDate {
  static inline void operation(int64_t& year, int64_t& month, int64_t& day,
                               common::date_t& result) {
    result = common::Date::fromDate(year, month, day);
  }
};

struct CurrentDate {
  static void operation(common::date_t& result, void* dataPtr);
};

struct CurrentTimestamp {
  static void operation(common::timestamp_tz_t& result, void* dataPtr);
};

}  // namespace function
}  // namespace gs
