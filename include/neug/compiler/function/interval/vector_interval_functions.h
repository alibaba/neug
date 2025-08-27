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

#include "interval_functions.h"
#include "neug/compiler/function/scalar_function.h"

namespace gs {
namespace function {

struct IntervalFunction {
 public:
  template <class OPERATION>
  static function_set getUnaryIntervalFunction(std::string funcName) {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(
        funcName,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::INT64},
        common::LogicalTypeID::INTERVAL,
        ScalarFunction::UnaryExecFunction<int64_t, common::interval_t,
                                          OPERATION>));
    return result;
  }
};

struct ToYearsFunction {
  static constexpr const char* name = "TO_YEARS";

  static function_set getFunctionSet() {
    return IntervalFunction::getUnaryIntervalFunction<ToYears>(name);
  }
};

struct ToMonthsFunction {
  static constexpr const char* name = "TO_MONTHS";

  static function_set getFunctionSet() {
    return IntervalFunction::getUnaryIntervalFunction<ToMonths>(name);
  }
};

struct ToDaysFunction {
  static constexpr const char* name = "TO_DAYS";

  static function_set getFunctionSet() {
    return IntervalFunction::getUnaryIntervalFunction<ToDays>(name);
  }
};

struct ToHoursFunction {
  static constexpr const char* name = "TO_HOURS";

  static function_set getFunctionSet() {
    return IntervalFunction::getUnaryIntervalFunction<ToHours>(name);
  }
};

struct ToMinutesFunction {
  static constexpr const char* name = "TO_MINUTES";

  static function_set getFunctionSet() {
    return IntervalFunction::getUnaryIntervalFunction<ToMinutes>(name);
  }
};

struct ToSecondsFunction {
  static constexpr const char* name = "TO_SECONDS";

  static function_set getFunctionSet() {
    return IntervalFunction::getUnaryIntervalFunction<ToSeconds>(name);
  }
};

struct ToMillisecondsFunction {
  static constexpr const char* name = "TO_MILLISECONDS";

  static function_set getFunctionSet() {
    return IntervalFunction::getUnaryIntervalFunction<ToMilliseconds>(name);
  }
};

struct ToMicrosecondsFunction {
  static constexpr const char* name = "TO_MICROSECONDS";

  static function_set getFunctionSet() {
    return IntervalFunction::getUnaryIntervalFunction<ToMicroseconds>(name);
  }
};

}  // namespace function
}  // namespace gs
