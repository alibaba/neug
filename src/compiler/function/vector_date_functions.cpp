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

#include "neug/compiler/function/date/vector_date_functions.h"

#include "neug/compiler/function/date/date_functions.h"
#include "neug/compiler/function/scalar_function.h"

using namespace gs::common;

namespace gs {
namespace function {

function_set DatePartFunction::getFunctionSet() {
  function_set result;
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::DATE},
      LogicalTypeID::INT64,
      ScalarFunction::BinaryExecFunction<neug_string_t, date_t, int64_t,
                                         DatePart>));
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING,
                                 LogicalTypeID::TIMESTAMP},
      LogicalTypeID::INT64,
      ScalarFunction::BinaryExecFunction<neug_string_t, gs::common::timestamp_t,
                                         int64_t, DatePart>));
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING,
                                 LogicalTypeID::INTERVAL},
      LogicalTypeID::INT64,
      ScalarFunction::BinaryExecFunction<neug_string_t, interval_t, int64_t,
                                         DatePart>));
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::DATE32},
      LogicalTypeID::INT64,
      ScalarFunction::BinaryExecFunction<neug_string_t, interval_t, int64_t,
                                         DatePart>));
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING,
                                 LogicalTypeID::TIMESTAMP64},
      LogicalTypeID::INT64,
      ScalarFunction::BinaryExecFunction<neug_string_t, interval_t, int64_t,
                                         DatePart>));
  return result;
}

function_set DateTruncFunction::getFunctionSet() {
  function_set result;
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::DATE},
      LogicalTypeID::DATE,
      ScalarFunction::BinaryExecFunction<neug_string_t, date_t, date_t,
                                         DateTrunc>));
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING,
                                 LogicalTypeID::TIMESTAMP},
      LogicalTypeID::TIMESTAMP,
      ScalarFunction::BinaryExecFunction<neug_string_t, gs::common::timestamp_t,
                                         gs::common::timestamp_t, DateTrunc>));
  return result;
}

function_set DayNameFunction::getFunctionSet() {
  function_set result;
  result.push_back(make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::DATE},
      LogicalTypeID::STRING,
      ScalarFunction::UnaryExecFunction<date_t, neug_string_t, DayName>));
  result.push_back(make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP},
      LogicalTypeID::STRING,
      ScalarFunction::UnaryExecFunction<gs::common::timestamp_t, neug_string_t,
                                        DayName>));
  return result;
}

function_set GreatestFunction::getFunctionSet() {
  function_set result;
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::DATE},
      LogicalTypeID::DATE,
      ScalarFunction::BinaryExecFunction<date_t, date_t, date_t, Greatest>));
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP,
                                 LogicalTypeID::TIMESTAMP},
      LogicalTypeID::TIMESTAMP,
      ScalarFunction::BinaryExecFunction<gs::common::timestamp_t,
                                         gs::common::timestamp_t,
                                         gs::common::timestamp_t, Greatest>));
  return result;
}

function_set LastDayFunction::getFunctionSet() {
  function_set result;
  result.push_back(make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::DATE},
      LogicalTypeID::DATE,
      ScalarFunction::UnaryExecFunction<date_t, date_t, LastDay>));
  result.push_back(make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP},
      LogicalTypeID::DATE,
      ScalarFunction::UnaryExecFunction<gs::common::timestamp_t, date_t,
                                        LastDay>));
  return result;
}

function_set LeastFunction::getFunctionSet() {
  function_set result;
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::DATE},
      LogicalTypeID::DATE,
      ScalarFunction::BinaryExecFunction<date_t, date_t, date_t, Least>));
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP,
                                 LogicalTypeID::TIMESTAMP},
      LogicalTypeID::TIMESTAMP,
      ScalarFunction::BinaryExecFunction<gs::common::timestamp_t,
                                         gs::common::timestamp_t,
                                         gs::common::timestamp_t, Least>));
  return result;
}

function_set MakeDateFunction::getFunctionSet() {
  function_set result;
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::INT64, LogicalTypeID::INT64,
                                 LogicalTypeID::INT64},
      LogicalTypeID::DATE,
      ScalarFunction::TernaryExecFunction<int64_t, int64_t, int64_t, date_t,
                                          MakeDate>));
  return result;
}

function_set MonthNameFunction::getFunctionSet() {
  function_set result;
  result.push_back(make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::DATE},
      LogicalTypeID::STRING,
      ScalarFunction::UnaryExecFunction<date_t, neug_string_t, MonthName>));
  result.push_back(make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP},
      LogicalTypeID::STRING,
      ScalarFunction::UnaryExecFunction<gs::common::timestamp_t, neug_string_t,
                                        MonthName>));
  return result;
}

function_set CurrentDateFunction::getFunctionSet() {
  function_set result;
  result.push_back(make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{}, LogicalTypeID::DATE,
      ScalarFunction::NullaryAuxilaryExecFunction<date_t, CurrentDate>));
  return result;
}

function_set CurrentTimestampFunction::getFunctionSet() {
  function_set result;
  result.push_back(make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{}, LogicalTypeID::TIMESTAMP,
      ScalarFunction::NullaryAuxilaryExecFunction<gs::common::timestamp_tz_t,
                                                  CurrentTimestamp>));
  return result;
}

}  // namespace function
}  // namespace gs
