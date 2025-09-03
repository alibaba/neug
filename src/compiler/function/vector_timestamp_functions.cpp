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

#include "neug/compiler/function/timestamp/vector_timestamp_functions.h"

#include "neug/compiler/function/scalar_function.h"
#include "neug/compiler/function/timestamp/timestamp_function.h"

using namespace gs::common;

namespace gs {
namespace function {

function_set CenturyFunction::getFunctionSet() {
  function_set result;
  result.push_back(make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP},
      LogicalTypeID::INT64,
      ScalarFunction::UnaryExecFunction<timestamp_t, int64_t, Century>));
  return result;
}

function_set EpochMsFunction::getFunctionSet() {
  function_set result;
  result.push_back(make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::INT64},
      LogicalTypeID::TIMESTAMP,
      ScalarFunction::UnaryExecFunction<int64_t, timestamp_t, EpochMs>));
  return result;
}

function_set ToTimestampFunction::getFunctionSet() {
  function_set result;
  result.push_back(make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::DOUBLE},
      LogicalTypeID::TIMESTAMP,
      ScalarFunction::UnaryExecFunction<double, timestamp_t, ToTimestamp>));
  return result;
}

}  // namespace function
}  // namespace gs
