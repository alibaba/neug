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

using namespace neug::common;

namespace neug {
namespace function {

function_set DatePartFunction::getFunctionSet() {
  function_set result;
  result.push_back(make_unique<ScalarFunction>(
      name, std::vector<DataTypeId>{DataTypeId::kVarchar, DataTypeId::kDate},
      DataTypeId::kInt64,
      ScalarFunction::BinaryExecFunction<neug_string_t, date_t, int64_t,
                                         DatePart>));
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<DataTypeId>{DataTypeId::kVarchar, DataTypeId::kTimestampMs},
      DataTypeId::kInt64,
      ScalarFunction::BinaryExecFunction<
          neug_string_t, neug::common::timestamp_t, int64_t, DatePart>));
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<DataTypeId>{DataTypeId::kVarchar, DataTypeId::kInterval},
      DataTypeId::kInt64,
      ScalarFunction::BinaryExecFunction<neug_string_t, interval_t, int64_t,
                                         DatePart>));
  result.push_back(make_unique<ScalarFunction>(
      name, std::vector<DataTypeId>{DataTypeId::kVarchar, DataTypeId::kDate},
      DataTypeId::kInt64,
      ScalarFunction::BinaryExecFunction<neug_string_t, interval_t, int64_t,
                                         DatePart>));
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<DataTypeId>{DataTypeId::kVarchar, DataTypeId::kTimestampMs},
      DataTypeId::kInt64,
      ScalarFunction::BinaryExecFunction<neug_string_t, interval_t, int64_t,
                                         DatePart>));
  return result;
}

}  // namespace function
}  // namespace neug
