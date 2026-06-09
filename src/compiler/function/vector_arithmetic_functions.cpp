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

#include "neug/compiler/function/arithmetic/vector_arithmetic_functions.h"

#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/common/types/date_t.h"
#include "neug/compiler/common/types/int128_t.h"
#include "neug/compiler/common/types/interval_t.h"
#include "neug/compiler/common/types/timestamp_t.h"
#include "neug/compiler/function/arithmetic/abs.h"
#include "neug/compiler/function/arithmetic/add.h"
#include "neug/compiler/function/arithmetic/arithmetic_functions.h"
#include "neug/compiler/function/arithmetic/divide.h"
#include "neug/compiler/function/arithmetic/modulo.h"
#include "neug/compiler/function/arithmetic/multiply.h"
#include "neug/compiler/function/arithmetic/negate.h"
#include "neug/compiler/function/arithmetic/subtract.h"
#include "neug/compiler/function/list/functions/list_concat_function.h"
#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/compiler/function/string/vector_string_functions.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;

namespace neug {
namespace function {

template <typename FUNC>
static std::unique_ptr<ScalarFunction> getUnaryFunction(
    std::string name, LogicalTypeID operandTypeID) {
  function::scalar_func_exec_t execFunc;
  common::TypeUtils::visit(
      LogicalType(operandTypeID),
      [&]<NumericTypes T>(T) {
        execFunc = ScalarFunction::UnaryExecFunction<T, T, FUNC>;
      },
      [](auto) { NEUG_UNREACHABLE; });
  return std::make_unique<ScalarFunction>(
      std::move(name), std::vector<LogicalTypeID>{operandTypeID}, operandTypeID,
      execFunc);
}

template <typename FUNC, typename OPERAND_TYPE,
          typename RETURN_TYPE = OPERAND_TYPE>
static std::unique_ptr<ScalarFunction> getUnaryFunction(
    std::string name, LogicalTypeID operandTypeID, LogicalTypeID resultTypeID) {
  return std::make_unique<ScalarFunction>(
      std::move(name), std::vector<LogicalTypeID>{operandTypeID}, resultTypeID,
      ScalarFunction::UnaryExecFunction<OPERAND_TYPE, RETURN_TYPE, FUNC>);
}

template <typename FUNC>
static std::unique_ptr<ScalarFunction> getBinaryFunction(
    std::string name, common::LogicalTypeID operandTypeID) {
  function::scalar_func_exec_t execFunc;
  common::TypeUtils::visit(
      common::LogicalType(operandTypeID),
      [&]<common::NumericTypes T>(T) {
        execFunc = ScalarFunction::BinaryExecFunction<T, T, T, FUNC>;
      },
      [](auto) { NEUG_UNREACHABLE; });
  return std::make_unique<ScalarFunction>(
      std::move(name),
      std::vector<common::LogicalTypeID>{operandTypeID, operandTypeID},
      operandTypeID, execFunc);
}

template <typename FUNC, typename OPERAND_TYPE,
          typename RETURN_TYPE = OPERAND_TYPE>
static std::unique_ptr<ScalarFunction> getBinaryFunction(
    std::string name, LogicalTypeID operandTypeID, LogicalTypeID resultTypeID) {
  return std::make_unique<ScalarFunction>(
      std::move(name), std::vector<LogicalTypeID>{operandTypeID, operandTypeID},
      resultTypeID,
      ScalarFunction::BinaryExecFunction<OPERAND_TYPE, OPERAND_TYPE,
                                         RETURN_TYPE, FUNC>);
}

function_set AddFunction::getFunctionSet() {
  function_set result;
  for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
    result.push_back(getBinaryFunction<Add>(name, typeID));
  }

  // list + list -> list
  auto func = std::make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::LIST},
      LogicalTypeID::LIST,
      ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
                                                   list_entry_t, ListConcat>);
  // interval + interval → interval
  result.push_back(getBinaryFunction<Add, interval_t, interval_t>(
      name, LogicalTypeID::INTERVAL, LogicalTypeID::INTERVAL));
  // date + int → date
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INT64},
      LogicalTypeID::DATE,
      ScalarFunction::BinaryExecFunction<date_t, int64_t, date_t, Add>));
  // int + date → date
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::INT64, LogicalTypeID::DATE},
      LogicalTypeID::DATE,
      ScalarFunction::BinaryExecFunction<int64_t, date_t, date_t, Add>));
  // date + interval → date
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INTERVAL},
      LogicalTypeID::DATE,
      ScalarFunction::BinaryExecFunction<date_t, interval_t, date_t, Add>));
  // interval + date → date
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::DATE},
      LogicalTypeID::DATE,
      ScalarFunction::BinaryExecFunction<interval_t, date_t, date_t, Add>));
  // timestamp + interval → timestamp
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP,
                                 LogicalTypeID::INTERVAL},
      LogicalTypeID::TIMESTAMP,
      ScalarFunction::BinaryExecFunction<neug::common::timestamp_t, interval_t,
                                         neug::common::timestamp_t, Add>));
  // interval + timestamp → timestamp
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL,
                                 LogicalTypeID::TIMESTAMP},
      LogicalTypeID::TIMESTAMP,
      ScalarFunction::BinaryExecFunction<interval_t, neug::common::timestamp_t,
                                         neug::common::timestamp_t, Add>));
  return result;
}

function_set SubtractFunction::getFunctionSet() {
  function_set result;
  for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
    result.push_back(getBinaryFunction<Subtract>(name, typeID));
  }
  // date - date → interval
  result.push_back(getBinaryFunction<Subtract, date_t, int64_t>(
      name, LogicalTypeID::DATE, LogicalTypeID::INTERVAL));
  // date - integer → date
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INT64},
      LogicalTypeID::DATE,
      ScalarFunction::BinaryExecFunction<date_t, int64_t, date_t, Subtract>));
  // date - interval → date
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INTERVAL},
      LogicalTypeID::DATE,
      ScalarFunction::BinaryExecFunction<date_t, interval_t, date_t,
                                         Subtract>));
  // timestamp - timestamp → interval
  result.push_back(
      getBinaryFunction<Subtract, neug::common::timestamp_t, interval_t>(
          name, LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL));
  // timestamp - interval → timestamp
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP,
                                 LogicalTypeID::INTERVAL},
      LogicalTypeID::TIMESTAMP,
      ScalarFunction::BinaryExecFunction<neug::common::timestamp_t, interval_t,
                                         neug::common::timestamp_t, Subtract>));
  // interval - interval → interval
  result.push_back(getBinaryFunction<Subtract, interval_t, interval_t>(
      name, LogicalTypeID::INTERVAL, LogicalTypeID::INTERVAL));
  return result;
}

function_set MultiplyFunction::getFunctionSet() {
  function_set result;
  for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
    result.push_back(getBinaryFunction<Multiply>(name, typeID));
  }
  return result;
}

function_set DivideFunction::getFunctionSet() {
  function_set result;
  for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
    result.push_back(getBinaryFunction<Divide>(name, typeID));
  }
  // interval / int → interval
  result.push_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::INT64},
      LogicalTypeID::INTERVAL,
      ScalarFunction::BinaryExecFunction<interval_t, int64_t, interval_t,
                                         Divide>));
  return result;
}

function_set ModuloFunction::getFunctionSet() {
  function_set result;
  for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
    result.push_back(getBinaryFunction<Modulo>(name, typeID));
  }
  return result;
}

function_set PowerFunction::getFunctionSet() {
  function_set result;
  // double ^ double -> double
  result.push_back(getBinaryFunction<Power, double>(name, LogicalTypeID::DOUBLE,
                                                    LogicalTypeID::DOUBLE));
  return result;
}

function_set AbsFunction::getFunctionSet() {
  function_set result;
  for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
    result.push_back(getUnaryFunction<Abs>(name, typeID));
  }
  return result;
}

function_set NegateFunction::getFunctionSet() {
  function_set result;
  for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
    result.push_back(getUnaryFunction<Negate>(name, typeID));
  }
  return result;
}

}  // namespace function
}  // namespace neug
