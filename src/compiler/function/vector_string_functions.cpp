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

#include "neug/compiler/function/string/vector_string_functions.h"

#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/compiler/function/string/functions/array_extract_function.h"

#include "neug/execution/common/types/value.h"

using namespace gs::common;

namespace gs {
namespace function {

function_set ContainsFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
      LogicalTypeID::BOOL, nullptr, nullptr));
  return functionSet;
}

function_set EndsWithFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
      LogicalTypeID::BOOL, nullptr, nullptr));
  return functionSet;
}

function_set StartsWithFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
      LogicalTypeID::BOOL, nullptr, nullptr));
  return functionSet;
}

function_set UpperFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(std::make_unique<NeugScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
      LogicalTypeID::STRING, UpperFunction::Exec));
  return functionSet;
}

runtime::Value UpperFunction::Exec(const std::vector<runtime::Value>& args) {
  if (args.size() != 1) {
    THROW_RUNTIME_ERROR("UPPER: expect exactly 1 argument, got " +
                        std::to_string(args.size()));
  }
  const auto& val = args[0];
  if (val.type().id() != DataTypeId::kVarchar) {
    THROW_RUNTIME_ERROR("UPPER: input value is not a string");
  }
  std::string str(runtime::StringValue::Get(val));
  std::transform(str.begin(), str.end(), str.begin(), ::toupper);
  return runtime::Value::STRING(str);
}

function_set LowerFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(std::make_unique<NeugScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
      LogicalTypeID::STRING, LowerFunction::Exec));
  return functionSet;
}

runtime::Value LowerFunction::Exec(const std::vector<runtime::Value>& args) {
  if (args.size() != 1) {
    THROW_RUNTIME_ERROR("LOWER: expect exactly 1 argument, got " +
                        std::to_string(args.size()));
  }
  const auto& val = args[0];
  if (val.type().id() != DataTypeId::kVarchar) {
    THROW_RUNTIME_ERROR("LOWER: input value is not a string");
  }
  std::string str(runtime::StringValue::Get(val));
  std::transform(str.begin(), str.end(), str.begin(), ::tolower);
  return runtime::Value::STRING(str);
}

function_set ReverseFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(std::make_unique<NeugScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
      LogicalTypeID::STRING, ReverseFunction::Exec));
  return functionSet;
}

runtime::Value ReverseFunction::Exec(const std::vector<runtime::Value>& args) {
  if (args.size() != 1) {
    THROW_RUNTIME_ERROR("REVERSE: expect exactly 1 argument, got " +
                        std::to_string(args.size()));
  }
  const auto& val = args[0];
  if (val.type().id() != DataTypeId::kVarchar) {
    THROW_RUNTIME_ERROR("REVERSE: input value is not a string");
  }
  std::string str(runtime::StringValue::Get(val));
  std::reverse(str.begin(), str.end());
  return runtime::Value::STRING(str);
}

}  // namespace function
}  // namespace gs
