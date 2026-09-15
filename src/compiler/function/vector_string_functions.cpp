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

#include <string_view>

#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/compiler/function/string/functions/array_extract_function.h"
#include "neug/execution/expression/exprs/logical_expr.h"

#include "neug/common/types/value.h"

using namespace neug::common;

namespace neug {
namespace function {

namespace {

std::string escapeRegexLiteral(std::string_view value) {
  constexpr std::string_view regexMetacharacters = R"(\.^$|()[]{}*+?)";
  std::string escaped;
  escaped.reserve(value.size());
  for (const auto character : value) {
    if (regexMetacharacters.find(character) != std::string_view::npos) {
      escaped.push_back('\\');
    }
    escaped.push_back(character);
  }
  return escaped;
}

neug::Value evaluateStringPredicate(const std::vector<neug::Value>& args,
                                    const std::string& prefix,
                                    const std::string& suffix,
                                    const std::string& functionName) {
  if (args.size() != 2) {
    THROW_RUNTIME_ERROR(functionName + ": expect exactly 2 arguments, got " +
                        std::to_string(args.size()));
  }
  if (args[0].IsNull() || args[1].IsNull()) {
    return neug::Value(DataType::BOOLEAN);
  }
  if (args[0].type().id() != DataTypeId::kVarchar ||
      args[1].type().id() != DataTypeId::kVarchar) {
    THROW_RUNTIME_ERROR(functionName + ": inputs must be strings");
  }
  auto pattern =
      prefix + escapeRegexLiteral(neug::StringValue::Get(args[1])) + suffix;
  return execution::evaluate_regex(args[0], neug::Value::STRING(pattern));
}

}  // namespace

function_set ContainsFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(make_unique<NeugScalarFunction>(
      name, std::vector<DataTypeId>{DataTypeId::kVarchar, DataTypeId::kVarchar},
      DataTypeId::kBoolean, ContainsFunction::Exec));
  return functionSet;
}

neug::Value ContainsFunction::Exec(const std::vector<neug::Value>& args) {
  return evaluateStringPredicate(args, ".*", ".*", name);
}

function_set EndsWithFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(make_unique<NeugScalarFunction>(
      name, std::vector<DataTypeId>{DataTypeId::kVarchar, DataTypeId::kVarchar},
      DataTypeId::kBoolean, EndsWithFunction::Exec));
  return functionSet;
}

neug::Value EndsWithFunction::Exec(const std::vector<neug::Value>& args) {
  return evaluateStringPredicate(args, ".*", "$", name);
}

function_set StartsWithFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(make_unique<NeugScalarFunction>(
      name, std::vector<DataTypeId>{DataTypeId::kVarchar, DataTypeId::kVarchar},
      DataTypeId::kBoolean, StartsWithFunction::Exec));
  return functionSet;
}

neug::Value StartsWithFunction::Exec(const std::vector<neug::Value>& args) {
  return evaluateStringPredicate(args, "^", ".*", name);
}

function_set UpperFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(std::make_unique<NeugScalarFunction>(
      name, std::vector<DataTypeId>{DataTypeId::kVarchar}, DataTypeId::kVarchar,
      UpperFunction::Exec));
  return functionSet;
}

neug::Value UpperFunction::Exec(const std::vector<neug::Value>& args) {
  if (args.size() != 1) {
    THROW_RUNTIME_ERROR("UPPER: expect exactly 1 argument, got " +
                        std::to_string(args.size()));
  }
  const auto& val = args[0];
  if (val.IsNull()) {
    return neug::Value(DataType(DataTypeId::kVarchar));
  }
  if (val.type().id() != DataTypeId::kVarchar) {
    THROW_RUNTIME_ERROR("UPPER: input value is not a string");
  }
  std::string str(neug::StringValue::Get(val));
  std::transform(str.begin(), str.end(), str.begin(), ::toupper);
  return neug::Value::STRING(str);
}

function_set LowerFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(std::make_unique<NeugScalarFunction>(
      name, std::vector<DataTypeId>{DataTypeId::kVarchar}, DataTypeId::kVarchar,
      LowerFunction::Exec));
  return functionSet;
}

neug::Value LowerFunction::Exec(const std::vector<neug::Value>& args) {
  if (args.size() != 1) {
    THROW_RUNTIME_ERROR("LOWER: expect exactly 1 argument, got " +
                        std::to_string(args.size()));
  }
  const auto& val = args[0];
  if (val.IsNull()) {
    return neug::Value(DataType(DataTypeId::kVarchar));
  }
  if (val.type().id() != DataTypeId::kVarchar) {
    THROW_RUNTIME_ERROR("LOWER: input value is not a string");
  }
  std::string str(neug::StringValue::Get(val));
  std::transform(str.begin(), str.end(), str.begin(), ::tolower);
  return neug::Value::STRING(str);
}

function_set ReverseFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(std::make_unique<NeugScalarFunction>(
      name, std::vector<DataTypeId>{DataTypeId::kVarchar}, DataTypeId::kVarchar,
      ReverseFunction::Exec));
  return functionSet;
}

neug::Value ReverseFunction::Exec(const std::vector<neug::Value>& args) {
  if (args.size() != 1) {
    THROW_RUNTIME_ERROR("REVERSE: expect exactly 1 argument, got " +
                        std::to_string(args.size()));
  }
  const auto& val = args[0];
  if (val.IsNull()) {
    return neug::Value(DataType(DataTypeId::kVarchar));
  }
  if (val.type().id() != DataTypeId::kVarchar) {
    THROW_RUNTIME_ERROR("REVERSE: input value is not a string");
  }
  std::string str(neug::StringValue::Get(val));
  std::reverse(str.begin(), str.end());
  return neug::Value::STRING(str);
}

}  // namespace function
}  // namespace neug
