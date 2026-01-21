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

#include "neug/utils/runtime/rt_any.h"

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

runtime::RTAny UpperFunction::Exec(runtime::Arena& arena,
                                   const std::vector<runtime::RTAny>& args) {
  if (args.size() != 1) {
    THROW_RUNTIME_ERROR("UPPER: expect exactly 1 argument, got " +
                        std::to_string(args.size()));
  }
  const auto& val = args[0];
  if (val.type().id() != DataTypeId::kVarchar) {
    THROW_RUNTIME_ERROR("UPPER: input value is not a string");
  }
  std::string str(val.as_string());
  std::transform(str.begin(), str.end(), str.begin(), ::toupper);
  auto ptr = runtime::StringImpl::make_string_impl(str);
  auto str_view = ptr->str_view();
  arena.emplace_back(std::move(ptr));
  return runtime::RTAny::from_string(str_view);
}

function_set LowerFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(std::make_unique<NeugScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
      LogicalTypeID::STRING, LowerFunction::Exec));
  return functionSet;
}

runtime::RTAny LowerFunction::Exec(runtime::Arena& arena,
                                   const std::vector<runtime::RTAny>& args) {
  if (args.size() != 1) {
    THROW_RUNTIME_ERROR("LOWER: expect exactly 1 argument, got " +
                        std::to_string(args.size()));
  }
  const auto& val = args[0];
  if (val.type().id() != DataTypeId::kVarchar) {
    THROW_RUNTIME_ERROR("LOWER: input value is not a string");
  }
  std::string str(val.as_string());
  std::transform(str.begin(), str.end(), str.begin(), ::tolower);
  auto ptr = runtime::StringImpl::make_string_impl(str);
  auto str_view = ptr->str_view();
  arena.emplace_back(std::move(ptr));
  return runtime::RTAny::from_string(str_view);
}

function_set ReverseFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.emplace_back(std::make_unique<NeugScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
      LogicalTypeID::STRING, ReverseFunction::Exec));
  return functionSet;
}

runtime::RTAny ReverseFunction::Exec(runtime::Arena& arena,
                                     const std::vector<runtime::RTAny>& args) {
  if (args.size() != 1) {
    THROW_RUNTIME_ERROR("REVERSE: expect exactly 1 argument, got " +
                        std::to_string(args.size()));
  }
  const auto& val = args[0];
  if (val.type().id() != DataTypeId::kVarchar) {
    THROW_RUNTIME_ERROR("REVERSE: input value is not a string");
  }
  std::string str(val.as_string());
  std::reverse(str.begin(), str.end());
  auto ptr = runtime::StringImpl::make_string_impl(str);
  auto str_view = ptr->str_view();
  arena.emplace_back(std::move(ptr));
  return runtime::RTAny::from_string(str_view);
}

}  // namespace function
}  // namespace gs
