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

#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/path/path_function_executor.h"
#include "neug/compiler/function/path/vector_path_functions.h"
#include "neug/compiler/function/scalar_function.h"

using namespace gs::binder;
using namespace gs::common;

namespace gs {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  return FunctionBindData::getSimpleBindData(input.arguments,
                                             LogicalType::BOOL());
}

static void IsTrailExecFunc(
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& parameterSelVectors,
    common::ValueVector& result, common::SelectionVector*, void* /*dataPtr*/) {
  UnaryPathExecutor::executeRelIDs(*parameters[0], *parameterSelVectors[0],
                                   result);
}

static bool IsTrailSelectFunc(
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    SelectionVector& selectionVector, void* /* dataPtr */) {
  return UnaryPathExecutor::selectRelIDs(*parameters[0], selectionVector);
}

function_set IsTrailFunction::getFunctionSet() {
  function_set functionSet;
  auto function = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL},
      LogicalTypeID::BOOL, IsTrailExecFunc, IsTrailSelectFunc);
  function->bindFunc = bindFunc;
  functionSet.push_back(std::move(function));
  return functionSet;
}

static void IsACyclicExecFunc(
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& parameterSelVectors,
    common::ValueVector& result, common::SelectionVector*, void* /*dataPtr*/) {
  UnaryPathExecutor::executeNodeIDs(*parameters[0], *parameterSelVectors[0],
                                    result);
}

static bool IsACyclicSelectFunc(
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    SelectionVector& selectionVector, void* /* dataPtr */) {
  return UnaryPathExecutor::selectNodeIDs(*parameters[0], selectionVector);
}

function_set IsACyclicFunction::getFunctionSet() {
  function_set functionSet;
  auto function = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL},
      LogicalTypeID::BOOL, IsACyclicExecFunc, IsACyclicSelectFunc);
  function->bindFunc = bindFunc;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace gs
