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

#include "neug/compiler/function/scalar_function.h"
#include "neug/compiler/function/utility/function_string_bind_data.h"
#include "neug/compiler/function/utility/vector_utility_functions.h"

using namespace gs::common;

namespace gs {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  std::unique_ptr<FunctionBindData> bindData;
  if (input.arguments[0]->getDataType().getLogicalTypeID() ==
      LogicalTypeID::ANY) {
    bindData = std::make_unique<FunctionStringBindData>("NULL");
    bindData->paramTypes.push_back(LogicalType::STRING());
  } else {
    bindData = std::make_unique<FunctionStringBindData>(
        input.arguments[0]->getDataType().toString());
  }
  return bindData;
}

static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>&,
                     const std::vector<common::SelectionVector*>&,
                     common::ValueVector& result,
                     common::SelectionVector* resultSelVector, void* dataPtr) {
  result.resetAuxiliaryBuffer();
  auto typeData = reinterpret_cast<FunctionStringBindData*>(dataPtr);
  for (auto i = 0u; i < resultSelVector->getSelSize(); ++i) {
    auto resultPos = (*resultSelVector)[i];
    StringVector::addString(&result, resultPos, typeData->str);
  }
}

function_set TypeOfFunction::getFunctionSet() {
  function_set functionSet;
  auto function = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::ANY},
      LogicalTypeID::STRING, execFunc);
  function->bindFunc = bindFunc;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace gs
