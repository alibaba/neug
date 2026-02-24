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

#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/scalar_function.h"

using namespace neug::common;

namespace neug {
namespace function {

void ListCreationFunction::execFunc(
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& parameterSelVectors,
    common::ValueVector& result, common::SelectionVector* resultSelVector,
    void* /*dataPtr*/) {
  result.resetAuxiliaryBuffer();
  for (auto selectedPos = 0u; selectedPos < resultSelVector->getSelSize();
       ++selectedPos) {
    auto pos = (*resultSelVector)[selectedPos];
    auto resultEntry = ListVector::addList(&result, parameters.size());
    result.setValue(pos, resultEntry);
    auto resultDataVector = ListVector::getDataVector(&result);
    auto resultPos = resultEntry.offset;
    for (auto i = 0u; i < parameters.size(); i++) {
      const auto& parameter = parameters[i];
      const auto& parameterSelVector = *parameterSelVectors[i];
      auto paramPos = parameter->state->isFlat() ? parameterSelVector[0] : pos;
      resultDataVector->copyFromVectorData(resultPos++, parameter.get(),
                                           paramPos);
    }
  }
}

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  LogicalType combinedType(LogicalTypeID::ANY);
  // check if all arguments have the same type, if not, set to ANY.
  auto& args = input.arguments;
  // if all arguments have the same type, set sameType to true and
  // combinedType to that type
  bool sameType = true;
  // if any argument is ANY, set anyType to true
  bool anyType = false;
  for (auto& arg : args) {
    if (arg->getDataType() == LogicalType::ANY()) {
      combinedType = LogicalType::ANY();
      anyType = true;
      break;
    }
    if (combinedType == LogicalType::ANY()) {
      combinedType = arg->getDataType().copy();
    } else if (combinedType != arg->getDataType()) {
      sameType = false;
      break;
    }
  }
  LogicalType resultType;
  // convert to struct type
  if (!anyType && !sameType) {
    std::vector<StructField> fields;
    for (auto& arg : args) {
      fields.emplace_back("", arg->getDataType().copy());
    }
    resultType = LogicalType::STRUCT(std::move(fields));
  } else {
    resultType = LogicalType::LIST(combinedType.copy());
  }
  auto bindData = std::make_unique<FunctionBindData>(std::move(resultType));
  for (auto& arg : input.arguments) {
    bindData->paramTypes.push_back(arg->getDataType().copy());
  }
  return bindData;
}

function_set ListCreationFunction::getFunctionSet() {
  function_set result;
  auto function = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::LIST,
      execFunc);
  function->bindFunc = bindFunc;
  function->isVarLength = true;
  result.push_back(std::move(function));
  return result;
}

}  // namespace function
}  // namespace neug
