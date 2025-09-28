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
#include "neug/compiler/function/utility/vector_utility_functions.h"

using namespace gs::common;

namespace gs {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  logical_type_vec_t paramTypes;
  for (auto& argument : input.arguments) {
    if (argument->getDataType().getLogicalTypeID() == LogicalTypeID::ANY) {
      paramTypes.push_back(LogicalType::STRING());
    } else {
      paramTypes.push_back(argument->getDataType().copy());
    }
  }
  auto bindData = std::make_unique<FunctionBindData>(paramTypes[0].copy());
  bindData->paramTypes = std::move(paramTypes);
  return bindData;
}

static void execFunc(
    const std::vector<std::shared_ptr<common::ValueVector>>& params,
    const std::vector<common::SelectionVector*>& paramSelVectors,
    common::ValueVector& result, common::SelectionVector* resultSelVector,
    void* /*dataPtr*/) {
  NEUG_ASSERT(params.size() == 2);
  result.resetAuxiliaryBuffer();
  for (auto i = 0u; i < resultSelVector->getSelSize(); ++i) {
    auto resultPos = (*resultSelVector)[i];
    auto firstParamPos =
        params[0]->state->isFlat() ? (*paramSelVectors[0])[0] : resultPos;
    auto secondParamPos =
        params[1]->state->isFlat() ? (*paramSelVectors[1])[0] : resultPos;
    if (params[1]->isNull(secondParamPos) || params[0]->isNull(firstParamPos)) {
      result.setNull(resultPos, true);
    } else {
      result.setNull(resultPos, false);
      result.copyFromVectorData(resultPos, params[0].get(), firstParamPos);
    }
  }
}

static bool selectFunc(const std::vector<std::shared_ptr<ValueVector>>& params,
                       SelectionVector& selVector, void* /* dataPtr */) {
  NEUG_ASSERT(params.size() == 2);
  auto unFlatVectorIdx = 0u;
  for (auto i = 0u; i < params.size(); ++i) {
    if (!params[i]->state->isFlat()) {
      unFlatVectorIdx = i;
      break;
    }
  }
  auto numSelectedValues = 0u;
  auto selectedPositionsBuffer = selVector.getMutableBuffer();
  for (auto i = 0u;
       i < params[unFlatVectorIdx]->state->getSelVector().getSelSize(); ++i) {
    auto resultPos = params[unFlatVectorIdx]->state->getSelVector()[i];
    auto resultValue = false;
    auto firstParamPos = params[0]->state->isFlat()
                             ? params[0]->state->getSelVector()[0]
                             : resultPos;
    auto secondParamPos = params[1]->state->isFlat()
                              ? params[1]->state->getSelVector()[0]
                              : resultPos;
    if (!params[1]->isNull(secondParamPos) &&
        !params[0]->isNull(firstParamPos)) {
      resultValue = params[0]->getValue<bool>(firstParamPos);
    }
    selectedPositionsBuffer[numSelectedValues] = resultPos;
    numSelectedValues += resultValue;
  }
  selVector.setSelSize(numSelectedValues);
  return numSelectedValues > 0;
}

function_set ConstantOrNullFunction::getFunctionSet() {
  function_set functionSet;
  auto function = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::ANY, LogicalTypeID::ANY},
      LogicalTypeID::ANY, execFunc, selectFunc);
  function->bindFunc = bindFunc;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace gs
