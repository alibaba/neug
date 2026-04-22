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

#include "neug/compiler/common/types/types.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/compiler/function/struct/vector_struct_functions.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::binder;

namespace neug {
namespace function {
static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  std::vector<StructField> fields;
  if (input.arguments.size() > INVALID_STRUCT_FIELD_IDX - 1) {
    THROW_BINDER_EXCEPTION(
        stringFormat("Too many fields in STRUCT literal (max {}, got {})",
                     INVALID_STRUCT_FIELD_IDX - 1, input.arguments.size()));
  }
  std::unordered_set<std::string> fieldNameSet;
  for (auto i = 0u; i < input.arguments.size(); i++) {
    auto& argument = input.arguments[i];
    if (argument->getDataType().getLogicalTypeID() == LogicalTypeID::ANY) {
      argument->cast(LogicalType::STRING());
    }
    if (i >= input.optionalArguments.size()) {
      THROW_BINDER_EXCEPTION(stringFormat("Cannot infer field name for {}.",
                                          argument->toString()));
    }
    auto fieldName = input.optionalArguments[i];
    if (fieldNameSet.contains(fieldName)) {
      THROW_BINDER_EXCEPTION(
          stringFormat("Found duplicate field {} in STRUCT.", fieldName));
    } else {
      fieldNameSet.insert(fieldName);
    }
    fields.emplace_back(fieldName, argument->getDataType().copy());
  }
  const auto resultType = LogicalType::STRUCT(std::move(fields));
  return FunctionBindData::getSimpleBindData(input.arguments, resultType);
}

static void copyParameterValueToStructFieldVector(
    const ValueVector* parameter, ValueVector* structField,
    DataChunkState* structVectorState) {
  // If the parameter is unFlat, then its state must be consistent with the
  // result's state. Thus, we don't need to copy values to structFieldVector.
  NEUG_ASSERT(parameter->state->isFlat());
  auto paramPos = parameter->state->getSelVector()[0];
  if (structVectorState->isFlat()) {
    auto pos = structVectorState->getSelVector()[0];
    structField->copyFromVectorData(pos, parameter, paramPos);
  } else {
    for (auto i = 0u; i < structVectorState->getSelVector().getSelSize(); i++) {
      auto pos = structVectorState->getSelVector()[i];
      structField->copyFromVectorData(pos, parameter, paramPos);
    }
  }
}

void StructPackFunctions::execFunc(
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& parameterSelVectors,
    common::ValueVector& result, common::SelectionVector* resultSelVector,
    void* /*dataPtr*/) {
  for (auto i = 0u; i < parameters.size(); i++) {
    auto* parameter = parameters[i].get();
    auto* parameterSelVector = parameterSelVectors[i];
    if (parameterSelVector == resultSelVector) {
      continue;
    }
    // If the parameter's state is inconsistent with the result's state, we need
    // to copy the parameter's value to the corresponding child vector.
    StructVector::getFieldVector(&result, i)->resetAuxiliaryBuffer();
    copyParameterValueToStructFieldVector(
        parameter, StructVector::getFieldVector(&result, i).get(),
        result.state.get());
  }
}

function_set StructPackFunctions::getFunctionSet() {
  function_set functions;
  auto function = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::ANY},
      LogicalTypeID::STRUCT, execFunc);
  function->bindFunc = bindFunc;
  function->isVarLength = true;
  functions.push_back(std::move(function));
  return functions;
}

}  // namespace function
}  // namespace neug
