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
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;

namespace neug {
namespace function {

void ListCreationFunction::execFunc(
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& parameterSelVectors,
    common::ValueVector& result, common::SelectionVector* resultSelVector,
    void* dataPtr) {
  if (dataPtr == nullptr) {
    THROW_RUNTIME_ERROR("LIST_CREATION requires function bind data");
  }
  const auto* bindData = reinterpret_cast<FunctionBindData*>(dataPtr);
  if (bindData->resultType != result.dataType) {
    THROW_RUNTIME_ERROR(
        "LIST_CREATION bind result type does not match result vector type");
  }

  switch (result.dataType.id()) {
  case DataTypeId::kList:
  case DataTypeId::kArray: {
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
        auto paramPos = parameter->state->isFlat()
                            ? parameterSelVector[0]
                            : parameterSelVector[selectedPos];
        resultDataVector->copyFromVectorData(resultPos++, parameter.get(),
                                             paramPos);
      }
    }
  } break;
  case DataTypeId::kStruct: {
    const auto& fieldVectors = StructVector::getFieldVectors(&result);
    if (fieldVectors.size() != parameters.size()) {
      THROW_RUNTIME_ERROR(
          "LIST_CREATION STRUCT field count does not match parameter count");
    }
    for (const auto& fieldVector : fieldVectors) {
      fieldVector->resetAuxiliaryBuffer();
    }
    for (auto selectedPos = 0u; selectedPos < resultSelVector->getSelSize();
         ++selectedPos) {
      auto pos = (*resultSelVector)[selectedPos];
      result.setNull(pos, false);
      for (auto i = 0u; i < parameters.size(); i++) {
        const auto& parameter = parameters[i];
        const auto& parameterSelVector = *parameterSelVectors[i];
        auto paramPos = parameter->state->isFlat()
                            ? parameterSelVector[0]
                            : parameterSelVector[selectedPos];
        fieldVectors[i]->copyFromVectorData(pos, parameter.get(), paramPos);
      }
    }
  } break;
  default:
    THROW_RUNTIME_ERROR("LIST_CREATION does not support result type " +
                        result.dataType.ToString());
  }
}

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  DataType combinedType(DataTypeId::kUnknown);
  // check if all arguments have the same type, if not, set to ANY.
  auto& args = input.arguments;
  // if all arguments have the same type, set sameType to true and
  // combinedType to that type
  bool sameType = true;
  // if any argument is ANY, set anyType to true
  bool anyType = false;
  for (auto& arg : args) {
    if (arg->getDataType() == DataType(DataTypeId::kUnknown)) {
      combinedType = DataType(DataTypeId::kUnknown);
      anyType = true;
      break;
    }
    if (combinedType == DataType(DataTypeId::kUnknown)) {
      combinedType = arg->getDataType().copy();
    } else if (combinedType != arg->getDataType()) {
      sameType = false;
      break;
    }
  }
  DataType resultType;
  // convert to struct type
  if (!anyType && !sameType) {
    std::vector<std::string> fieldNames;
    std::vector<DataType> fieldTypes;
    for (auto& arg : args) {
      fieldNames.push_back("");
      fieldTypes.push_back(arg->getDataType().copy());
    }
    resultType = DataType::Struct(std::move(fieldNames), std::move(fieldTypes));
  } else if (args.empty()) {
    // An empty list literal (e.g. []) has no fixed size; use a variable-
    // length List type instead of creating a size-0 Array.
    resultType = DataType::List(combinedType.copy());
  } else {
    resultType = DataType::Array(combinedType.copy(), args.size());
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
      name, std::vector<DataTypeId>{DataTypeId::kUnknown}, DataTypeId::kList,
      execFunc);
  function->bindFunc = bindFunc;
  function->isVarLength = true;
  result.push_back(std::move(function));
  return result;
}

}  // namespace function
}  // namespace neug
