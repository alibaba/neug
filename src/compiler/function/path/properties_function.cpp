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

#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/path/vector_path_functions.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::binder;

namespace neug {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  if (input.arguments[1]->expressionType != ExpressionType::LITERAL) {
    THROW_BINDER_EXCEPTION(
        stringFormat("Expected literal input as the second argument for {}().",
                     PropertiesFunction::name));
  }
  auto literalExpr = input.arguments[1]->constPtrCast<LiteralExpression>();
  auto key = literalExpr->getValue().getValue<std::string>();
  const auto& listType = input.arguments[0]->getDataType();
  const auto& childType = ListType::getChildType(listType);
  struct_field_idx_t fieldIdx = 0;
  if (childType.getLogicalTypeID() == LogicalTypeID::NODE ||
      childType.getLogicalTypeID() == LogicalTypeID::REL) {
    fieldIdx = StructType::getFieldIdx(childType, key);
    if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
      THROW_BINDER_EXCEPTION(stringFormat("Invalid property name: {}.", key));
    }
  } else {
    THROW_BINDER_EXCEPTION(stringFormat("Cannot extract properties from {}.",
                                        listType.toString()));
  }
  const auto& field = StructType::getField(childType, fieldIdx);
  auto returnType = LogicalType::LIST(field.getType().copy());
  auto bindData =
      std::make_unique<PropertiesBindData>(std::move(returnType), fieldIdx);
  bindData->paramTypes.push_back(input.arguments[0]->getDataType().copy());
  bindData->paramTypes.push_back(
      LogicalType(input.definition->parameterTypeIDs[1]));
  return bindData;
}

static void compileFunc(
    FunctionBindData* bindData,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
  NEUG_ASSERT(parameters[0]->dataType.getPhysicalType() ==
              PhysicalTypeID::LIST);
  auto& propertiesBindData = bindData->cast<PropertiesBindData>();
  auto fieldVector = StructVector::getFieldVector(
      ListVector::getDataVector(parameters[0].get()),
      propertiesBindData.childIdx);
  ListVector::setDataVector(result.get(), fieldVector);
}

static void execFunc(
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& parameterSelVectors,
    common::ValueVector& result, common::SelectionVector* resultSelVector,
    void* /*dataPtr*/) {
  ListVector::copyListEntryAndBufferMetaData(
      result, *resultSelVector, *parameters[0], *parameterSelVectors[0]);
}

function_set PropertiesFunction::getFunctionSet() {
  function_set functions;
  auto function = std::make_unique<ScalarFunction>(
      name,
      std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::STRING},
      LogicalTypeID::ANY, execFunc);
  function->bindFunc = bindFunc;
  function->compileFunc = compileFunc;
  functions.push_back(std::move(function));
  return functions;
}

}  // namespace function
}  // namespace neug
