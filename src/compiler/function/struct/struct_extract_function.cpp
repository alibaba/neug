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
#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/common/constants.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/compiler/function/struct/vector_struct_functions.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::binder;

namespace neug {
namespace function {

std::unique_ptr<FunctionBindData> StructExtractFunctions::bindFunc(
    const ScalarBindFuncInput& input) {
  const auto& structType = input.arguments[0]->getDataType();
  if (input.arguments[1]->expressionType != ExpressionType::LITERAL) {
    THROW_BINDER_EXCEPTION(
        "Key name for struct/union extract must be STRING literal.");
  }
  auto key = input.arguments[1]
                 ->constPtrCast<LiteralExpression>()
                 ->getValue()
                 .getValue<std::string>();
  auto fieldIdx = StructType::getFieldIdx(structType, key);
  if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
    THROW_BINDER_EXCEPTION(stringFormat("Invalid struct field name: {}.", key));
  }
  auto paramTypes = ExpressionUtil::getDataTypes(input.arguments);
  auto resultType = StructType::getField(structType, fieldIdx).getType().copy();
  // the default type of START_NODE(e) is the INNER_ID of the source vertex, but
  // in NEUG, we support it as the source vertex directly, here convert the type
  // from INNER_ID to NODE. END_NODE(e) is the same.
  if ((key == common::InternalKeyword::SRC ||
       key == common::InternalKeyword::DST) &&
      resultType.getLogicalTypeID() == common::LogicalTypeID::INTERNAL_ID) {
    resultType = LogicalType(common::LogicalTypeID::NODE);
  }
  auto bindData =
      std::make_unique<StructExtractBindData>(std::move(resultType), fieldIdx);
  bindData->paramTypes.push_back(input.arguments[0]->getDataType().copy());
  bindData->paramTypes.push_back(
      LogicalType(input.definition->parameterTypeIDs[1]));
  return bindData;
}

void StructExtractFunctions::compileFunc(
    FunctionBindData* bindData,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
  NEUG_ASSERT(parameters[0]->dataType.getPhysicalType() ==
              PhysicalTypeID::STRUCT);
  auto& structBindData = bindData->cast<StructExtractBindData>();
  result = StructVector::getFieldVector(parameters[0].get(),
                                        structBindData.childIdx);
  result->state = parameters[0]->state;
}

static std::unique_ptr<ScalarFunction> getStructExtractFunction(
    LogicalTypeID logicalTypeID) {
  auto function = std::make_unique<ScalarFunction>(
      StructExtractFunctions::name,
      std::vector<LogicalTypeID>{logicalTypeID, LogicalTypeID::STRING},
      LogicalTypeID::ANY);
  function->bindFunc = StructExtractFunctions::bindFunc;
  function->compileFunc = StructExtractFunctions::compileFunc;
  return function;
}

function_set StructExtractFunctions::getFunctionSet() {
  function_set functions;
  auto inputTypeIDs = std::vector<LogicalTypeID>{
      LogicalTypeID::STRUCT, LogicalTypeID::NODE, LogicalTypeID::REL};
  for (auto inputTypeID : inputTypeIDs) {
    functions.push_back(getStructExtractFunction(inputTypeID));
  }
  return functions;
}

}  // namespace function
}  // namespace neug
