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

#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace function {

using namespace gs::common;

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  if (input.arguments[1]->expressionType != ExpressionType::LAMBDA) {
    THROW_BINDER_EXCEPTION(stringFormat(
        "The second argument of LIST_REDUCE should be a lambda expression but "
        "got {}.",
        ExpressionTypeUtil::toString(input.arguments[1]->expressionType)));
  }
  std::vector<LogicalType> paramTypes;
  paramTypes.push_back(input.arguments[0]->getDataType().copy());
  paramTypes.push_back(input.arguments[1]->getDataType().copy());
  return std::make_unique<FunctionBindData>(
      std::move(paramTypes),
      ListType::getChildType(input.arguments[0]->getDataType()).copy());
}

static void execFunc(
    const std::vector<std::shared_ptr<common::ValueVector>>& input,
    const std::vector<common::SelectionVector*>& inputSelVectors,
    common::ValueVector& result, common::SelectionVector* resultSelVector,
    void* bindData) {}

function_set ListReduceFunction::getFunctionSet() {
  function_set result;
  auto function = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY},
      LogicalTypeID::LIST, execFunc);
  function->bindFunc = bindFunc;
  function->isListLambda = true;
  result.push_back(std::move(function));
  return result;
}

}  // namespace function
}  // namespace gs
