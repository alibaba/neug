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
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/function/rewrite_function.h"
#include "neug/compiler/function/struct/vector_struct_functions.h"

using namespace gs::common;
using namespace gs::binder;

namespace gs {
namespace function {

static std::shared_ptr<Expression> rewriteFunc(
    const RewriteFunctionBindInput& input) {
  NEUG_ASSERT(input.arguments.size() == 1);
  auto uniqueExpressionName = ScalarFunctionExpression::getUniqueName(
      KeysFunctions::name, input.arguments);
  const auto& resultType = LogicalType::LIST(LogicalType::STRING());
  auto fields = common::StructType::getFieldNames(input.arguments[0]->dataType);
  std::vector<std::unique_ptr<Value>> children;
  for (auto field : fields) {
    if (field == InternalKeyword::ID || field == InternalKeyword::LABEL ||
        field == InternalKeyword::SRC || field == InternalKeyword::DST) {
      continue;
    }
    children.push_back(std::make_unique<Value>(field));
  }
  auto resultExpr = std::make_shared<binder::LiteralExpression>(
      Value{resultType.copy(), std::move(children)},
      std::move(uniqueExpressionName));
  return resultExpr;
}

static std::unique_ptr<Function> getKeysFunction(LogicalTypeID logicalTypeID) {
  return std::make_unique<function::RewriteFunction>(
      KeysFunctions::name, std::vector<LogicalTypeID>{logicalTypeID},
      rewriteFunc);
}

function_set KeysFunctions::getFunctionSet() {
  function_set functions;
  auto inputTypeIDs =
      std::vector<LogicalTypeID>{LogicalTypeID::NODE, LogicalTypeID::REL};
  for (auto inputTypeID : inputTypeIDs) {
    functions.push_back(getKeysFunction(inputTypeID));
  }
  return functions;
}

}  // namespace function
}  // namespace gs
