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
#include "neug/compiler/binder/expression/rel_expression.h"
#include "neug/compiler/binder/expression_binder.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/function/arithmetic/vector_arithmetic_functions.h"
#include "neug/compiler/function/path/vector_path_functions.h"
#include "neug/compiler/function/rewrite_function.h"

using namespace neug::binder;
using namespace neug::common;

namespace neug {
namespace function {

static std::shared_ptr<Expression> rewriteFunc(
    const RewriteFunctionBindInput& input) {
  NEUG_ASSERT(input.arguments.size() == 1);
  auto param = input.arguments[0].get();
  auto binder = input.expressionBinder;
  if (param->expressionType == ExpressionType::PATH) {
    int64_t numRels = 0u;
    std::vector<const RelExpression*> recursiveRels;
    for (auto& child : param->getChildren()) {
      if (ExpressionUtil::isRelPattern(*child)) {
        numRels++;
      } else if (ExpressionUtil::isRecursiveRelPattern(*child)) {
        recursiveRels.push_back(child->constPtrCast<RelExpression>());
      }
    }
    auto numRelsExpression = binder->createLiteralExpression(Value(numRels));
    if (recursiveRels.empty()) {
      return numRelsExpression;
    }
    expression_vector children;
    children.push_back(std::move(numRelsExpression));
    children.push_back(recursiveRels[0]->getLengthExpression());
    auto result =
        binder->bindScalarFunctionExpression(children, AddFunction::name);
    for (auto i = 1u; i < recursiveRels.size(); ++i) {
      children[0] = std::move(result);
      children[1] = recursiveRels[i]->getLengthExpression();
      result =
          binder->bindScalarFunctionExpression(children, AddFunction::name);
    }
    return result;
  } else if (ExpressionUtil::isRecursiveRelPattern(*param)) {
    return param->constPtrCast<RelExpression>()->getLengthExpression();
  }
  NEUG_UNREACHABLE;
}

function_set LengthFunction::getFunctionSet() {
  function_set result;
  auto function = std::make_unique<RewriteFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL},
      rewriteFunc);
  result.push_back(std::move(function));
  return result;
}

}  // namespace function
}  // namespace neug
