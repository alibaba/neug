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
#include "neug/compiler/function/rewrite_function.h"
#include "neug/compiler/function/schema/vector_node_rel_functions.h"
#include "neug/compiler/function/struct/vector_struct_functions.h"

using namespace neug::common;
using namespace neug::binder;

namespace neug {
namespace function {

static std::shared_ptr<Expression> startRewriteFunc(
    const RewriteFunctionBindInput& input) {
  NEUG_ASSERT(input.arguments.size() == 1);
  // auto param = input.arguments[0].get();
  // if (ExpressionUtil::isRelPattern(*param)) {
  //   return param->constCast<RelExpression>().getSrcNode();
  // }
  auto extractKey =
      input.expressionBinder->createLiteralExpression(InternalKeyword::SRC);
  return input.expressionBinder->bindScalarFunctionExpression(
      {input.arguments[0], extractKey}, StructExtractFunctions::name);
}

function_set StartNodeFunction::getFunctionSet() {
  function_set set;
  auto function = std::make_unique<RewriteFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::REL}, startRewriteFunc);
  set.push_back(std::move(function));
  return set;
}

static std::shared_ptr<Expression> endRewriteFunc(
    const RewriteFunctionBindInput& input) {
  NEUG_ASSERT(input.arguments.size() == 1);
  // auto param = input.arguments[0].get();
  // if (ExpressionUtil::isRelPattern(*param)) {
  //   return param->constCast<RelExpression>().getDstNode();
  // }
  auto extractKey =
      input.expressionBinder->createLiteralExpression(InternalKeyword::DST);
  return input.expressionBinder->bindScalarFunctionExpression(
      {input.arguments[0], extractKey}, StructExtractFunctions::name);
}

function_set EndNodeFunction::getFunctionSet() {
  function_set set;
  auto function = std::make_unique<RewriteFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::REL}, endRewriteFunc);
  set.push_back(std::move(function));
  return set;
}

}  // namespace function
}  // namespace neug
