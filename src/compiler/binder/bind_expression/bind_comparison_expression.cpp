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

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/binder/expression_binder.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/function/built_in_function_utils.h"
#include "neug/compiler/main/client_context.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::parser;
using namespace neug::function;

namespace neug {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    const ParsedExpression& parsedExpression) {
  expression_vector children;
  for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
    auto child = bindExpression(*parsedExpression.getChild(i));
    children.push_back(std::move(child));
  }
  return bindComparisonExpression(parsedExpression.getExpressionType(),
                                  children);
}

std::shared_ptr<Expression> ExpressionBinder::bindComparisonExpression(
    ExpressionType expressionType, const expression_vector& children) {
  auto catalog = context->getCatalog();
  auto transaction = context->getTransaction();
  auto functionName = ExpressionTypeUtil::toString(expressionType);
  LogicalType combinedType(LogicalTypeID::ANY);
  if (!ExpressionUtil::tryCombineDataType(children, combinedType)) {
    THROW_BINDER_EXCEPTION(stringFormat(
        "Type Mismatch: Cannot compare types {} and {}",
        children[0]->dataType.toString(), children[1]->dataType.toString()));
  }
  if (combinedType.getLogicalTypeID() == LogicalTypeID::ANY) {
    combinedType = LogicalType(LogicalTypeID::INT8);
  }
  std::vector<LogicalType> childrenTypes;
  for (auto i = 0u; i < children.size(); i++) {
    childrenTypes.push_back(combinedType.copy());
  }
  auto entry = catalog->getFunctionEntry(transaction, functionName);
  auto function = BuiltInFunctionsUtils::matchFunction(
                      functionName, childrenTypes,
                      entry->ptrCast<catalog::FunctionCatalogEntry>())
                      ->ptrCast<ScalarFunction>();
  expression_vector childrenAfterCast;
  for (auto i = 0u; i < children.size(); ++i) {
    if (children[i]->dataType != combinedType) {
      childrenAfterCast.push_back(forceCast(children[i], combinedType));
    } else {
      childrenAfterCast.push_back(children[i]);
    }
  }
  if (function->bindFunc) {
    // Resolve exec and select function if necessary
    // Only used for decimal at the moment. See `bindDecimalCompare`.
    function->bindFunc({childrenAfterCast, function, nullptr,
                        std::vector<std::string>{} /* optionalParams */});
  }
  auto bindData =
      std::make_unique<FunctionBindData>(LogicalType(function->returnTypeID));
  auto uniqueExpressionName = ScalarFunctionExpression::getUniqueName(
      function->name, childrenAfterCast);
  return std::make_shared<ScalarFunctionExpression>(
      expressionType, function->copy(), std::move(bindData),
      std::move(childrenAfterCast), uniqueExpressionName);
}

std::shared_ptr<Expression>
ExpressionBinder::createEqualityComparisonExpression(
    std::shared_ptr<Expression> left, std::shared_ptr<Expression> right) {
  return bindComparisonExpression(
      ExpressionType::EQUALS,
      expression_vector{std::move(left), std::move(right)});
}

}  // namespace binder
}  // namespace neug
