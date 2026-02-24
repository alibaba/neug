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

#include <memory>
#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/binder/query/reading_clause/bound_unwind_clause.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/parser/query/reading_clause/unwind_clause.h"

using namespace neug::parser;
using namespace neug::common;

namespace neug {
namespace binder {

// E.g. UNWIND $1. We cannot validate $1 has data type LIST until we see the
// actual parameter.
static bool skipDataTypeValidation(const Expression& expr) {
  return expr.expressionType == ExpressionType::PARAMETER &&
         expr.getDataType().getLogicalTypeID() == LogicalTypeID::ANY;
}

std::shared_ptr<Expression> Binder::createAlias(
    const std::string& name, const LogicalType& dataType,
    std::shared_ptr<binder::Expression> boundExpr) {
  if (scope.contains(name)) {
    THROW_BINDER_EXCEPTION("Variable " + name + " already exists.");
  }
  if (dataType.getLogicalTypeID() == LogicalTypeID::NODE) {
    auto nodeExpr = createChildNodeExpr(boundExpr, dataType,
                                        getUniqueExpressionName(name), name);
    addToScope(name, nodeExpr);
    return nodeExpr;
  }
  auto expression =
      expressionBinder.createVariableExpression(dataType.copy(), name);
  expression->setAlias(name);
  addToScope(name, expression);
  return expression;
}

std::unique_ptr<BoundReadingClause> Binder::bindUnwindClause(
    const ReadingClause& readingClause) {
  auto& unwindClause = readingClause.constCast<UnwindClause>();
  auto boundExpression =
      expressionBinder.bindExpression(*unwindClause.getExpression());
  auto aliasName = unwindClause.getAlias();
  std::shared_ptr<Expression> alias;
  if (boundExpression->getDataType().getLogicalTypeID() ==
      LogicalTypeID::ARRAY) {
    auto targetType = LogicalType::LIST(
        ArrayType::getChildType(boundExpression->dataType).copy());
    boundExpression =
        expressionBinder.implicitCast(boundExpression, targetType);
  }
  if (!skipDataTypeValidation(*boundExpression)) {
    ExpressionUtil::validateDataType(*boundExpression, LogicalTypeID::LIST);
    alias = createAlias(aliasName,
                        neug::common::ListType::getChildType(boundExpression->dataType),
                        boundExpression);
  } else {
    alias = createAlias(aliasName, LogicalType::ANY(), boundExpression);
  }
  std::shared_ptr<Expression> idExpr = nullptr;
  if (scope.hasMemorizedTableIDs(boundExpression->getAlias())) {
    auto entries = scope.getMemorizedTableEntries(boundExpression->getAlias());
    auto node = createQueryNode(aliasName, entries);
    idExpr = node->getInternalID();
    scope.addNodeReplacement(node);
  }
  return make_unique<BoundUnwindClause>(std::move(boundExpression),
                                        std::move(alias), std::move(idExpr));
}

}  // namespace binder
}  // namespace neug
