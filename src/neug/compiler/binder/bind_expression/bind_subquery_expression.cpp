#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/expression/aggregate_function_expression.h"
#include "neug/compiler/binder/expression/subquery_expression.h"
#include "neug/compiler/binder/expression_binder.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/function/aggregate/count_star.h"
#include "neug/compiler/function/built_in_function_utils.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/expression/parsed_subquery_expression.h"

using namespace gs::parser;
using namespace gs::common;
using namespace gs::function;

namespace gs {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindSubqueryExpression(
    const ParsedExpression& parsedExpr) {
  auto& subqueryExpr =
      ku_dynamic_cast<const ParsedSubqueryExpression&>(parsedExpr);
  auto prevScope = binder->saveScope();
  auto boundGraphPattern =
      binder->bindGraphPattern(subqueryExpr.getPatternElements());
  if (subqueryExpr.hasWhereClause()) {
    boundGraphPattern.where =
        binder->bindWhereExpression(*subqueryExpr.getWhereClause());
  }
  binder->rewriteMatchPattern(boundGraphPattern);
  auto subqueryType = subqueryExpr.getSubqueryType();
  auto dataType = subqueryType == SubqueryType::COUNT ? LogicalType::INT64()
                                                      : LogicalType::BOOL();
  auto rawName = subqueryExpr.getRawName();
  auto uniqueName = binder->getUniqueExpressionName(rawName);
  auto boundSubqueryExpr = make_shared<SubqueryExpression>(
      subqueryType, std::move(dataType),
      std::move(boundGraphPattern.queryGraphCollection), uniqueName,
      std::move(rawName));
  boundSubqueryExpr->setWhereExpression(boundGraphPattern.where);
  // Bind projection
  auto entry = context->getCatalog()->getFunctionEntry(
      context->getTransaction(), CountStarFunction::name);
  auto function = BuiltInFunctionsUtils::matchAggregateFunction(
      CountStarFunction::name, std::vector<LogicalType>{}, false,
      entry->ptrCast<catalog::FunctionCatalogEntry>());
  auto bindData =
      std::make_unique<FunctionBindData>(LogicalType(function->returnTypeID));
  auto countStarExpr = std::make_shared<AggregateFunctionExpression>(
      function->copy(), std::move(bindData), expression_vector{},
      binder->getUniqueExpressionName(CountStarFunction::name));
  boundSubqueryExpr->setCountStarExpr(countStarExpr);
  std::shared_ptr<Expression> projectionExpr;
  switch (subqueryType) {
  case SubqueryType::COUNT: {
    // Rewrite COUNT subquery as COUNT(*)
    projectionExpr = countStarExpr;
  } break;
  case SubqueryType::EXISTS: {
    // Rewrite EXISTS subquery as COUNT(*) > 0
    auto literalExpr = createLiteralExpression(Value(static_cast<int64_t>(0)));
    projectionExpr =
        bindComparisonExpression(ExpressionType::GREATER_THAN,
                                 expression_vector{countStarExpr, literalExpr});
  } break;
  default:
    KU_UNREACHABLE;
  }
  // Use the same unique identifier for projection & subquery expression. We
  // will replace subquery expression with projection expression during
  // processing.
  projectionExpr->setUniqueName(uniqueName);
  boundSubqueryExpr->setProjectionExpr(projectionExpr);
  if (subqueryExpr.hasHint()) {
    boundSubqueryExpr->setHint(
        binder->bindJoinHint(*boundSubqueryExpr->getQueryGraphCollection(),
                             *subqueryExpr.getHint()));
  }
  binder->restoreScope(std::move(prevScope));
  return boundSubqueryExpr;
}

}  // namespace binder
}  // namespace gs
