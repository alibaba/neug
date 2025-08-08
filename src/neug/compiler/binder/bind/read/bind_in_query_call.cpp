#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/query/reading_clause/bound_table_function_call.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/expression/parsed_function_expression.h"
#include "neug/compiler/parser/query/reading_clause/in_query_call_clause.h"
#include "neug/utils/exception/exception.h"

using namespace gs::common;
using namespace gs::catalog;
using namespace gs::parser;
using namespace gs::function;
using namespace gs::catalog;

namespace gs {
namespace binder {

std::unique_ptr<BoundReadingClause> Binder::bindInQueryCall(
    const ReadingClause& readingClause) {
  auto& call = readingClause.constCast<InQueryCallClause>();
  auto expr = call.getFunctionExpression();
  auto functionExpr = expr->constPtrCast<ParsedFunctionExpression>();
  auto functionName = functionExpr->getFunctionName();
  std::unique_ptr<BoundReadingClause> boundReadingClause;
  auto entry = clientContext->getCatalog()->getFunctionEntry(
      clientContext->getTransaction(), functionName);
  switch (entry->getType()) {
  case CatalogEntryType::TABLE_FUNCTION_ENTRY: {
    auto boundTableFunction =
        bindTableFunc(functionName, *functionExpr, call.getYieldVariables());
    boundReadingClause =
        std::make_unique<BoundTableFunctionCall>(std::move(boundTableFunction));
  } break;
  default:
    THROW_BINDER_EXCEPTION(
        stringFormat("{} is not a table or algorithm function.", functionName));
  }
  if (call.hasWherePredicate()) {
    auto wherePredicate = bindWhereExpression(*call.getWherePredicate());
    boundReadingClause->setPredicate(std::move(wherePredicate));
  }
  return boundReadingClause;
}

}  // namespace binder
}  // namespace gs
