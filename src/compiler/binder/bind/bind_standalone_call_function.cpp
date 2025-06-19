#include "src/include/binder/binder.h"
#include "src/include/binder/bound_standalone_call_function.h"
#include "src/include/catalog/catalog.h"
#include "src/include/common/exception/binder.h"
#include "src/include/main/client_context.h"
#include "src/include/parser/expression/parsed_function_expression.h"
#include "src/include/parser/standalone_call_function.h"

using namespace gs::common;

namespace gs {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindStandaloneCallFunction(
    const parser::Statement& statement) {
  auto& callStatement = statement.constCast<parser::StandaloneCallFunction>();
  auto& funcExpr = callStatement.getFunctionExpression()
                       ->constCast<parser::ParsedFunctionExpression>();
  auto funcName = funcExpr.getFunctionName();
  auto entry = clientContext->getCatalog()->getFunctionEntry(
      clientContext->getTransaction(), funcName,
      clientContext->useInternalCatalogEntry());
  KU_ASSERT(entry);
  if (entry->getType() !=
      catalog::CatalogEntryType::STANDALONE_TABLE_FUNCTION_ENTRY) {
    throw BinderException(
        "Only standalone table functions can be called without return "
        "statement.");
  }
  auto boundTableFunction =
      bindTableFunc(funcName, funcExpr, {} /* yieldVariables */);
  return std::make_unique<BoundStandaloneCallFunction>(
      std::move(boundTableFunction));
}

}  // namespace binder
}  // namespace gs
