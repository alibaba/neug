#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/bound_standalone_call_function.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/expression/parsed_function_expression.h"
#include "neug/compiler/parser/standalone_call_function.h"
#include "neug/utils/exception/exception.h"

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
    throw exception::BinderException(
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
