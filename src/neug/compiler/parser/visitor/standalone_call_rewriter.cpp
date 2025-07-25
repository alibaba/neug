#include "neug/compiler/parser/visitor/standalone_call_rewriter.h"

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/bound_standalone_call_function.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/common/exception/parser.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/expression/parsed_function_expression.h"
#include "neug/compiler/parser/standalone_call_function.h"

namespace gs {
namespace parser {

std::string StandaloneCallRewriter::getRewriteQuery(
    const Statement& statement) {
  visit(statement);
  return rewriteQuery;
}

void StandaloneCallRewriter::visitStandaloneCallFunction(
    const Statement& statement) {
  auto& standaloneCallFunc = statement.constCast<StandaloneCallFunction>();
  main::ClientContext::TransactionHelper::runFuncInTransaction(
      *context->getTransactionContext(),
      [&]() -> void {
        auto funcName = standaloneCallFunc.getFunctionExpression()
                            ->constPtrCast<parser::ParsedFunctionExpression>()
                            ->getFunctionName();
        if (!context->getCatalog()->containsFunction(context->getTransaction(),
                                                     funcName) &&
            !singleStatement) {
          throw common::ParserException{funcName +
                                        " must be called in a query which "
                                        "doesn't have other statements."};
        }
        binder::Binder binder{context};
        const auto boundStatement = binder.bind(standaloneCallFunc);
        auto& boundStandaloneCall =
            boundStatement->constCast<binder::BoundStandaloneCallFunction>();
        const auto func = boundStandaloneCall.getTableFunction()
                              .constPtrCast<function::TableFunction>();
        if (func->rewriteFunc) {
          rewriteQuery =
              func->rewriteFunc(*context, *boundStandaloneCall.getBindData());
        }
      },
      true /*readOnlyStatement*/, false /*isTransactionStatement*/,
      main::ClientContext::TransactionHelper::TransactionCommitAction::
          COMMIT_IF_NEW);
}

}  // namespace parser
}  // namespace gs
