#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/bound_transaction_statement.h"
#include "neug/compiler/parser/transaction_statement.h"

using namespace gs::parser;

namespace gs {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindTransaction(
    const Statement& statement) {
  auto& transactionStatement = statement.constCast<TransactionStatement>();
  return std::make_unique<BoundTransactionStatement>(
      transactionStatement.getTransactionAction());
}

}  // namespace binder
}  // namespace gs
