#include "gopt/g_transaction_manager.h"

#include "gopt/g_constants.h"

namespace gs {
namespace transaction {
GTransactionManager::GTransactionManager(storage::WAL& wal)
    : TransactionManager(wal) {}

std::unique_ptr<Transaction> GTransactionManager::beginTransaction(
    main::ClientContext& clientContext, TransactionType type) {
  return std::make_unique<Transaction>(
      gs::transaction::TransactionType::DUMMY,
      gs::transaction::Transaction::DUMMY_TRANSACTION_ID,
      common::INVALID_TRANSACTION);
}

void GTransactionManager::commit(main::ClientContext& clientContext) {}
void rollback(main::ClientContext& clientContext, Transaction* transaction) {}

void GTransactionManager::checkpoint(main::ClientContext& clientContext) {}
void GTransactionManager::rollback(main::ClientContext& clientContext,
                                   Transaction* transaction) {}
}  // namespace transaction
}  // namespace gs