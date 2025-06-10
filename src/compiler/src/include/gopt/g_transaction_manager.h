#pragma once
#include "storage/wal/wal.h"
#include "transaction/transaction_manager.h"

namespace gs {
namespace transaction {
class GTransactionManager : public TransactionManager {
 public:
  explicit GTransactionManager(storage::WAL& wal);
  std::unique_ptr<Transaction> beginTransaction(
      main::ClientContext& clientContext, TransactionType type) override;
  void commit(main::ClientContext& clientContext) override;
  void rollback(main::ClientContext& clientContext,
                Transaction* transaction) override;
  void checkpoint(main::ClientContext& clientContext) override;
};
}  // namespace transaction
}  // namespace gs