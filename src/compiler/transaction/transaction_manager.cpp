#include "src/include/transaction/transaction_manager.h"

#include <thread>

#include "src/include/common/exception/checkpoint.h"
#include "src/include/common/exception/transaction_manager.h"
#include "src/include/main/client_context.h"
#include "src/include/main/db_config.h"
#include "src/include/storage/storage_manager.h"

using namespace gs::common;
using namespace gs::storage;

namespace gs {
namespace transaction {

std::unique_ptr<Transaction> TransactionManager::beginTransaction(
    main::ClientContext& clientContext, TransactionType type) {
  return nullptr;
}

void TransactionManager::commit(main::ClientContext& clientContext) {}

void TransactionManager::rollback(main::ClientContext& clientContext,
                                  Transaction* transaction) {}

void TransactionManager::rollbackCheckpoint(
    main::ClientContext& clientContext) {}

void TransactionManager::checkpoint(main::ClientContext& clientContext) {}

UniqLock
TransactionManager::stopNewTransactionsAndWaitUntilAllTransactionsLeave() {
  UniqLock startTransactionLock{mtxForStartingNewTransactions};
  uint64_t numTimesWaited = 0;
  while (true) {
    if (!canCheckpointNoLock()) {
      numTimesWaited++;
      if (numTimesWaited * THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS >
          checkpointWaitTimeoutInMicros) {
        throw TransactionManagerException(
            "Timeout waiting for active transactions to leave the system "
            "before "
            "checkpointing. If you have an open transaction, please close it "
            "and try "
            "again.");
      }
      std::this_thread::sleep_for(
          std::chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
    } else {
      break;
    }
  }
  return startTransactionLock;
}

bool TransactionManager::canAutoCheckpoint(
    const main::ClientContext& clientContext) const {
  if (main::DBConfig::isDBPathInMemory(clientContext.getDatabasePath())) {
    return false;
  }
  if (!clientContext.getDBConfig()->autoCheckpoint) {
    return false;
  }
  if (clientContext.getTransaction()->isRecovery()) {
    return false;
  }
  const auto expectedSize =
      clientContext.getTransaction()->getEstimatedMemUsage() +
      wal.getFileSize();
  return expectedSize > clientContext.getDBConfig()->checkpointThreshold;
}

bool TransactionManager::canCheckpointNoLock() const {
  return activeWriteTransactions.empty() && activeReadOnlyTransactions.empty();
}

void TransactionManager::finalizeCheckpointNoLock(
    main::ClientContext& clientContext) {}

void TransactionManager::checkpointNoLock(main::ClientContext& clientContext) {}

}  // namespace transaction
}  // namespace gs
