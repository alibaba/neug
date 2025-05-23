#pragma once

#include <memory>
#include <mutex>
#include <unordered_set>

#include "common/constants.h"
#include "common/uniq_lock.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace testing {
class DBTest;
class FlakyBufferManager;
} // namespace testing

namespace transaction {

class TransactionManager {
    friend class testing::DBTest;
    friend class testing::FlakyBufferManager;

public:
    explicit TransactionManager(storage::WAL& wal)
        : wal{wal}, lastTransactionID{Transaction::START_TRANSACTION_ID}, lastTimestamp{1} {};

    virtual std::unique_ptr<Transaction> beginTransaction(main::ClientContext& clientContext,
        TransactionType type) = 0;

    virtual void commit(main::ClientContext& clientContext) = 0;
    virtual void rollback(main::ClientContext& clientContext, Transaction* transaction) = 0;

    virtual void checkpoint(main::ClientContext& clientContext);

private:
    bool canAutoCheckpoint(const main::ClientContext& clientContext) const;
    bool canCheckpointNoLock() const;
    void checkpointNoLock(main::ClientContext& clientContext);
    void finalizeCheckpointNoLock(main::ClientContext& clientContext);
    void rollbackCheckpoint(main::ClientContext& clientContext);

    common::UniqLock stopNewTransactionsAndWaitUntilAllTransactionsLeave();

    bool hasActiveWriteTransactionNoLock() const { return !activeWriteTransactions.empty(); }

    void setCheckPointWaitTimeoutForTransactionsToLeaveInMicros(uint64_t waitTimeInMicros) {
        checkpointWaitTimeoutInMicros = waitTimeInMicros;
    }

private:
    storage::WAL& wal;
    std::unordered_set<common::transaction_t> activeWriteTransactions;
    std::unordered_set<common::transaction_t> activeReadOnlyTransactions;
    common::transaction_t lastTransactionID;
    common::transaction_t lastTimestamp;
    std::mutex mtxForSerializingPublicFunctionCalls;
    std::mutex mtxForStartingNewTransactions;
    uint64_t checkpointWaitTimeoutInMicros = common::DEFAULT_CHECKPOINT_WAIT_TIMEOUT_IN_MICROS;
};
} // namespace transaction
} // namespace kuzu
