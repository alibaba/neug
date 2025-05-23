#pragma once

#include <cstdint>

#include "storage/wal/wal.h"
#include "transaction/transaction.h"

namespace kuzu {
class Constants {
public:
    static inline uint64_t VARCHAR_MAX_LENGTH = 65536;
    static inline kuzu::transaction::Transaction DEFAULT_TRANSACTION =
        kuzu::transaction::Transaction(kuzu::transaction::TransactionType::DUMMY,
            kuzu::transaction::Transaction::DUMMY_TRANSACTION_ID, common::INVALID_TRANSACTION);
    static inline kuzu::storage::WAL DEFAULT_WAL = kuzu::storage::WAL();
};
} // namespace kuzu