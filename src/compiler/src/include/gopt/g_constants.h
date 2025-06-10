#pragma once

#include <cstdint>

#include "storage/wal/wal.h"
#include "transaction/transaction.h"

namespace gs {
class Constants {
 public:
  static inline uint64_t VARCHAR_MAX_LENGTH = 65536;
  static inline gs::transaction::Transaction DEFAULT_TRANSACTION =
      gs::transaction::Transaction(
          gs::transaction::TransactionType::DUMMY,
          gs::transaction::Transaction::DUMMY_TRANSACTION_ID,
          common::INVALID_TRANSACTION);
};
}  // namespace gs