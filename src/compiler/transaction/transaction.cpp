#include "neug/compiler/transaction/transaction.h"

#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"
#include "neug/compiler/catalog/catalog_entry/rel_group_catalog_entry.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/main/option_config.h"
#include "neug/compiler/storage/wal/wal.h"
#include "neug/utils/exception/exception.h"

using namespace neug::catalog;

namespace neug {
namespace transaction {

bool LocalCacheManager::put(std::unique_ptr<LocalCacheObject> object) {
  std::unique_lock lck{mtx};
  auto key = object->getKey();
  if (cachedObjects.contains(key)) {
    return false;
  }
  cachedObjects[object->getKey()] = std::move(object);
  return true;
}

Transaction::Transaction(main::ClientContext& clientContext,
                         TransactionType transactionType,
                         common::transaction_t transactionID,
                         common::transaction_t startTS)
    : type{transactionType},
      ID{transactionID},
      startTS{startTS},
      commitTS{common::INVALID_TRANSACTION},
      forceCheckpoint{false},
      hasCatalogChanges{false} {}

Transaction::Transaction(TransactionType transactionType) noexcept
    : type{transactionType},
      ID{DUMMY_TRANSACTION_ID},
      startTS{DUMMY_START_TIMESTAMP},
      commitTS{common::INVALID_TRANSACTION},
      clientContext{nullptr},
      forceCheckpoint{false},
      hasCatalogChanges{false} {
  currentTS = common::Timestamp::getCurrentTimestamp().value;
}

Transaction::Transaction(TransactionType transactionType,
                         common::transaction_t ID,
                         common::transaction_t startTS) noexcept
    : type{transactionType},
      ID{ID},
      startTS{startTS},
      commitTS{common::INVALID_TRANSACTION},
      clientContext{nullptr},
      forceCheckpoint{false},
      hasCatalogChanges{false} {
  currentTS = common::Timestamp::getCurrentTimestamp().value;
}

void Transaction::commit(storage::WAL* wal) {}

void Transaction::rollback(storage::WAL* wal) {}

uint64_t Transaction::getEstimatedMemUsage() const { return 0; }

void Transaction::pushCreateDropCatalogEntry(CatalogSet& catalogSet,
                                             CatalogEntry& catalogEntry,
                                             bool isInternal,
                                             bool skipLoggingToWAL) {}

void Transaction::pushAlterCatalogEntry(
    CatalogSet& catalogSet, CatalogEntry& catalogEntry,
    const binder::BoundAlterInfo& alterInfo) {}

void Transaction::pushSequenceChange(SequenceCatalogEntry* sequenceEntry,
                                     int64_t kCount,
                                     const SequenceRollbackData& data) {}

void Transaction::pushInsertInfo(
    common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
    common::row_idx_t numRows,
    const storage::VersionRecordHandler* versionRecordHandler) const {}

void Transaction::pushDeleteInfo(
    common::node_group_idx_t nodeGroupIdx, common::row_idx_t startRow,
    common::row_idx_t numRows,
    const storage::VersionRecordHandler* versionRecordHandler) const {}

void Transaction::pushVectorUpdateInfo(
    storage::UpdateInfo& updateInfo, const common::idx_t vectorIdx,
    storage::VectorUpdateInfo& vectorUpdateInfo) const {}

Transaction::~Transaction() = default;

Transaction::Transaction(
    TransactionType transactionType, common::transaction_t ID,
    common::transaction_t startTS,
    std::unordered_map<common::table_id_t, common::offset_t>
        minUncommittedNodeOffsets)
    : type{transactionType},
      ID{ID},
      startTS{startTS},
      commitTS{common::INVALID_TRANSACTION},
      currentTS{INT64_MAX},
      clientContext{nullptr},
      forceCheckpoint{false},
      hasCatalogChanges{false},
      minUncommittedNodeOffsets{std::move(minUncommittedNodeOffsets)} {}

Transaction Transaction::getDummyTransactionFromExistingOne(
    const Transaction& other) {
  return Transaction(TransactionType::DUMMY, DUMMY_TRANSACTION_ID,
                     DUMMY_START_TIMESTAMP, other.minUncommittedNodeOffsets);
}

Transaction DUMMY_TRANSACTION = Transaction(TransactionType::DUMMY);
Transaction DUMMY_CHECKPOINT_TRANSACTION =
    Transaction(TransactionType::CHECKPOINT, Transaction::DUMMY_TRANSACTION_ID,
                Transaction::START_TRANSACTION_ID - 1);

}  // namespace transaction
}  // namespace neug
