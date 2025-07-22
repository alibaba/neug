#pragma once

#include <cstdint>

#include "src/include/catalog/catalog_entry/node_table_catalog_entry.h"
#include "src/include/common/enums/table_type.h"
#include "src/include/common/types/types.h"
#include "src/include/storage/stats/table_stats.h"
#include "src/include/storage/store/table.h"

namespace gs {
namespace evaluator {
class ExpressionEvaluator;
}

namespace catalog {
class NodeTableCatalogEntry;
}

namespace transaction {
class Transaction;
}

namespace storage {
class NodeTable;

class StorageManager;

class KUZU_API NodeTable : public Table {
 public:
  NodeTable() = default;
  NodeTable(const StorageManager* storageManager,
            const catalog::NodeTableCatalogEntry* nodeTableEntry)
      : Table(nodeTableEntry, storageManager) {}
  NodeTable(const StorageManager* storageManager,
            const catalog::NodeTableCatalogEntry* nodeTableEntry,
            MemoryManager* memoryManager, common::VirtualFileSystem* vfs,
            main::ClientContext* context, common::Deserializer* deSer = nullptr)
      : Table(nodeTableEntry, storageManager, memoryManager) {}

  ~NodeTable() = default;

  virtual common::row_idx_t getNumTotalRows(
      const transaction::Transaction* transaction) override = 0;

  virtual TableStats getStats(
      const transaction::Transaction* transaction) const = 0;

 private:
 private:
};

}  // namespace storage
}  // namespace gs
