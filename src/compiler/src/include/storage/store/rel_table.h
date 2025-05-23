#pragma once

#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "storage/store/table.h"

namespace kuzu {
namespace evaluator {
class ExpressionEvaluator;
}
namespace transaction {
class Transaction;
}
namespace storage {
class MemoryManager;

struct LocalRelTableScanState;

class KUZU_API RelTable : public Table {
public:
    RelTable(catalog::RelTableCatalogEntry* relTableEntry, const StorageManager* storageManager)
        : Table{relTableEntry, storageManager}, fromNodeTableID{relTableEntry->getSrcTableID()},
          toNodeTableID{relTableEntry->getDstTableID()}, nextRelOffset{0} {}

    RelTable(catalog::RelTableCatalogEntry* relTableEntry, const StorageManager* storageManager,
        MemoryManager* memoryManager, common::Deserializer* deSer = nullptr)
        : Table{relTableEntry, storageManager, memoryManager},
          fromNodeTableID{relTableEntry->getSrcTableID()},
          toNodeTableID{relTableEntry->getDstTableID()}, nextRelOffset{0} {}

    virtual common::row_idx_t getNumTotalRows(
        const transaction::Transaction* transaction) override = 0;

private:
private:
    common::table_id_t fromNodeTableID;
    common::table_id_t toNodeTableID;
    std::mutex relOffsetMtx;
    common::offset_t nextRelOffset;
};

} // namespace storage
} // namespace kuzu
