#pragma once

#include <vector>

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/types/types.h"
#include "storage/store/node_table.h"
#include <span>

namespace kuzu {
namespace storage {
class GNodeTable : public NodeTable {
private:
    common::row_idx_t numRows;

public:
    GNodeTable(const catalog::NodeTableCatalogEntry* tableEntry, StorageManager* storageManager,
        MemoryManager* memoryManager, common::VirtualFileSystem* vfs, main::ClientContext* context,
        common::row_idx_t numRows)
        : NodeTable{storageManager, tableEntry}, numRows{numRows} {}

    ~GNodeTable() override = default;

    common::row_idx_t getNumTotalRows(const transaction::Transaction* transaction) override {
        return numRows;
    }

    TableStats getStats(const transaction::Transaction* transaction) const override {
        std::vector<common::LogicalType> types;
        auto stats = TableStats{std::span<common::LogicalType>(types)};
        stats.incrementCardinality(numRows);
        return stats;
    }
};
} // namespace storage
} // namespace kuzu