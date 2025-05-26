#pragma once

#include "storage/store/rel_table.h"

namespace kuzu {
namespace storage {
class GRelTable : public RelTable {
 private:
  common::row_idx_t numRows;

 public:
  GRelTable(common::row_idx_t numRows,
            catalog::RelTableCatalogEntry* tableEntry,
            StorageManager* storageManager)
      : RelTable{tableEntry, storageManager}, numRows{numRows} {}

  ~GRelTable() override = default;

  common::row_idx_t getNumTotalRows(
      const transaction::Transaction* transaction) override {
    return this->numRows;
  }
};
}  // namespace storage
}  // namespace kuzu