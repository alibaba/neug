#pragma once

#include "neug/compiler/catalog/catalog_entry/table_catalog_entry.h"
#include "neug/compiler/common/enums/rel_direction.h"
#include "neug/compiler/common/mask.h"

namespace gs {
namespace evaluator {
class ExpressionEvaluator;
}
namespace storage {
class MemoryManager;
class Table;

class LocalTable;
class StorageManager;
class KUZU_API Table {
 public:
  Table(const catalog::TableCatalogEntry* tableEntry,
        const StorageManager* storageManager)
      : tableType{tableEntry->getTableType()},
        tableID{tableEntry->getTableID()},
        tableName{tableEntry->getName()} {}

  Table(const catalog::TableCatalogEntry* tableEntry,
        const StorageManager* storageManager, MemoryManager* memoryManager);
  virtual ~Table() = default;

  common::TableType getTableType() const { return tableType; }
  common::table_id_t getTableID() const { return tableID; }
  std::string getTableName() const { return tableName; }

  virtual common::row_idx_t getNumTotalRows(
      const transaction::Transaction* transaction) = 0;

  template <class TARGET>
  TARGET& cast() {
    return common::ku_dynamic_cast<TARGET&>(*this);
  }
  template <class TARGET>
  const TARGET& cast() const {
    return common::ku_dynamic_cast<const TARGET&>(*this);
  }
  template <class TARGET>
  TARGET* ptrCast() {
    return common::ku_dynamic_cast<TARGET*>(this);
  }

  MemoryManager& getMemoryManager() const { return *memoryManager; }

 protected:
 protected:
  common::TableType tableType;
  common::table_id_t tableID;
  std::string tableName;
  bool enableCompression;
  MemoryManager* memoryManager;
  bool hasChanges;
};

}  // namespace storage
}  // namespace gs
