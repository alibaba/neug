/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include "neug/compiler/catalog/catalog_entry/rel_table_catalog_entry.h"
#include "neug/compiler/storage/store/table.h"

namespace neug {
namespace evaluator {
class ExpressionEvaluator;
}
namespace transaction {
class Transaction;
}
namespace storage {
class MemoryManager;

struct LocalRelTableScanState;

class NEUG_API RelTable : public Table {
 public:
  RelTable(catalog::RelTableCatalogEntry* relTableEntry,
           const StatsManager* storageManager)
      : Table{relTableEntry, storageManager},
        fromNodeTableID{relTableEntry->getSrcTableID()},
        toNodeTableID{relTableEntry->getDstTableID()},
        nextRelOffset{0} {}

  RelTable(catalog::RelTableCatalogEntry* relTableEntry,
           const StatsManager* storageManager, MemoryManager* memoryManager,
           common::Deserializer* deSer = nullptr)
      : Table{relTableEntry, storageManager, memoryManager},
        fromNodeTableID{relTableEntry->getSrcTableID()},
        toNodeTableID{relTableEntry->getDstTableID()},
        nextRelOffset{0} {}

  virtual common::row_idx_t getNumTotalRows(
      const transaction::Transaction* transaction) override = 0;

 private:
 private:
  common::table_id_t fromNodeTableID;
  common::table_id_t toNodeTableID;
  std::mutex relOffsetMtx;
  common::offset_t nextRelOffset;
};

}  // namespace storage
}  // namespace neug
