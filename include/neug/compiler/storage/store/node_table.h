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

#include <cstdint>

#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"
#include "neug/compiler/common/enums/table_type.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/storage/stats/table_stats.h"
#include "neug/compiler/storage/store/table.h"

namespace neug {
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

class StatsManager;

class NEUG_API NodeTable : public Table {
 public:
  NodeTable() = delete;
  NodeTable(const StatsManager* storageManager,
            const catalog::NodeTableCatalogEntry* nodeTableEntry)
      : Table(nodeTableEntry, storageManager) {}
  NodeTable(const StatsManager* storageManager,
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
}  // namespace neug
