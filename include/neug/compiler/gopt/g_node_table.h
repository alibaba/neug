/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <vector>

#include <span>
#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/storage/stats/table_stats.h"
#include "neug/compiler/storage/store/node_table.h"

namespace neug {
namespace storage {
class GNodeTable : public NodeTable {
 private:
  common::row_idx_t numRows;

 public:
  GNodeTable(const catalog::NodeTableCatalogEntry* tableEntry,
             StatsManager* storageManager, MemoryManager* memoryManager,
             common::row_idx_t numRows)
      : NodeTable{storageManager, tableEntry}, numRows{numRows} {}

  ~GNodeTable() override = default;

  common::row_idx_t getNumTotalRows(
      const transaction::Transaction* transaction) override {
    return numRows;
  }

  TableStats getStats(
      const transaction::Transaction* transaction) const override {
    std::vector<common::LogicalType> types;
    auto stats = TableStats{std::span<common::LogicalType>(types)};
    stats.incrementCardinality(numRows);
    return stats;
  }
};
}  // namespace storage
}  // namespace neug