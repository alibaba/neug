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

#include "neug/compiler/storage/store/rel_table.h"

namespace neug {
namespace storage {
class GRelTable : public RelTable {
 private:
  common::row_idx_t numRows;
  common::table_id_t srcTableId;
  common::table_id_t dstTableId;

 public:
  GRelTable(common::row_idx_t numRows,
            catalog::RelTableCatalogEntry* tableEntry,
            StatsManager* storageManager)
      : RelTable{tableEntry, storageManager},
        numRows{numRows},
        srcTableId{tableEntry->getSrcTableID()},
        dstTableId{tableEntry->getDstTableID()} {}

  ~GRelTable() override = default;

  common::row_idx_t getNumTotalRows(
      const transaction::Transaction* transaction) override {
    return this->numRows;
  }

  common::table_id_t getSrcTableId() const { return this->srcTableId; }
  common::table_id_t getDstTableId() const { return this->dstTableId; }
};
}  // namespace storage
}  // namespace neug