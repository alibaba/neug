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

#include "storage/store/rel_table.h"

namespace gs {
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
}  // namespace gs