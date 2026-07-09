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

#include "neug/compiler/common/enums/rel_direction.h"
#include "neug/compiler/common/mask.h"
#include "neug/storages/graph/schema.h"

namespace neug {
namespace evaluator {
class ExpressionEvaluator;
}
namespace storage {
class MemoryManager;
class Table;

class LocalTable;
class GraphStats;
class NEUG_API Table {
 public:
  Table(const SchemaEntry* tableEntry, const GraphStats* storageManager)
      : tableType{tableEntry->get_entry_type()},
        tableID{tableEntry->get_entry_id()},
        tableName{const_cast<SchemaEntry*>(tableEntry)->get_label()} {}

  Table(const SchemaEntry* tableEntry, const GraphStats* storageManager,
        MemoryManager* memoryManager);
  virtual ~Table() = default;

  SchemaEntryType getTableType() const { return tableType; }
  common::table_id_t getTableID() const { return tableID; }
  std::string getTableName() const { return tableName; }

  virtual common::row_idx_t getNumTotalRows(
      const transaction::Transaction* transaction) = 0;

  template <class TARGET>
  TARGET& cast() {
    return common::neug_dynamic_cast<TARGET&>(*this);
  }
  template <class TARGET>
  const TARGET& cast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }
  template <class TARGET>
  TARGET* ptrCast() {
    return common::neug_dynamic_cast<TARGET*>(this);
  }

  MemoryManager& getMemoryManager() const { return *memoryManager; }

 protected:
 protected:
  SchemaEntryType tableType;
  common::table_id_t tableID;
  std::string tableName;
  bool enableCompression;
  MemoryManager* memoryManager;
  bool hasChanges;
};

}  // namespace storage
}  // namespace neug
