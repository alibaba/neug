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

#include <memory>

#include "neug/compiler/catalog/catalog_entry/function_catalog_entry.h"
#include "neug/compiler/catalog/catalog_set.h"
#include "neug/compiler/common/cast.h"
#include "neug/compiler/function/function.h"
#include "neug/storages/graph/schema.h"

namespace neug::main {
struct DBConfig;
}  // namespace neug::main

namespace neug {
namespace main {
class AttachedKuzuDatabase;
}  // namespace main

namespace binder {
struct BoundAlterInfo;
struct BoundCreateTableInfo;
struct BoundCreateSequenceInfo;
}  // namespace binder

namespace common {
class VirtualFileSystem;
}  // namespace common

namespace function {
struct ScalarMacroFunction;
}  // namespace function

namespace storage {
class WAL;
}  // namespace storage

namespace transaction {
class Transaction;
}  // namespace transaction

namespace extension {
class ExtensionAPI;
}

namespace catalog {
class FunctionCatalogEntry;
class SequenceCatalogEntry;
class IndexCatalogEntry;

class NEUG_API Catalog {
  friend class main::AttachedKuzuDatabase;
  friend class neug::extension::ExtensionAPI;

 public:
  // This is extended by DuckCatalog and PostgresCatalog.
  Catalog();
  Catalog(const std::string& directory, common::VirtualFileSystem* vfs);
  Catalog(const Catalog& other) = default;
  virtual ~Catalog() = default;

  // ----------------------------- Tables ----------------------------

  // Check if table entry exists.
  bool containsTable(const transaction::Transaction* transaction,
                     const std::string& tableName,
                     bool useInternal = true) const;
  bool containsTable(const transaction::Transaction* transaction,
                     common::table_id_t tableID, bool useInternal = true) const;
  // Get table entry with name.
  SchemaEntry* getTableCatalogEntry(const transaction::Transaction* transaction,
                                    const std::string& tableName,
                                    bool useInternal = true) const;
  // Get table entry with id.
  const SchemaEntry* getTableCatalogEntry(
      const transaction::Transaction* transaction,
      common::table_id_t tableID) const;
  // Get all node table entries.
  std::vector<VertexSchema*> getNodeTableEntries(
      const transaction::Transaction* transaction,
      bool useInternal = true) const;
  // Get all rel table entries.
  std::vector<EdgeSchema*> getRelTableEntries(
      const transaction::Transaction* transaction,
      bool useInternal = true) const;
  // Get all table entries.
  std::vector<SchemaEntry*> getTableEntries(
      const transaction::Transaction* transaction,
      bool useInternal = true) const;

  // ----------------------------- Rel groups ----------------------------

  // Check if rel group entry exists.
  bool containsRelGroup(const transaction::Transaction* transaction,
                        const std::string& name) const;
  // Get rel group entry with name.
  std::vector<EdgeSchema*> getRelGroupEntry(
      const transaction::Transaction* transaction,
      const std::string& name) const;

  // ----------------------------- Types ----------------------------

  // Check if type entry exists.
  bool containsType(const transaction::Transaction* transaction,
                    const std::string& name) const;
  // Get type entry with name.
  common::DataType getType(const transaction::Transaction*,
                           const std::string& name) const;

  // Create type entry.
  void createType(transaction::Transaction* transaction, std::string name,
                  common::DataType type);

  // ----------------------------- Functions ----------------------------

  // Check if function exists.
  bool containsFunction(const transaction::Transaction* transaction,
                        const std::string& name,
                        bool useInternal = false) const;
  // Get function entry by name.
  // Note we cannot cast to FunctionEntry here because result could also be a
  // MacroEntry.
  CatalogEntry* getFunctionEntry(const transaction::Transaction* transaction,
                                 const std::string& name,
                                 bool useInternal = false) const;
  // Get all function entries.
  std::vector<FunctionCatalogEntry*> getFunctionEntries(
      const transaction::Transaction* transaction) const;

  // Add function with name.
  void addFunction(transaction::Transaction* transaction,
                   CatalogEntryType entryType, std::string name,
                   function::function_set functionSet, bool isInternal = false);

  // Drop function with name.
  void dropFunction(transaction::Transaction* transaction,
                    const std::string& name);

  void incrementVersion() { version++; }
  uint64_t getVersion() const { return version; }

  template <class TARGET>
  TARGET* ptrCast() {
    return common::neug_dynamic_cast<TARGET*>(this);
  }

  virtual std::unique_ptr<Catalog> clone(const Schema* schema) const;

 private:
  void initCatalogSets();

 protected:
  void setSchema(const Schema* schema);

  const Schema* schema;

 private:
  std::shared_ptr<CatalogSet> sequences;
  std::shared_ptr<CatalogSet> functions;
  std::shared_ptr<CatalogSet> types;
  std::shared_ptr<CatalogSet> indexes;
  std::shared_ptr<CatalogSet> internalTables;
  std::shared_ptr<CatalogSet> internalSequences;
  std::shared_ptr<CatalogSet> internalFunctions;

  uint64_t version;
};

}  // namespace catalog
}  // namespace neug
