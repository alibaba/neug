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

#include "neug/compiler/catalog/catalog.h"

#include <limits>

#include "neug/compiler/catalog/catalog_entry/function_catalog_entry.h"
#include "neug/compiler/catalog/catalog_entry/type_catalog_entry.h"
#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/compiler/common/serializer/deserializer.h"
#include "neug/compiler/common/serializer/serializer.h"
#include "neug/compiler/common/string_format.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/extension/extension_api.h"
#include "neug/compiler/extension/extension_manager.h"
#include "neug/compiler/function/function_collection.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/compiler/main/option_config.h"
#include "neug/compiler/transaction/transaction.h"
#include "neug/utils/exception/exception.h"

using namespace neug::binder;
using namespace neug::common;
using namespace neug::storage;
using namespace neug::transaction;

namespace neug {
namespace catalog {

namespace {
std::string getChildRelTableName(const EdgeSchema& edgeSchema) {
  return edgeSchema.edge_label_name + "_" + edgeSchema.src_label_name + "_" +
         edgeSchema.dst_label_name;
}

bool nameEquals(std::string_view lhs, std::string_view rhs) {
  return common::StringUtils::caseInsensitiveEquals(lhs, rhs);
}
}  // namespace

Catalog::Catalog() : schema{nullptr}, version{0} { initCatalogSets(); }

Catalog::Catalog(const std::string& directory, VirtualFileSystem* vfs)
    : schema{nullptr}, version{0} {}

std::unique_ptr<Catalog> Catalog::clone(const Schema* schema) const {
  auto cloned = std::make_unique<Catalog>(*this);
  cloned->setSchema(schema);
  return cloned;
}

void Catalog::setSchema(const Schema* schema) {
  this->schema = schema;
  incrementVersion();
}

void Catalog::initCatalogSets() {
  sequences = std::make_shared<CatalogSet>();
  functions = std::make_shared<CatalogSet>();
  types = std::make_shared<CatalogSet>();
  indexes = std::make_shared<CatalogSet>();
  internalTables = std::make_shared<CatalogSet>(true /* isInternal */);
  internalSequences = std::make_shared<CatalogSet>(true /* isInternal */);
  internalFunctions = std::make_shared<CatalogSet>(true /* isInternal */);
}

bool Catalog::containsTable(const Transaction* transaction,
                            const std::string& tableName,
                            bool useInternal) const {
  if (schema == nullptr) {
    return false;
  }
  for (auto& entry : schema->get_all_vertex_schemas()) {
    if (entry != nullptr &&
        schema->is_vertex_label_valid(entry->get_entry_id()) &&
        nameEquals(entry->label_name, tableName)) {
      return true;
    }
  }
  for (auto& [_, edgeSchema] : schema->get_all_edge_schemas()) {
    if (!schema->is_vertex_label_valid(edgeSchema->getSrcTableID()) ||
        !schema->is_vertex_label_valid(edgeSchema->getDstTableID())) {
      continue;
    }
    if (nameEquals(edgeSchema->edge_label_name, tableName) ||
        nameEquals(getChildRelTableName(*edgeSchema), tableName)) {
      return true;
    }
  }
  return false;
}

bool Catalog::containsTable(const Transaction* transaction, table_id_t tableID,
                            bool useInternal) const {
  if (schema == nullptr) {
    return false;
  }
  if (tableID <= std::numeric_limits<label_t>::max() &&
      schema->is_vertex_label_valid(static_cast<label_t>(tableID))) {
    return true;
  }
  for (auto& [_, edgeSchema] : schema->get_all_edge_schemas()) {
    if (edgeSchema->get_entry_id() == tableID ||
        edgeSchema->getLabelId() == tableID) {
      return true;
    }
  }
  return false;
}

const SchemaEntry* Catalog::getTableCatalogEntry(const Transaction* transaction,
                                                 table_id_t tableID) const {
  if (schema != nullptr && tableID <= std::numeric_limits<label_t>::max() &&
      schema->is_vertex_label_valid(static_cast<label_t>(tableID))) {
    return schema->get_vertex_schema(static_cast<label_t>(tableID)).get();
  }
  if (schema != nullptr) {
    const EdgeSchema* labelMatch = nullptr;
    for (auto& [_, edgeSchema] : schema->get_all_edge_schemas()) {
      if (edgeSchema->get_entry_id() == tableID) {
        return edgeSchema.get();
      }
      if (edgeSchema->getLabelId() == tableID) {
        if (labelMatch != nullptr) {
          THROW_SCHEMA_MISMATCH(
              stringFormat("Edge label id {} maps to multiple edge schemas.",
                           std::to_string(tableID)));
        }
        labelMatch = edgeSchema.get();
      }
    }
    if (labelMatch != nullptr) {
      return labelMatch;
    }
  }
  THROW_SCHEMA_MISMATCH(stringFormat(
      "Cannot find table catalog entry with id {}.", std::to_string(tableID)));
}

SchemaEntry* Catalog::getTableCatalogEntry(const Transaction* transaction,
                                           const std::string& tableName,
                                           bool useInternal) const {
  if (schema != nullptr) {
    VertexSchema* vertexResult = nullptr;
    for (auto& entry : schema->get_all_vertex_schemas()) {
      if (entry == nullptr ||
          !schema->is_vertex_label_valid(entry->get_entry_id()) ||
          !nameEquals(entry->label_name, tableName)) {
        continue;
      }
      if (vertexResult != nullptr) {
        THROW_SCHEMA_MISMATCH(stringFormat(
            "{} maps to multiple vertex labels in catalog.", tableName));
      }
      vertexResult = entry.get();
    }
    if (vertexResult != nullptr) {
      return vertexResult;
    }
  }
  EdgeSchema* result = nullptr;
  if (schema != nullptr) {
    for (auto& [_, edgeSchema] : schema->get_all_edge_schemas()) {
      if (!schema->is_vertex_label_valid(edgeSchema->getSrcTableID()) ||
          !schema->is_vertex_label_valid(edgeSchema->getDstTableID())) {
        continue;
      }
      if (!nameEquals(edgeSchema->edge_label_name, tableName) &&
          !nameEquals(getChildRelTableName(*edgeSchema), tableName)) {
        continue;
      }
      if (result != nullptr) {
        THROW_SCHEMA_MISMATCH(stringFormat(
            "{} has multiple source/destination pairs in catalog.", tableName));
      }
      result = edgeSchema.get();
    }
  }
  if (result == nullptr) {
    THROW_SCHEMA_MISMATCH(
        stringFormat("{} does not exist in catalog.", tableName));
  }
  return result;
}

std::vector<VertexSchema*> Catalog::getNodeTableEntries(
    const Transaction* transaction, bool useInternal) const {
  std::vector<VertexSchema*> result;
  if (schema == nullptr) {
    return result;
  }
  for (auto& entry : schema->get_all_vertex_schemas()) {
    if (entry != nullptr &&
        schema->is_vertex_label_valid(entry->get_entry_id())) {
      result.push_back(entry.get());
    }
  }
  return result;
}

std::vector<EdgeSchema*> Catalog::getRelTableEntries(
    const Transaction* transaction, bool useInternal) const {
  std::vector<EdgeSchema*> result;
  if (schema == nullptr) {
    return result;
  }
  for (auto& [_, entry] : schema->get_all_edge_schemas()) {
    if (!schema->is_vertex_label_valid(entry->getSrcTableID()) ||
        !schema->is_vertex_label_valid(entry->getDstTableID())) {
      continue;
    }
    result.push_back(entry.get());
  }
  std::sort(result.begin(), result.end(), [](const auto* lhs, const auto* rhs) {
    return std::tie(lhs->edge_label_id, lhs->src_label_id, lhs->dst_label_id,
                    lhs->entry_id) < std::tie(rhs->edge_label_id,
                                              rhs->src_label_id,
                                              rhs->dst_label_id, rhs->entry_id);
  });
  return result;
}

std::vector<SchemaEntry*> Catalog::getTableEntries(
    const Transaction* transaction, bool useInternal) const {
  std::vector<SchemaEntry*> result;
  for (auto* entry : getNodeTableEntries(transaction, useInternal)) {
    result.push_back(entry);
  }
  for (auto* entry : getRelTableEntries(transaction, useInternal)) {
    result.push_back(entry);
  }
  return result;
}

bool Catalog::containsRelGroup(const Transaction* transaction,
                               const std::string& name) const {
  if (schema == nullptr) {
    return false;
  }
  common::idx_t count = 0;
  for (auto& [_, edgeSchema] : schema->get_all_edge_schemas()) {
    if (!schema->is_vertex_label_valid(edgeSchema->getSrcTableID()) ||
        !schema->is_vertex_label_valid(edgeSchema->getDstTableID())) {
      continue;
    }
    if (nameEquals(edgeSchema->edge_label_name, name)) {
      ++count;
    }
  }
  return count > 1;
}

std::vector<EdgeSchema*> Catalog::getRelGroupEntry(
    const Transaction* transaction, const std::string& name) const {
  std::vector<EdgeSchema*> result;
  if (schema != nullptr) {
    for (auto& [_, edgeSchema] : schema->get_all_edge_schemas()) {
      if (!schema->is_vertex_label_valid(edgeSchema->getSrcTableID()) ||
          !schema->is_vertex_label_valid(edgeSchema->getDstTableID())) {
        continue;
      }
      if (nameEquals(edgeSchema->edge_label_name, name)) {
        result.push_back(edgeSchema.get());
      }
    }
  }
  std::sort(result.begin(), result.end(), [](const auto* lhs, const auto* rhs) {
    return std::tie(lhs->edge_label_id, lhs->src_label_id, lhs->dst_label_id,
                    lhs->entry_id) < std::tie(rhs->edge_label_id,
                                              rhs->src_label_id,
                                              rhs->dst_label_id, rhs->entry_id);
  });
  if (result.empty()) {
    THROW_SCHEMA_MISMATCH(
        stringFormat("Cannot find rel group entry {}.", name));
  }
  return result;
}

void Catalog::createType(Transaction* transaction, std::string name,
                         DataType type) {
  if (types->containsEntry(transaction, name)) {
    return;
  }
  auto entry =
      std::make_unique<TypeCatalogEntry>(std::move(name), std::move(type));
  types->createEntry(transaction, std::move(entry));
}

static std::string getInstallExtensionMessage(std::string_view extensionName,
                                              std::string_view entryType) {
  return stringFormat(
      "This {} exists in the {} "
      "extension. You can install and load the "
      "extension by running 'INSTALL {}; LOAD EXTENSION {};'.",
      entryType, extensionName, extensionName, extensionName);
}

static std::string getTypeDoesNotExistMessage(std::string_view entryName) {
  std::string message = stringFormat(
      "{} is neither an internal type nor a user defined type.", entryName);
  return message;
}

DataType Catalog::getType(const Transaction* transaction,
                          const std::string& name) const {
  if (!types->containsEntry(transaction, name)) {
    THROW_CATALOG_EXCEPTION(getTypeDoesNotExistMessage(name));
  }
  return types->getEntry(transaction, name)
      ->constCast<TypeCatalogEntry>()
      .getLogicalType()
      .copy();
}

bool Catalog::containsType(const Transaction* transaction,
                           const std::string& typeName) const {
  return types->containsEntry(transaction, typeName);
}

bool Catalog::containsFunction(const Transaction* transaction,
                               const std::string& name,
                               bool useInternal) const {
  auto hasEntry = functions->containsEntry(transaction, name);
  if (!hasEntry && useInternal) {
    return internalFunctions->containsEntry(transaction, name);
  }
  return hasEntry;
}

void Catalog::addFunction(Transaction* transaction, CatalogEntryType entryType,
                          std::string name, function::function_set functionSet,
                          bool isInternal) {
  auto& catalogSet = isInternal ? internalFunctions : functions;
  if (catalogSet->containsEntry(transaction, name)) {
    THROW_CATALOG_EXCEPTION(stringFormat("function {} already exists.", name));
  }
  catalogSet->createEntry(
      transaction, std::make_unique<FunctionCatalogEntry>(
                       entryType, std::move(name), std::move(functionSet)));
}

static std::string getFunctionDoesNotExistMessage(std::string_view entryName) {
  std::string message = stringFormat("function {} does not exist.", entryName);
  return message;
}

void Catalog::dropFunction(Transaction* transaction, const std::string& name) {
  if (!containsFunction(transaction, name)) {
    THROW_CATALOG_EXCEPTION(stringFormat("function {} doesn't exist.", name));
  }
  auto entry = getFunctionEntry(transaction, name);
  functions->dropEntry(transaction, name, entry->getOID());
}

CatalogEntry* Catalog::getFunctionEntry(const Transaction* transaction,
                                        const std::string& name,
                                        bool useInternal) const {
  CatalogEntry* result = nullptr;
  if (!functions->containsEntry(transaction, name)) {
    if (!useInternal) {
      THROW_CATALOG_EXCEPTION(getFunctionDoesNotExistMessage(name));
    }
    result = internalFunctions->getEntry(transaction, name);
  } else {
    result = functions->getEntry(transaction, name);
  }
  return result;
}

std::vector<FunctionCatalogEntry*> Catalog::getFunctionEntries(
    const Transaction* transaction) const {
  std::vector<FunctionCatalogEntry*> result;
  for (auto& [_, entry] : functions->getEntries(transaction)) {
    result.push_back(entry->ptrCast<FunctionCatalogEntry>());
  }
  return result;
}

}  // namespace catalog
}  // namespace neug
