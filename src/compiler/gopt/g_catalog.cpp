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

#include "neug/compiler/gopt/g_catalog.h"

#include <filesystem>

#include <yaml-cpp/node/node.h>
#include <string>
#include "neug/compiler/common/constants.h"
#include "neug/compiler/common/string_format.h"
#include "neug/compiler/function/function_signature_util.h"
#include "neug/compiler/gopt/g_constants.h"
#include "neug/compiler/transaction/transaction.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/string_utils.h"
#include "neug/compiler/function/function_collection.h"

namespace neug {
namespace catalog {

GCatalog::GCatalog() : Catalog() {
  registerBuiltInFunctions();
}

GCatalog::GCatalog(const std::filesystem::path& schemaPath) : Catalog() {
  if (!std::filesystem::exists(schemaPath)) {
    THROW_RUNTIME_ERROR(common::stringFormat("YAML file does not exist: {}",
                                             schemaPath.string()));
  }

  loadSchema(YAML::LoadFile(schemaPath));
}

GCatalog::GCatalog(const std::string& schemaData) : Catalog() {
  loadSchema(YAML::Load(schemaData));
}

GCatalog::GCatalog(const YAML::Node& schema) : Catalog() { loadSchema(schema); }

void GCatalog::updateSchema(const YAML::Node& schema_yaml_node) {
  // destroy the previous schema nodes and rels
  this->tables = std::make_unique<CatalogSet>();
  this->relGroups = std::make_unique<CatalogSet>();
  // create new schema nodes and rels
  loadSchema(schema_yaml_node);
}

void GCatalog::updateSchema(const std::filesystem::path& schemaPath) {
  if (!std::filesystem::exists(schemaPath)) {
    THROW_RUNTIME_ERROR(common::stringFormat("YAML file does not exist: {}",
                                             schemaPath.string()));
  }
  this->tables = std::make_unique<CatalogSet>();
  this->relGroups = std::make_unique<CatalogSet>();
  loadSchema(YAML::LoadFile(schemaPath));
}

void GCatalog::updateSchema(const std::string& schema) {
  this->tables = std::make_unique<CatalogSet>();
  this->relGroups = std::make_unique<CatalogSet>();
  loadSchema(YAML::Load(schema));
}

void GCatalog::registerBuiltInFunctions() {
  auto functionCollection = function::FunctionCollection::getFunctions();

  for (auto i = 0u; functionCollection[i].name != nullptr; ++i) {
    auto& f = functionCollection[i];
    auto functionSet = f.getFunctionSetFunc();
    addFunctionWithSignature(&neug::transaction::DUMMY_TRANSACTION, 
                           f.catalogEntryType, 
                           f.name, 
                           std::move(functionSet), 
                           false);
  }
}

void GCatalog::addFunctionWithSignature(transaction::Transaction* transaction,
                                        CatalogEntryType entryType,
                                        std::string name,
                                        function::function_set functionSet,
                                        bool isInternal) {
  for (auto& func : functionSet) {
    func->computeSignature();
  }
  addFunction(transaction, entryType, std::move(name), std::move(functionSet),
              isInternal);
}

function::Function* GCatalog::getFunctionWithSignature(
    const std::string& signatureName) {
  return getFunctionWithSignature(&neug::Constants::DEFAULT_TRANSACTION,
                                  signatureName);
}

function::Function* GCatalog::getFunctionWithSignature(
    transaction::Transaction* transaction, const std::string& signatureName) {
  auto funcName =
      function::FunctionSignatureUtil::getFunctionName(signatureName);
  auto entry = getFunctionEntry(transaction, funcName);
  if (!entry) {
    THROW_CATALOG_EXCEPTION("cannot find function entry with name " + funcName);
  }
  auto funcEntry = entry->ptrCast<catalog::FunctionCatalogEntry>();
  auto& functionSet = funcEntry->getFunctionSet();
  for (auto& func : functionSet) {
    if (func->signatureName == signatureName) {
      return func.get();
    }
  }
  THROW_CATALOG_EXCEPTION("cannot find function with signature name " +
                          signatureName);
}

void GCatalog::loadSchema(const YAML::Node& schema) {
  validateYAMLStructure(schema);

  auto info = schema["schema"];
  std::unordered_set<std::string> existingNames;
  std::unordered_set<common::table_id_t> existingIds;

  if (info["vertex_types"]) {
    auto vertexTypes = info["vertex_types"].as<std::vector<YAML::Node>>();
    for (const auto& vertexType : vertexTypes) {
      validateVertexType(vertexType, existingNames, existingIds);
      auto name = vertexType["type_name"].as<std::string>();
      auto id = vertexType["type_id"].as<common::table_id_t>();
      existingNames.insert(name);
      existingIds.insert(id);
    }
  }

  if (info["edge_types"]) {
    auto edgeTypes = info["edge_types"].as<std::vector<YAML::Node>>();
    existingIds.clear();
    for (const auto& edgeType : edgeTypes) {
      validateEdgeType(edgeType, existingNames, existingIds);
      auto name = edgeType["type_name"].as<std::string>();
      auto id = edgeType["type_id"].as<common::table_id_t>();
      existingNames.insert(name);
      existingIds.insert(id);
    }
  }

  common::table_id_t maxLabelId = 0;
  // map vertex type name to node table entry, when building rel table entry,
  // we need to find the corresponding src and dst node table entries
  std::unordered_map<std::string, NodeTableCatalogEntry*> nameToVertexMap;

  auto vertexTypes = info["vertex_types"].as<std::vector<YAML::Node>>();
  for (const auto& vertexType : vertexTypes) {
    auto labelId = vertexType["type_id"].as<common::table_id_t>();
    if (labelId > maxLabelId) {
      maxLabelId = labelId;
    }
    auto nodeTableEntry = createNodeTableEntry(vertexType);
    nameToVertexMap[nodeTableEntry->getName()] = nodeTableEntry.get();
    tables->emplaceNoLock(std::move(nodeTableEntry));
  }

  if (!info["edge_types"])
    return;

  auto edgeTypes = info["edge_types"].as<std::vector<YAML::Node>>();
  // map rel type name to list of rel table entries, each rel table entry is
  // conresponding to a <src, dst> pair here, we assign a unique table id to
  // each rel table entry by combining the rel type id and the src/dst node
  // table id
  std::unordered_map<std::string,
                     std::vector<std::unique_ptr<GRelTableCatalogEntry>>>
      nameToSDPairMap;
  std::unordered_map<std::string, common::table_id_t> labelNameToIdMap;
  for (const auto& edgeType : edgeTypes) {
    auto labelName = edgeType["type_name"].as<std::string>();
    auto labelId = edgeType["type_id"].as<common::table_id_t>();
    auto sDPairs =
        edgeType["vertex_type_pair_relations"].as<std::vector<YAML::Node>>();
    labelNameToIdMap[labelName] = labelId;
    for (auto& sDPair : sDPairs) {
      auto tableId = ++maxLabelId;
      auto relTableEntry = createRelTableEntry(tableId, labelId, labelName,
                                               sDPair, nameToVertexMap);
      setTableEntry(edgeType,
                    static_cast<TableCatalogEntry*>(relTableEntry.get()),
                    common::TableType::REL);
      // table id of each rel table entry is a combination of rel type id and
      // src/dst node table id
      nameToSDPairMap[labelName].emplace_back(std::move(relTableEntry));
      // // relTableEntry will lose ownership of the rel table entry after
      // invoking std::move() tables->emplaceNoLock(std::move(relTableEntry));
    }
  }

  auto& transaction = neug::Constants::DEFAULT_TRANSACTION;
  for (auto& group : nameToSDPairMap) {
    // if the edge type has more than one rel table entries, we need to create
    // a rel group entry to record the mapping between the edge type and
    // src/dst pairs
    if (group.second.size() > 1) {
      std::vector<common::table_id_t> relTableIDs;
      for (auto& relTableEntry : group.second) {
        auto srcEntry =
            tables->getEntryOfOID(&transaction, relTableEntry->getSrcTableID());
        auto dstEntry =
            tables->getEntryOfOID(&transaction, relTableEntry->getDstTableID());
        auto childName = RelGroupCatalogEntry::getChildTableName(
            group.first, srcEntry->getName(), dstEntry->getName());
        relTableEntry->rename(childName);
        relTableIDs.push_back(relTableEntry->getTableID());
        tables->emplaceNoLock(std::move(relTableEntry));
      }
      auto relGroupEntry =
          std::make_unique<RelGroupCatalogEntry>(group.first, relTableIDs);
      relGroupEntry->setOID(labelNameToIdMap[group.first]);
      relGroups->emplaceNoLock(std::move(relGroupEntry));
    } else if (group.second.size() == 1) {
      tables->emplaceNoLock(std::move(group.second.at(0)));
    }
  }
}

void GCatalog::setTableEntry(const YAML::Node& info, TableCatalogEntry* result,
                             common::TableType type) {
  result->setPropertyCollection(
      std::move(createPropertyDefinitionCollection(info, type)));
}

std::unique_ptr<NodeTableCatalogEntry> GCatalog::createNodeTableEntry(
    const YAML::Node& info) {
  std::string primaryKey;
  if (!info["primary_keys"] || info["primary_keys"].size() == 0) {
    primaryKey = "";
  } else {
    auto pks = info["primary_keys"].as<std::vector<std::string>>();
    if (pks.size() != 1) {
      THROW_RUNTIME_ERROR(
          common::stringFormat("Only one primary key is supported for node "
                               "table, but {} are provided.",
                               pks.size()));
    }
    primaryKey = pks[0];
  }
  auto labelName = info["type_name"].as<std::string>();
  auto result = std::make_unique<NodeTableCatalogEntry>(labelName, primaryKey);
  auto labelId = info["type_id"].as<common::table_id_t>();
  result->setOID(labelId);
  setTableEntry(info, result.get(), common::TableType::NODE);
  return result;
}

std::unique_ptr<GRelTableCatalogEntry> GCatalog::createRelTableEntry(
    common::table_id_t tableId, common::table_id_t labelId,
    const std::string& labelName, const YAML::Node& relation,
    const std::unordered_map<std::string, NodeTableCatalogEntry*>&
        nodeTableMap) {
  auto srcName = relation["source_vertex"].as<std::string>();
  auto dstName = relation["destination_vertex"].as<std::string>();
  auto srcEntry = nodeTableMap.at(srcName);
  if (!srcEntry) {
    THROW_RUNTIME_ERROR(common::stringFormat(
        "Cannot find source vertex {} for rel table {}.", srcName, labelName));
  }
  auto dstEntry = nodeTableMap.at(dstName);
  if (!dstEntry) {
    THROW_RUNTIME_ERROR(common::stringFormat(
        "Cannot find destination vertex {} for rel table {}.", dstName,
        labelName));
  }
  auto srcID = srcEntry->getTableID();
  auto dstID = dstEntry->getTableID();
  auto multiplicity = neug::to_lower_copy(relation["relation"].as<std::string>());
  common::RelMultiplicity srcMultiplicity = common::RelMultiplicity::MANY;
  common::RelMultiplicity dstMultiplicity = common::RelMultiplicity::MANY;
  if (multiplicity == "one_to_one") {
    srcMultiplicity = common::RelMultiplicity::ONE;
    dstMultiplicity = common::RelMultiplicity::ONE;
  } else if (multiplicity == "many_to_one") {
    srcMultiplicity = common::RelMultiplicity::MANY;
    dstMultiplicity = common::RelMultiplicity::ONE;
  } else if (multiplicity == "one_to_many") {
    srcMultiplicity = common::RelMultiplicity::ONE;
    dstMultiplicity = common::RelMultiplicity::MANY;
  }

  auto result = std::make_unique<GRelTableCatalogEntry>(
      labelName, srcMultiplicity, dstMultiplicity, tableId, labelId, srcID,
      dstID, common::ExtendDirection::BOTH);
  return result;
}

PropertyDefinitionCollection GCatalog::createPropertyDefinitionCollection(
    const YAML::Node& info, common::TableType type) {
  PropertyDefinitionCollection result;
  switch (type) {
  case common::TableType::NODE: {
    for (auto& inner : getBaseNodeStructFields()) {
      result.add(neug::binder::PropertyDefinition(std::move(inner)));
    }
    break;
  }
  case common::TableType::REL: {
    for (auto& inner : getBaseRelStructFields()) {
      result.add(neug::binder::PropertyDefinition(std::move(inner)));
    }
    break;
  }
  }
  if (info["properties"]) {
    auto properties = info["properties"].as<std::vector<YAML::Node>>();
    for (const auto& property : properties) {
      if (!property["property_name"] || !property["property_type"]) {
        THROW_RUNTIME_ERROR(
            "Property must have both property_name and property_type");
      }
      auto name = property["property_name"].as<std::string>();
      validatePropertyName(name, type);
      auto type = property["property_type"];
      auto columnDefinition = neug::binder::ColumnDefinition(
          name, GTypeUtils::createLogicalType(type));
      result.add(neug::binder::PropertyDefinition(std::move(columnDefinition)));
    }
  }

  return result;
}

std::vector<binder::ColumnDefinition> GCatalog::getBaseNodeStructFields() {
  std::vector<binder::ColumnDefinition> fields;
  fields.emplace_back(binder::ColumnDefinition(
      common::InternalKeyword::ID, common::LogicalType::INTERNAL_ID()));
  fields.emplace_back(binder::ColumnDefinition(common::InternalKeyword::LABEL,
                                               common::LogicalType::STRING()));
  return fields;
}

std::vector<binder::ColumnDefinition> GCatalog::getBaseRelStructFields() {
  std::vector<binder::ColumnDefinition> fields;
  fields.emplace_back(binder::ColumnDefinition(
      common::InternalKeyword::ID, common::LogicalType::INTERNAL_ID()));
  fields.emplace_back(binder::ColumnDefinition(
      common::InternalKeyword::SRC, common::LogicalType::INTERNAL_ID()));
  fields.emplace_back(binder::ColumnDefinition(
      common::InternalKeyword::DST, common::LogicalType::INTERNAL_ID()));
  fields.emplace_back(binder::ColumnDefinition(common::InternalKeyword::LABEL,
                                               common::LogicalType::STRING()));
  return fields;
}

void GCatalog::validateYAMLStructure(const YAML::Node& schema) {
  if (!schema.IsMap()) {
    THROW_RUNTIME_ERROR("Invalid YAML structure: root node must be a map");
  }

  auto info = schema["schema"];
  if (!info) {
    THROW_RUNTIME_ERROR("Cannot find schema in the YAML file.");
  }

  if (!info.IsMap()) {
    THROW_RUNTIME_ERROR("Invalid YAML structure: schema node must be a map");
  }
}

void GCatalog::validateVertexType(
    const YAML::Node& vertexType,
    const std::unordered_set<std::string>& existingNames,
    const std::unordered_set<common::table_id_t>& existingIds) {
  if (!vertexType.IsMap()) {
    THROW_RUNTIME_ERROR("Invalid vertex type structure: must be a map");
  }

  if (!vertexType["type_name"] || !vertexType["type_id"]) {
    THROW_RUNTIME_ERROR("Vertex type must have both type_name and type_id");
  }

  auto name = vertexType["type_name"].as<std::string>();
  auto id = vertexType["type_id"].as<common::table_id_t>();

  if (existingNames.count(name) > 0) {
    THROW_RUNTIME_ERROR(
        common::stringFormat("Duplicate vertex type name: {}", name));
  }

  if (existingIds.count(id) > 0) {
    THROW_RUNTIME_ERROR(
        common::stringFormat("Duplicate vertex type ID: {}", id));
  }
}

void GCatalog::validateEdgeType(
    const YAML::Node& edgeType,
    const std::unordered_set<std::string>& existingNames,
    const std::unordered_set<common::table_id_t>& existingIds) {
  if (!edgeType.IsMap()) {
    THROW_RUNTIME_ERROR("Invalid edge type structure: must be a map");
  }

  if (!edgeType["type_name"] || !edgeType["type_id"] ||
      !edgeType["vertex_type_pair_relations"]) {
    THROW_RUNTIME_ERROR(
        "Edge type must have type_name, type_id, "
        "and vertex_type_pair_relations");
  }

  auto name = edgeType["type_name"].as<std::string>();
  auto id = edgeType["type_id"].as<common::table_id_t>();

  if (existingNames.count(name) > 0) {
    THROW_RUNTIME_ERROR(
        common::stringFormat("Duplicate edge type name: {}", name));
  }

  if (existingIds.count(id) > 0) {
    THROW_RUNTIME_ERROR(common::stringFormat("Duplicate edge type ID: {}", id));
  }

  auto relations = edgeType["vertex_type_pair_relations"];
  if (!relations.IsSequence()) {
    THROW_RUNTIME_ERROR("vertex_type_pair_relations must be a sequence");
  }

  for (const auto& relation : relations) {
    if (!relation["source_vertex"] || !relation["destination_vertex"] ||
        !relation["relation"]) {
      THROW_RUNTIME_ERROR(
          "Each relation must have source_vertex, "
          "destination_vertex, and relation");
    }
  }
}

void GCatalog::validatePropertyName(const std::string& name,
                                    common::TableType type) {
  // Check for conflicts with base properties
  if (type == common::TableType::NODE) {
    if (name == common::InternalKeyword::ID ||
        name == common::InternalKeyword::LABEL) {
      THROW_RUNTIME_ERROR(common::stringFormat(
          "Property name '{}' conflicts with base node property", name));
    }
  } else if (type == common::TableType::REL) {
    if (name == common::InternalKeyword::ID ||
        name == common::InternalKeyword::SRC ||
        name == common::InternalKeyword::DST ||
        name == common::InternalKeyword::LABEL) {
      THROW_RUNTIME_ERROR(common::stringFormat(
          "Property name '{}' conflicts with base edge property", name));
    }
  }
}
}  // namespace catalog
}  // namespace neug