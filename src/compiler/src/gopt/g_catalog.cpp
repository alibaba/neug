#include "gopt/g_catalog.h"

#include <filesystem>
#include <iostream>

#include <boost/algorithm/string.hpp>
#include "common/constants.h"
#include "common/exception/runtime.h"
#include "common/string_format.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace catalog {
GCatalog::GCatalog(const std::string& yamlPath) : Catalog() {
  // load flex schema from file
  // std::cout << "before yaml" << std::endl;
  // if (std::filesystem::exists(yamlPath)) {
  //     std::cout << "yaml file exists" << std::endl;
  // } else {
  //     std::cout << "yaml file does not exist" << std::endl;
  // }
  auto schema = YAML::LoadFile(yamlPath);
  auto info = schema["schema"];
  if (!info) {
    throw common::RuntimeException("Cannot find schema in the yaml file.");
  }
  // std::cout << "after yaml" << std::endl;
  auto vertexTypes = info["vertex_types"].as<std::vector<YAML::Node>>();
  // map vertex type name to node table entry, when building rel table entry, we
  // need to find the corresponding src and dst node table entries
  std::unordered_map<std::string, NodeTableCatalogEntry*> nameToVertexMap;
  common::table_id_t maxLabelId = 0;
  for (const auto& vertexType : vertexTypes) {
    auto labelId = vertexType["type_id"].as<common::table_id_t>();
    if (labelId > maxLabelId) {
      maxLabelId = labelId;
    }
    auto nodeTableEntry = createNodeTableEntry(vertexType);
    nameToVertexMap[nodeTableEntry->getName()] = nodeTableEntry.get();
    tables->emplaceNoLock(std::move(nodeTableEntry));
  }
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
  kuzu::transaction::Transaction transaction(
      kuzu::transaction::TransactionType::DUMMY,
      kuzu::transaction::Transaction::DUMMY_TRANSACTION_ID,
      common::INVALID_TRANSACTION);
  for (auto& group : nameToSDPairMap) {
    // if the edge type has more than one rel table entries, we need to create a
    // rel group entry to record the mapping between the edge type and src/dst
    // pairs
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
  auto pks = info["primary_keys"].as<std::vector<std::string>>();
  if (pks.size() != 1) {
    throw common::RuntimeException(
        common::stringFormat("Only one primary key is supported for node "
                             "table, but {} are provided.",
                             pks.size()));
  }
  auto labelName = info["type_name"].as<std::string>();
  auto result = std::make_unique<NodeTableCatalogEntry>(labelName, pks[0]);
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
    throw common::RuntimeException(common::stringFormat(
        "Cannot find source vertex {} for rel table {}.", srcName, labelName));
  }
  auto dstEntry = nodeTableMap.at(dstName);
  if (!dstEntry) {
    throw common::RuntimeException(common::stringFormat(
        "Cannot find destination vertex {} for rel table {}.", dstName,
        labelName));
  }
  auto srcID = srcEntry->getTableID();
  auto dstID = dstEntry->getTableID();
  auto multiplicity = relation["relation"].as<std::string>();
  common::RelMultiplicity srcMultiplicity = common::RelMultiplicity::MANY;
  common::RelMultiplicity dstMultiplicity = common::RelMultiplicity::MANY;
  if (boost::iequals(multiplicity, "ONE_TO_ONE")) {
    srcMultiplicity = common::RelMultiplicity::ONE;
    dstMultiplicity = common::RelMultiplicity::ONE;
  } else if (boost::iequals(multiplicity, "MANY_TO_ONE")) {
    srcMultiplicity = common::RelMultiplicity::MANY;
    dstMultiplicity = common::RelMultiplicity::ONE;
  } else if (boost::iequals(multiplicity, "ONE_TO_MANY")) {
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
      result.add(kuzu::binder::PropertyDefinition(std::move(inner)));
    }
    break;
  }
  case common::TableType::REL: {
    for (auto& inner : getBaseRelStructFields()) {
      result.add(kuzu::binder::PropertyDefinition(std::move(inner)));
    }
    break;
  }
  }
  if (info["properties"]) {
    auto properties = info["properties"].as<std::vector<YAML::Node>>();
    for (const auto& property : properties) {
      auto name = property["property_name"].as<std::string>();
      auto type = property["property_type"];
      // auto id = property["property_id"].as<common::property_id_t>();
      auto columnDefinition = kuzu::binder::ColumnDefinition(
          name, GTypeUtils::createLogicalType(type));
      result.add(kuzu::binder::PropertyDefinition(std::move(columnDefinition)));
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
}  // namespace catalog
}  // namespace kuzu