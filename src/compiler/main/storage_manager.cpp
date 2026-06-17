#include <fstream>
#include <sstream>
#include "neug/compiler/storage/stats_manager.h"

#include "neug/compiler/catalog/catalog_entry/rel_group_catalog_entry.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/gopt/g_constants.h"
#include "neug/compiler/gopt/g_node_table.h"
#include "neug/compiler/gopt/g_rel_table.h"
#include "neug/compiler/main/metadata_manager.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace storage {

namespace {
StatsTableKey getStatsTableKey(catalog::SchemaEntry* entry) {
  return {entry->getTableType(), entry->getTableID()};
}

catalog::SchemaEntry* getTableCatalogEntry(
    const catalog::Catalog& catalog,
    const transaction::Transaction* transaction, common::table_id_t tableID,
    common::TableType tableType) {
  if (tableType == common::TableType::NODE) {
    for (auto* entry : catalog.getNodeTableEntries(transaction)) {
      if (entry->getTableID() == tableID) {
        return entry;
      }
    }
  } else {
    for (auto* entry : catalog.getRelTableEntries(transaction)) {
      if (entry->getTableID() == tableID) {
        return entry;
      }
    }
  }
  THROW_EXCEPTION_WITH_FILE_LINE("Cannot find table catalog entry with id " +
                                 std::to_string(tableID));
}
}  // namespace

StatsManager::StatsManager(const std::string& statsData,
                           main::MetadataManager* database,
                           MemoryManager& memoryManager)
    : memoryManager(memoryManager), database(database) {
  std::unordered_map<std::string, common::row_idx_t> countMap;
  getCardMap(statsData, countMap);
  if (!database || !database->getCatalog()) {
    THROW_EXCEPTION_WITH_FILE_LINE("Database or catalog is not initialized");
  }
  loadStats(*database->getCatalog(), countMap);
}

bool StatsManager::checkTableConsistency(Table* oldTable,
                                         catalog::SchemaEntry* curEntry) {
  if (oldTable->getTableType() != curEntry->getTableType() ||
      oldTable->getTableName() !=
          curEntry->getLabel(database->getCatalog(),
                             &neug::Constants::DEFAULT_TRANSACTION)) {
    return false;
  }
  if (oldTable->getTableType() == common::TableType::REL) {
    auto catalog = database->getCatalog();
    auto& transaction = neug::Constants ::DEFAULT_TRANSACTION;
    // check if the src and dst table ids are consistent
    auto oldRelTable = oldTable->ptrCast<GRelTable>();
    if (!catalog->containsTable(&transaction, oldRelTable->getSrcTableId()) ||
        !catalog->containsTable(&transaction, oldRelTable->getDstTableId())) {
      return false;
    }
    auto oldSrcEntry = catalog->getTableCatalogEntry(
        &transaction, oldRelTable->getSrcTableId());
    auto oldDstEntry = catalog->getTableCatalogEntry(
        &transaction, oldRelTable->getDstTableId());
    auto* curRelEntry = dynamic_cast<EdgeSchema*>(curEntry);
    NEUG_ASSERT(curRelEntry != nullptr);
    auto curSrcEntry = catalog->getTableCatalogEntry(
        &transaction, curRelEntry->getSrcTableID());
    auto curDstEntry = catalog->getTableCatalogEntry(
        &transaction, curRelEntry->getDstTableID());
    if (curSrcEntry->getTableType() != oldSrcEntry->getTableType() ||
        curDstEntry->getTableType() != oldDstEntry->getTableType() ||
        curSrcEntry->getLabel(catalog, &transaction) !=
            oldSrcEntry->getLabel(catalog, &transaction) ||
        curDstEntry->getLabel(catalog, &transaction) !=
            oldDstEntry->getLabel(catalog, &transaction)) {
      return false;
    }
  }
  return true;
}

// check if cached table data is consistent with the schema, return false if
// not, otherwise return true
Table* StatsManager::getTableByEntry(catalog::SchemaEntry* curEntry) {
  auto tableKey = getStatsTableKey(curEntry);
  if (tables.contains(tableKey)) {
    auto oldTable = tables.at(tableKey).get();
    if (checkTableConsistency(oldTable, curEntry)) {
      return oldTable;
    }
  }
  for (auto& [_, table] : tables) {
    if (checkTableConsistency(table.get(), curEntry)) {
      return table.get();
    }
  }
  return nullptr;
}

Table* StatsManager::getTable(common::table_id_t tableID) {
  auto& transaction = neug::Constants::DEFAULT_TRANSACTION;
  auto catalog = database->getCatalog();
  if (!catalog) {
    THROW_EXCEPTION_WITH_FILE_LINE("Catalog is not initialized");
  }
  NEUG_ASSERT(catalog->containsTable(&transaction, tableID));
  auto curEntry = catalog->getTableCatalogEntry(&transaction, tableID);
  return getTable(curEntry);
}

Table* StatsManager::getTable(common::table_id_t tableID,
                              common::TableType tableType) {
  auto& transaction = neug::Constants::DEFAULT_TRANSACTION;
  auto catalog = database->getCatalog();
  if (!catalog) {
    THROW_EXCEPTION_WITH_FILE_LINE("Catalog is not initialized");
  }
  auto curEntry =
      getTableCatalogEntry(*catalog, &transaction, tableID, tableType);
  return getTable(curEntry);
}

Table* StatsManager::getTable(catalog::SchemaEntry* curEntry) {
  Table* oldTable = getTableByEntry(curEntry);
  if (oldTable) {
    return oldTable;
  }
  switch (curEntry->getTableType()) {
  case common::TableType::NODE: {
    auto* vertexSchema = dynamic_cast<VertexSchema*>(curEntry);
    NEUG_ASSERT(vertexSchema != nullptr);
    auto defaultNode =
        std::make_unique<GNodeTable>(vertexSchema, this, &memoryManager, 1);
    return defaultNode.release();
  }
  case common::TableType::REL:
  default: {
    auto* edgeSchema = dynamic_cast<EdgeSchema*>(curEntry);
    NEUG_ASSERT(edgeSchema != nullptr);
    auto defaultRel = std::make_unique<GRelTable>(1, edgeSchema, this);
    return defaultRel.release();
  }
  }
}

StatsManager::StatsManager(const std::filesystem::path& statsPath,
                           main::MetadataManager* database,
                           MemoryManager& memoryManager)
    : memoryManager(memoryManager), database(database) {
  std::ifstream file(statsPath);
  if (!file.is_open()) {
    THROW_EXCEPTION_WITH_FILE_LINE("Statistics file " + statsPath.string() +
                                   " not found");
  }

  std::stringstream buffer;
  buffer << file.rdbuf();
  std::string statsJson = buffer.str();

  std::unordered_map<std::string, common::row_idx_t> countMap;
  getCardMap(statsJson, countMap);
  if (!database || !database->getCatalog()) {
    THROW_EXCEPTION_WITH_FILE_LINE("Database or catalog is not initialized");
  }
  loadStats(*database->getCatalog(), countMap);
}

void StatsManager::getCardMap(
    const std::string& json,
    std::unordered_map<std::string, common::row_idx_t>& countMap) {
  try {
    if (json.empty()) {
      // If JSON is empty, just return an empty countMap
      return;
    }

    rapidjson::Document jsonData;

    if (jsonData.Parse(json.c_str()).HasParseError()) {
      THROW_EXCEPTION_WITH_FILE_LINE("Invalid JSON format: " +
                                     std::to_string(jsonData.GetParseError()));
    }
    // Process node statistics if valid
    if (jsonData.HasMember("vertex_type_statistics") &&
        jsonData["vertex_type_statistics"].IsArray()) {
      for (const auto& nodeStat :
           jsonData["vertex_type_statistics"].GetArray()) {
        if (nodeStat.HasMember("type_name") && nodeStat.HasMember("count") &&
            nodeStat["type_name"].IsString() && nodeStat["count"].IsInt64()) {
          auto nodeName = nodeStat["type_name"].GetString();
          countMap[nodeName] = nodeStat["count"].GetInt64();
        }
      }
    }

    if (jsonData.HasMember("edge_type_statistics") &&
        jsonData["edge_type_statistics"].IsArray()) {
      for (const auto& relStat : jsonData["edge_type_statistics"].GetArray()) {
        if (relStat.HasMember("type_name") &&
            relStat.HasMember("vertex_type_pair_statistics") &&
            relStat["type_name"].IsString() &&
            relStat["vertex_type_pair_statistics"].IsArray()) {
          auto relName = relStat["type_name"].GetString();

          for (const auto& srcDst :
               relStat["vertex_type_pair_statistics"].GetArray()) {
            if (srcDst.HasMember("source_vertex") &&
                srcDst.HasMember("destination_vertex") &&
                srcDst.HasMember("count") &&
                srcDst["source_vertex"].IsString() &&
                srcDst["destination_vertex"].IsString() &&
                srcDst["count"].IsInt64()) {
              auto srcName = srcDst["source_vertex"].GetString();
              auto dstName = srcDst["destination_vertex"].GetString();
              auto childName = catalog::RelGroupCatalogEntry::getChildTableName(
                  relName, srcName, dstName);
              auto count = srcDst["count"].GetInt64();
              countMap[childName] = count;
            }
          }
        }
      }
    }
  } catch (const std::exception& e) {
    // If any error occurs during JSON processing, just continue with empty
    // countMap All counts will default to 0
  }
}

void StatsManager::loadStats(
    const catalog::Catalog& catalog,
    const std::unordered_map<std::string, common::row_idx_t>& countMap) {
  auto& transaction = neug::Constants::DEFAULT_TRANSACTION;

  // Process all node tables from catalog
  for (auto& tableEntry : catalog.getTableEntries(&transaction)) {
    if (tableEntry->getTableType() == common::TableType::NODE) {
      auto* nodeTableEntry = dynamic_cast<VertexSchema*>(tableEntry);
      NEUG_ASSERT(nodeTableEntry != nullptr);
      auto nodeName = nodeTableEntry->getLabel(&catalog, &transaction);
      auto count = countMap.count(nodeName) ? countMap.at(nodeName) : 1;
      tables[getStatsTableKey(nodeTableEntry)] = std::make_unique<GNodeTable>(
          nodeTableEntry, this, &memoryManager, count);
    } else if (tableEntry->getTableType() == common::TableType::REL) {
      auto* relTableEntry = dynamic_cast<EdgeSchema*>(tableEntry);
      NEUG_ASSERT(relTableEntry != nullptr);
      auto relName = catalog::RelGroupCatalogEntry::getChildTableName(
          relTableEntry->getEdgeLabelName(), relTableEntry->src_label_name,
          relTableEntry->dst_label_name);
      auto count = countMap.count(relName) ? countMap.at(relName) : 1;
      tables[getStatsTableKey(relTableEntry)] =
          std::make_unique<neug::storage::GRelTable>(count, relTableEntry,
                                                     this);
    }
  }
}

}  // namespace storage
}  // namespace neug
