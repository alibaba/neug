#include "gopt/g_storage_manager.h"

#include <fstream>

#include <nlohmann/json.hpp>
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "gopt/g_constants.h"
#include "gopt/g_node_table.h"
#include "gopt/g_rel_table.h"

namespace kuzu {
namespace storage {

void GStorageManager::loadTables(const catalog::Catalog& catalog,
                                 common::VirtualFileSystem* vfs,
                                 main::ClientContext* context) {
  std::ifstream jsonFile(statsPath);
  if (!jsonFile.is_open()) {
    throw std::runtime_error("Could not open JSON file: " + statsPath);
  }
  // Parse the JSON file
  nlohmann::json jsonData;
  jsonFile >> jsonData;

  auto& transaction = kuzu::Constants::DEFAULT_TRANSACTION;

  for (const auto& nodeStat : jsonData["vertex_type_statistics"]) {
    // Process each node's statistics
    auto nodeName = nodeStat["type_name"].get<std::string>();
    if (!catalog.containsTable(&transaction, nodeName)) {
      throw std::runtime_error("Node table " + nodeName +
                               " not found in catalog.");
    }
    auto nodeTableEntry = dynamic_cast<catalog::NodeTableCatalogEntry*>(
        catalog.getTableCatalogEntry(&transaction, nodeName));
    auto count = nodeStat["count"].get<common::row_idx_t>();
    tables[nodeTableEntry->getTableID()] = std::make_unique<GNodeTable>(
        nodeTableEntry, this, &memoryManager, vfs, context, count);
  }

  for (const auto& relStat : jsonData["edge_type_statistics"]) {
    // Process each relationship's statistics
    auto relName = relStat["type_name"].get<std::string>();
    bool containsGroup = catalog.containsRelGroup(&transaction, relName);
    for (const auto& srcDst : relStat["vertex_type_pair_statistics"]) {
      auto srcName = srcDst["source_vertex"].get<std::string>();
      auto dstName = srcDst["destination_vertex"].get<std::string>();
      auto childName = relName;
      if (containsGroup) {
        childName = kuzu::catalog::RelGroupCatalogEntry::getChildTableName(
            relName, srcName, dstName);
      }
      if (!catalog.containsTable(&transaction, childName)) {
        throw std::runtime_error("Rel table " + childName +
                                 " not found in catalog.");
      }
      auto relTableEntry = dynamic_cast<catalog::RelTableCatalogEntry*>(
          catalog.getTableCatalogEntry(&transaction, childName));
      auto count = srcDst["count"].get<common::row_idx_t>();
      tables[relTableEntry->getTableID()] =
          std::make_unique<kuzu::storage::GRelTable>(count, relTableEntry,
                                                     this);
    }
  }
}
}  // namespace storage
}  // namespace kuzu