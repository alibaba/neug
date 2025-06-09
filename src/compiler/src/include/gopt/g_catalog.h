#pragma once

#include <yaml-cpp/yaml.h>
#include <filesystem>
#include "binder/ddl/property_definition.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "gopt/g_rel_table_entry.h"
#include "gopt/g_type_utils.h"

namespace kuzu {
namespace catalog {
class GCatalog : public Catalog {
 public:
  GCatalog(const std::filesystem::path& schemaPath);
  GCatalog(const std::string& schemaData);
  ~GCatalog() = default;

 private:
  void loadSchema(const YAML::Node& schema);
  std::unique_ptr<TableCatalogEntry> createTableEntry(CatalogEntryType type,
                                                      const YAML::Node& info);
  std::unique_ptr<NodeTableCatalogEntry> createNodeTableEntry(
      const YAML::Node& info);
  void setTableEntry(const YAML::Node& info, TableCatalogEntry* result,
                     common::TableType type);
  std::unique_ptr<GRelTableCatalogEntry> createRelTableEntry(
      common::table_id_t tableId, common::table_id_t labelId,
      const std::string& labelName, const YAML::Node& relation,
      const std::unordered_map<std::string, NodeTableCatalogEntry*>&
          nodeTableMap);
  PropertyDefinitionCollection createPropertyDefinitionCollection(
      const YAML::Node& info, common::TableType type);
  static std::vector<binder::ColumnDefinition> getBaseNodeStructFields();
  static std::vector<binder::ColumnDefinition> getBaseRelStructFields();
  void validateYAMLStructure(const YAML::Node& schema);
  void validateVertexType(
      const YAML::Node& vertexType,
      const std::unordered_set<std::string>& existingNames,
      const std::unordered_set<common::table_id_t>& existingIds);
  void validateEdgeType(
      const YAML::Node& edgeType,
      const std::unordered_set<std::string>& existingNames,
      const std::unordered_set<common::table_id_t>& existingIds);
  void validatePropertyName(const std::string& name, common::TableType type);
};
}  // namespace catalog
}  // namespace kuzu