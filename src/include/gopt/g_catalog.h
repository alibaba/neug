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

#include <yaml-cpp/yaml.h>
#include <filesystem>
#include "src/include/binder/ddl/property_definition.h"
#include "src/include/catalog/catalog.h"
#include "src/include/catalog/catalog_entry/node_table_catalog_entry.h"
#include "src/include/catalog/catalog_entry/rel_group_catalog_entry.h"
#include "src/include/catalog/catalog_entry/table_catalog_entry.h"
#include "src/include/gopt/g_rel_table_entry.h"
#include "src/include/gopt/g_type_utils.h"

namespace gs {
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
}  // namespace gs