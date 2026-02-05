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

#include <yaml-cpp/node/node.h>
#include <vector>
#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/expression/rel_expression.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/gopt/g_catalog.h"
#include "neug/compiler/gopt/g_constants.h"

namespace neug {
namespace gopt {
struct GNodeType {
  GNodeType(const std::vector<catalog::NodeTableCatalogEntry*>& nodeTables_)
      : nodeTables{nodeTables_} {}

  GNodeType(const binder::NodeExpression& nodeExpr) {
    nodeTables.reserve(nodeExpr.getNumEntries());
    for (auto& table : nodeExpr.getEntries()) {
      auto nodeTable = dynamic_cast<catalog::NodeTableCatalogEntry*>(table);
      if (nodeTable) {
        nodeTables.emplace_back(nodeTable);
      } else {
        THROW_EXCEPTION_WITH_FILE_LINE("Expected a NodeTableCatalogEntry.");
      }
    }
  }

  void setNodeTables(
      std::vector<catalog::NodeTableCatalogEntry*>&& nodeTables) {
    this->nodeTables = std::move(nodeTables);
  }

  std::vector<common::table_id_t> getLabelIds() const {
    std::vector<common::table_id_t> labelIds;
    labelIds.reserve(nodeTables.size());
    for (const auto& nodeTable : nodeTables) {
      if (std::find(labelIds.begin(), labelIds.end(),
                    nodeTable->getTableID()) == labelIds.end()) {
        labelIds.emplace_back(
            nodeTable->getTableID());  // avoid duplicate label ids
      }
    }
    return labelIds;
  }

  std::vector<catalog::NodeTableCatalogEntry*> nodeTables;

  std::string toString() const {
    std::stringstream ss;
    ss << "LABELS(";
    for (auto& nodeTable : nodeTables) {
      ss << nodeTable->getName() << ",";
    }
    ss << ")";
    return ss.str();
  }

  // shadow copy of nodeTables
  std::unique_ptr<GNodeType> copy() const {
    return std::make_unique<GNodeType>(nodeTables);
  }

  YAML::Node toYAML() const {
    YAML::Node type;
    type["element_opt"] = "VERTEX";
    YAML::Node labels = YAML::Node(YAML::NodeType::Sequence);
    for (auto& nodeTable : nodeTables) {
      YAML::Node label;
      label["id"] = nodeTable->getTableID();
      label["name"] = nodeTable->getName();
      YAML::Node labelNode;
      labelNode["label"] = label;
      labels.push_back(labelNode);
    }
    type["graph_data_type"] = labels;
    YAML::Node typeNode;
    typeNode["graph_type"] = type;
    return typeNode;
  }
};

struct GRelType {
  GRelType(const std::vector<catalog::GRelTableCatalogEntry*>& relTables_)
      : relTables{relTables_} {}
  GRelType(const binder::RelExpression& relExpr) {
    relTables.reserve(relExpr.getNumEntries());
    for (auto& table : relExpr.getEntries()) {
      auto relTable = dynamic_cast<catalog::GRelTableCatalogEntry*>(table);
      if (relTable) {
        relTables.emplace_back(relTable);
      } else {
        THROW_EXCEPTION_WITH_FILE_LINE("Expected a GRelTableCatalogEntry.");
      }
    }
  }

  void setRelTables(std::vector<catalog::GRelTableCatalogEntry*>&& relTables) {
    this->relTables = std::move(relTables);
  }

  std::vector<common::table_id_t> getLabelIds() const {
    std::vector<common::table_id_t> labelIds;
    labelIds.reserve(relTables.size());
    for (const auto& relTable : relTables) {
      if (std::find(labelIds.begin(), labelIds.end(), relTable->getLabelId()) ==
          labelIds.end()) {
        labelIds.emplace_back(
            relTable->getLabelId());  // avoid duplicate label ids
      }
    }
    return labelIds;
  }

  std::string toString() const {
    std::stringstream ss;
    ss << "LABELS(";
    for (auto& relTable : relTables) {
      ss << relTable->getName() << ",";
    }
    ss << ")";
    return ss.str();
  }

  // shadow copy of relTables
  std::unique_ptr<GRelType> copy() const {
    return std::make_unique<GRelType>(relTables);
  }

  YAML::Node toYAML(catalog::Catalog* catalog) const {
    YAML::Node type;
    type["element_opt"] = "EDGE";
    YAML::Node labels = YAML::Node(YAML::NodeType::Sequence);
    auto& transaction = neug::Constants::DEFAULT_TRANSACTION;
    for (auto& relTable : relTables) {
      YAML::Node label;
      label["id"] = relTable->getLabelId();
      label["name"] = relTable->getLabel(catalog, &transaction);
      auto srcEntry = catalog->getTableCatalogEntry(&transaction,
                                                    relTable->getSrcTableID());
      if (srcEntry->getType() != catalog::CatalogEntryType::NODE_TABLE_ENTRY) {
        THROW_EXCEPTION_WITH_FILE_LINE("src table is not a node table");
      }
      auto srcTable = srcEntry->ptrCast<catalog::NodeTableCatalogEntry>();
      auto dstEntry = catalog->getTableCatalogEntry(&transaction,
                                                    relTable->getDstTableID());
      if (dstEntry->getType() != catalog::CatalogEntryType::NODE_TABLE_ENTRY) {
        THROW_EXCEPTION_WITH_FILE_LINE("dst table is not a node table");
      }
      auto dstTable = dstEntry->ptrCast<catalog::NodeTableCatalogEntry>();
      label["src_id"] = srcTable->getTableID();
      label["src_name"] = srcTable->getName();
      label["dst_id"] = dstTable->getTableID();
      label["dst_name"] = dstTable->getName();
      YAML::Node labelNode;
      labelNode["label"] = label;
      labels.push_back(labelNode);
    }
    type["graph_data_type"] = labels;
    YAML::Node typeNode;
    typeNode["graph_type"] = type;
    return typeNode;
  }

  std::vector<catalog::GRelTableCatalogEntry*> relTables;
};
}  // namespace gopt
}  // namespace neug