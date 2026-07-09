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
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/gopt/g_catalog.h"
#include "neug/compiler/gopt/g_constants.h"
#include "neug/storages/graph/schema.h"

namespace neug {
namespace gopt {
struct GNodeType {
  GNodeType() = default;
  GNodeType(const std::vector<const VertexSchema*>& nodeTables_)
      : nodeTables{nodeTables_} {}
  GNodeType(const std::vector<VertexSchema*>& nodeTables_) {
    nodeTables.reserve(nodeTables_.size());
    for (auto* table : nodeTables_) {
      nodeTables.push_back(table);
    }
  }

  GNodeType(const binder::NodeExpression& nodeExpr) {
    nodeTables.reserve(nodeExpr.getNumEntries());
    for (auto& table : nodeExpr.getEntries()) {
      auto nodeTable = dynamic_cast<const VertexSchema*>(table);
      if (nodeTable) {
        nodeTables.emplace_back(nodeTable);
      } else {
        THROW_EXCEPTION_WITH_FILE_LINE("Expected a VertexSchema.");
      }
    }
  }

  void setNodeTables(std::vector<const VertexSchema*>&& nodeTables) {
    this->nodeTables = std::move(nodeTables);
  }
  void setNodeTables(std::vector<VertexSchema*>&& nodeTables) {
    this->nodeTables.clear();
    this->nodeTables.reserve(nodeTables.size());
    for (auto* table : nodeTables) {
      this->nodeTables.push_back(table);
    }
  }

  std::vector<common::table_id_t> getLabelIds() const {
    std::vector<common::table_id_t> labelIds;
    labelIds.reserve(nodeTables.size());
    for (const auto& nodeTable : nodeTables) {
      if (std::find(labelIds.begin(), labelIds.end(),
                    nodeTable->get_entry_id()) == labelIds.end()) {
        labelIds.emplace_back(
            nodeTable->get_entry_id());  // avoid duplicate label ids
      }
    }
    return labelIds;
  }

  std::vector<const VertexSchema*> nodeTables;

  std::string toString() const {
    std::stringstream ss;
    ss << "LABELS(";
    for (auto& nodeTable : nodeTables) {
      ss << nodeTable->label_name << ",";
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
      label["id"] = nodeTable->get_entry_id();
      label["name"] = nodeTable->label_name;
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
  GRelType() = default;
  GRelType(const std::vector<const EdgeSchema*>& relTables_)
      : relTables{relTables_} {}
  GRelType(const std::vector<EdgeSchema*>& relTables_) {
    relTables.reserve(relTables_.size());
    for (auto* table : relTables_) {
      relTables.push_back(table);
    }
  }
  GRelType(const binder::RelExpression& relExpr) {
    relTables.reserve(relExpr.getNumEntries());
    for (auto& table : relExpr.getEntries()) {
      auto relTable = dynamic_cast<const EdgeSchema*>(table);
      if (relTable) {
        relTables.emplace_back(relTable);
      } else {
        THROW_EXCEPTION_WITH_FILE_LINE("Expected an EdgeSchema.");
      }
    }
  }

  void setRelTables(std::vector<const EdgeSchema*>&& relTables) {
    this->relTables = std::move(relTables);
  }
  void setRelTables(std::vector<EdgeSchema*>&& relTables) {
    this->relTables.clear();
    this->relTables.reserve(relTables.size());
    for (auto* table : relTables) {
      this->relTables.push_back(table);
    }
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
      ss << relTable->edge_label_name << ",";
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
      label["name"] = relTable->getEdgeLabelName();
      auto srcEntry = catalog->getTableCatalogEntry(&transaction,
                                                    relTable->getSrcTableID());
      if (srcEntry->get_entry_type() != SchemaEntryType::NODE) {
        THROW_EXCEPTION_WITH_FILE_LINE("src table is not a node table");
      }
      auto dstEntry = catalog->getTableCatalogEntry(&transaction,
                                                    relTable->getDstTableID());
      if (dstEntry->get_entry_type() != SchemaEntryType::NODE) {
        THROW_EXCEPTION_WITH_FILE_LINE("dst table is not a node table");
      }
      label["src_id"] = srcEntry->get_entry_id();
      label["src_name"] = srcEntry->get_label();
      label["dst_id"] = dstEntry->get_entry_id();
      label["dst_name"] = dstEntry->get_label();
      YAML::Node labelNode;
      labelNode["label"] = label;
      labels.push_back(labelNode);
    }
    type["graph_data_type"] = labels;
    YAML::Node typeNode;
    typeNode["graph_type"] = type;
    return typeNode;
  }

  std::vector<const EdgeSchema*> relTables;
};
}  // namespace gopt
}  // namespace neug
