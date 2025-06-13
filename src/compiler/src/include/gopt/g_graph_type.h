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

#include <vector>
#include "binder/expression/node_expression.h"
#include "binder/expression/rel_expression.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/types/types.h"
#include "gopt/g_catalog.h"

namespace gs {
namespace gopt {
struct GNodeType {
  GNodeType(std::vector<catalog::NodeTableCatalogEntry*> nodeTables_)
      : nodeTables{std::move(nodeTables_)} {}

  GNodeType(const binder::NodeExpression& nodeExpr) {
    nodeTables.reserve(nodeExpr.getNumEntries());
    for (auto& table : nodeExpr.getEntries()) {
      auto nodeTable = dynamic_cast<catalog::NodeTableCatalogEntry*>(table);
      if (nodeTable) {
        nodeTables.emplace_back(nodeTable);
      } else {
        throw common::Exception("Expected a NodeTableCatalogEntry.");
      }
    }
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
};

struct GRelType {
  GRelType(std::vector<catalog::GRelTableCatalogEntry*> relTables_)
      : relTables{std::move(relTables_)} {}
  GRelType(const binder::RelExpression& relExpr) {
    relTables.reserve(relExpr.getNumEntries());
    for (auto& table : relExpr.getEntries()) {
      auto relTable = dynamic_cast<catalog::GRelTableCatalogEntry*>(table);
      if (relTable) {
        relTables.emplace_back(relTable);
      } else {
        throw common::Exception("Expected a GRelTableCatalogEntry.");
      }
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

  std::vector<catalog::GRelTableCatalogEntry*> relTables;
};
}  // namespace gopt
}  // namespace gs