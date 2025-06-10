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
  std::vector<catalog::GRelTableCatalogEntry*> relTables;
};
}  // namespace gopt
}  // namespace gs