#pragma once

#include <memory>
#include <string>
#include <vector>
#include "binder/expression/expression.h"
#include "binder/expression/property_expression.h"
#include "binder/expression/rel_expression.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "gopt/g_constants.h"
#include "planner/operator/logical_operator.h"
#include "planner/operator/schema.h"

namespace kuzu {
namespace gopt {

struct GNodeType;

class LogicalGetV : public planner::LogicalOperator {
 public:
  LogicalGetV(std::shared_ptr<binder::Expression> nodeID,
              std::vector<common::table_id_t> nodeTableIDs,
              binder::expression_vector properties,
              std::shared_ptr<binder::RelExpression> boundRel,
              std::shared_ptr<planner::LogicalOperator> child,
              std::unique_ptr<planner::Schema> schema,
              common::ExtendDirection direction,
              common::cardinality_t cardinality = 0)
      : LogicalOperator{planner::LogicalOperatorType::GET_V, child,
                        cardinality},
        nodeID{std::move(nodeID)},
        nodeTableIDs{std::move(nodeTableIDs)},
        properties{std::move(properties)},
        boundRel{std::move(boundRel)},
        direction{direction} {
    this->schema = std::move(schema);
  }

  std::string getExpressionsForPrinting() const override { return "GET_V()"; }

  void computeFlatSchema() override {}

  virtual void computeFactorizedSchema() {}

  std::unique_ptr<planner::LogicalOperator> copy() override {
    return std::make_unique<LogicalGetV>(
        nodeID, nodeTableIDs, properties, boundRel, children[0]->copy(),
        schema->copy(), direction, cardinality);
  }

  // Getters
  inline std::shared_ptr<binder::Expression> getNodeID() const {
    return nodeID;
  }

  inline const binder::expression_vector& getProperties() const {
    return properties;
  }

  inline std::vector<common::table_id_t> getTableIDs() const {
    return nodeTableIDs;
  }

  inline common::ExtendDirection getDirection() const { return direction; }

  inline std::string getAliasName() const {
    // get the alias name from the node ID expression
    auto nodeId = getNodeID();
    if (!nodeId || nodeId->expressionType != common::ExpressionType::PROPERTY) {
      throw common::Exception(
          "Node ID expression is not a property expression.");
    }
    auto propertyExpr = nodeId->constCast<binder::PropertyExpression>();
    auto varName = propertyExpr.getRawVariableName();
    return varName;
  }

  inline std::unique_ptr<gopt::GNodeType> getNodeType(
      catalog::Catalog* catalog) const {
    // get node table from catalog by table ids
    std::vector<catalog::NodeTableCatalogEntry*> nodeTables;
    auto& transaction = kuzu::Constants::DEFAULT_TRANSACTION;
    for (auto tableId : getTableIDs()) {
      auto tableEntry = catalog->getTableCatalogEntry(&transaction, tableId);
      auto nodeTableEntry =
          dynamic_cast<catalog::NodeTableCatalogEntry*>(tableEntry);
      if (!nodeTableEntry) {
        throw common::Exception("Table with ID " + std::to_string(tableId) +
                                " is not a node table in the catalog.");
      }
      nodeTables.push_back(nodeTableEntry);
    }
    return std::make_unique<gopt::GNodeType>(nodeTables);
  }

 private:
  std::shared_ptr<binder::Expression> nodeID;
  std::vector<common::table_id_t> nodeTableIDs;
  binder::expression_vector properties;
  std::shared_ptr<binder::RelExpression> boundRel;
  common::ExtendDirection direction;
};

}  // namespace gopt
}  // namespace kuzu