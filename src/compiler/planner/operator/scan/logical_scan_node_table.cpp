#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"
#include <memory>
#include <optional>
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/gopt/g_graph_type.h"

namespace neug {
namespace planner {

LogicalScanNodeTable::LogicalScanNodeTable(const LogicalScanNodeTable& other)
    : LogicalOperator{type_},
      scanType{other.scanType},
      nodeID{other.nodeID},
      nodeTableIDs{other.nodeTableIDs},
      properties{other.properties},
      propertyPredicates{copyVector(other.propertyPredicates)} {
  if (other.extraInfo != nullptr) {
    setExtraInfo(other.extraInfo->copy());
  }
  this->cardinality = other.cardinality;
  this->predicates = other.predicates;
}

void LogicalScanNodeTable::computeFactorizedSchema() {
  createEmptySchema();
  const auto groupPos = schema->createGroup();
  NEUG_ASSERT(groupPos == 0);
  schema->insertToGroupAndScope(nodeID, groupPos);
  for (auto& property : properties) {
    schema->insertToGroupAndScope(property, groupPos);
  }
  switch (scanType) {
  case LogicalScanNodeTableType::PRIMARY_KEY_SCAN: {
    schema->setGroupAsSingleState(groupPos);
  } break;
  default:
    break;
  }
}

void LogicalScanNodeTable::computeFlatSchema() {
  createEmptySchema();
  schema->createGroup();
  schema->insertToGroupAndScope(nodeID, 0);
  for (auto& property : properties) {
    schema->insertToGroupAndScope(property, 0);
  }
}

std::unique_ptr<LogicalOperator> LogicalScanNodeTable::copy() {
  auto scan = std::make_unique<LogicalScanNodeTable>(*this);
  scan->setPredicates(predicates);
  return scan;
}

std::string LogicalScanNodeTable::getAliasName() const {
  // get the alias name from the node ID expression
  auto nodeId = getNodeID();
  if (!nodeId || nodeId->expressionType != common::ExpressionType::PROPERTY) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Node ID expression is not a property expression.");
  }
  auto propertyExpr = nodeId->constCast<binder::PropertyExpression>();
  return propertyExpr.getVariableName();
}

gopt::GAliasName LogicalScanNodeTable::getGAliasName() const {
  // get the alias name from the node ID expression
  auto nodeId = getNodeID();
  if (!nodeId || nodeId->expressionType != common::ExpressionType::PROPERTY) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Node ID expression is not a property expression.");
  }
  auto propertyExpr = nodeId->constCast<binder::PropertyExpression>();
  auto queryName = propertyExpr.getRawVariableName().empty()
                       ? std::nullopt
                       : std::make_optional(propertyExpr.getRawVariableName());
  return gopt::GAliasName{propertyExpr.getVariableName(), queryName};
}

std::unique_ptr<gopt::GNodeType> LogicalScanNodeTable::getNodeType(
    catalog::Catalog* catalog) const {
  // get node table from catalog by table ids
  std::vector<catalog::NodeTableCatalogEntry*> nodeTables;
  auto& transaction = neug::Constants::DEFAULT_TRANSACTION;
  for (auto tableId : getTableIDs()) {
    auto tableEntry = catalog->getTableCatalogEntry(&transaction, tableId);
    auto nodeTableEntry =
        dynamic_cast<catalog::NodeTableCatalogEntry*>(tableEntry);
    if (!nodeTableEntry) {
      THROW_EXCEPTION_WITH_FILE_LINE("Table with ID " +
                                     std::to_string(tableId) +
                                     " is not a node table in the catalog.");
    }
    nodeTables.push_back(nodeTableEntry);
  }
  return std::make_unique<gopt::GNodeType>(nodeTables);
}

std::optional<PrimaryKey> LogicalScanNodeTable::getPrimaryKey(
    catalog::Catalog* catalog) const {
  if (auto pkExtraInfo = dynamic_cast<PrimaryKeyScanInfo*>(getExtraInfo())) {
    auto tableIds = getTableIDs();
    if (tableIds.empty()) {
      THROW_EXCEPTION_WITH_FILE_LINE(
          "No table IDs found for primary key scan.");
    }
    auto tableEntry = catalog->getTableCatalogEntry(
        &neug::Constants::DEFAULT_TRANSACTION, tableIds.at(0));
    auto nodeTableEntry =
        dynamic_cast<catalog::NodeTableCatalogEntry*>(tableEntry);
    if (!nodeTableEntry) {
      THROW_EXCEPTION_WITH_FILE_LINE(
          "Primary key scan is only supported for node "
          "tables, but got: " +
          tableEntry->getName());
    }
    auto pkName = nodeTableEntry->getPrimaryKeyName();
    if (pkName.empty()) {
      THROW_EXCEPTION_WITH_FILE_LINE("Node table " + nodeTableEntry->getName() +
                                     " does not have a primary key.");
    }
    return PrimaryKey{pkName, pkExtraInfo};
  }
  return std::nullopt;
}

}  // namespace planner
}  // namespace neug
