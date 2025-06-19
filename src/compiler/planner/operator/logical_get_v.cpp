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

#include "src/include/planner/operator/logical_get_v.h"
#include "src/include/common/constants.h"
#include "src/include/common/exception/exception.h"

namespace gs {
namespace planner {

std::string LogicalGetV::getExpressionsForPrinting() const {
  std::string result = "GET_V(";
  if (predicates) {
    result += predicates->toString();
  }
  result += ")";
  return result;
}

void LogicalGetV::computeFlatSchema() {}

void LogicalGetV::computeFactorizedSchema() {}

std::unique_ptr<LogicalOperator> LogicalGetV::copy() {
  auto getV = std::make_unique<LogicalGetV>(nodeID, nodeTableIDs, properties,
                                            opt, boundRel, children[0]->copy(),
                                            schema->copy(), cardinality);
  getV->setPredicates(predicates);
  return getV;
}

std::string LogicalGetV::getAliasName() const {
  // get the alias name from the node ID expression
  auto nodeId = getNodeID();
  if (!nodeId || nodeId->expressionType != common::ExpressionType::PROPERTY) {
    throw common::Exception("Node ID expression is not a property expression.");
  }
  auto propertyExpr = nodeId->constCast<binder::PropertyExpression>();
  return propertyExpr.getVariableName();
}

gopt::GAliasName LogicalGetV::getGAliasName() const {
  // get the alias name from the node ID expression
  auto nodeId = getNodeID();
  if (!nodeId || nodeId->expressionType != common::ExpressionType::PROPERTY) {
    throw common::Exception("Node ID expression is not a property expression.");
  }
  auto propertyExpr = nodeId->constCast<binder::PropertyExpression>();
  auto queryName = propertyExpr.getRawVariableName().empty()
                       ? std::nullopt
                       : std::make_optional(propertyExpr.getRawVariableName());
  return gopt::GAliasName{propertyExpr.getVariableName(), queryName};
}

std::unique_ptr<gopt::GNodeType> LogicalGetV::getNodeType(
    catalog::Catalog* catalog) const {
  // get node table from catalog by table ids
  std::vector<catalog::NodeTableCatalogEntry*> nodeTables;
  auto& transaction = gs::Constants::DEFAULT_TRANSACTION;
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

}  // namespace planner
}  // namespace gs