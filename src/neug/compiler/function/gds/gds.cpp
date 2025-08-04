#include "neug/compiler/function/gds/gds.h"

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/query/reading_clause/bound_table_function_call.h"
#include "neug/compiler/catalog/catalog_entry/rel_group_catalog_entry.h"
#include "neug/compiler/graph/on_disk_graph.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/parser.h"
#include "neug/compiler/planner/operator/logical_table_function_call.h"
#include "neug/compiler/planner/operator/sip/logical_semi_masker.h"
#include "neug/compiler/planner/planner.h"
#include "neug/utils/exception/exception.h"

using namespace gs::catalog;
using namespace gs::common;
using namespace gs::binder;
using namespace gs::main;
using namespace gs::graph;
using namespace gs::processor;
using namespace gs::planner;

namespace gs {
namespace function {

void GDSFuncSharedState::setGraphNodeMask(
    std::unique_ptr<NodeOffsetMaskMap> maskMap) {
  auto onDiskGraph = ku_dynamic_cast<OnDiskGraph*>(graph.get());
  onDiskGraph->setNodeOffsetMask(maskMap.get());
  graphNodeMask = std::move(maskMap);
}

static expression_vector getResultColumns(const std::string& cypher,
                                          ClientContext* context) {
  auto parsedStatements = parser::Parser::parseQuery(cypher);
  KU_ASSERT(parsedStatements.size() == 1);
  auto binder = Binder(context);
  auto boundStatement = binder.bind(*parsedStatements[0]);
  return boundStatement->getStatementResult()->getColumns();
}

static void validateNodeProjected(table_id_t tableID,
                                  const table_id_set_t& projectedNodeIDSet,
                                  const std::string& relName, Catalog* catalog,
                                  transaction::Transaction* transaction) {
  if (projectedNodeIDSet.contains(tableID)) {
    return;
  }
  auto entryName =
      catalog->getTableCatalogEntry(transaction, tableID)->getName();
  throw BinderException(stringFormat("{} is connected to {} but not projected.",
                                     entryName, relName));
}

static void validateRelSrcDstNodeAreProjected(
    const TableCatalogEntry& entry, const std::string printableRelName,
    const table_id_set_t& projectedNodeIDSet, Catalog* catalog,
    transaction::Transaction* transaction) {
  auto& relEntry = entry.constCast<RelTableCatalogEntry>();
  validateNodeProjected(relEntry.getSrcTableID(), projectedNodeIDSet,
                        printableRelName, catalog, transaction);
  validateNodeProjected(relEntry.getDstTableID(), projectedNodeIDSet,
                        printableRelName, catalog, transaction);
}

GraphEntry GDSFunction::bindGraphEntry(ClientContext& context,
                                       const std::string& name) {
  if (!context.getGraphEntrySetUnsafe().hasGraph(name)) {
    throw BinderException(stringFormat("Cannot find graph {}.", name));
  }
  return bindGraphEntry(context,
                        context.getGraphEntrySetUnsafe().getEntry(name));
}

static BoundGraphEntryTableInfo bindNodeEntry(ClientContext& context,
                                              const std::string& tableName,
                                              const std::string& predicate) {
  auto catalog = context.getCatalog();
  auto transaction = context.getTransaction();
  auto nodeEntry = catalog->getTableCatalogEntry(transaction, tableName);
  if (nodeEntry->getType() != CatalogEntryType::NODE_TABLE_ENTRY) {
    throw BinderException(stringFormat("{} is not a NODE table.", tableName));
  }
  if (!predicate.empty()) {
    auto cypher = stringFormat("MATCH (n:`{}`) RETURN n, {}",
                               nodeEntry->getName(), predicate);
    auto columns = getResultColumns(cypher, &context);
    KU_ASSERT(columns.size() == 2);
    return {nodeEntry, columns[0], columns[1]};
  } else {
    auto cypher = stringFormat("MATCH (n:`{}`) RETURN n", nodeEntry->getName());
    auto columns = getResultColumns(cypher, &context);
    KU_ASSERT(columns.size() == 1);
    return {nodeEntry, columns[0], nullptr /* empty predicate */};
  }
}

static BoundGraphEntryTableInfo bindRelEntry(ClientContext& context,
                                             const std::string& tableName,
                                             const std::string& predicate) {
  auto catalog = context.getCatalog();
  auto transaction = context.getTransaction();
  auto relEntry = catalog->getTableCatalogEntry(transaction, tableName);
  if (relEntry->getType() != CatalogEntryType::REL_TABLE_ENTRY) {
    throw BinderException(stringFormat(
        "{} has catalog entry type. REL entry was expected.", tableName));
  }
  if (!predicate.empty()) {
    auto cypher = stringFormat("MATCH ()-[r:`{}`]->() RETURN r, {}",
                               relEntry->getName(), predicate);
    auto columns = getResultColumns(cypher, &context);
    KU_ASSERT(columns.size() == 2);
    return {relEntry, columns[0], columns[1]};
  } else {
    auto cypher =
        stringFormat("MATCH ()-[r:`{}`]->() RETURN r", relEntry->getName());
    auto columns = getResultColumns(cypher, &context);
    KU_ASSERT(columns.size() == 1);
    return {relEntry, columns[0], nullptr /* empty predicate */};
  }
}

GraphEntry GDSFunction::bindGraphEntry(ClientContext& context,
                                       const ParsedGraphEntry& entry) {
  auto catalog = context.getCatalog();
  auto transaction = context.getTransaction();
  auto result = GraphEntry();
  table_id_set_t projectedNodeTableIDSet;
  for (auto& nodeInfo : entry.nodeInfos) {
    auto boundInfo =
        bindNodeEntry(context, nodeInfo.tableName, nodeInfo.predicate);
    projectedNodeTableIDSet.insert(boundInfo.entry->getTableID());
    result.nodeInfos.push_back(std::move(boundInfo));
  }
  for (auto& relInfo : entry.relInfos) {
    if (catalog->containsTable(transaction, relInfo.tableName)) {
      auto boundInfo =
          bindRelEntry(context, relInfo.tableName, relInfo.predicate);
      validateRelSrcDstNodeAreProjected(*boundInfo.entry, relInfo.tableName,
                                        projectedNodeTableIDSet, catalog,
                                        transaction);
      result.relInfos.push_back(std::move(boundInfo));
    } else if (catalog->containsRelGroup(transaction, relInfo.tableName)) {
      auto groupEntry =
          catalog->getRelGroupEntry(transaction, relInfo.tableName);
      for (auto tableID : groupEntry->getRelTableIDs()) {
        auto relEntry = catalog->getTableCatalogEntry(transaction, tableID);
        auto boundInfo =
            bindRelEntry(context, relEntry->getName(), relInfo.predicate);
        validateRelSrcDstNodeAreProjected(*boundInfo.entry, relInfo.tableName,
                                          projectedNodeTableIDSet, catalog,
                                          transaction);
        result.relInfos.push_back(std::move(boundInfo));
      }
    } else {
      throw BinderException(
          stringFormat("{} is not a REL table.", relInfo.tableName));
    }
  }
  return result;
}

std::shared_ptr<Expression> GDSFunction::bindNodeOutput(
    const TableFuncBindInput& bindInput,
    const std::vector<catalog::TableCatalogEntry*>& nodeEntries) {
  std::string nodeColumnName = NODE_COLUMN_NAME;
  if (!bindInput.yieldVariables.empty()) {
    nodeColumnName =
        bindColumnName(bindInput.yieldVariables[0], nodeColumnName);
  }
  auto node = bindInput.binder->createQueryNode(nodeColumnName, nodeEntries);
  bindInput.binder->addToScope(nodeColumnName, node);
  return node;
}

std::string GDSFunction::bindColumnName(
    const parser::YieldVariable& yieldVariable, std::string expressionName) {
  if (yieldVariable.name != expressionName) {
    throw common::BinderException{
        common::stringFormat("Unknown variable name: {}.", yieldVariable.name)};
  }
  if (yieldVariable.hasAlias()) {
    return yieldVariable.alias;
  }
  return expressionName;
}

std::unique_ptr<TableFuncSharedState> GDSFunction::initSharedState(
    const TableFuncInitSharedStateInput& input) {
  return nullptr;
}

std::vector<std::shared_ptr<LogicalOperator>> getNodeMaskPlanRoots(
    const GDSBindData& bindData, Planner* planner) {
  std::vector<std::shared_ptr<LogicalOperator>> nodeMaskPlanRoots;
  for (auto& nodeInfo : bindData.graphEntry.nodeInfos) {
    if (nodeInfo.predicate == nullptr) {
      continue;
    }
    auto p = planner->getNodeSemiMaskPlan(
        SemiMaskTargetType::GDS_GRAPH_NODE,
        nodeInfo.nodeOrRel->constCast<NodeExpression>(), nodeInfo.predicate);
    nodeMaskPlanRoots.push_back(p.getLastOperator());
  }
  return nodeMaskPlanRoots;
};

void GDSFunction::getLogicalPlan(
    Planner* planner, const BoundReadingClause& readingClause,
    binder::expression_vector predicates,
    std::vector<std::unique_ptr<LogicalPlan>>& logicalPlans) {
  auto& call = readingClause.constCast<binder::BoundTableFunctionCall>();
  auto bindData = call.getBindData()->constPtrCast<GDSBindData>();
  auto maskRoots = getNodeMaskPlanRoots(*bindData, planner);
  for (auto& plan : logicalPlans) {
    auto op = std::make_shared<LogicalTableFunctionCall>(call.getTableFunc(),
                                                         bindData->copy());
    op->setNodeMaskRoots(maskRoots);
    op->computeFactorizedSchema();
    planner->planReadOp(std::move(op), predicates, *plan);
  }
  auto nodeOutput = bindData->nodeOutput->ptrCast<NodeExpression>();
  KU_ASSERT(nodeOutput != nullptr);
  auto scanPlan = planner->getNodePropertyScanPlan(*nodeOutput);
  if (scanPlan.isEmpty()) {
    return;
  }
  expression_vector joinConditions;
  joinConditions.push_back(nodeOutput->getInternalID());
  for (auto& plan : logicalPlans) {
    planner->appendHashJoin(joinConditions, JoinType::INNER, *plan, scanPlan,
                            *plan);
  }
}

}  // namespace function
}  // namespace gs
