#include <iostream>

#include "neug/compiler/binder/query/updating_clause/bound_insert_info.h"
#include "neug/compiler/catalog/catalog_entry/rel_table_catalog_entry.h"
#include "neug/compiler/catalog/catalog_entry/table_catalog_entry.h"
#include "neug/compiler/planner/operator/persistent/logical_insert.h"
#include "neug/compiler/planner/planner.h"

using namespace neug::common;
using namespace neug::binder;

namespace neug {
namespace planner {

std::unique_ptr<LogicalInsertInfo> Planner::createLogicalInsertInfo(
    const BoundInsertInfo* info) const {
  auto insertInfo = std::make_unique<LogicalInsertInfo>(
      info->tableType, info->pattern, info->columnExprs, info->columnDataExprs,
      info->conflictAction);
  binder::expression_set propertyExprSet;
  for (auto& expr : getProperties(*info->pattern)) {
    propertyExprSet.insert(expr);
  }
  for (auto& expr : insertInfo->columnExprs) {
    insertInfo->isReturnColumnExprs.push_back(propertyExprSet.contains(expr));
  }
  return insertInfo;
}

void Planner::appendInsertNode(
    const std::vector<const binder::BoundInsertInfo*>& boundInsertInfos,
    LogicalPlan& plan) {
  std::vector<LogicalInsertInfo> logicalInfos;
  logicalInfos.reserve(boundInsertInfos.size());
  for (auto& boundInfo : boundInsertInfos) {
    logicalInfos.push_back(createLogicalInsertInfo(boundInfo)->copy());
  }
  auto insertNode = std::make_shared<LogicalInsert>(std::move(logicalInfos),
                                                    plan.getLastOperator());
  appendFlattens(insertNode->getGroupsPosToFlatten(), plan);
  insertNode->setChild(0, plan.getLastOperator());
  insertNode->computeFactorizedSchema();
  plan.setLastOperator(insertNode);
}

std::string extend_string(ExtendDirection direction) {
  switch (direction) {
  case ExtendDirection::FWD:
    return "FWD";
  case ExtendDirection::BWD:
    return "BWD";
  default:
    return "UNKNOWN";
  }
}

void Planner::appendInsertRel(
    const std::vector<const binder::BoundInsertInfo*>& boundInsertInfos,
    LogicalPlan& plan) {
  std::vector<LogicalInsertInfo> logicalInfos;
  logicalInfos.reserve(boundInsertInfos.size());
  for (auto& boundInfo : boundInsertInfos) {
    logicalInfos.push_back(createLogicalInsertInfo(boundInfo)->copy());
  }
  auto insertRel = std::make_shared<LogicalInsert>(std::move(logicalInfos),
                                                   plan.getLastOperator());
  appendFlattens(insertRel->getGroupsPosToFlatten(), plan);
  insertRel->setChild(0, plan.getLastOperator());
  insertRel->computeFactorizedSchema();
  // std::cout << "insert pattern is " << std::endl;
  // for (auto& info : insertRel->getInfos()) {
  //     binder::Expression* expr = info.pattern.get();
  //     RelExpression* relExpr = dynamic_cast<RelExpression*>(expr);
  //     std::cout << relExpr->getSrcNode()->getInternalID()->getUniqueName() <<
  //     ", "
  //               << extend_string(relExpr->getExtendDirections().at(0)) << ",
  //               "
  //               << relExpr->getDstNode()->getInternalID()->getUniqueName() <<
  //               std::endl;
  //     if (relExpr != nullptr) {
  //         printLabelInfo(relExpr->getEntries());
  //     }
  // }
  // std::cout << "end of insert pattern" << std::endl;
  plan.setLastOperator(insertRel);
}
}  // namespace planner
}  // namespace neug
