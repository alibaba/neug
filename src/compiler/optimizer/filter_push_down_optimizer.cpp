/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#include "neug/compiler/optimizer/filter_push_down_optimizer.h"
#include <vector>

#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/planner/operator/extend/logical_extend.h"
#include "neug/compiler/planner/operator/logical_empty_result.h"
#include "neug/compiler/planner/operator/logical_filter.h"
#include "neug/compiler/planner/operator/logical_hash_join.h"
#include "neug/compiler/planner/operator/logical_table_function_call.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"

using namespace neug::binder;
using namespace neug::common;
using namespace neug::planner;
using namespace neug::storage;

namespace neug {
namespace optimizer {

void FilterPushDownOptimizer::rewrite(LogicalPlan* plan) {
  visitOperator(plan->getLastOperator());
}

std::shared_ptr<LogicalOperator> FilterPushDownOptimizer::visitOperator(
    const std::shared_ptr<LogicalOperator>& op) {
  switch (op->getOperatorType()) {
  case LogicalOperatorType::FILTER: {
    return visitFilterReplace(op);
  }
  case LogicalOperatorType::CROSS_PRODUCT: {
    return visitCrossProductReplace(op);
  }
  case LogicalOperatorType::EXTEND: {
    return visitExtendReplace(op);
  }
  case LogicalOperatorType::SCAN_NODE_TABLE: {
    return visitScanNodeTableReplace(op);
  }
  case LogicalOperatorType::TABLE_FUNCTION_CALL: {
    return visitTableFunctionCallReplace(op);
  }
  default: {
    return visitChildren(op);
  }
  }
}

std::shared_ptr<LogicalOperator> FilterPushDownOptimizer::visitChildren(
    const std::shared_ptr<LogicalOperator>& op) {
  for (auto i = 0u; i < op->getNumChildren(); ++i) {
    auto optimizer = FilterPushDownOptimizer(context);
    op->setChild(i, optimizer.visitOperator(op->getChild(i)));
  }
  op->computeFlatSchema();
  return finishPushDown(op);
}

std::shared_ptr<LogicalOperator> FilterPushDownOptimizer::visitFilterReplace(
    const std::shared_ptr<LogicalOperator>& op) {
  auto& filter = op->constCast<LogicalFilter>();
  auto predicate = filter.getPredicate();
  if (predicate->expressionType == ExpressionType::LITERAL) {
    auto& literalExpr = predicate->constCast<LiteralExpression>();
    if (literalExpr.isNull() || !literalExpr.getValue().getValue<bool>()) {
      return std::make_shared<LogicalEmptyResult>(*op->getSchema());
    }
  } else {
    predicateSet.addPredicate(predicate);
  }
  return visitOperator(filter.getChild(0));
}

std::shared_ptr<LogicalOperator>
FilterPushDownOptimizer::visitCrossProductReplace(
    const std::shared_ptr<LogicalOperator>& op) {
  auto remainingPSet = PredicateSet();
  auto probePSet = PredicateSet();
  auto buildPSet = PredicateSet();
  for (auto& p : predicateSet.getAllPredicates()) {
    auto inProbe = op->getChild(0)->getSchema()->evaluable(*p);
    auto inBuild = op->getChild(1)->getSchema()->evaluable(*p);
    if (inProbe && !inBuild) {
      probePSet.addPredicate(p);
    } else if (!inProbe && inBuild) {
      buildPSet.addPredicate(p);
    } else {
      remainingPSet.addPredicate(p);
    }
  }
  NEUG_ASSERT(op->getNumChildren() == 2);
  auto probeOptimizer = FilterPushDownOptimizer(context, std::move(probePSet));
  op->setChild(0, probeOptimizer.visitOperator(op->getChild(0)));
  auto buildOptimizer = FilterPushDownOptimizer(context, std::move(buildPSet));
  op->setChild(1, buildOptimizer.visitOperator(op->getChild(1)));

  auto probeSchema = op->getChild(0)->getSchema();
  auto buildSchema = op->getChild(1)->getSchema();
  expression_vector predicates;
  std::vector<join_condition_t> joinConditions;
  for (auto& predicate : remainingPSet.equalityPredicates) {
    auto left = predicate->getChild(0);
    auto right = predicate->getChild(1);
    if (probeSchema->isExpressionInScope(*left) &&
        buildSchema->isExpressionInScope(*right)) {
      joinConditions.emplace_back(left, right);
    } else if (probeSchema->isExpressionInScope(*right) &&
               buildSchema->isExpressionInScope(*left)) {
      joinConditions.emplace_back(right, left);
    } else {
      predicates.push_back(predicate);
    }
  }
  if (joinConditions.empty()) {
    return finishPushDown(op);
  }
  auto hashJoin = std::make_shared<LogicalHashJoin>(
      joinConditions, JoinType::INNER, nullptr /* mark */, op->getChild(0),
      op->getChild(1), 0 /* cardinality */);
  hashJoin->getSIPInfoUnsafe().position = SemiMaskPosition::PROHIBIT;
  hashJoin->computeFlatSchema();
  predicates.insert(predicates.end(),
                    remainingPSet.nonEqualityPredicates.begin(),
                    remainingPSet.nonEqualityPredicates.end());
  if (predicates.empty()) {
    return hashJoin;
  }
  return appendFilters(predicates, hashJoin);
}

static bool isConstantExpression(const std::shared_ptr<Expression> expression) {
  switch (expression->expressionType) {
  case ExpressionType::LITERAL:
  case ExpressionType::PARAMETER: {
    return true;
  }
  case ExpressionType::FUNCTION: {
    auto& func = expression->constCast<ScalarFunctionExpression>();
    if (func.getFunction().name == "CAST") {
      return isConstantExpression(func.getChild(0));
    } else {
      return false;
    }
  }
  default:
    return false;
  }
}

std::shared_ptr<LogicalOperator>
FilterPushDownOptimizer::visitScanNodeTableReplace(
    const std::shared_ptr<LogicalOperator>& op) {
  auto& scan = op->cast<LogicalScanNodeTable>();
  auto nodeID = scan.getNodeID();
  auto tableIDs = scan.getTableIDs();
  std::shared_ptr<Expression> primaryKeyEqualityComparison = nullptr;
  primaryKeyEqualityComparison =
      predicateSet.popNodePKEqualityComparison(*nodeID, tableIDs);
  if (primaryKeyEqualityComparison != nullptr) {
    auto rhs = primaryKeyEqualityComparison->getChild(1);
    if (isConstantExpression(rhs)) {
      auto extraInfo = std::make_unique<PrimaryKeyScanInfo>(rhs);
      scan.setScanType(LogicalScanNodeTableType::PRIMARY_KEY_SCAN);
      scan.setExtraInfo(std::move(extraInfo));
      scan.computeFlatSchema();
    } else {
      predicateSet.addPredicate(primaryKeyEqualityComparison);
    }
  }
  return finishPushDown(op);
}

std::shared_ptr<LogicalOperator>
FilterPushDownOptimizer::visitTableFunctionCallReplace(
    const std::shared_ptr<LogicalOperator>& op) {
  return op;
}

std::shared_ptr<LogicalOperator> FilterPushDownOptimizer::visitExtendReplace(
    const std::shared_ptr<LogicalOperator>& op) {
  if (op->ptrCast<BaseLogicalExtend>()->isRecursive() ||
      !context->getClientConfig()->enableZoneMap) {
    return visitChildren(op);
  }
  auto& extend = op->cast<LogicalExtend>();
  return visitChildren(op);
}

std::shared_ptr<LogicalOperator> FilterPushDownOptimizer::finishPushDown(
    std::shared_ptr<LogicalOperator> op) {
  if (predicateSet.isEmpty()) {
    return op;
  }
  auto predicates = predicateSet.getAllPredicates();
  auto root = appendFilters(predicates, op);
  predicateSet.clear();
  return root;
}

std::shared_ptr<LogicalOperator> FilterPushDownOptimizer::appendScanNodeTable(
    std::shared_ptr<binder::Expression> nodeID,
    std::vector<common::table_id_t> nodeTableIDs,
    binder::expression_vector properties,
    std::shared_ptr<planner::LogicalOperator> child) {
  if (properties.empty()) {
    return child;
  }
  auto printInfo = std::make_unique<OPPrintInfo>();
  auto scanNodeTable = std::make_shared<LogicalScanNodeTable>(
      std::move(nodeID), std::move(nodeTableIDs), std::move(properties));
  scanNodeTable->computeFlatSchema();
  return scanNodeTable;
}

std::shared_ptr<LogicalOperator> FilterPushDownOptimizer::appendFilters(
    const expression_vector& predicates,
    std::shared_ptr<LogicalOperator> child) {
  if (predicates.empty()) {
    return child;
  }
  auto root = child;
  for (auto& p : predicates) {
    root = appendFilter(p, root);
  }
  return root;
}

std::shared_ptr<LogicalOperator> FilterPushDownOptimizer::appendFilter(
    std::shared_ptr<Expression> predicate,
    std::shared_ptr<LogicalOperator> child) {
  auto printInfo = std::make_unique<OPPrintInfo>();
  auto filter =
      std::make_shared<LogicalFilter>(std::move(predicate), std::move(child));
  filter->computeFlatSchema();
  return filter;
}

void PredicateSet::addPredicate(std::shared_ptr<Expression> predicate) {
  if (predicate->expressionType == ExpressionType::EQUALS) {
    equalityPredicates.push_back(std::move(predicate));
  } else {
    nonEqualityPredicates.push_back(std::move(predicate));
  }
}

static bool isNodePrimaryKey(const Expression& expression,
                             const Expression& nodeID,
                             const std::vector<common::table_id_t>& tableIDs) {
  if (expression.expressionType != ExpressionType::PROPERTY) {
    return false;
  }
  auto& property = expression.constCast<PropertyExpression>();
  if (property.getVariableName() !=
      nodeID.constCast<PropertyExpression>().getVariableName()) {
    return false;
  }
  if (tableIDs.empty())
    return false;
  for (auto tableID : tableIDs) {
    if (!property.isPrimaryKey(tableID)) {
      return false;
    }
  }
  return true;
}

std::shared_ptr<Expression> PredicateSet::popNodePKEqualityComparison(
    const Expression& nodeID, const std::vector<common::table_id_t>& tableIDs) {
  auto resultPredicateIdx = INVALID_IDX;
  for (auto i = 0u; i < equalityPredicates.size(); ++i) {
    auto predicate = equalityPredicates[i];
    if (isNodePrimaryKey(*predicate->getChild(0), nodeID, tableIDs)) {
      resultPredicateIdx = i;
      break;
    } else if (isNodePrimaryKey(*predicate->getChild(1), nodeID, tableIDs)) {
      auto leftChild = predicate->getChild(0);
      auto rightChild = predicate->getChild(1);
      predicate->setChild(1, leftChild);
      predicate->setChild(0, rightChild);
      resultPredicateIdx = i;
      break;
    }
  }
  if (resultPredicateIdx != INVALID_IDX) {
    auto result = equalityPredicates[resultPredicateIdx];
    equalityPredicates.erase(equalityPredicates.begin() + resultPredicateIdx);
    return result;
  }
  return nullptr;
}

expression_vector PredicateSet::getAllPredicates() {
  expression_vector result;
  result.insert(result.end(), equalityPredicates.begin(),
                equalityPredicates.end());
  result.insert(result.end(), nonEqualityPredicates.begin(),
                nonEqualityPredicates.end());
  return result;
}

}  // namespace optimizer
}  // namespace neug
