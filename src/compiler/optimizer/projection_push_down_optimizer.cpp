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

#include "neug/compiler/optimizer/projection_push_down_optimizer.h"
#include <algorithm>

#include "neug/compiler/binder/expression_visitor.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/function/gds/rec_joins.h"
#include "neug/compiler/gopt/g_scalar_type.h"
#include "neug/compiler/planner/operator/extend/logical_extend.h"
#include "neug/compiler/planner/operator/extend/logical_recursive_extend.h"
#include "neug/compiler/planner/operator/logical_accumulate.h"
#include "neug/compiler/planner/operator/logical_filter.h"
#include "neug/compiler/planner/operator/logical_hash_join.h"
#include "neug/compiler/planner/operator/logical_intersect.h"
#include "neug/compiler/planner/operator/logical_node_label_filter.h"
#include "neug/compiler/planner/operator/logical_order_by.h"
#include "neug/compiler/planner/operator/logical_path_property_probe.h"
#include "neug/compiler/planner/operator/logical_projection.h"
#include "neug/compiler/planner/operator/logical_table_function_call.h"
#include "neug/compiler/planner/operator/logical_unwind.h"
#include "neug/compiler/planner/operator/persistent/logical_copy_from.h"
#include "neug/compiler/planner/operator/persistent/logical_delete.h"
#include "neug/compiler/planner/operator/persistent/logical_insert.h"
#include "neug/compiler/planner/operator/persistent/logical_merge.h"
#include "neug/compiler/planner/operator/persistent/logical_set.h"

using namespace neug::common;
using namespace neug::planner;
using namespace neug::binder;
using namespace neug::function;

namespace neug {
namespace optimizer {

void ProjectionPushDownOptimizer::rewrite(LogicalPlan* plan) {
  visitOperator(plan->getLastOperator().get());
}

void ProjectionPushDownOptimizer::visitOperator(LogicalOperator* op) {
  visitOperatorSwitch(op);
  if (op->getOperatorType() == LogicalOperatorType::PROJECTION) {
    return;
  }
  for (auto i = 0u; i < op->getNumChildren(); ++i) {
    visitOperator(op->getChild(i).get());
  }
  op->computeFlatSchema();
}

void ProjectionPushDownOptimizer::visitPathPropertyProbe(LogicalOperator* op) {
  auto& pathPropertyProbe = op->cast<LogicalPathPropertyProbe>();
  auto child = pathPropertyProbe.getChild(0);
  NEUG_ASSERT(child->getOperatorType() ==
              LogicalOperatorType::RECURSIVE_EXTEND);
  if (nodeOrRelInUse.contains(pathPropertyProbe.getRel())) {
    return;
  }
  pathPropertyProbe.setJoinType(planner::RecursiveJoinType::TRACK_NONE);
  auto extend = child->ptrCast<LogicalRecursiveExtend>();
  auto functionName = extend->getFunction().getFunctionName();
  extend->setResultColumns(
      extend->getFunction().getResultColumns(extend->getBindData()));
}

void ProjectionPushDownOptimizer::visitExtend(LogicalOperator* op) {
  auto& extend = op->cast<LogicalExtend>();
  const auto boundNodeID = extend.getBoundNode()->getInternalID();
  collectExpressionsInUse(boundNodeID);
  const auto nbrNodeID = extend.getNbrNode()->getInternalID();
  extend.setScanNbrID(propertiesInUse.contains(nbrNodeID));
}

void ProjectionPushDownOptimizer::visitAccumulate(LogicalOperator* op) {
  auto& accumulate = op->constCast<LogicalAccumulate>();
  if (accumulate.getAccumulateType() != AccumulateType::REGULAR) {
    return;
  }
  auto expressionsBeforePruning = accumulate.getPayloads();
  auto expressionsAfterPruning = pruneExpressions(expressionsBeforePruning);
  if (expressionsBeforePruning.size() == expressionsAfterPruning.size()) {
    return;
  }
  preAppendProjection(op, 0, expressionsAfterPruning);
}

void ProjectionPushDownOptimizer::visitFilter(LogicalOperator* op) {
  auto& filter = op->constCast<LogicalFilter>();
  collectExpressionsInUse(filter.getPredicate());
}

void ProjectionPushDownOptimizer::visitNodeLabelFilter(LogicalOperator* op) {
  auto& filter = op->constCast<LogicalNodeLabelFilter>();
  collectExpressionsInUse(filter.getNodeID());
}

void ProjectionPushDownOptimizer::visitHashJoin(LogicalOperator* op) {
  auto& hashJoin = op->constCast<LogicalHashJoin>();
  for (auto& [probeJoinKey, buildJoinKey] : hashJoin.getJoinConditions()) {
    collectExpressionsInUse(probeJoinKey);
    collectExpressionsInUse(buildJoinKey);
  }
  // We skip column pruning applied on HashJoin now, which will influence the
  // current test cases.
  // if (hashJoin.getJoinType() == JoinType::MARK) {
  //   return;
  // }
  // auto expressionsBeforePruning = hashJoin.getExpressionsToMaterialize();
  // auto expressionsAfterPruning = pruneExpressions(expressionsBeforePruning);
  // if (expressionsBeforePruning.size() == expressionsAfterPruning.size()) {
  //   return;
  // }
  // preAppendProjection(op, 1, expressionsAfterPruning);
}

void ProjectionPushDownOptimizer::visitIntersect(LogicalOperator* op) {}

void ProjectionPushDownOptimizer::visitProjection(LogicalOperator* op) {
  ProjectionPushDownOptimizer optimizer(this->semantic, ctx);
  auto& projection = op->cast<LogicalProjection>();
  for (auto& expression : projection.getExpressionsToProject()) {
    optimizer.collectExpressionsInUse(expression);
    // Collect type info from CAST expressions.
    optimizer.collectVariableTypes(expression);
  }
  optimizer.visitOperator(op->getChild(0).get());
  // Check if CAST is unnecessary and remove it.
  for (auto& expression : projection.getExpressionsToProjectRef()) {
    if (expression->expressionType != common::ExpressionType::FUNCTION) {
      continue;
    }
    gopt::GScalarType scalarType{*expression};
    if (scalarType.getType() != gopt::ScalarType::CAST ||
        expression->getChildren().empty()) {
      continue;
    }
    auto child = expression->getChild(0);
    // CAST is unnecessary if the child type is the same as the cast expression
    // type
    if (child->getDataType() == expression->getDataType()) {
      expression = child;
    }
  }
}

void ProjectionPushDownOptimizer::visitOrderBy(LogicalOperator* op) {
  auto& orderBy = op->constCast<LogicalOrderBy>();
  for (auto& expression : orderBy.getExpressionsToOrderBy()) {
    collectExpressionsInUse(expression);
  }
  auto expressionsBeforePruning =
      orderBy.getChild(0)->getSchema()->getExpressionsInScope();
  auto expressionsAfterPruning = pruneExpressions(expressionsBeforePruning);
  if (expressionsBeforePruning.size() == expressionsAfterPruning.size()) {
    return;
  }
  preAppendProjection(op, 0, expressionsAfterPruning);
}

void ProjectionPushDownOptimizer::visitUnwind(LogicalOperator* op) {
  auto& unwind = op->constCast<LogicalUnwind>();
  collectExpressionsInUse(unwind.getInExpr());
}

void ProjectionPushDownOptimizer::visitInsert(LogicalOperator* op) {
  auto& insert = op->constCast<LogicalInsert>();
  for (auto& info : insert.getInfos()) {
    visitInsertInfo(info);
  }
}

void ProjectionPushDownOptimizer::visitDelete(LogicalOperator* op) {
  auto& delete_ = op->constCast<LogicalDelete>();
  auto& infos = delete_.getInfos();
  NEUG_ASSERT(!infos.empty());
  switch (infos[0].tableType) {
  case TableType::NODE: {
    for (auto& info : infos) {
      auto& node = info.pattern->constCast<NodeExpression>();
      collectExpressionsInUse(node.getInternalID());
      for (auto entry : node.getEntries()) {
        collectExpressionsInUse(node.getPrimaryKey(entry->getTableID()));
      }
    }
  } break;
  case TableType::REL: {
    for (auto& info : infos) {
      auto& rel = info.pattern->constCast<RelExpression>();
      collectExpressionsInUse(rel.getSrcNode()->getInternalID());
      collectExpressionsInUse(rel.getDstNode()->getInternalID());
      NEUG_ASSERT(rel.getRelType() == QueryRelType::NON_RECURSIVE);
      if (!rel.isEmpty()) {
        collectExpressionsInUse(rel.getInternalIDProperty());
      }
    }
  } break;
  default:
    NEUG_UNREACHABLE;
  }
}

void ProjectionPushDownOptimizer::visitMerge(LogicalOperator* op) {
  auto& merge = op->constCast<LogicalMerge>();
  collectExpressionsInUse(merge.getExistenceMark());
  for (auto& info : merge.getInsertNodeInfos()) {
    visitInsertInfo(info);
  }
  for (auto& info : merge.getInsertRelInfos()) {
    visitInsertInfo(info);
  }
  for (auto& info : merge.getOnCreateSetNodeInfos()) {
    visitSetInfo(info);
  }
  for (auto& info : merge.getOnMatchSetNodeInfos()) {
    visitSetInfo(info);
  }
  for (auto& info : merge.getOnCreateSetRelInfos()) {
    visitSetInfo(info);
  }
  for (auto& info : merge.getOnMatchSetRelInfos()) {
    visitSetInfo(info);
  }
}

void ProjectionPushDownOptimizer::visitSetProperty(LogicalOperator* op) {
  auto& set = op->constCast<LogicalSetProperty>();
  for (auto& info : set.getInfos()) {
    visitSetInfo(info);
  }
}

void ProjectionPushDownOptimizer::visitCopyFrom(LogicalOperator* op) {
  auto& copyFrom = op->constCast<LogicalCopyFrom>();
  for (auto& expr : copyFrom.getInfo()->getSourceColumns()) {
    collectExpressionsInUse(expr);
  }
  if (copyFrom.getInfo()->offset) {
    collectExpressionsInUse(copyFrom.getInfo()->offset);
  }
}

void ProjectionPushDownOptimizer::visitTableFunctionCall(LogicalOperator* op) {
  auto& tableFunctionCall = op->cast<LogicalTableFunctionCall>();
  // filtering has been pushed down to the table function call
  auto skipRows = tableFunctionCall.getBindData()->getRowSkips();
  if (skipRows) {
    collectExpressionsInUse(skipRows);
  }
  auto bindData = tableFunctionCall.getBindData();
  auto scanBindData = dynamic_cast<function::ScanFileBindData*>(bindData);
  // Column pruning can only be applied on DataSource operator with non-null
  // scanBindData.
  if (!scanBindData)
    return;
  // Set column types using type info collected from CAST expressions.
  for (auto& column : scanBindData->columns) {
    if (column->expressionType == common::ExpressionType::VARIABLE) {
      auto it = variableTypes.find(column->getUniqueName());
      if (it != variableTypes.end() && column->dataType != it->second) {
        column->dataType = it->second.copy();
      }
    }
  }
  std::vector<bool> columnSkips;
  for (auto& column : scanBindData->columns) {
    columnSkips.push_back(!variablesInUse.contains(column));
  }
  // Keep at least one column to handle the query like 'LOAD FROM "file.csv"
  // RETURN count(*)'.
  if (!columnSkips.empty() &&
      std::all_of(columnSkips.begin(), columnSkips.end(),
                  [](bool skip) { return skip; })) {
    columnSkips[0] = false;
  }
  tableFunctionCall.setColumnSkips(std::move(columnSkips));
}

void ProjectionPushDownOptimizer::visitSetInfo(
    const binder::BoundSetPropertyInfo& info) {
  switch (info.tableType) {
  case TableType::NODE: {
    auto& node = info.pattern->constCast<NodeExpression>();
    collectExpressionsInUse(node.getInternalID());
    if (info.updatePk) {
      collectExpressionsInUse(info.column);
    }
  } break;
  case TableType::REL: {
    auto& rel = info.pattern->constCast<RelExpression>();
    collectExpressionsInUse(rel.getSrcNode()->getInternalID());
    collectExpressionsInUse(rel.getDstNode()->getInternalID());
    collectExpressionsInUse(rel.getInternalIDProperty());
  } break;
  default:
    NEUG_UNREACHABLE;
  }
  collectExpressionsInUse(info.columnData);
}

void ProjectionPushDownOptimizer::visitInsertInfo(
    const LogicalInsertInfo& info) {
  if (info.tableType == common::TableType::REL) {
    auto& rel = info.pattern->constCast<RelExpression>();
    collectExpressionsInUse(rel.getSrcNode()->getInternalID());
    collectExpressionsInUse(rel.getDstNode()->getInternalID());
    collectExpressionsInUse(rel.getInternalIDProperty());
  }
  for (auto i = 0u; i < info.columnExprs.size(); ++i) {
    if (info.isReturnColumnExprs[i]) {
      collectExpressionsInUse(info.columnExprs[i]);
    }
    collectExpressionsInUse(info.columnDataExprs[i]);
  }
}

void ProjectionPushDownOptimizer::collectVariableTypes(
    std::shared_ptr<binder::Expression> expression) {
  VariableCastTypeCollector collector(variableTypes, ctx);
  collector.visit(expression);
}

void ProjectionPushDownOptimizer::collectExpressionsInUse(
    std::shared_ptr<binder::Expression> expression) {
  switch (expression->expressionType) {
  case ExpressionType::PROPERTY: {
    propertiesInUse.insert(expression);
    return;
  }
  case ExpressionType::VARIABLE: {
    variablesInUse.insert(expression);
    return;
  }
  case ExpressionType::PATTERN: {
    nodeOrRelInUse.insert(expression);
    for (auto& child :
         ExpressionChildrenCollector::collectChildren(*expression)) {
      collectExpressionsInUse(child);
    }
    return;
  }
  default:
    for (auto& child :
         ExpressionChildrenCollector::collectChildren(*expression)) {
      collectExpressionsInUse(child);
    }
  }
}

binder::expression_vector ProjectionPushDownOptimizer::pruneExpressions(
    const binder::expression_vector& expressions) {
  expression_set expressionsAfterPruning;
  for (auto& expression : expressions) {
    switch (expression->expressionType) {
    case ExpressionType::PROPERTY: {
      if (propertiesInUse.contains(expression)) {
        expressionsAfterPruning.insert(expression);
      }
    } break;
    case ExpressionType::VARIABLE: {
      if (variablesInUse.contains(expression)) {
        expressionsAfterPruning.insert(expression);
      }
    } break;
    case ExpressionType::PATTERN: {
      if (nodeOrRelInUse.contains(expression)) {
        expressionsAfterPruning.insert(expression);
      }
    } break;
    default:
      expressionsAfterPruning.insert(expression);
    }
  }
  return expression_vector{expressionsAfterPruning.begin(),
                           expressionsAfterPruning.end()};
}

void ProjectionPushDownOptimizer::preAppendProjection(
    LogicalOperator* op, idx_t childIdx,
    binder::expression_vector expressions) {
  if (expressions.empty()) {
    return;
  }
  auto projection = std::make_shared<LogicalProjection>(std::move(expressions),
                                                        op->getChild(childIdx));
  projection->computeFlatSchema();
  op->setChild(childIdx, std::move(projection));
}

}  // namespace optimizer
}  // namespace neug
