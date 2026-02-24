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

#include "neug/compiler/optimizer/filter_push_down_pattern.h"
#include "neug/compiler/gopt/g_alias_manager.h"

namespace neug {
namespace optimizer {

void FilterPushDownPattern::rewrite(planner::LogicalPlan* plan) {
  auto root = plan->getLastOperator();
  auto rootOpt = visitOperator(root);
  plan->setLastOperator(rootOpt);
}

std::shared_ptr<planner::LogicalOperator> FilterPushDownPattern::visitOperator(
    const std::shared_ptr<planner::LogicalOperator>& op) {
  // bottom-up traversal
  for (auto i = 0u; i < op->getNumChildren(); ++i) {
    op->setChild(i, visitOperator(op->getChild(i)));
  }
  auto result = visitOperatorReplaceSwitch(op);
  // schema of each operator is unchanged
  // result->computeFlatSchema();
  return result;
}

std::shared_ptr<planner::LogicalOperator>
FilterPushDownPattern::visitNodeLabelFilterReplace(
    std::shared_ptr<planner::LogicalOperator> op) {
  auto filter = op->ptrCast<planner::LogicalNodeLabelFilter>();
  auto child = op->getChild(0);
  if (child->getOperatorType() !=
      planner::LogicalOperatorType::SCAN_NODE_TABLE) {
    return op;
  }
  auto scanOp = child->ptrCast<planner::LogicalScanNodeTable>();
  if (scanOp->getNodeID()->getUniqueName() ==
      filter->getNodeID()->getUniqueName()) {
    auto tableIDSet = filter->getTableIDSet();
    scanOp->setTableIDs(
        std::vector<common::table_id_t>(tableIDSet.begin(), tableIDSet.end()));
    return child;
  }
  return op;
}

std::shared_ptr<planner::LogicalOperator>
FilterPushDownPattern::visitFilterReplace(
    std::shared_ptr<planner::LogicalOperator> op) {
  auto filter = op->constPtrCast<planner::LogicalFilter>();
  auto predicate = filter->getPredicate();
  auto child = op->getChild(0);
  if (canPushDown(child, predicate)) {
    return perform(child, predicate);
  }
  return op;
}

std::shared_ptr<binder::Expression> FilterPushDownPattern::andPredicate(
    std::shared_ptr<binder::Expression> left,
    std::shared_ptr<binder::Expression> right) {
  if (left == nullptr) {
    return right;
  }
  if (right == nullptr) {
    return left;
  }
  auto children = binder::expression_vector{std::move(left), std::move(right)};
  return bindBooleanExpression(common::ExpressionType::AND, children);
}

bool FilterPushDownPattern::canPushDown(
    std::shared_ptr<planner::LogicalOperator> child,
    std::shared_ptr<binder::Expression> predicate) {
  auto childType = child->getOperatorType();
  if (childType != planner::LogicalOperatorType::SCAN_NODE_TABLE &&
      childType != planner::LogicalOperatorType::EXTEND &&
      childType != planner::LogicalOperatorType::GET_V) {
    return false;
  }
  auto uniqueName = getUniqueName(child);
  binder::DependentVarNameCollector varCollector;
  varCollector.visit(predicate);
  for (const auto& varName : varCollector.getVarNames()) {
    // the expression may have been set if the child is one of sub plans in
    // intersect.
    if (varName != uniqueName && varName != gopt::DEFAULT_ALIAS_NAME) {
      return false;
    }
  }
  return true;
}

std::shared_ptr<planner::LogicalOperator> FilterPushDownPattern::perform(
    std::shared_ptr<planner::LogicalOperator> child,
    std::shared_ptr<binder::Expression> predicate) {
  switch (child.get()->getOperatorType()) {
  case planner::LogicalOperatorType::SCAN_NODE_TABLE: {
    auto scanOp = child->ptrCast<planner::LogicalScanNodeTable>();
    auto newPredicate = andPredicate(scanOp->getPredicates(), predicate);
    scanOp->setPredicates(newPredicate);
    return child;
  }
  case planner::LogicalOperatorType::EXTEND: {
    auto extendOp = child->ptrCast<planner::LogicalExtend>();
    auto newPredicate = andPredicate(extendOp->getPredicates(), predicate);
    extendOp->setPredicates(newPredicate);
    return child;
  }
  case planner::LogicalOperatorType::GET_V: {
    auto getVOp = child->ptrCast<planner::LogicalGetV>();
    auto newPredicate = andPredicate(getVOp->getPredicates(), predicate);
    getVOp->setPredicates(newPredicate);
    return child;
  }
  default:
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Unsupported operator type for filter push down: " +
        std::to_string(static_cast<int>(child->getOperatorType())));
  }
}

std::string FilterPushDownPattern::getUniqueName(
    std::shared_ptr<planner::LogicalOperator> child) {
  auto childType = child->getOperatorType();
  switch (childType) {
  case planner::LogicalOperatorType::SCAN_NODE_TABLE: {
    auto scanOp = child->constPtrCast<planner::LogicalScanNodeTable>();
    return scanOp->getAliasName();
  }
  case planner::LogicalOperatorType::EXTEND: {
    auto extendOp = child->constPtrCast<planner::LogicalExtend>();
    return extendOp->getAliasName();
  }
  case planner::LogicalOperatorType::GET_V: {
    auto getVOp = child->constPtrCast<planner::LogicalGetV>();
    return getVOp->getAliasName();
  }
  default:
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Unsupported operator type for getting unique name: " +
        std::to_string(static_cast<int>(childType)));
  }
}

std::shared_ptr<binder::Expression>
FilterPushDownPattern::bindBooleanExpression(
    common::ExpressionType expressionType,
    const binder::expression_vector& children) {
  binder::expression_vector childrenAfterCast;
  std::vector<common::LogicalTypeID> inputTypeIDs;
  for (auto& child : children) {
    childrenAfterCast.push_back(child);
    inputTypeIDs.push_back(common::LogicalTypeID::BOOL);
  }
  auto functionName = common::ExpressionTypeUtil::toString(expressionType);
  function::scalar_func_exec_t execFunc = nullptr;
  function::scalar_func_select_t selectFunc = nullptr;
  auto bindData =
      std::make_unique<function::FunctionBindData>(common::LogicalType::BOOL());
  auto uniqueExpressionName = binder::ScalarFunctionExpression::getUniqueName(
      functionName, childrenAfterCast);
  auto func = std::make_unique<function::ScalarFunction>(
      functionName, inputTypeIDs, common::LogicalTypeID::BOOL, execFunc,
      selectFunc);
  return std::make_shared<binder::ScalarFunctionExpression>(
      expressionType, std::move(func), std::move(bindData),
      std::move(childrenAfterCast), uniqueExpressionName);
}

}  // namespace optimizer
}  // namespace neug