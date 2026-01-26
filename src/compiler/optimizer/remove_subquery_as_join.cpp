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

#include "neug/compiler/optimizer/remove_subquery_as_join.h"
#include "neug/compiler/binder/expression/subquery_expression.h"
#include "neug/compiler/common/enums/join_type.h"
#include "neug/compiler/common/enums/subquery_type.h"
#include "neug/compiler/planner/operator/logical_filter.h"
#include "neug/compiler/planner/operator/logical_hash_join.h"

namespace neug {
namespace optimizer {

void RemoveSubqueryAsJoin::rewrite(planner::LogicalPlan* plan) {
  auto root = plan->getLastOperator();
  auto rootOpt = visitOperator(root);
  plan->setLastOperator(rootOpt);
}

std::shared_ptr<planner::LogicalOperator> RemoveSubqueryAsJoin::visitOperator(
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
RemoveSubqueryAsJoin::visitFilterReplace(
    std::shared_ptr<planner::LogicalOperator> op) {
  if (op->getNumChildren() == 0)
    return op;
  auto child = op->getChild(0);
  while (child &&
         child->getOperatorType() == planner::LogicalOperatorType::FILTER) {
    child = child->getChild(0);
  }
  if (!child ||
      child->getOperatorType() != planner::LogicalOperatorType::HASH_JOIN) {
    return op;
  }
  auto join = child->ptrCast<planner::LogicalHashJoin>();
  if (join->getJoinType() != common::JoinType::MARK) {
    return op;
  }
  auto filter = op->ptrCast<planner::LogicalFilter>();
  if (existSubQuery(filter->getPredicate())) {
    join->setJoinType(common::JoinType::SEMI);
    return op->getChild(0);
  } else if (notExistSubQuery(filter->getPredicate())) {
    join->setJoinType(common::JoinType::ANTI);
    return op->getChild(0);
  }
  return op;
}

bool RemoveSubqueryAsJoin::existSubQuery(
    std::shared_ptr<binder::Expression> expr) {
  if (!expr)
    return false;
  if (expr->expressionType != common::ExpressionType::SUBQUERY)
    return false;
  auto subqueryExpr = expr->ptrCast<binder::SubqueryExpression>();
  return subqueryExpr->getSubqueryType() == common::SubqueryType::EXISTS;
}

bool RemoveSubqueryAsJoin::notExistSubQuery(
    std::shared_ptr<binder::Expression> expr) {
  if (!expr)
    return false;

  return expr->expressionType == common::ExpressionType::NOT &&
         expr->getNumChildren() > 0 && existSubQuery(expr->getChild(0));
}
}  // namespace optimizer
}  // namespace neug