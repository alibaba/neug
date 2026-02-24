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

#include "neug/compiler/optimizer/correlated_subquery_unnest_solver.h"

#include "neug/compiler/planner/operator/logical_hash_join.h"
#include "neug/compiler/planner/operator/scan/logical_expressions_scan.h"
#include "neug/utils/exception/exception.h"

using namespace neug::planner;

namespace neug {
namespace optimizer {

void CorrelatedSubqueryUnnestSolver::solve(planner::LogicalOperator* root_) {
  visitOperator(root_);
}

void CorrelatedSubqueryUnnestSolver::visitOperator(LogicalOperator* op) {
  visitOperatorSwitch(op);
  if (LogicalOperatorUtils::isAccHashJoin(*op)) {
    solveAccHashJoin(op);
    return;
  }
  for (auto i = 0u; i < op->getNumChildren(); ++i) {
    visitOperator(op->getChild(i).get());
  }
}

void CorrelatedSubqueryUnnestSolver::solveAccHashJoin(
    LogicalOperator* op) const {
  auto& hashJoin = op->cast<LogicalHashJoin>();
  auto& sipInfo = hashJoin.getSIPInfoUnsafe();
  sipInfo.dependency = SIPDependency::BUILD_DEPENDS_ON_PROBE;
  sipInfo.direction = SIPDirection::PROBE_TO_BUILD;
  auto acc = op->getChild(0).get();
  auto rightSolver = std::make_unique<CorrelatedSubqueryUnnestSolver>(acc);
  rightSolver->solve(hashJoin.getChild(1).get());
  auto leftSolver =
      std::make_unique<CorrelatedSubqueryUnnestSolver>(accumulateOp);
  leftSolver->solve(acc->getChild(0).get());
}

void CorrelatedSubqueryUnnestSolver::visitExpressionsScan(LogicalOperator* op) {
  auto expressionsScan = op->ptrCast<LogicalExpressionsScan>();
  // // LCOV_EXCL_START
  // if (accumulateOp == nullptr) {
  //   THROW_INTERNAL_EXCEPTION(
  //       "Failed to execute CorrelatedSubqueryUnnestSolver. This should not "
  //       "happen.");
  // }
  // // LCOV_EXCL_STOP
  if (accumulateOp) {
    expressionsScan->setOuterAccumulate(accumulateOp);
  }
}

}  // namespace optimizer
}  // namespace neug
