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

#pragma once

#include "logical_operator_visitor.h"

namespace neug {
namespace optimizer {

class CorrelatedSubqueryUnnestSolver : public LogicalOperatorVisitor {
 public:
  explicit CorrelatedSubqueryUnnestSolver(
      planner::LogicalOperator* accumulateOp)
      : accumulateOp{accumulateOp} {}
  void solve(planner::LogicalOperator* root_);

 private:
  void visitOperator(planner::LogicalOperator* op);
  void visitExpressionsScan(planner::LogicalOperator* op) final;

  void solveAccHashJoin(planner::LogicalOperator* op) const;

 private:
  planner::LogicalOperator* accumulateOp;
};

}  // namespace optimizer
}  // namespace neug
