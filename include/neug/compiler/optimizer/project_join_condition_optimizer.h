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
#include "neug/compiler/gopt/g_alias_name.h"
#include "neug/compiler/planner/operator/logical_plan.h"

namespace neug {
namespace main {
class ClientContext;
}
namespace optimizer {

// ProjectJoinConditionOptimizer implements the logic to project join conditions
// before hash join operations. This optimization ensures that expressions used
// in join conditions are materialized early, which can improve join
// performance by avoiding repeated expression evaluation.
class ProjectJoinConditionOptimizer : public LogicalOperatorVisitor {
 public:
  void rewrite(planner::LogicalPlan* plan);
  explicit ProjectJoinConditionOptimizer(main::ClientContext* ctx) : ctx{ctx} {}

 private:
  void visitOperator(planner::LogicalOperator* op);

  void visitHashJoin(planner::LogicalOperator* op) override;

  // Collect expressions that need to be projected from join conditions
  void collectExpressionsFromJoinConditions(
      const std::vector<binder::expression_pair>& joinConditions,
      binder::expression_vector& expressions);

  // Add projection before join if needed
  void addProjectionBeforeJoin(planner::LogicalOperator* op,
                               common::idx_t childIdx,
                               const binder::expression_vector& expressions,
                               bool isAppend = true);

 private:
  main::ClientContext* ctx;

 private:
  std::unique_ptr<common::LogicalType> getDataType(
      const std::string& uniqueVarName, planner::LogicalOperator* op);
};

}  // namespace optimizer
}  // namespace neug
