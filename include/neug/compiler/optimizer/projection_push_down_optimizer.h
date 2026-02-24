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

#include <unordered_map>
#include "logical_operator_visitor.h"
#include "neug/compiler/common/enums/path_semantic.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/planner/operator/logical_plan.h"

namespace neug {
namespace main {
class ClientContext;
}
namespace binder {
struct BoundSetPropertyInfo;
}
namespace planner {
struct LogicalInsertInfo;
}
namespace optimizer {

// ProjectionPushDownOptimizer implements the logic to avoid materializing
// unnecessary properties for hash join build. Note the optimization is for
// properties & variables only but not for general expressions. This is because
// it's hard to figure out what expression is in-use, e.g. COUNT(a.age) + 1, it
// could be either the whole expression was evaluated in a WITH clause or only
// COUNT(a.age) was evaluated or only a.age is evaluate. For simplicity, we only
// consider the push down for property.
class ProjectionPushDownOptimizer : public LogicalOperatorVisitor {
 public:
  void rewrite(planner::LogicalPlan* plan);
  explicit ProjectionPushDownOptimizer(common::PathSemantic semantic,
                                       main::ClientContext* ctx)
      : semantic(semantic), ctx{ctx} {};

 private:
  void visitOperator(planner::LogicalOperator* op);

  void visitPathPropertyProbe(planner::LogicalOperator* op) override;
  void visitExtend(planner::LogicalOperator* op) override;
  void visitAccumulate(planner::LogicalOperator* op) override;
  void visitFilter(planner::LogicalOperator* op) override;
  void visitNodeLabelFilter(planner::LogicalOperator* op) override;
  void visitHashJoin(planner::LogicalOperator* op) override;
  void visitIntersect(planner::LogicalOperator* op) override;
  void visitProjection(planner::LogicalOperator* op) override;
  void visitOrderBy(planner::LogicalOperator* op) override;
  void visitUnwind(planner::LogicalOperator* op) override;
  void visitSetProperty(planner::LogicalOperator* op) override;
  void visitInsert(planner::LogicalOperator* op) override;
  void visitDelete(planner::LogicalOperator* op) override;
  void visitMerge(planner::LogicalOperator* op) override;
  void visitCopyFrom(planner::LogicalOperator* op) override;
  void visitTableFunctionCall(planner::LogicalOperator*) override;

  void visitSetInfo(const binder::BoundSetPropertyInfo& info);
  void visitInsertInfo(const planner::LogicalInsertInfo& info);

  void collectExpressionsInUse(std::shared_ptr<binder::Expression> expression);

  binder::expression_vector pruneExpressions(
      const binder::expression_vector& expressions);

  void preAppendProjection(planner::LogicalOperator* op, common::idx_t childIdx,
                           binder::expression_vector expressions);

  void collectVariableTypes(std::shared_ptr<binder::Expression> expression);

 private:
  binder::expression_set propertiesInUse;
  binder::expression_set variablesInUse;
  binder::expression_set nodeOrRelInUse;
  common::PathSemantic semantic;
  // To store the type info of variables in use.
  std::unordered_map<std::string, common::LogicalType> variableTypes;
  main::ClientContext* ctx;
};

}  // namespace optimizer
}  // namespace neug
