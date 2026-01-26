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
#include "neug/compiler/planner/operator/logical_plan.h"

namespace neug {
namespace optimizer {

class FactorizationRewriter final : public LogicalOperatorVisitor {
 public:
  void rewrite(planner::LogicalPlan* plan);

  void visitOperator(planner::LogicalOperator* op);

 private:
  void visitHashJoin(planner::LogicalOperator* op) override;
  void visitIntersect(planner::LogicalOperator* op) override;
  void visitProjection(planner::LogicalOperator* op) override;
  void visitAccumulate(planner::LogicalOperator* op) override;
  void visitAggregate(planner::LogicalOperator* op) override;
  void visitOrderBy(planner::LogicalOperator* op) override;
  void visitLimit(planner::LogicalOperator* op) override;
  void visitDistinct(planner::LogicalOperator* op) override;
  void visitUnwind(planner::LogicalOperator* op) override;
  void visitUnion(planner::LogicalOperator* op) override;
  void visitFilter(planner::LogicalOperator* op) override;
  void visitSetProperty(planner::LogicalOperator* op) override;
  void visitDelete(planner::LogicalOperator* op) override;
  void visitInsert(planner::LogicalOperator* op) override;
  void visitMerge(planner::LogicalOperator* op) override;
  void visitCopyTo(planner::LogicalOperator* op) override;

  std::shared_ptr<planner::LogicalOperator> appendFlattens(
      std::shared_ptr<planner::LogicalOperator> op,
      const std::unordered_set<planner::f_group_pos>& groupsPos);
  std::shared_ptr<planner::LogicalOperator> appendFlattenIfNecessary(
      std::shared_ptr<planner::LogicalOperator> op,
      planner::f_group_pos groupPos);
};

}  // namespace optimizer
}  // namespace neug
