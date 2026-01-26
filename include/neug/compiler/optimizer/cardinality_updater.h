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

#include "neug/compiler/optimizer/logical_operator_visitor.h"
namespace neug {
namespace planner {
class LogicalPlan;
class CardinalityEstimator;
}  // namespace planner

namespace transaction {
class Transaction;
}

namespace optimizer {
class CardinalityUpdater : public LogicalOperatorVisitor {
 public:
  explicit CardinalityUpdater(
      const planner::CardinalityEstimator& cardinalityEstimator,
      const transaction::Transaction* transaction)
      : cardinalityEstimator(cardinalityEstimator), transaction(transaction) {}

  void rewrite(planner::LogicalPlan* plan);

 private:
  void visitOperator(planner::LogicalOperator* op);
  void visitOperatorSwitchWithDefault(planner::LogicalOperator* op);

  void visitOperatorDefault(planner::LogicalOperator* op);
  void visitScanNodeTable(planner::LogicalOperator* op) override;
  void visitExtend(planner::LogicalOperator* op) override;
  void visitHashJoin(planner::LogicalOperator* op) override;
  void visitCrossProduct(planner::LogicalOperator* op) override;
  void visitIntersect(planner::LogicalOperator* op) override;
  void visitFlatten(planner::LogicalOperator* op) override;
  void visitFilter(planner::LogicalOperator* op) override;
  void visitAggregate(planner::LogicalOperator* op) override;
  void visitLimit(planner::LogicalOperator* op) override;

  const planner::CardinalityEstimator& cardinalityEstimator;
  const transaction::Transaction* transaction;
};
}  // namespace optimizer
}  // namespace neug
