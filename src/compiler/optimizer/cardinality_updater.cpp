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

#include "neug/compiler/optimizer/cardinality_updater.h"

#include "neug/compiler/planner/join_order/cardinality_estimator.h"
#include "neug/compiler/planner/operator/extend/logical_extend.h"
#include "neug/compiler/planner/operator/logical_aggregate.h"
#include "neug/compiler/planner/operator/logical_filter.h"
#include "neug/compiler/planner/operator/logical_flatten.h"
#include "neug/compiler/planner/operator/logical_hash_join.h"
#include "neug/compiler/planner/operator/logical_intersect.h"
#include "neug/compiler/planner/operator/logical_limit.h"
#include "neug/compiler/planner/operator/logical_plan.h"

namespace neug::optimizer {
void CardinalityUpdater::rewrite(planner::LogicalPlan* plan) {
  visitOperator(plan->getLastOperator().get());
}

void CardinalityUpdater::visitOperator(planner::LogicalOperator* op) {
  if (op->getOperatorType() != planner::LogicalOperatorType::INTERSECT) {
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
      visitOperator(op->getChild(i).get());
    }
  }
  visitOperatorSwitchWithDefault(op);
}

void CardinalityUpdater::visitOperatorSwitchWithDefault(
    planner::LogicalOperator* op) {
  switch (op->getOperatorType()) {
  case planner::LogicalOperatorType::SCAN_NODE_TABLE: {
    visitScanNodeTable(op);
    break;
  }
  case planner::LogicalOperatorType::EXTEND: {
    visitExtend(op);
    break;
  }
  case planner::LogicalOperatorType::HASH_JOIN: {
    visitHashJoin(op);
    break;
  }
  case planner::LogicalOperatorType::CROSS_PRODUCT: {
    visitCrossProduct(op);
    break;
  }
  case planner::LogicalOperatorType::INTERSECT: {
    visitIntersect(op);
    break;
  }
  case planner::LogicalOperatorType::FLATTEN: {
    visitFlatten(op);
    break;
  }
  case planner::LogicalOperatorType::FILTER: {
    visitFilter(op);
    break;
  }
  case planner::LogicalOperatorType::LIMIT: {
    visitLimit(op);
    break;
  }
  case planner::LogicalOperatorType::AGGREGATE: {
    visitAggregate(op);
    break;
  }
  default: {
    visitOperatorDefault(op);
    break;
  }
  }
}

void CardinalityUpdater::visitOperatorDefault(planner::LogicalOperator* op) {
  if (op->getNumChildren() == 1) {
    op->setCardinality(op->getChild(0)->getCardinality());
  }
}

void CardinalityUpdater::visitScanNodeTable(planner::LogicalOperator* op) {
  op->setCardinality(cardinalityEstimator.estimateScanNode(*op));
}

void CardinalityUpdater::visitExtend(planner::LogicalOperator* op) {
  NEUG_ASSERT(transaction);
  auto& extend = op->cast<planner::LogicalExtend&>();
  const auto extensionRate = cardinalityEstimator.getExtensionRate(
      *extend.getRel(), *extend.getBoundNode(), transaction);
  extend.setCardinality(cardinalityEstimator.multiply(
      extensionRate, op->getChild(0)->getCardinality()));
}

void CardinalityUpdater::visitHashJoin(planner::LogicalOperator* op) {
  auto& hashJoin = op->cast<planner::LogicalHashJoin&>();
  NEUG_ASSERT(hashJoin.getNumChildren() >= 2);
  hashJoin.setCardinality(cardinalityEstimator.estimateHashJoin(
      hashJoin.getJoinConditions(), *hashJoin.getChild(0),
      *hashJoin.getChild(1)));
}

void CardinalityUpdater::visitCrossProduct(planner::LogicalOperator* op) {
  op->setCardinality(cardinalityEstimator.estimateCrossProduct(
      *op->getChild(0), *op->getChild(1)));
}

void CardinalityUpdater::visitIntersect(planner::LogicalOperator* op) {
  auto& intersect = op->cast<planner::LogicalIntersect&>();
  NEUG_ASSERT(intersect.getNumChildren() >= 2);
  visitOperator(intersect.getChild(0).get());
  auto buildCards = intersect.getBuildCards();
  if (buildCards.size() + 1 != intersect.getNumChildren()) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "buildCards size is not consistent with buildPlans size in intersect");
  }
  for (size_t i = 1; i < intersect.getNumChildren(); ++i) {
    intersect.getChild(i)->setCardinality(buildCards[i - 1]);
  }
  intersect.setCardinality(buildCards[buildCards.size() - 1]);
}

void CardinalityUpdater::visitFlatten(planner::LogicalOperator* op) {
  auto& flatten = op->cast<planner::LogicalFlatten&>();
  flatten.setCardinality(cardinalityEstimator.estimateFlatten(
      *flatten.getChild(0), flatten.getGroupPos()));
}

void CardinalityUpdater::visitFilter(planner::LogicalOperator* op) {
  auto& filter = op->cast<planner::LogicalFilter&>();
  filter.setCardinality(cardinalityEstimator.estimateFilter(
      *filter.getChild(0), *filter.getPredicate()));
}

void CardinalityUpdater::visitLimit(planner::LogicalOperator* op) {
  auto& limit = op->cast<planner::LogicalLimit&>();
  if (limit.canEvaluateLimitNum()) {
    limit.setCardinality(limit.evaluateLimitNum());
  }
}

void CardinalityUpdater::visitAggregate(planner::LogicalOperator* op) {
  auto& aggregate = op->cast<planner::LogicalAggregate&>();
  aggregate.setCardinality(cardinalityEstimator.estimateAggregate(aggregate));
}

}  // namespace neug::optimizer
