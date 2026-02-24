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

#include "neug/compiler/optimizer/limit_push_down_optimizer.h"

#include "neug/compiler/planner/operator/extend/logical_recursive_extend.h"
#include "neug/compiler/planner/operator/logical_distinct.h"
#include "neug/compiler/planner/operator/logical_hash_join.h"
#include "neug/compiler/planner/operator/logical_limit.h"
#include "neug/utils/exception/exception.h"

using namespace neug::binder;
using namespace neug::common;
using namespace neug::planner;

namespace neug {
namespace optimizer {

void LimitPushDownOptimizer::rewrite(LogicalPlan* plan) {
  visitOperator(plan->getLastOperator().get());
}

void LimitPushDownOptimizer::visitOperator(planner::LogicalOperator* op) {
  switch (op->getOperatorType()) {
  case LogicalOperatorType::LIMIT: {
    auto& limit = op->constCast<LogicalLimit>();
    if (limit.hasSkipNum() && limit.canEvaluateSkipNum()) {
      skipNumber = limit.evaluateSkipNum();
    }
    if (limit.hasLimitNum() && limit.canEvaluateLimitNum()) {
      limitNumber = limit.evaluateLimitNum();
    }
    visitOperator(limit.getChild(0).get());
    return;
  }
  case LogicalOperatorType::MULTIPLICITY_REDUCER:
  case LogicalOperatorType::EXPLAIN:
  case LogicalOperatorType::ACCUMULATE:
  case LogicalOperatorType::PROJECTION: {
    visitOperator(op->getChild(0).get());
    return;
  }
  case LogicalOperatorType::DISTINCT: {
    if (limitNumber == INVALID_LIMIT && skipNumber == 0) {
      return;
    }
    auto& distinctOp = op->cast<LogicalDistinct>();
    distinctOp.setLimitNum(limitNumber);
    distinctOp.setSkipNum(skipNumber);
    return;
  }
  case LogicalOperatorType::HASH_JOIN: {
    if (limitNumber == INVALID_LIMIT && skipNumber == 0) {
      return;
    }
    if (op->getChild(0)->getOperatorType() == LogicalOperatorType::HASH_JOIN) {
      op->ptrCast<LogicalHashJoin>()->getSIPInfoUnsafe().position =
          SemiMaskPosition::NONE;
      // OP is the hash join reading destination node property. Continue push
      // limit down.
      op = op->getChild(0).get();
    }
    if (op->getChild(0)->getOperatorType() ==
        LogicalOperatorType::PATH_PROPERTY_PROBE) {
      // LCOV_EXCL_START
      if (op->getChild(0)->getChild(0)->getOperatorType() !=
          LogicalOperatorType::RECURSIVE_EXTEND) {
        THROW_RUNTIME_ERROR(
            "Trying to push limit to a non RECURSIVE_EXTEND operator. "
            "This should never happen.");
      }
      // LCOV_EXCL_STOP
      auto& extend =
          op->getChild(0)->getChild(0)->cast<LogicalRecursiveExtend>();
      extend.setLimitNum(skipNumber + limitNumber);
    }
    return;
  }
  case LogicalOperatorType::UNION_ALL: {
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
      auto optimizer = LimitPushDownOptimizer();
      optimizer.visitOperator(op->getChild(i).get());
    }
    return;
  }
  default:
    return;
  }
}

}  // namespace optimizer
}  // namespace neug
