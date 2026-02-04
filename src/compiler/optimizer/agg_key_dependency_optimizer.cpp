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

#include "neug/compiler/optimizer/agg_key_dependency_optimizer.h"

#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/planner/operator/logical_aggregate.h"
#include "neug/compiler/planner/operator/logical_distinct.h"

using namespace neug::binder;
using namespace neug::common;
using namespace neug::planner;

namespace neug {
namespace optimizer {

void AggKeyDependencyOptimizer::rewrite(planner::LogicalPlan* plan) {
  visitOperator(plan->getLastOperator().get());
}

void AggKeyDependencyOptimizer::visitOperator(planner::LogicalOperator* op) {
  // bottom up traversal
  for (auto i = 0u; i < op->getNumChildren(); ++i) {
    visitOperator(op->getChild(i).get());
  }
  visitOperatorSwitch(op);
}

void AggKeyDependencyOptimizer::visitAggregate(planner::LogicalOperator* op) {
  auto agg = (LogicalAggregate*) op;
  auto [keys, dependentKeys] = resolveKeysAndDependentKeys(agg->getKeys());
  agg->setKeys(keys);
  agg->setDependentKeys(dependentKeys);
}

void AggKeyDependencyOptimizer::visitDistinct(planner::LogicalOperator* op) {
  // remove optimizations for distinct primary keys
}

std::pair<binder::expression_vector, binder::expression_vector>
AggKeyDependencyOptimizer::resolveKeysAndDependentKeys(
    const expression_vector& inputKeys) {
  // Consider example RETURN a.ID, a.age, COUNT(*).
  // We first collect a.ID into primaryKeys. Then collect "a" into
  // primaryVarNames. Finally, we loop through all group by keys to put
  // non-primary key properties under name "a" into dependentKeyExpressions.

  // Collect primary variables from keys.
  std::unordered_set<std::string> primaryVarNames;
  for (auto& key : inputKeys) {
    if (key->expressionType == ExpressionType::PROPERTY) {
      auto property = (PropertyExpression*) key.get();
      if (property->isPrimaryKey() || property->isInternalID()) {
        primaryVarNames.insert(property->getVariableName());
      }
    }
  }
  // Resolve key dependency.
  binder::expression_vector keys;
  binder::expression_vector dependentKeys;
  for (auto& key : inputKeys) {
    if (key->expressionType == ExpressionType::PROPERTY) {
      auto property = (PropertyExpression*) key.get();
      if (property->isPrimaryKey() ||
          property->isInternalID()) {  // NOLINT(bugprone-branch-clone):
                                       // Collapsing is a logical error.
        // Primary properties are always keys.
        keys.push_back(key);
      } else if (primaryVarNames.contains(property->getVariableName())) {
        // Properties depend on any primary property are dependent keys.
        // e.g. a.age depends on a._id
        dependentKeys.push_back(key);
      } else {
        keys.push_back(key);
      }
    } else if (ExpressionUtil::isNodePattern(*key) ||
               ExpressionUtil::isRelPattern(*key)) {
      if (primaryVarNames.contains(key->getUniqueName())) {
        // e.g. a depends on a._id
        dependentKeys.push_back(key);
      } else {
        keys.push_back(key);
      }
    } else {
      keys.push_back(key);
    }
  }
  return std::make_pair(std::move(keys), std::move(dependentKeys));
}

}  // namespace optimizer
}  // namespace neug
