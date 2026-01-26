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

// This optimizer analyzes the dependency between group by keys. If key2 depends
// on key1 (e.g. key1 is a primary key column) we only hash on key1 and saves
// key2 as a payload.
class AggKeyDependencyOptimizer : public LogicalOperatorVisitor {
 public:
  void rewrite(planner::LogicalPlan* plan);

 private:
  void visitOperator(planner::LogicalOperator* op);

  void visitAggregate(planner::LogicalOperator* op) override;
  void visitDistinct(planner::LogicalOperator* op) override;

  std::pair<binder::expression_vector, binder::expression_vector>
  resolveKeysAndDependentKeys(const binder::expression_vector& keys);
};

}  // namespace optimizer
}  // namespace neug
