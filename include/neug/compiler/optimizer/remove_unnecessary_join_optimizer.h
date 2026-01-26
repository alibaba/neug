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

/* Due to the nature of graph pattern, a (node)-[rel]-(node) is always
 * interpreted as two joins. However, in many cases, a single join is
 * sufficient. E.g. MATCH (a)-[e]->(b) RETURN e.date Our planner will generate a
 * plan where the HJ is redundant. HJ
 *     /  \
 *   E(e) S(b)
 *    |
 *   S(a)
 * This optimizer prunes such redundant joins.
 */
class RemoveUnnecessaryJoinOptimizer : public LogicalOperatorVisitor {
 public:
  void rewrite(planner::LogicalPlan* plan);

 private:
  std::shared_ptr<planner::LogicalOperator> visitOperator(
      const std::shared_ptr<planner::LogicalOperator>& op);

  std::shared_ptr<planner::LogicalOperator> visitHashJoinReplace(
      std::shared_ptr<planner::LogicalOperator> op) override;
};

}  // namespace optimizer
}  // namespace neug
