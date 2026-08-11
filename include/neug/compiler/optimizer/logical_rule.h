/** Copyright 2020 Alibaba Group Holding Limited.
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

#pragma once

#include "neug/compiler/main/client_context.h"
#include "neug/compiler/optimizer/logical_operator_visitor.h"
#include "neug/compiler/planner/operator/logical_plan.h"

namespace neug::optimizer {

class LogicalRule : public LogicalOperatorVisitor {
 public:
  ~LogicalRule() override = default;

  virtual void rewrite(main::ClientContext* ctx, planner::LogicalPlan* plan) {
    auto root = plan->getLastOperator();
    auto rootOpt = visitOperator(root);
    plan->setLastOperator(rootOpt);
  }

  virtual std::shared_ptr<planner::LogicalOperator> visitOperator(
      const std::shared_ptr<planner::LogicalOperator>& op) {
    // bottom-up traversal
    for (auto i = 0u; i < op->getNumChildren(); ++i) {
      op->setChild(i, visitOperator(op->getChild(i)));
    }
    auto result = visitOperatorReplaceSwitch(op);
    // schema of each operator is unchanged
    // result->computeFlatSchema();
    return result;
  }
};

}  // namespace neug::optimizer
