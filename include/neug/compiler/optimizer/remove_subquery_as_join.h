/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/optimizer/logical_operator_visitor.h"
#include "neug/compiler/planner/operator/logical_filter.h"
#include "neug/compiler/planner/operator/logical_plan.h"

namespace neug {
namespace optimizer {
class RemoveSubqueryAsJoin : public LogicalOperatorVisitor {
 public:
  void rewrite(planner::LogicalPlan* plan);
  std::shared_ptr<planner::LogicalOperator> visitOperator(
      const std::shared_ptr<planner::LogicalOperator>& op);
  std::shared_ptr<planner::LogicalOperator> visitFilterReplace(
      std::shared_ptr<planner::LogicalOperator> op) override;

 private:
  bool existSubQuery(std::shared_ptr<binder::Expression> expr);
  bool notExistSubQuery(std::shared_ptr<binder::Expression> expr);
};
}  // namespace optimizer
}  // namespace neug