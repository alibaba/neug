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

#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/binder/expression_visitor.h"
#include "neug/compiler/optimizer/logical_operator_visitor.h"
#include "neug/compiler/planner/operator/extend/logical_extend.h"
#include "neug/compiler/planner/operator/logical_filter.h"
#include "neug/compiler/planner/operator/logical_get_v.h"
#include "neug/compiler/planner/operator/logical_node_label_filter.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"

namespace neug {
namespace optimizer {

class FilterPushDownPattern : public LogicalOperatorVisitor {
 public:
  void rewrite(planner::LogicalPlan* plan);
  std::shared_ptr<planner::LogicalOperator> visitOperator(
      const std::shared_ptr<planner::LogicalOperator>& op);
  std::shared_ptr<planner::LogicalOperator> visitFilterReplace(
      std::shared_ptr<planner::LogicalOperator> op) override;
  std::shared_ptr<planner::LogicalOperator> visitNodeLabelFilterReplace(
      std::shared_ptr<planner::LogicalOperator> op) override;

 private:
  std::shared_ptr<binder::Expression> andPredicate(
      std::shared_ptr<binder::Expression> left,
      std::shared_ptr<binder::Expression> right);
  bool canPushDown(std::shared_ptr<planner::LogicalOperator> child,
                   std::shared_ptr<binder::Expression> predicate);
  std::shared_ptr<planner::LogicalOperator> perform(
      std::shared_ptr<planner::LogicalOperator> child,
      std::shared_ptr<binder::Expression> predicate);
  std::string getUniqueName(std::shared_ptr<planner::LogicalOperator> child);
  std::shared_ptr<binder::Expression> bindBooleanExpression(
      common::ExpressionType expressionType,
      const binder::expression_vector& children);
};

}  // namespace optimizer
}  // namespace neug
