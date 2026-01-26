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

#include "neug/compiler/binder/expression_visitor.h"
#include "neug/compiler/gopt/g_alias_manager.h"
#include "neug/compiler/optimizer/logical_operator_visitor.h"
#include "neug/compiler/planner/operator/extend/logical_extend.h"
#include "neug/compiler/planner/operator/logical_get_v.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/compiler/planner/operator/scan/logical_scan_node_table.h"

namespace neug {
namespace optimizer {

class RenameDependentVarOpt : public LogicalOperatorVisitor {
 public:
  void rewrite(planner::LogicalPlan* plan) {
    auto root = plan->getLastOperator();
    visitOperator(root.get());
  }

  void visitOperator(planner::LogicalOperator* op) {
    for (auto child : op->getChildren()) {
      visitOperator(child.get());
    }
    visitOperatorSwitch(op);
  }

  void visitScanNodeTable(planner::LogicalOperator* op) override {
    auto scanOp = op->ptrCast<planner::LogicalScanNodeTable>();
    auto predicate = scanOp->getPredicates();
    if (predicate) {
      renameDependentVar(predicate, gopt::DEFAULT_ALIAS_NAME);
    }
  }

  void visitExtend(planner::LogicalOperator* op) override {
    auto extendOp = op->ptrCast<planner::LogicalExtend>();
    auto predicate = extendOp->getPredicates();
    if (predicate) {
      renameDependentVar(predicate, gopt::DEFAULT_ALIAS_NAME);
    }
  }

  void visitGetV(planner::LogicalOperator* op) override {
    auto getVOp = op->ptrCast<planner::LogicalGetV>();
    auto predicate = getVOp->getPredicates();
    if (predicate) {
      renameDependentVar(predicate, gopt::DEFAULT_ALIAS_NAME);
    }
  }

 private:
  void renameDependentVar(std::shared_ptr<binder::Expression> predicate,
                          const std::string& newVarName) {
    binder::RenameDependentVar renameVisitor(newVarName);
    renameVisitor.visit(predicate);
  }
};

}  // namespace optimizer
}  // namespace neug