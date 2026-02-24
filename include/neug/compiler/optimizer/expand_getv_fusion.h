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

#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/optimizer/logical_operator_visitor.h"
#include "neug/compiler/planner/operator/extend/logical_extend.h"
#include "neug/compiler/planner/operator/logical_get_v.h"
#include "neug/compiler/planner/operator/logical_operator.h"
#include "neug/compiler/planner/operator/logical_plan.h"

namespace neug {
namespace optimizer {

enum FusionType {
  EXPANDE_GETV,  // maintain the original plan before the optimization
  EXPANDV_GETV,  // fuse expandE+getV, but need another getV for label or
                 // predicates filtering
  EXPANDV        // fuse expandE+getV into a single expandV operator
};

class ExpandGetVFusion : public LogicalOperatorVisitor {
 private:
  catalog::Catalog* catalog;

  FusionType analyze(std::shared_ptr<planner::LogicalOperator> getV,
                     std::shared_ptr<planner::LogicalOperator> expand);
  bool hasGetVFiltering(std::shared_ptr<planner::LogicalOperator> getV,
                        std::shared_ptr<planner::LogicalOperator> expand);
  std::shared_ptr<planner::LogicalOperator> perform(
      std::shared_ptr<planner::LogicalOperator> getV,
      std::shared_ptr<planner::LogicalOperator> expand, FusionType fusionType);

 public:
  ExpandGetVFusion(catalog::Catalog* catalog) : catalog{catalog} {};
  ~ExpandGetVFusion() = default;

  void rewrite(planner::LogicalPlan* plan);
  std::shared_ptr<planner::LogicalOperator> visitOperator(
      const std::shared_ptr<planner::LogicalOperator>& op);
  std::shared_ptr<planner::LogicalOperator> visitGetVReplace(
      std::shared_ptr<planner::LogicalOperator> op) override;
  std::shared_ptr<planner::LogicalOperator> visitRecursiveExtendReplace(
      std::shared_ptr<planner::LogicalOperator> op) override;

  static bool hasLabelFiltering(const gopt::GNodeType& getVType,
                                const gopt::GRelType& expandType,
                                const gopt::GNodeType& sourceType,
                                common::ExtendDirection direction,
                                catalog::Catalog* catalog);
  static gopt::GRelType transformExpandType(const gopt::GRelType& expandType,
                                            catalog::Catalog* catalog);
};

}  // namespace optimizer
}  // namespace neug
