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
#include "neug/compiler/gopt/g_ddl_converter.h"
#include "neug/compiler/gopt/g_physical_analyzer.h"
#include "neug/compiler/gopt/g_query_converter.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/compiler/planner/operator/simple/logical_extension.h"
#include "neug/generated/proto/plan/physical.pb.h"

namespace neug {
namespace gopt {

class GPhysicalConvertor {
 public:
  GPhysicalConvertor(std::shared_ptr<GAliasManager> aliasManager,
                     neug::catalog::Catalog* catalog)
      : aliasManager{aliasManager}, catalog{catalog} {}

  std::unique_ptr<::physical::PhysicalPlan> createEmptyPlan() {
    auto physicalPlan = std::make_unique<::physical::PhysicalPlan>();
    // Set default read flag
    physicalPlan->mutable_flag()->set_read(true);
    return physicalPlan;
  }

  std::unique_ptr<::physical::PhysicalPlan> convert(
      const planner::LogicalPlan& plan, bool skipSink = false) {
    GPhysicalAnalyzer analyzer(catalog);
    auto flagPB = convertExecutionFlag(analyzer.analyze(plan));
    skipSink |= updateClause(plan.getLastOperator());
    skipSink |= ddlClause(plan.getLastOperator());
    auto queryPlan = convertQuery(plan, skipSink);
    queryPlan->set_allocated_flag(flagPB.release());
    return queryPlan;
  }

 private:
  std::unique_ptr<::physical::ExecutionFlag> convertExecutionFlag(
      const ExecutionFlag& flag) {
    auto flagPB = std::make_unique<::physical::ExecutionFlag>();
    flagPB->set_read(flag.read);
    flagPB->set_insert(flag.insert);
    flagPB->set_update(flag.update);
    flagPB->set_schema(flag.schema);
    flagPB->set_batch(flag.batch);
    flagPB->set_create_temp_table(flag.create_temp_table);
    flagPB->set_checkpoint(flag.transaction);
    flagPB->set_procedure_call(flag.procedure_call);
    return flagPB;
  }

  bool updateClause(std::shared_ptr<planner::LogicalOperator> op) {
    return op->getOperatorType() == planner::LogicalOperatorType::INSERT ||
           op->getOperatorType() == planner::LogicalOperatorType::MERGE ||
           op->getOperatorType() ==
               planner::LogicalOperatorType::SET_PROPERTY ||
           op->getOperatorType() == planner::LogicalOperatorType::DELETE;
  }

  bool ddlClause(std::shared_ptr<planner::LogicalOperator> op) {
    return op->getOperatorType() ==
               planner::LogicalOperatorType::CREATE_TABLE ||
           op->getOperatorType() == planner::LogicalOperatorType::ALTER ||
           op->getOperatorType() == planner::LogicalOperatorType::DROP;
  }

 private:
  std::unique_ptr<::physical::PhysicalPlan> convertQuery(
      const planner::LogicalPlan& plan, bool skipSink) {
    auto converter = std::make_unique<GQueryConvertor>(aliasManager, catalog);
    return converter->convert(plan, skipSink);
  }

 private:
  std::shared_ptr<GAliasManager> aliasManager;
  neug::catalog::Catalog* catalog;
};

}  // namespace gopt
}  // namespace neug
