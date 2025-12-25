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
#include "neug/compiler/gopt/g_admin_convertor.h"
#include "neug/compiler/gopt/g_ddl_converter.h"
#include "neug/compiler/gopt/g_physical_analyzer.h"
#include "neug/compiler/gopt/g_query_converter.h"
#include "neug/compiler/planner/operator/logical_plan.h"
#include "neug/compiler/planner/operator/simple/logical_extension.h"
#include "neug/generated/proto/plan/physical.pb.h"

namespace gs {
namespace gopt {

class GPhysicalConvertor {
 public:
  GPhysicalConvertor(std::shared_ptr<GAliasManager> aliasManager,
                     gs::catalog::Catalog* catalog)
      : aliasManager{aliasManager}, catalog{catalog} {}

  std::unique_ptr<::physical::PhysicalPlan> createEmptyPlan() {
    auto physicalPlan = std::make_unique<::physical::PhysicalPlan>();
    auto queryPlan = std::make_unique<::physical::QueryPlan>();
    queryPlan->set_mode(::physical::QueryPlan::READ_ONLY);
    physicalPlan->set_allocated_query_plan(queryPlan.release());
    return physicalPlan;
  }

  std::unique_ptr<::physical::PhysicalPlan> convert(
      const planner::LogicalPlan& plan, bool skipSink = false) {
    GPhysicalAnalyzer analyzer;
    auto mode = analyzer.analyze(plan);
    switch (mode) {
    case PhysicalMode::DDL: {
      auto ddlPlan = convertDDL(plan);
      auto physicalPlan = std::make_unique<::physical::PhysicalPlan>();
      physicalPlan->set_allocated_ddl_plan(ddlPlan.release());
      return physicalPlan;
    }
    case PhysicalMode::READ_ONLY: {
      auto queryPlan = convertQuery(plan, skipSink);
      queryPlan->set_mode(::physical::QueryPlan::READ_ONLY);
      auto physicalPlan = std::make_unique<::physical::PhysicalPlan>();
      physicalPlan->set_allocated_query_plan(queryPlan.release());
      return physicalPlan;
    }
    case PhysicalMode::READ_WRITE: {
      // remove tailing sink operator is last operator is update clause
      skipSink |= updateClause(plan.getLastOperator());
      auto queryPlan = convertQuery(plan, skipSink);
      queryPlan->set_mode(::physical::QueryPlan::READ_WRITE);
      auto physicalPlan = std::make_unique<::physical::PhysicalPlan>();
      physicalPlan->set_allocated_query_plan(queryPlan.release());
      return physicalPlan;
    }
    case PhysicalMode::ADMIN: {
      auto adminPlan = convertAdmin(plan);
      auto physicalPlan = std::make_unique<::physical::PhysicalPlan>();
      physicalPlan->set_allocated_admin_plan(adminPlan.release());
      return physicalPlan;
    }
    default:
      THROW_EXCEPTION_WITH_FILE_LINE("Unknown physical mode " +
                                     std::to_string(static_cast<int>(mode)));
    }
  }

  bool updateClause(std::shared_ptr<planner::LogicalOperator> op) {
    return op->getOperatorType() == planner::LogicalOperatorType::INSERT ||
           op->getOperatorType() == planner::LogicalOperatorType::MERGE ||
           op->getOperatorType() ==
               planner::LogicalOperatorType::SET_PROPERTY ||
           op->getOperatorType() == planner::LogicalOperatorType::DELETE;
  }

 private:
  std::unique_ptr<::physical::DDLPlan> convertDDL(
      const planner::LogicalPlan& plan) {
    auto converter = std::make_unique<GDDLConverter>(catalog);
    return converter->convert(plan);
  }

  std::unique_ptr<::physical::QueryPlan> convertQuery(
      const planner::LogicalPlan& plan, bool skipSink) {
    auto converter = std::make_unique<GQueryConvertor>(aliasManager, catalog);
    return converter->convert(plan, skipSink);
  }

  std::unique_ptr<::physical::AdminPlan> convertAdmin(
      const planner::LogicalPlan& plan) {
    auto converter = std::make_unique<GAdminConvertor>();
    return converter->convert(plan);
  }

 private:
  std::shared_ptr<GAliasManager> aliasManager;
  gs::catalog::Catalog* catalog;
};

}  // namespace gopt
}  // namespace gs
