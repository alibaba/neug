// the class is used to convert the logical plan to physical plan in pb, it
// analyze the physical mode by PhysicalAnalyzer, if it is a DDL plan, it invoke
// the GDDLConvertor to convert the plan, otherwise it invoke the
// GQueryConvertor to convert the plan.
#pragma once

#include <memory>
#include "gopt/g_ddl_converter.h"
#include "gopt/g_physical_analyzer.h"
#include "gopt/g_query_converter.h"
#include "planner/operator/logical_plan.h"
#include "src/proto_generated_gie/physical.pb.h"

namespace kuzu {
namespace gopt {

class GPhysicalConvertor {
 public:
  GPhysicalConvertor(std::shared_ptr<GAliasManager> aliasManager,
                     kuzu::catalog::Catalog* catalog)
      : aliasManager{aliasManager}, catalog{catalog} {}

  std::unique_ptr<::physical::PhysicalPlan> convert(
      const planner::LogicalPlan& plan) {
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
      auto queryPlan = convertQuery(plan);
      queryPlan->set_mode(::physical::QueryPlan::READ_ONLY);
      auto physicalPlan = std::make_unique<::physical::PhysicalPlan>();
      physicalPlan->set_allocated_query_plan(queryPlan.release());
      return physicalPlan;
    }
    case PhysicalMode::READ_WRITE: {
      auto queryPlan = convertQuery(plan);
      queryPlan->set_mode(::physical::QueryPlan::READ_WRITE);
      auto physicalPlan = std::make_unique<::physical::PhysicalPlan>();
      physicalPlan->set_allocated_query_plan(queryPlan.release());
      return physicalPlan;
    }
    default:
      throw common::Exception("Unknown physical mode " +
                              std::to_string(static_cast<int>(mode)));
    }
  }

 private:
  std::unique_ptr<::physical::DDLPlan> convertDDL(
      const planner::LogicalPlan& plan) {
    auto converter = std::make_unique<GDDLConverter>(catalog);
    return converter->convert(plan);
  }

  std::unique_ptr<::physical::QueryPlan> convertQuery(
      const planner::LogicalPlan& plan) {
    auto converter = std::make_unique<GQueryConvertor>(aliasManager, catalog);
    return converter->convert(plan);
  }

 private:
  std::shared_ptr<GAliasManager> aliasManager;
  kuzu::catalog::Catalog* catalog;
};

}  // namespace gopt
}  // namespace kuzu
