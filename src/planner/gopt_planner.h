// this class will inheritate from graph_planner.h. and it implement the
// function of compilePlan by using the GDatabase, just like the behaviors in
// GOpt.Query test
#pragma once

#include <memory>
#include <string>

#include <yaml-cpp/yaml.h>
#include "src/compiler/src/include/gopt/g_alias_manager.h"
#include "src/compiler/src/include/gopt/g_database.h"
#include "src/compiler/src/include/gopt/g_physical_convertor.h"
#include "src/compiler/src/include/main/client_context.h"
#include "src/compiler/src/include/main/database.h"
#include "src/planner/graph_planner.h"
#include "third_party/nlohmann_json/json.hpp"

#include <glog/logging.h>

namespace gs {

class GOptPlanner : public gs::IGraphPlanner {
 public:
  GOptPlanner(const std::string& compiler_config_path)
      : IGraphPlanner(compiler_config_path) {
    gs::main::SystemConfig sysConfig;
    sysConfig.readOnly = false;
    database = std::make_unique<gs::main::GDatabase>(sysConfig);
    ctx = std::make_unique<gs::main::ClientContext>(database.get());
  }

  std::string type() const override { return "gopt"; }

  gs::Plan compilePlan(const std::string& query,
                       const std::string& graph_schema_yaml,
                       const std::string& graph_statistic_json) override {
    LOG(INFO) << "[GOptPlanner] compilePlan called with query: " << query;
    gs::Plan plan;

    try {
      if (!graph_schema_yaml.empty()) {
        // Update schema
        database->updateSchema(graph_schema_yaml, graph_statistic_json);
      }

      // Prepare and compile query
      auto statement = ctx->prepare(query);
      if (!statement->success) {
        plan.error_code = StatusCode::ERR_QUERY_SYNTAX;
        plan.full_message = statement->errMsg;
        return plan;
      }

      std::cout << "Logical Plan: " << std::endl
                << statement->logicalPlan->toString() << std::endl;

      auto aliasManager =
          std::make_shared<gs::gopt::GAliasManager>(*statement->logicalPlan);
      gs::gopt::GPhysicalConvertor converter(aliasManager,
                                             database->getCatalog());
      auto physicalPlan = converter.convert(*statement->logicalPlan);

      plan.error_code = StatusCode::OK;
      plan.physical_plan = std::move(*physicalPlan);
      // todo: set result schema

      return plan;

    } catch (const std::exception& e) {
      plan.error_code = StatusCode::ERR_UNKNOWN;
      plan.full_message = e.what();
      return plan;
    }
  }

 private:
  std::unique_ptr<gs::main::GDatabase> database;
  std::unique_ptr<gs::main::ClientContext> ctx;
};

}  // namespace gs
