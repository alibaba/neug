// this class will inheritate from graph_planner.h. and it implement the
// function of compilePlan by using the GDatabase, just like the behaviors in
// GOpt.Query test
#pragma once

#include <memory>
#include <string>

#include <rapidjson/document.h>
#include <yaml-cpp/yaml.h>
#include "src/compiler/src/include/gopt/g_alias_manager.h"
#include "src/compiler/src/include/gopt/g_database.h"
#include "src/compiler/src/include/gopt/g_physical_convertor.h"
#include "src/compiler/src/include/main/client_context.h"
#include "src/compiler/src/include/main/database.h"
#include "src/planner/graph_planner.h"

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

  inline std::string type() const override { return "gopt"; }

  gs::Plan compilePlan(const std::string& query,
                       const std::string& graph_schema_yaml,
                       const std::string& graph_statistic_json) override;

 private:
  std::unique_ptr<gs::main::GDatabase> database;
  std::unique_ptr<gs::main::ClientContext> ctx;
};

}  // namespace gs
