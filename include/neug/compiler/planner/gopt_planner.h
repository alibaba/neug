// this class will inheritate from graph_planner.h. and it implement the
// function of compilePlan by using the GDatabase, just like the behaviors in
// GOpt.Query test
#pragma once

#include <memory>
#include <shared_mutex>
#include <string>

#include <yaml-cpp/yaml.h>
#include "neug/compiler/gopt/g_alias_manager.h"
#include "neug/compiler/gopt/g_physical_convertor.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/main/metadata_manager.h"
#include "neug/compiler/planner/graph_planner.h"

namespace gs {

class GOptPlanner : public gs::IGraphPlanner {
 public:
  GOptPlanner() : IGraphPlanner() {
    database = std::make_unique<gs::main::MetadataManager>();
    ctx = std::make_unique<gs::main::ClientContext>(database.get());
  }

  inline std::string type() const override { return "gopt"; }

  result<std::pair<physical::PhysicalPlan, std::string>> compilePlan(
      const std::string& query) override;

  void update_meta(const YAML::Node& schema_yaml_node) override;

  void update_statistics(const std::string& graph_statistic_json) override;

 private:
  std::unique_ptr<gs::main::MetadataManager> database;
  std::unique_ptr<gs::main::ClientContext> ctx;
  std::shared_mutex planner_mutex;  // Protects access to the planner
};

}  // namespace gs
