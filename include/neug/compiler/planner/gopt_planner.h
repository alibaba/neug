// this class will inheritate from graph_planner.h. and it implement the
// function of compilePlan by using the GDatabase, just like the behaviors in
// GOpt.Query test
#pragma once

#include <memory>
#include <string>

#include <yaml-cpp/yaml.h>
#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/main/metadata_manager.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/compiler/planner/graph_planner.h"

namespace neug {

/**
 * @brief GOptPlanner is an implementation of IGraphPlanner that uses the GOpt
 * optimization framework to compile Cypher queries into executable physical
 * plans.
 * @note GOptPlanner is not thread-safe. Concurrent access to its methods
 * should be synchronized externally.
 * compilePlan: need read-lock.
 * update_meta/update_statistics: need write-lock.
 */
class GOptPlanner : public neug::IGraphPlanner {
 public:
  GOptPlanner() : IGraphPlanner() {
    database = std::make_unique<neug::main::MetadataManager>();
    ctx = std::make_unique<neug::main::ClientContext>(database.get());
    neug::main::MetadataRegistry::registerMetadata(database.get());
  }

  inline std::string type() const override { return "gopt"; }

  result<std::pair<physical::PhysicalPlan, std::string>> compilePlan(
      const std::string& query) override;

  void update_meta(const YAML::Node& schema_yaml_node) override;

  void update_statistics(const std::string& graph_statistic_json) override;

  AccessMode analyzeMode(const std::string& query) const override;

 private:
  std::unique_ptr<neug::main::MetadataManager> database;
  std::unique_ptr<neug::main::ClientContext> ctx;

 private:
  // return string pattern of update operators
  const common::case_insensitve_set_t& getUpdateOpTokens() const;
  const common::case_insensitve_set_t& getSchemaOpTokens() const;
};

}  // namespace neug
