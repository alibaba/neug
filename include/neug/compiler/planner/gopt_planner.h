// this class will inheritate from graph_planner.h. and it implement the
// function of compilePlan by using the GDatabase, just like the behaviors in
// GOpt.Query test
#pragma once

#include <memory>
#include <string>

#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/main/metadata_manager.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/compiler/planner/graph_planner.h"
#include "neug/storages/graph/schema.h"

namespace neug {

/**
 * @brief GOptPlanner is an implementation of IGraphPlanner that uses the GOpt
 * optimization framework to compile Cypher queries into executable physical
 * plans.
 * @note GOptPlanner is not thread-safe. Concurrent access to its methods
 * should be synchronized externally.
 * compilePlan: need read-lock.
 */
class GOptPlanner : public neug::IGraphPlanner {
 public:
  GOptPlanner() : IGraphPlanner() {
    database = std::make_unique<neug::main::MetadataManager>();
    neug::main::MetadataRegistry::registerMetadata(database.get());
  }

  inline std::string type() const override { return "gopt"; }

  virtual result<std::pair<physical::PhysicalPlan, std::string>> compilePlan(
      const std::string& query, const Schema* schema,
      const GraphStats& stats) override;

  AccessMode analyzeMode(const std::string& query) const override;

 private:
  std::unique_ptr<neug::main::MetadataManager> database;

 private:
  // return string pattern of update operators
  const common::case_insensitve_set_t& getUpdateOpTokens() const;
  const common::case_insensitve_set_t& getSchemaOpTokens() const;
};

}  // namespace neug
