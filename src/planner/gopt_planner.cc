/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include "src/planner/gopt_planner.h"
#include <yaml-cpp/node/emit.h>
#include "src/include/gopt/g_result_schema.h"

namespace gs {

Plan GOptPlanner::compilePlan(const std::string& query,
                              const std::string& graph_schema_yaml,
                              const std::string& graph_statistic_json) {
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

    // set result schema
    auto resultYaml = gopt::GResultSchema::infer(
        *statement->logicalPlan, aliasManager, database->getCatalog());
    plan.result_schema = YAML::Dump(resultYaml);

    return plan;

  } catch (const std::exception& e) {
    plan.error_code = StatusCode::ERR_UNKNOWN;
    plan.full_message = e.what();
    return plan;
  }
}
}  // namespace gs