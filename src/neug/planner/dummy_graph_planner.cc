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

#include "neug/planner/dummy_graph_planner.h"

#include "neug/utils/result.h"

namespace gs {

Plan DummyGraphPlanner::compilePlan(const std::string& cypher_query_string) {
  Plan plan;
  plan.error_code = StatusCode::OK;
  plan.full_message = "OK";
  return plan;
}

void DummyGraphPlanner::update_meta(const YAML::Node& schema_yaml_node) {}

void DummyGraphPlanner::update_statistics(
    const std::string& graph_statistic_json) {}

}  // namespace gs