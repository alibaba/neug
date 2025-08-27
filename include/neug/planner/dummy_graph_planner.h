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

#ifndef SRC_PLANNER_DUMMY_GRAPH_PLANNER_H_
#define SRC_PLANNER_DUMMY_GRAPH_PLANNER_H_

#include <string>

#include "neug/planner/graph_planner.h"

namespace gs {

class DummyGraphPlanner : public IGraphPlanner {
 public:
  DummyGraphPlanner() : IGraphPlanner("") {}
  ~DummyGraphPlanner() {}

  std::string type() const override { return "dummy"; }

  Result<std::pair<physical::PhysicalPlan, std::string>> compilePlan(
      const std::string& cypher_query_string) override;

  void update_meta(const YAML::Node& schema_yaml_node) override;

  void update_statistics(const std::string& graph_statistic_json) override;
};

}  // namespace gs
#endif  // SRC_PLANNER_DUMMY_GRAPH_PLANNER_H_