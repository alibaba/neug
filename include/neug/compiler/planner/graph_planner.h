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

#ifndef SRC_PLANNER_GRAPH_PLANNER_H_
#define SRC_PLANNER_GRAPH_PLANNER_H_

#include <string>
#include <vector>

#include <yaml-cpp/yaml.h>
#include "neug/utils/leaf_utils.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/physical.pb.h"
#else
#include "neug/utils/proto/plan/physical.pb.h"
#endif
#include "neug/utils/result.h"

namespace gs {

/**
 * @brief Graph planner interface. Receive the cypher query, and generate the
 * executable plan.
 */
class IGraphPlanner {
 public:
  IGraphPlanner(const std::string& compiler_config_path)
      : compiler_config_path_(compiler_config_path) {}

  virtual std::string type() const = 0;

  virtual ~IGraphPlanner() = default;

  /**
   * @brief Generate the executable plan.
   * @param query The cypher query.
   * @return The executable plan.
   */
  virtual Result<std::pair<physical::PhysicalPlan, std::string>> compilePlan(
      const std::string& query) = 0;

  /**
   * @brief Update the metadata of the graph. To let the planner be aware of the
   * changes in the graph schema and statistics.
   * @param schema_yaml_node The YAML node of the graph schema.
   */
  virtual void update_meta(const YAML::Node& schema_yaml_node) = 0;

  /**
   * @brief Update the statistics of the graph.
   * @param graph_statistic_json The JSON string of the graph statistics.
   */
  virtual void update_statistics(const std::string& graph_statistic_json) = 0;

 protected:
  std::string compiler_config_path_;
};

}  // namespace gs

#endif  // SRC_PLANNER_GRAPH_PLANNER_H_