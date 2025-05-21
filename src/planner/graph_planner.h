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

#include "src/proto_generated_gie/physical.pb.h"

namespace gs {
struct Plan {
  // TODO(zhanglei,xiaoli): Use a general error code definition for the whole
  // system.
  std::string error_code;
  std::string full_message;
  physical::PhysicalPlan physical_plan;
  std::string result_schema;
};

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
  virtual Plan compilePlan(const std::string& query,
                           const std::string& graph_schema_yaml,
                           const std::string& graph_statistic_json) = 0;

 protected:
  std::string compiler_config_path_;
};

}  // namespace gs

#endif  // SRC_PLANNER_GRAPH_PLANNER_H_