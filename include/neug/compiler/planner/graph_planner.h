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

#pragma once

#include <string>
#include <vector>

#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/storages/graph/graph_stats.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/access_mode.h"
#include "neug/utils/result.h"

namespace neug {

/**
 * @brief Graph planner interface. Receive the cypher query, and generate the
 * executable plan.
 */
class IGraphPlanner {
 public:
  IGraphPlanner() {}

  virtual std::string type() const = 0;

  virtual ~IGraphPlanner() = default;

  /**
   * @brief Generate the executable plan.
   * @param query The cypher query.
   * @return The executable plan.
   */
  virtual result<std::pair<physical::PhysicalPlan, std::string>> compilePlan(
      const std::string& query, const Schema* schema,
      const GraphStats& stats) = 0;

  // Attempts to infer the execution access mode from the given query.
  // The current implementation relies on static analysis of the query string
  // and can only distinguish between "update" and "read" modes.
  // Inferring an "insert" mode would require more complex operator
  // combination analysis, which is not supported at the moment.
  virtual AccessMode analyzeMode(const std::string& query) const = 0;
};

}  // namespace neug
