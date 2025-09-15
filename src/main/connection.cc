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

#include "neug/main/connection.h"

#include "neug/main/neug_db.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/yaml_utils.h"

namespace gs {

physical::PhysicalPlan load_plan_from_resource(
    const std::string& resource_path) {
  physical::PhysicalPlan plan;
  // Load the plan from the resource file.
  std::ifstream plan_file(resource_path, std::ios::in | std::ios::binary);
  if (!plan_file.is_open()) {
    LOG(ERROR) << "Failed to open plan file: " << resource_path;
    return plan;
  }
  if (!plan.ParseFromIstream(&plan_file)) {
    LOG(ERROR) << "Failed to parse plan file: " << resource_path;
    return plan;
  }
  plan_file.close();
  LOG(INFO) << "Loaded plan from resource: " << resource_path
            << ", plan size: " << plan.ByteSizeLong();
  return plan;
}

const Schema& Connection::GetSchema() const {
  if (IsClosed()) {
    LOG(ERROR) << "Connection is closed, cannot get schema.";
    THROW_RUNTIME_ERROR("Connection is closed, cannot get schema.");
  }
  return graph_.schema();
}

void Connection::Close() {
  if (is_closed_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Connection is already closed.";
    return;
  }
  LOG(INFO) << "Closing connection.";
  is_closed_.store(true);
  // Any necessary cleanup can be done here.
}

Result<QueryResult> Connection::Query(const std::string& query_string) {
  LOG(INFO) << "Query: " << query_string;
  if (IsClosed()) {
    LOG(ERROR) << "Connection is closed, cannot execute query.";
    return Result<QueryResult>(
        Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed."));
  }
  auto result = query_impl(query_string);
  if (result.ok()) {
    return Result<QueryResult>(
        QueryResult::From(std::move(result.move_value())));
  } else {
    return result.status();
  }
}

Result<results::CollectiveResults> Connection::query_impl(
    const std::string& query_string) {
  LOG(INFO) << "Executing query: " << query_string;

  auto plan_res = planner_->compilePlan(query_string);
  if (!plan_res.ok()) {
    LOG(ERROR) << "Failed to compile plan for query: " << query_string
               << ", error code: " << plan_res.status().error_code()
               << ", message: " << plan_res.status().error_message();
    return plan_res.status();
  }

  const auto& physical_plan = plan_res.value().first;
  const auto& result_schema = plan_res.value().second;

  VLOG(10) << "Physical plan: " << physical_plan.DebugString();
  LOG(INFO) << "Got physical plan, " << physical_plan.query_plan().plan_size()
            << " operators.";

  auto result = query_processor_->execute(physical_plan);
  if (result.ok()) {
    LOG(INFO) << "Query executed successfully.";
  } else {
    LOG(ERROR) << "Error in executing query: " << query_string
               << ", error code: " << result.status().error_code()
               << ", message: " << result.status().error_message();
    return result.status();
  }
  result.value().set_result_schema(result_schema);
  // If the query contains operator that may mutable the graph schema or data,
  // we should update the schema and statistics for the planner.
  if (physical_plan.has_ddl_plan()) {
    auto yaml = graph_.schema().to_yaml();
    if (!yaml.ok()) {
      LOG(ERROR) << "Failed to convert schema to YAML: "
                 << yaml.status().error_message();
      THROW_RUNTIME_ERROR("Failed to convert schema to YAML: " +
                          yaml.status().error_message());
    }
    planner_->update_meta(yaml.value());
    planner_->update_statistics(graph_.get_statistics_json());
  } else if (physical_plan.query_plan().mode() ==
                 physical::QueryPlan::Mode::QueryPlan_Mode_READ_WRITE ||
             physical_plan.query_plan().mode() ==
                 physical::QueryPlan::Mode::QueryPlan_Mode_WRITE_ONLY) {
    planner_->update_statistics(graph_.get_statistics_json());
  }

  return Result(std::move(result.move_value()));
}

}  // namespace gs
