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

std::string Connection::GetSchema() const {
  if (IsClosed()) {
    LOG(ERROR) << "Connection is closed, cannot get schema.";
    THROW_RUNTIME_ERROR("Connection is closed, cannot get schema.");
  }
  auto yaml = graph_.schema().to_yaml();
  return gs::get_json_string_from_yaml(yaml.value()).value();
}

void Connection::Close() {
  if (is_closed_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Connection is already closed.";
    return;
  }
  LOG(INFO) << "Closing connection.";
  is_closed_.store(true);
  // Necessary cleanup can be done here.
}

result<QueryResult> Connection::Query(const std::string& query_string) {
  LOG(INFO) << "Query: " << query_string;
  if (IsClosed()) {
    LOG(ERROR) << "Connection is closed, cannot execute query.";
    RETURN_ERROR(
        Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed."));
  }
  auto result = query_impl(query_string);
  if (result) {
    return QueryResult::From(std::move(result.value()));
  } else {
    RETURN_ERROR(result.error());
  }
}

result<results::CollectiveResults> Connection::query_impl(
    const std::string& query_string) {
  LOG(INFO) << "Executing query: " << query_string;

  auto plan_res = planner_->compilePlan(query_string);
  if (!plan_res) {
    LOG(ERROR) << "Failed to compile plan for query: " << query_string
               << ", error code: " << plan_res.error().error_code()
               << ", message: " << plan_res.error().error_message();
    RETURN_ERROR(plan_res.error());
  }

  const auto& physical_plan = plan_res.value().first;
  const auto& result_schema = plan_res.value().second;

  VLOG(20) << "Physical plan: " << physical_plan.DebugString();
  LOG(INFO) << "Got physical plan, " << physical_plan.query_plan().plan_size()
            << " operators.";

  auto result = query_processor_->execute(physical_plan);
  if (result) {
    LOG(INFO) << "Query executed successfully.";
  } else {
    LOG(ERROR) << "Error in executing query: " << query_string
               << ", error code: " << result.error().error_code()
               << ", message: " << result.error().error_message();
    RETURN_ERROR(result.error());
  }
  result.value().set_result_schema(result_schema);
  // If the query contains operator that may mutable the graph schema or data,
  // we should update the schema and statistics for the planner.
  if (physical_plan.has_ddl_plan()) {
    auto yaml = graph_.schema().to_yaml();
    if (!yaml) {
      LOG(ERROR) << "Failed to convert schema to YAML: "
                 << yaml.error().error_message();
      THROW_RUNTIME_ERROR("Failed to convert schema to YAML: " +
                          yaml.error().error_message());
    }
    planner_->update_meta(yaml.value());
    planner_->update_statistics(graph_.get_statistics_json());
  } else if (physical_plan.query_plan().mode() ==
                 physical::QueryPlan::Mode::QueryPlan_Mode_READ_WRITE ||
             physical_plan.query_plan().mode() ==
                 physical::QueryPlan::Mode::QueryPlan_Mode_WRITE_ONLY) {
    planner_->update_statistics(graph_.get_statistics_json());
  }

  return result;
}

}  // namespace gs
