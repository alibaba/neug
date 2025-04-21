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

#include "src/main/connection.h"

namespace gs {

Result<results::CollectiveResults> Connection::query(
    const std::string& query_string) {
  LOG(INFO) << "Executing query: " << query_string;
  auto plan = planner_->compilePlan(
      query_string, read_yaml_file_to_string(db_.get_schema_yaml_path()),
      db_.get_statistics_json());
  if (plan.error_code != "OK") {
    LOG(ERROR) << "Error in query: " << query_string
               << ", error code: " << plan.error_code
               << ", message: " << plan.full_message;
    return Result<results::CollectiveResults>(
        // TODO: Use the error code from the plan after plan.error_code is
        // defined as int.
        Status(StatusCode::COMPILATION_FAILURE, plan.full_message));
  }
  VLOG(10) << "Physical plan: " << plan.physical_plan.DebugString();
  LOG(INFO) << "Got physical plan, " << plan.physical_plan.plan_size()
            << " operators.";
  auto result = query_processor_->execute(plan.physical_plan);
  if (result.ok()) {
    LOG(INFO) << "Query executed successfully.";
  } else {
    LOG(ERROR) << "Error in executing query: " << query_string
               << ", error code: " << result.status().error_code()
               << ", message: " << result.status().error_message();
  }
  return result;
}

}  // namespace gs
