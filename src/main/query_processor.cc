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

#include "neug/main/query_processor.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/main/neug_db.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/utils/pb_utils.h"

namespace neug {

result<results::CollectiveResults> QueryProcessor::execute(
    const std::string& query_string, const std::string& user_access_mode,
    const runtime::ParamsMap& parameters, int32_t num_threads) {
  if (num_threads == 0) {
    num_threads = max_num_threads_;
  }
  if (num_threads > max_num_threads_) {
    num_threads = max_num_threads_;
  }
  LOG(INFO) << "Executing plan with " << num_threads
            << " threads, max_num_threads: " << max_num_threads_;
  if (num_threads < 1) {
    RETURN_ERROR(neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                              "Number of threads must be greater than 0"));
  }

  AccessMode access_mode = user_access_mode.empty()
                               ? planner_->analyzeMode(query_string)
                               : ParseAccessMode(user_access_mode);

  if (need_exclusive_lock(access_mode)) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    return execute_internal(query_string, parameters, num_threads);
  } else {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return execute_internal(query_string, parameters, num_threads);
  }
}

// The concurrency control is done outside this function.
result<results::CollectiveResults> QueryProcessor::execute_internal(
    const std::string& query_string, const runtime::ParamsMap& parameters,
    int32_t num_threads) {
  auto plan_res = planner_->compilePlan(query_string);
  if (!plan_res) {
    LOG(ERROR) << "Failed to compile plan for query: " << query_string
               << ", error code: " << plan_res.error().error_code()
               << ", message: " << plan_res.error().error_message();
    RETURN_ERROR(plan_res.error());
  }

  const auto& plan = plan_res.value().first;
  const auto& result_schema = plan_res.value().second;
  VLOG(20) << "Physical plan: " << plan.DebugString();

  if (is_read_only_) {
    const auto& flags = plan.flag();
    if (flags.insert() || flags.update() || flags.schema() || flags.batch() ||
        flags.create_temp_table() || flags.checkpoint() ||
        flags.procedure_call()) {
      RETURN_ERROR(
          neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                       "Write queries are not supported in read-only mode"));
    }
  }

  // TODO(zhanglei,lexiao): Implement corresponding operators for these DDL
  // operations
  result<results::CollectiveResults> exec_result =
      execute_query(plan, parameters, num_threads);
  if (!exec_result) {
    LOG(ERROR) << "Error in executing query: " << query_string
               << ", error code: " << exec_result.error().error_code()
               << ", message: " << exec_result.error().error_message();
    return exec_result;
  }
  exec_result.value().set_result_schema(result_schema);
  update_compiler_meta_if_needed(plan);
  return exec_result;
}

result<results::CollectiveResults> QueryProcessor::execute_query(
    const physical::PhysicalPlan& plan, const runtime::ParamsMap& parameters,
    int32_t num_threads) {
  std::unique_ptr<runtime::OprTimer> timer = nullptr;
  StorageAPUpdateInterface gii(g_, 0, allocator_);
  std::unique_ptr<runtime::OprTimer> timer_ptr = nullptr;
  auto ctx = runtime::ParseAndExecuteQueryPipeline(gii, plan, parameters,
                                                   timer_ptr.get());

  if (!ctx) {
    LOG(ERROR) << "Error: " << ctx.error().ToString();
    RETURN_ERROR(ctx.error());
  }
  return runtime::Sink::sink(ctx.value(), gii);
}

bool QueryProcessor::need_exclusive_lock(AccessMode access_mode) {
  if (access_mode == AccessMode::kRead) {
    return false;
  }
  return true;  // For Insert and Update operations
}

void QueryProcessor::update_compiler_meta_if_needed(
    const physical::PhysicalPlan& plan) {
  const auto flags = plan.flag();
  if (flags.schema() || flags.create_temp_table()) {
    auto yaml = g_.schema().to_yaml();
    if (!yaml) {
      LOG(ERROR) << "Failed to convert schema to YAML: "
                 << yaml.error().error_message();
      THROW_RUNTIME_ERROR("Failed to convert schema to YAML: " +
                          yaml.error().error_message());
    }
    planner_->update_meta(yaml.value());
    planner_->update_statistics(g_.get_statistics_json());
  }
  if (flags.batch() || flags.insert() || flags.update()) {
    planner_->update_statistics(g_.get_statistics_json());
  }
}

}  // namespace neug
