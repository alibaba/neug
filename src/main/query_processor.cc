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

namespace gs {

result<results::CollectiveResults> QueryProcessor::execute(
    const std::string& query_string, const std::string& access_mode,
    int32_t num_threads) {
  if (num_threads == 0) {
    num_threads = max_num_threads_;
  }
  if (num_threads > max_num_threads_) {
    num_threads = max_num_threads_;
  }
  LOG(INFO) << "Executing plan with " << num_threads
            << " threads, max_num_threads: " << max_num_threads_;
  if (num_threads < 1) {
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                            "Number of threads must be greater than 0"));
  }

  if (need_exclusive_lock(access_mode)) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    return execute_internal(query_string, num_threads);
  } else {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return execute_internal(query_string, num_threads);
  }
}

// The concurrency control is done outside this function.
result<results::CollectiveResults> QueryProcessor::execute_internal(
    const std::string& query_string, int32_t num_threads) {
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
    if (plan.has_ddl_plan() || plan.has_admin_plan()) {
      RETURN_ERROR(
          gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                     "DDL/Admin queries are not supported in read-only "
                     "mode"));
    }
    if (plan.has_query_plan()) {
      auto mode = plan.query_plan().mode();
      if (mode == physical::QueryPlan::WRITE_ONLY ||
          mode == physical::QueryPlan::READ_WRITE) {
        if (is_read_only_) {
          RETURN_ERROR(
              gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                         "Write queries are not supported in read-only mode"));
        }
      }
    }
  }

  result<results::CollectiveResults> exec_result;
  // TODO(zhanglei,lexiao): Implement corresponding operators for these DDL
  // operations
  if (plan.has_ddl_plan()) {
    exec_result = execute_ddl(plan.ddl_plan(), num_threads);
  } else if (plan.has_admin_plan()) {
    exec_result = execute_admin(plan.admin_plan(), num_threads);
  } else if (plan.has_query_plan()) {
    exec_result = execute_query(plan, num_threads);
  } else {
    LOG(WARNING) << "Empty physical plan for query: " << query_string;
  }
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

result<results::CollectiveResults> QueryProcessor::execute_admin(
    const physical::AdminPlan& admin_plan, int32_t num_threads) {
  // For admin plan, we always use update transaction.
  StorageAPUpdateInterface gui(g_, 0, allocator_);

  std::unique_ptr<runtime::OprTimer> timer = nullptr;
  auto ctx =
      runtime::ParseAndExecuteAdminPipeline(gui, admin_plan, timer.get());

  if (!ctx) {
    LOG(ERROR) << "Error: " << ctx.error().ToString();
    RETURN_ERROR(ctx.error());
  }
  return runtime::Sink::sink(ctx.value(), gui);
}

result<results::CollectiveResults> QueryProcessor::execute_query(
    const physical::PhysicalPlan& plan, int32_t num_threads) {
  std::unique_ptr<runtime::OprTimer> timer = nullptr;
  StorageAPUpdateInterface gii(g_, 0, allocator_);
  std::unique_ptr<runtime::OprTimer> timer_ptr = nullptr;
  auto ctx = runtime::ParseAndExecuteQueryPipeline(gii, plan, timer_ptr.get());

  if (!ctx) {
    LOG(ERROR) << "Error: " << ctx.error().ToString();
    RETURN_ERROR(ctx.error());
  }
  return runtime::Sink::sink(ctx.value(), gii);
}

result<results::CollectiveResults> QueryProcessor::execute_ddl(
    const physical::DDLPlan& ddl_plan, int32_t num_threads) {
  StorageAPUpdateInterface gii(g_, std::numeric_limits<timestamp_t>::max(),
                               allocator_);
  std::unique_ptr<runtime::OprTimer> timer = nullptr;
  auto ctx = runtime::ParseAndExecuteDDLPipeline(gii, ddl_plan, timer.get());
  if (!ctx) {
    LOG(ERROR) << "Error: " << ctx.error().ToString();
    RETURN_ERROR(ctx.error());
  }
  return runtime::Sink::sink(ctx.value(), gii);
}

bool QueryProcessor::need_exclusive_lock(const std::string& access_mode) {
  if (access_mode == "read" || access_mode == "READ" || access_mode == "r" ||
      access_mode == "R") {
    return false;
  }
  return true;  // For Insert and Update operations
}

void QueryProcessor::update_compiler_meta_if_needed(
    const physical::PhysicalPlan& plan) {
  if (plan.has_ddl_plan()) {
    auto yaml = g_.schema().to_yaml();
    if (!yaml) {
      LOG(ERROR) << "Failed to convert schema to YAML: "
                 << yaml.error().error_message();
      THROW_RUNTIME_ERROR("Failed to convert schema to YAML: " +
                          yaml.error().error_message());
    }
    planner_->update_meta(yaml.value());
    planner_->update_statistics(g_.get_statistics_json());
  } else if (plan.query_plan().mode() ==
                 physical::QueryPlan::Mode::QueryPlan_Mode_READ_WRITE ||
             plan.query_plan().mode() ==
                 physical::QueryPlan::Mode::QueryPlan_Mode_WRITE_ONLY) {
    planner_->update_statistics(g_.get_statistics_json());
  }
}

}  // namespace gs
