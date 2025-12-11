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
#include "neug/main/app/cypher_update_app.h"
#include "neug/main/neug_db.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/utils/pb_utils.h"

namespace gs {

result<results::CollectiveResults> QueryProcessor::execute(
    const physical::PhysicalPlan& plan, int32_t num_threads) {
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
  VLOG(20) << "Executing plan: " << plan.DebugString();
  // TODO(zhanglei): Currently we get the read transaction with the thread id 0.
  // Ideally, we should be able to run queries with multiple threads.
  if (plan.has_ddl_plan()) {
    if (is_read_only_) {
      RETURN_ERROR(
          gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                     "DDL queries are not supported in read-only mode"));
    }
    return execute_ddl(plan.ddl_plan(), num_threads);
  }

  if (plan.has_admin_plan()) {
    return execute_admin(plan.admin_plan(), num_threads);
  }

  if (!plan.has_query_plan()) {
    LOG(ERROR) << "No query plan found";
    RETURN_ERROR(gs::Status(gs::StatusCode::OK, "Empty query plan"));
  }

  auto mode = plan.query_plan().mode();
  result<results::CollectiveResults> res;
  if (mode == physical::QueryPlan::READ_ONLY) {
    res = execute_read_only(plan, num_threads);
  } else if (mode == physical::QueryPlan::READ_WRITE) {
    if (is_read_only_) {
      RETURN_ERROR(
          gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                     "Read-write queries are not supported in read-only mode"));
    }
    res = execute_read_write(plan, num_threads);

  } else if (mode == physical::QueryPlan::WRITE_ONLY) {
    if (is_read_only_) {
      RETURN_ERROR(
          gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                     "Write-only queries are not supported in read-only mode"));
    }

    res = execute_read_write(plan, num_threads);
  } else {
    LOG(ERROR) << "Unknown query plan mode: " << mode;
    RETURN_ERROR(gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                            "Unknown query plan mode"));
  }
  return res;
}

result<results::CollectiveResults> QueryProcessor::execute_admin(
    const physical::AdminPlan& admin_plan, int32_t num_threads) {
  // For admin plan, we always use update transaction.
  StorageAPUpdateInterface gui(db_.graph(), 0, allocator_);

  std::unique_ptr<runtime::OprTimer> timer = nullptr;
  auto ctx =
      runtime::ParseAndExecuteAdminPipeline(gui, admin_plan, timer.get());

  if (!ctx) {
    LOG(ERROR) << "Error: " << ctx.error().ToString();
    RETURN_ERROR(ctx.error());
  }
  return runtime::Sink::sink(ctx.value(), gui);
}

result<results::CollectiveResults> QueryProcessor::execute_read_only(
    const physical::PhysicalPlan& plan, int32_t num_threads) {
  StorageReadInterface gri(db_.graph(), MAX_TIMESTAMP);

  std::unique_ptr<runtime::OprTimer> timer = nullptr;
  auto ctx = runtime::ParseAndExecuteQueryPipeline(gri, plan, timer.get());

  if (!ctx) {
    LOG(ERROR) << "Error: " << ctx.error().ToString();
    RETURN_ERROR(ctx.error());
  }
  return runtime::Sink::sink(ctx.value(), gri);
}

result<results::CollectiveResults> QueryProcessor::execute_read_write(
    const physical::PhysicalPlan& plan, int32_t num_threads) {
  std::unique_ptr<runtime::OprTimer> timer = nullptr;
  StorageAPUpdateInterface gii(db_.graph(), 0, allocator_);
  return CypherUpdateApp::execute_update_query(gii, plan, timer.get());
}

result<results::CollectiveResults> QueryProcessor::execute_ddl(
    const physical::DDLPlan& ddl_plan, int32_t num_threads) {
  StorageAPUpdateInterface gii(db_.graph(), 0, allocator_);
  return CypherUpdateApp::execute_ddl(gii, ddl_plan);
}

}  // namespace gs
