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
#include "neug/engines/graph_db/app/cypher_update_app.h"
#include "neug/engines/graph_db/runtime/common/context.h"
#include "neug/engines/graph_db/runtime/common/operators/retrieve/sink.h"
#include "neug/engines/graph_db/runtime/execute/plan_parser.h"
#include "neug/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "neug/utils/pb_utils.h"

namespace gs {

Result<results::CollectiveResults> QueryProcessor::execute(
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
    return Result<results::CollectiveResults>(
        gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                   "Number of threads must be greater than 0"));
  }
  VLOG(10) << "Executing plan: " << plan.DebugString();
  // TODO: Currently we get the read transaction with the thread id 0. Ideally,
  // we should be able to run queries with multiple threads.
  if (plan.has_ddl_plan()) {
    if (is_read_only_) {
      return Result<results::CollectiveResults>(
          gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                     "DDL queries are not supported in read-only mode"));
    }
    return execute_ddl(plan.ddl_plan(), num_threads);
  }

  if (!plan.has_query_plan()) {
    LOG(ERROR) << "No query plan found";
    return Result<results::CollectiveResults>(
        gs::Status(gs::StatusCode::OK, "Empty query plan"));
  }

  auto mode = plan.query_plan().mode();
  Result<results::CollectiveResults> res;
  if (mode == physical::QueryPlan::READ_ONLY) {
    res = execute_read_only(plan, num_threads);
  } else if (mode == physical::QueryPlan::READ_WRITE) {
    if (is_read_only_) {
      return Result<results::CollectiveResults>(
          gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                     "Read-write queries are not supported in read-only mode"));
    }
    res = execute_read_write(plan, num_threads);

  } else if (mode == physical::QueryPlan::WRITE_ONLY) {
    if (is_read_only_) {
      return Result<results::CollectiveResults>(
          gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                     "Write-only queries are not supported in read-only mode"));
    }

    res = execute_read_write(plan, num_threads);
  } else {
    LOG(ERROR) << "Unknown query plan mode: " << mode;
    return Result<results::CollectiveResults>(gs::Status(
        gs::StatusCode::ERR_INVALID_ARGUMENT, "Unknown query plan mode"));
  }
  return res;
}

Result<results::CollectiveResults> QueryProcessor::execute_read_only(
    const physical::PhysicalPlan& plan, int32_t num_threads) {
  auto txn = db_.GetReadTransaction();
  runtime::GraphReadInterface gri(txn);

  runtime::Context ctx;
  Status status;
  std::unique_ptr<runtime::OprTimer> timer = nullptr;
  std::tie(ctx, status) =
      runtime::ParseAndExecuteReadPipeline(gri, plan, timer.get());

  if (!status.ok()) {
    LOG(ERROR) << "Error: " << status.ToString();
    // We encode the error message to the output, so that the client can
    // get the error message.
    return Result<results::CollectiveResults>(status);
  }
  return Result<results::CollectiveResults>(Status::OK(),
                                            runtime::Sink::sink(ctx, gri));
}

Result<results::CollectiveResults> QueryProcessor::execute_read_write(
    const physical::PhysicalPlan& plan, int32_t num_threads) {
  std::unique_ptr<runtime::OprTimer> timer = nullptr;
  return CypherUpdateApp::execute_update_query(db_.GetSession(0), plan,
                                               timer.get(), true);
}

Result<results::CollectiveResults> QueryProcessor::execute_ddl(
    const physical::DDLPlan& ddl_plan, int32_t num_threads) {
  return CypherUpdateApp::execute_ddl(db_.GetSession(0), ddl_plan);
}

}  // namespace gs
