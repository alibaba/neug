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

#include "src/main/query_processor.h"
#include "src/engines/graph_db/app/cypher_update_app.h"
#include "src/engines/graph_db/runtime/common/context.h"
#include "src/engines/graph_db/runtime/common/operators/retrieve/sink.h"
#include "src/engines/graph_db/runtime/execute/plan_parser.h"
#include "src/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "src/utils/pb_utils.h"
#ifdef NEUG_BACKTRACE
#include <cpptrace/cpptrace.hpp>
#include <cpptrace/from_current.hpp>
#endif

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
#ifdef NEUG_BACKTRACE
    cpptrace::try_catch(
        [&] {  // try block
#endif
          return execute_ddl(plan.ddl_plan(), num_threads);
#ifdef NEUG_BACKTRACE
        },
        [&](const std::runtime_error& e) {
          std::cerr << "Runtime error: " << e.what() << std::endl;
          cpptrace::from_current_exception().print();
        },
        [&](const std::exception& e) {
          std::cerr << "Exception: " << e.what() << std::endl;
          cpptrace::from_current_exception().print();
        },
        [&]() {  // serves the same role as `catch(...)`, an any exception
                 // handler
          std::cerr << "Unknown exception occurred: " << std::endl;
          cpptrace::from_current_exception().print();
        });
#endif
  }

  if (!plan.has_query_plan()) {
    LOG(ERROR) << "No query plan found";
    return Result<results::CollectiveResults>(
        gs::Status(gs::StatusCode::OK, "Empty query plan"));
  }

  auto mode = plan.query_plan().mode();
  Result<results::CollectiveResults> res;
  if (mode == physical::QueryPlan::READ_ONLY) {
#ifdef NEUG_BACKTRACE
    cpptrace::try_catch(
        [&] {  // try block
#endif
          res = execute_read_only(plan, num_threads);
#ifdef NEUG_BACKTRACE
        },
        [&](const std::runtime_error& e) {
          std::cerr << "Runtime error: " << e.what() << std::endl;
          cpptrace::from_current_exception().print();
          return Status::RuntimeError("Runtime error: " +
                                      std::string(e.what()));
        },
        [&](const std::exception& e) {
          std::cerr << "Exception: " << e.what() << std::endl;
          cpptrace::from_current_exception().print();
          return Status::IntervalError("Exception: " + std::string(e.what()));
        },
        [&]() {  // serves the same role as `catch(...)`, an any exception
                 // handler
          std::cerr << "Unknown exception occurred: " << std::endl;
          cpptrace::from_current_exception().print();
          return Status::Unknown("Unknown exception occurred");
        });
#endif
  } else if (mode == physical::QueryPlan::READ_WRITE) {
    if (is_read_only_) {
      return Result<results::CollectiveResults>(
          gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                     "Read-write queries are not supported in read-only mode"));
    }
#ifdef NEUG_BACKTRACE
    cpptrace::try_catch(
        [&] {  // try block
#endif
          res = execute_read_write(plan, num_threads);
#ifdef NEUG_BACKTRACE
        },
        [&](const std::runtime_error& e) {
          std::cerr << "Runtime error: " << e.what() << std::endl;
          cpptrace::from_current_exception().print();
          return Status::RuntimeError("Runtime error: " +
                                      std::string(e.what()));
        },
        [&](const std::exception& e) {
          std::cerr << "Exception: " << e.what() << std::endl;
          cpptrace::from_current_exception().print();
          return Status::IntervalError("Exception: " + std::string(e.what()));
        },
        [&]() {  // serves the same role as `catch(...)`, an any exception
                 // handler
          std::cerr << "Unknown exception occurred: " << std::endl;
          cpptrace::from_current_exception().print();
          return Status::Unknown("Unknown exception occurred");
        });
#endif
  } else if (mode == physical::QueryPlan::WRITE_ONLY) {
    if (is_read_only_) {
      return Result<results::CollectiveResults>(
          gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                     "Write-only queries are not supported in read-only mode"));
    }
#ifdef NEUG_BACKTRACE
    cpptrace::try_catch(
        [&] {  // try block
#endif
          res = execute_read_write(plan, num_threads);
#ifdef NEUG_BACKTRACE
        },
        [&](const std::runtime_error& e) {
          std::cerr << "Runtime error: " << e.what() << std::endl;
          cpptrace::from_current_exception().print();
          return Status::RuntimeError("Runtime error: " +
                                      std::string(e.what()));
        },
        [&](const std::exception& e) {
          std::cerr << "Exception: " << e.what() << std::endl;
          cpptrace::from_current_exception().print();
          return Status::IntervalError("Exception: " + std::string(e.what()));
        },
        [&]() {  // serves the same role as `catch(...)`, an any exception
                 // handler
          std::cerr << "Unknown exception occurred: " << std::endl;
          cpptrace::from_current_exception().print();
          return Status::Unknown("Unknown exception occurred");
        });
#endif
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
  gs::Status status = gs::Status::OK();
  {
    ctx = bl::try_handle_all(
        [this, &gri, &plan]() -> bl::result<runtime::Context> {
          return runtime::PlanParser::get()
              .parse_read_pipeline(gri.schema(), gs::runtime::ContextMeta(),
                                   plan)
              .value()
              .Execute(gri, runtime::Context(), {}, timer_);
        },
        [&status](const gs::Status& err) {
          status = err;
          return runtime::Context();
        },
        [&](const bl::error_info& err) {
          status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR,
                              "Error: " + std::to_string(err.error().value()) +
                                  ", Exception: " + err.exception()->what());
          return runtime::Context();
        },
        [&]() {
          status = gs::Status(gs::StatusCode::ERR_UNKNOWN, "Unknown error");
          return runtime::Context();
        });
  }
  if (!status.ok()) {
    LOG(ERROR) << "Error: " << status.ToString();
    // We encode the error message to the output, so that the client can
    // get the error message.
    return Result<results::CollectiveResults>(status);
  }
  return Result<results::CollectiveResults>(Status::OK(),
                                            runtime::Sink::sink(ctx, txn));
}

Result<results::CollectiveResults> QueryProcessor::execute_read_write(
    const physical::PhysicalPlan& plan, int32_t num_threads) {
  return CypherUpdateApp::execute_update_query(db_.GetSession(0), plan, timer_);
}

Result<results::CollectiveResults> QueryProcessor::execute_ddl(
    const physical::DDLPlan& ddl_plan, int32_t num_threads) {
  return CypherUpdateApp::execute_ddl(db_.GetSession(0), ddl_plan);
}

}  // namespace gs
