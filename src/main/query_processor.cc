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
#include "src/engines/graph_db/runtime/common/context.h"
#include "src/engines/graph_db/runtime/common/operators/retrieve/sink.h"
#include "src/engines/graph_db/runtime/execute/plan_parser.h"

namespace gs {
Result<results::CollectiveResults> QueryProcessor::execute(
    const physical::PhysicalPlan& plan, int32_t num_threads) {
  VLOG(10) << "Executing plan: " << plan.DebugString();
  // TODO: Currently we get the read transaction with the thread id 0. Ideally,
  // we should be able to run queries with multiple threads.
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
          status = gs::Status(gs::StatusCode::INTERNAL_ERROR,
                              "Error: " + std::to_string(err.error().value()) +
                                  ", Exception: " + err.exception()->what());
          return runtime::Context();
        },
        [&]() {
          status = gs::Status(gs::StatusCode::UNKNOWN, "Unknown error");
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
}  // namespace gs
