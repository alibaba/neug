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

#include "neug/server/neug_db_session.h"

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>
#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <thread>
#include <tl/expected.hpp>
#include <tuple>
#include <vector>

#include "neug/config.h"
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/execution/utils/opr_timer.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/stored_procedure.pb.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/update_transaction.h"
#include "neug/transaction/version_manager.h"
#include "neug/utils/app_utils.h"
#include "neug/utils/likely.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace gs {

class Schema;

ReadTransaction NeugDBSession::GetReadTransaction() const {
  uint32_t ts = version_manager_->acquire_read_timestamp();
  return ReadTransaction(graph_, *version_manager_, ts);
}

InsertTransaction NeugDBSession::GetInsertTransaction() {
  uint32_t ts = version_manager_->acquire_insert_timestamp();
  return InsertTransaction(graph_, alloc_, logger_, *version_manager_, ts);
}

UpdateTransaction NeugDBSession::GetUpdateTransaction() {
  uint32_t ts = version_manager_->acquire_update_timestamp();
  return UpdateTransaction(graph_, alloc_, logger_, *version_manager_, ts);
}

const PropertyGraph& NeugDBSession::graph() const { return graph_; }

PropertyGraph& NeugDBSession::graph() { return graph_; }

const Schema& NeugDBSession::schema() const { return graph_.schema(); }

result<results::CollectiveResults> NeugDBSession::Eval(
    const physical::PhysicalPlan& plan, AccessMode access_mode) {
  const auto start = std::chrono::high_resolution_clock::now();
  // Acquire different transaction on provided access_mode.;
  std::unique_ptr<runtime::OprTimer> timer = nullptr;
  results::CollectiveResults ret;
  if (access_mode == AccessMode::kRead) {
    auto read_txn = GetReadTransaction();
    StorageReadInterface gri(read_txn.graph(), read_txn.timestamp());
    auto ctx = runtime::ParseAndExecuteQueryPipeline(gri, plan, timer.get());
    if (!ctx) {
      LOG(ERROR) << "Error: " << ctx.error().ToString();
      RETURN_ERROR(ctx.error());
    }
    if (!read_txn.Commit()) {
      LOG(ERROR) << "Read transaction commit failed.";
      RETURN_ERROR(Status::IntervalError("Read transaction commit failed."));
    }
    ret = runtime::Sink::sink(ctx.value(), gri);
  } else if (access_mode == AccessMode::kInsert) {
    auto insert_txn = GetInsertTransaction();
    StorageTPInsertInterface gii(insert_txn);
    auto ctx = runtime::ParseAndExecuteQueryPipeline(gii, plan, timer.get());
    if (!ctx) {
      LOG(ERROR) << "Error: " << ctx.error().ToString();
      RETURN_ERROR(ctx.error());
    }
    if (!insert_txn.Commit()) {
      LOG(ERROR) << "Insert transaction commit failed.";
      RETURN_ERROR(Status::IntervalError("Insert transaction commit failed."));
    }
    // TODO(zhanglei,lexiao): enable sink for insert interface
  } else if (access_mode == AccessMode::kUpdate) {  // Update mode
    auto update_txn = GetUpdateTransaction();
    StorageTPUpdateInterface gui(update_txn);
    gs::result<runtime::Context> ctx;
    if (plan.has_ddl_plan()) {  // UpdateSchema
      ctx = runtime::ParseAndExecuteDDLPipeline(gui, plan.ddl_plan(),
                                                timer.get());
    } else if (plan.has_admin_plan()) {  // UpdateAdmin
      ctx = runtime::ParseAndExecuteAdminPipeline(gui, plan.admin_plan(),
                                                  timer.get());
    } else {  // UpdateData
      ctx = runtime::ParseAndExecuteQueryPipeline(gui, plan, timer.get());
    }
    if (!ctx) {
      LOG(ERROR) << "Error: " << ctx.error().ToString();
      RETURN_ERROR(ctx.error());
    }
    if (!update_txn.Commit()) {
      LOG(ERROR) << "Update transaction commit failed.";
      RETURN_ERROR(Status::IntervalError("Update transaction commit failed."));
    }
    ret = runtime::Sink::sink(ctx.value(), gui);
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unsupported access mode for NeugDBSession::Eval" +
        std::to_string(static_cast<int>(access_mode)));
  }

  const auto end = std::chrono::high_resolution_clock::now();
  eval_duration_.fetch_add(
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count());
  ++query_num_;
  return ret;
}

int NeugDBSession::SessionId() const { return thread_id_; }

CompactTransaction NeugDBSession::GetCompactTransaction() {
  timestamp_t ts = version_manager_->acquire_update_timestamp();
  return CompactTransaction(graph_, logger_, *version_manager_,
                            db_config_.compact_csr,
                            db_config_.csr_reserve_ratio, ts);
}

double NeugDBSession::eval_duration() const {
  return static_cast<double>(eval_duration_.load()) / 1000000.0;
}

int64_t NeugDBSession::query_num() const { return query_num_.load(); }

}  // namespace gs
