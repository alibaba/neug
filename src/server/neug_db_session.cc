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
#include "neug/execution/common/params_map.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/execution/utils/opr_timer.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/generated/proto/plan/stored_procedure.pb.h"
#include "neug/generated/proto/response/response.pb.h"
#include "neug/main/checkpoint_coordinator.h"
#include "neug/main/query_request.h"
#include "neug/main/query_result.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/graph_stats.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/update_transaction.h"
#include "neug/transaction/version_manager.h"
#include "neug/utils/access_mode.h"
#include "neug/utils/encoder.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/likely.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

// Ordering invariant for all transaction factories below: acquire the
// VersionManager lease before touching snapshot, allocator, or WAL state,
// because checkpoint maintenance may rotate that state while no lease is held.
neug::ReadTransaction NeugDBSession::GetReadTransaction() const {
  auto ts = version_manager_->acquire_read_timestamp();
  SnapshotGuard guard(snapshot_store_);
  return neug::ReadTransaction(std::move(guard), *version_manager_, ts);
}

neug::InsertTransaction NeugDBSession::GetInsertTransaction() {
  auto ts = version_manager_->acquire_insert_timestamp();
  SnapshotGuard guard(snapshot_store_);
  return neug::InsertTransaction(std::move(guard), alloc_, wal_writer_,
                                 *version_manager_, ts);
}

neug::UpdateTransaction NeugDBSession::GetUpdateTransaction() {
  return createUpdateTransaction(getUpdateTimestampGuard());
}

UpdateTimestampGuard NeugDBSession::getUpdateTimestampGuard() {
  return UpdateTimestampGuard::Acquire(*version_manager_);
}

UpdateTransaction NeugDBSession::createUpdateTransaction(
    UpdateTimestampGuard timestamp_guard) {
  auto cow_graph = snapshot_store_.CurrentSnapshot().Clone();
  return neug::UpdateTransaction(std::move(cow_graph), alloc_, wal_writer_,
                                 std::move(timestamp_guard), snapshot_store_,
                                 pipeline_cache_);
}

Status validate_flags(AccessMode mode, const physical::ExecutionFlag& flags,
                      const NeugDBConfig& db_config) {
  if (flags.checkpoint() && mode != AccessMode::kUpdate) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "CHECKPOINT only accepts the default or update/u access "
                  "mode");
  }
  if (db_config.mode == DBMode::READ_ONLY) {
    if (!IsReadOnlyExecutionFlag(flags) || mode != AccessMode::kRead) {
      return neug::Status(
          neug::StatusCode::ERR_INVALID_ARGUMENT,
          "Database is in read-only mode; write operations are not allowed.");
    }
  }
  if (mode == neug::AccessMode::kRead) {
    if (!IsReadOnlyExecutionFlag(flags)) {
      return neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                          "Read-only mode does not support write operations.");
    }
  } else if (mode == neug::AccessMode::kInsert) {
    if (!IsInsertOnlyExecutionFlag(flags)) {
      return neug::Status(
          neug::StatusCode::ERR_INVALID_ARGUMENT,
          "Insert-only mode does not support read or update operations.");
    }
  }
  if (flags.create_temp_table() || flags.batch()) {
    return Status(StatusCode::ERR_NOT_SUPPORTED,
                  "Temporary table creation and batch operations are not "
                  "supported for TP service.");
  }
  return Status::OK();
}

inline neug::result<execution::Context> ExecutePreparedPipeline(
    execution::CacheValue& cache_value, AccessMode mode,
    const NeugDBConfig& db_config, const rapidjson::Document& param_json_obj,
    IStorageInterface& storage_interface, neug::QueryResponse* response) {
  RETURN_STATUS_ERROR_IF_NOT_OK(
      validate_flags(mode, cache_value.flags, db_config));

  auto params_map =
      ParamsParser::ParseFromJsonObj(cache_value.params_type, param_json_obj);
  const auto explain_mode = cache_value.explain_mode;

  // ================ EXPLAIN MODE =================
  // Build the operator tree only; the query is not executed.
  if (explain_mode == physical::ExplainMode::EXPLAIN) {
    auto tree_result =
        cache_value.pipeline.explain_tree(storage_interface, params_map);
    if (!tree_result) {
      RETURN_ERROR(tree_result.error());
    }
    if (tree_result.value()) {
      *response->mutable_profile_result() =
          execution::OprTimer::ToProfileResult(tree_result.value().get());
    }
    response->set_row_count(0);
    // Empty context: no data rows are produced for EXPLAIN.
    return execution::Context();
  }

  // ================ NORMAL / PROFILE EXECUTION =================
  std::unique_ptr<execution::OprTimer> timer_ptr = nullptr;
  if (explain_mode == physical::ExplainMode::PROFILE) {
    timer_ptr = std::make_unique<execution::OprTimer>();
  }

  auto ctx_res = cache_value.pipeline.Execute(
      storage_interface, execution::Context(), params_map, timer_ptr.get());
  if (!ctx_res) {
    RETURN_ERROR(ctx_res.error());
  }

  if (explain_mode == physical::ExplainMode::PROFILE && timer_ptr) {
    *response->mutable_profile_result() =
        execution::OprTimer::ToProfileResult(timer_ptr.get());
  }
  return ctx_res;
}

template <typename Transaction>
inline Status CommitTransaction(Transaction& txn) {
  if (!txn.Commit()) {
    LOG(ERROR) << "transaction commit failed.";
    return Status::InternalError("Transaction commit failed.");
  }
  return Status::OK();
}

neug::result<std::string> NeugDBSession::Eval(const std::string& req) {
  const auto start = std::chrono::high_resolution_clock::now();

  // Concurrency boundary: SessionGuard prevents reuse of this session, but it
  // does not synchronize with checkpoint maintenance. Until Get*Transaction()
  // acquires a VersionManager lease, do not access snapshot_store_, alloc_,
  // wal_writer_, or other checkpoint-rotated state. Keep only
  // request-local parsing and access-mode analysis in this pre-lease region.
  std::string query;
  AccessMode mode = AccessMode::kUnKnown;
  rapidjson::Document param_json_obj;
  auto parse_res =
      RequestParser::ParseFromString(req, query, mode, param_json_obj);
  if (!parse_res.ok()) {
    RETURN_ERROR(parse_res);
  }
  if (mode == AccessMode::kUnKnown) {
    // Token-based analyzeMode still treats "call" as update. Read-only CALL is
    // only accepted on the read path when access_mode=read is set explicitly.
    mode = planner_->analyzeMode(query);
  }
  // Explicit access_mode=read: GPhysicalAnalyzer sets flag.read (not
  // procedure_call) when Function::isReadOnly is true, so validate_flags()
  // accepts those plans on the read path.

  google::protobuf::Arena arena;
  // Create a QueryResponse message on the arena to hold the results.
  neug::QueryResponse* response =
      google::protobuf::Arena::CreateMessage<neug::QueryResponse>(&arena);

  if (mode == neug::AccessMode::kRead) {
    auto read_txn = GetReadTransaction();
    neug::StorageReadInterface gri(read_txn.view(), read_txn.timestamp());
    GS_AUTO(cache_value, pipeline_cache_.Get(read_txn.statistic(), query));
    GS_AUTO(ctx, ExecutePreparedPipeline(*cache_value, mode, db_config_,
                                         param_json_obj, gri, response));
    response->mutable_schema()->CopyFrom(cache_value->result_schema);
    neug::execution::Sink::sink_results(ctx, gri, response);
    // StorageReadInterface and graph-backed result columns reference the
    // transaction's pinned GraphView. Keep the read lease until result
    // materialization has finished so checkpoint/compact cannot reopen the
    // graph underneath the sink.
    RETURN_STATUS_ERROR_IF_NOT_OK(CommitTransaction(read_txn));
  } else if (mode == AccessMode::kInsert) {
    auto insert_txn = GetInsertTransaction();
    neug::StorageTPInsertInterface gii(insert_txn);
    GS_AUTO(cache_value, pipeline_cache_.Get(insert_txn.statistic(), query));
    GS_AUTO(ctx, ExecutePreparedPipeline(*cache_value, mode, db_config_,
                                         param_json_obj, gii, response));
    (void) ctx;
    RETURN_STATUS_ERROR_IF_NOT_OK(CommitTransaction(insert_txn));
  } else if (mode == AccessMode::kUpdate ||
             mode == AccessMode::kSchema) {  // Update mode
    CHECK(planner_ != nullptr);
    // Peek the plan under a short-lived read lease. EXPLAIN only builds the
    // operator tree and never touches storage, so it is served without the
    // global update lease and without a COW clone.
    auto peek_txn = GetReadTransaction();
    GS_AUTO(peek_value, pipeline_cache_.Get(peek_txn.statistic(), query));

    if (peek_value->explain_mode == physical::ExplainMode::EXPLAIN) {
      neug::StorageReadInterface gri(peek_txn.view(), peek_txn.timestamp());
      GS_AUTO(ctx, ExecutePreparedPipeline(*peek_value, mode, db_config_,
                                           param_json_obj, gri, response));
      (void) ctx;
      response->mutable_schema()->CopyFrom(peek_value->result_schema);
      RETURN_STATUS_ERROR_IF_NOT_OK(CommitTransaction(peek_txn));
    } else {
      // Release the read lease before acquiring the update lease: another
      // updater's commit phase drains readers, and spinning on the update
      // state while holding a read timestamp would self-deadlock.
      RETURN_STATUS_ERROR_IF_NOT_OK(CommitTransaction(peek_txn));
      auto timestamp_guard = getUpdateTimestampGuard();
      // Re-fetch under the update lease: the snapshot may have rotated since
      // the peek, so the plan must match the snapshot the update clones.
      GS_AUTO(cache_value,
              pipeline_cache_.Get(GraphStats(snapshot_store_.CurrentSnapshot()),
                                  query));

      if (cache_value->flags.checkpoint()) {
        RETURN_STATUS_ERROR_IF_NOT_OK(
            validate_flags(mode, cache_value->flags, db_config_));

        const bool profile_checkpoint =
            cache_value->explain_mode == physical::ExplainMode::PROFILE;
        execution::OprTimer checkpoint_timer;
        execution::TimerUnit checkpoint_timer_unit;
        if (profile_checkpoint) {
          checkpoint_timer.set_name("Checkpoint");
          checkpoint_timer_unit.start();
        }

        auto checkpoint_status =
            checkpoint_coordinator_.ExecuteTpManual(std::move(timestamp_guard));
        if (!checkpoint_status.ok()) {
          RETURN_ERROR(checkpoint_status);
        }

        response->set_row_count(0);
        response->mutable_schema()->CopyFrom(cache_value->result_schema);
        if (profile_checkpoint) {
          checkpoint_timer.record(checkpoint_timer_unit);
          *response->mutable_profile_result() =
              execution::OprTimer::ToProfileResult(&checkpoint_timer);
        }
      } else {
        auto update_txn = createUpdateTransaction(std::move(timestamp_guard));
        neug::StorageTPUpdateInterface gui(update_txn);
        GS_AUTO(ctx, ExecutePreparedPipeline(*cache_value, mode, db_config_,
                                             param_json_obj, gui, response));
        response->mutable_schema()->CopyFrom(cache_value->result_schema);
        neug::execution::Sink::sink_results(ctx, gui, response);
        // StorageTPUpdateInterface references the transaction's COW graph and
        // GraphView, which are reset by Commit(). Materialize the response
        // first.
        RETURN_STATUS_ERROR_IF_NOT_OK(CommitTransaction(update_txn));
      }
    }
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Access mode not supported in NeugDBSession::Eval: " +
        std::to_string(static_cast<int>(mode)));
  }
  // Only update schema, statistics will not changed.

  const auto end = std::chrono::high_resolution_clock::now();
  eval_duration_.fetch_add(
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count());
  ++query_num_;

  return response->SerializeAsString();
}

int NeugDBSession::SessionId() const { return thread_id_; }

neug::CompactTransaction NeugDBSession::GetCompactTransaction() {
  auto ts = version_manager_->acquire_compact_timestamp();
  return neug::CompactTransaction(snapshot_store_, wal_writer_,
                                  *version_manager_, ts);
}

double NeugDBSession::eval_duration() const {
  return static_cast<double>(eval_duration_.load()) / 1000000.0;
}

int64_t NeugDBSession::query_num() const { return query_num_.load(); }

void NeugDBSession::InvalidateGlobalQueryCache() {
  pipeline_cache_.clearGlobalCache();
}

}  // namespace neug
