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

#include "neug/main/execution_slot.h"

#include <glog/logging.h>
#include <google/protobuf/arena.h>

#include <algorithm>
#include <chrono>
#include <exception>
#include <memory>
#include <string>
#include <utility>

#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/execution/utils/opr_timer.h"
#include "neug/generated/proto/response/response.pb.h"
#include "neug/main/checkpoint_coordinator.h"
#include "neug/main/query_request.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/graph_stats.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/timestamp_lease.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/likely.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/yaml_utils.h"

namespace neug {

ExecutionSlotLease::~ExecutionSlotLease() { reset(); }

ExecutionSlotLease::ExecutionSlotLease(ExecutionSlotLease&& other) noexcept
    : slot_(other.slot_),
      owner_(other.owner_),
      slot_id_(other.slot_id_),
      releaser_(other.releaser_) {
  other.slot_ = nullptr;
  other.owner_ = nullptr;
  other.releaser_ = nullptr;
}

ExecutionSlotLease& ExecutionSlotLease::operator=(
    ExecutionSlotLease&& other) noexcept {
  if (this != &other) {
    reset();
    slot_ = other.slot_;
    owner_ = other.owner_;
    slot_id_ = other.slot_id_;
    releaser_ = other.releaser_;
    other.slot_ = nullptr;
    other.owner_ = nullptr;
    other.releaser_ = nullptr;
  }
  return *this;
}

void ExecutionSlotLease::reset() noexcept {
  if (slot_ != nullptr && releaser_ != nullptr) {
    releaser_(owner_, slot_id_);
  }
  slot_ = nullptr;
  owner_ = nullptr;
  releaser_ = nullptr;
}

namespace {

void markPlanningChangedIfNeeded(InPlaceWriteScope& write_scope,
                                 const execution::CacheValue* prepared_query,
                                 const Status& execution_status) {
  if (prepared_query == nullptr) {
    return;
  }
  if (!execution_status.ok() ||
      prepared_query->explain_mode == physical::ExplainMode::EXPLAIN) {
    return;
  }
  const auto& flags = prepared_query->flags;
  if (flags.batch() || flags.update()) {
    write_scope.MarkPlanningChanged();
  }
}

Status executePreparedQuery(execution::CacheValue& prepared_query,
                            const execution::ParamsMap& parameters,
                            IStorageInterface& storage,
                            neug::QueryResponse& response) {
  response.mutable_schema()->CopyFrom(prepared_query.result_schema);

  if (prepared_query.explain_mode == physical::ExplainMode::EXPLAIN) {
    auto tree_result =
        prepared_query.pipeline.explain_tree(storage, parameters);
    if (!tree_result) {
      return tree_result.error();
    }
    if (tree_result.value()) {
      *response.mutable_profile_result() =
          execution::OprTimer::ToProfileResult(tree_result.value().get());
    }
    response.set_row_count(0);
    return Status::OK();
  }

  std::unique_ptr<execution::OprTimer> timer;
  if (prepared_query.explain_mode == physical::ExplainMode::PROFILE) {
    timer = std::make_unique<execution::OprTimer>();
  }

  auto context = prepared_query.pipeline.Execute(storage, execution::Context(),
                                                 parameters, timer.get());
  if (!context) {
    return context.error();
  }

  if (storage.readable()) {
    auto* readable_storage = dynamic_cast<StorageReadInterface*>(&storage);
    CHECK(readable_storage != nullptr)
        << "Readable storage must implement StorageReadInterface";
    execution::Sink::sink_results(context.value(), *readable_storage,
                                  &response);
  }

  if (timer) {
    *response.mutable_profile_result() =
        execution::OprTimer::ToProfileResult(timer.get());
  }
  return Status::OK();
}

Status executeCheckpoint(physical::ExplainMode explain_mode,
                         CheckpointCoordinator& checkpoint_coordinator,
                         UpdateTimestampLease timestamp_lease,
                         neug::QueryResponse& response) {
  execution::OprTimer checkpoint_timer;
  execution::TimerUnit checkpoint_timer_unit;
  const bool profile = explain_mode == physical::ExplainMode::PROFILE;
  if (profile) {
    checkpoint_timer.set_name("Checkpoint");
    checkpoint_timer_unit.start();
  }

  RETURN_IF_NOT_OK(checkpoint_coordinator.PublishManualCheckpoint(
      std::move(timestamp_lease)));

  response.set_row_count(0);
  if (profile) {
    checkpoint_timer.record(checkpoint_timer_unit);
    *response.mutable_profile_result() =
        execution::OprTimer::ToProfileResult(&checkpoint_timer);
  }
  return Status::OK();
}

Status validateQueryAnalysis(const QueryAnalysis& analysis,
                             const execution::CacheValue& prepared_query) {
  if (analysis.explain_mode != prepared_query.explain_mode ||
      analysis.checkpoint() != prepared_query.flags.checkpoint()) {
    return Status::InternalError(
        "Lightweight query analysis does not match the compiled plan.");
  }
  return Status::OK();
}

}  // namespace

ReadTransaction ExecutionSlot::GetReadTransaction() const {
  return ReadTransaction(
      ReadSnapshotLease::Acquire(version_manager_, snapshot_store_));
}

InsertTransaction ExecutionSlot::GetInsertTransaction() {
  uint32_t ts = version_manager_.acquire_insert_timestamp();
  SnapshotGuard guard(snapshot_store_);
  return InsertTransaction(std::move(guard), alloc_, *wal_writer_,
                           version_manager_, ts);
}

UpdateTransaction ExecutionSlot::GetUpdateTransaction() {
  UpdateTimestampLease timestamp_lease(version_manager_);
  auto [cow_graph, planning_generation] =
      snapshot_store_.CloneCurrentForUpdate();
  return UpdateTransaction(std::move(cow_graph), planning_generation, alloc_,
                           *wal_writer_, snapshot_store_,
                           std::move(timestamp_lease));
}

CompactTransaction ExecutionSlot::GetCompactTransaction() {
  timestamp_t ts = version_manager_.acquire_compact_timestamp();
  return CompactTransaction(snapshot_store_, *wal_writer_, version_manager_,
                            ts);
}

result<std::shared_ptr<execution::CacheValue>> ExecutionSlot::prepareQuery(
    const GraphStats& stats, const std::string& query, int32_t num_threads) {
  if (num_threads == 0) {
    num_threads = db_config_.max_thread_num;
  }
  num_threads = std::min(num_threads, db_config_.max_thread_num);
  if (num_threads < 1) {
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "Number of threads must be greater than 0"));
  }

  GS_AUTO(cache_value, pipeline_cache_.Get(stats, query));
  return cache_value;
}

Status ExecutionSlot::validateCheckpointRequest(AccessMode access_mode) const {
  if (access_mode != AccessMode::kUpdate) {
    return Status(
        StatusCode::ERR_INVALID_ARGUMENT,
        "CHECKPOINT only accepts the default or update/u access mode");
  }
  if (db_config_.mode == DBMode::READ_ONLY) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Database is in read-only mode; write operations are not "
                  "allowed.");
  }
  return Status::OK();
}

Status ExecutionSlot::validatePlan(AccessMode mode,
                                   const physical::ExecutionFlag& flags,
                                   bool is_explain) const {
  if (execution_strategy_ == QueryExecutionStrategy::kTransactional &&
      (flags.batch() || flags.create_temp_table())) {
    return Status(
        StatusCode::ERR_NOT_SUPPORTED,
        "Temporary table creation and batch operations are not supported "
        "for TP service.");
  }
  // EXPLAIN never executes the plan; access-mode restrictions don't apply.
  if (is_explain) {
    return Status::OK();
  }
  if (execution_strategy_ == QueryExecutionStrategy::kTransactional &&
      mode == AccessMode::kInsert && !IsInsertOnlyExecutionFlag(flags)) {
    return Status(
        StatusCode::ERR_INVALID_ARGUMENT,
        "Insert-only mode does not support read or update operations.");
  }
  const bool database_read_only = db_config_.mode == DBMode::READ_ONLY;
  const bool plan_read_only = IsReadOnlyExecutionFlag(flags);
  if ((database_read_only && mode != AccessMode::kRead) ||
      ((database_read_only || mode == AccessMode::kRead) && !plan_read_only)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  database_read_only
                      ? "Database is in read-only mode; write operations are "
                        "not allowed."
                      : "Write queries are not supported in read-only mode");
  }
  // Index operators require the full update storage interface. Both execution
  // modes provide it only for kSchema and kUpdate statements.
  if (flags.index() && mode != AccessMode::kUpdate &&
      mode != AccessMode::kSchema) {
    return Status(StatusCode::ERR_NOT_SUPPORTED,
                  "Index operations are only supported in update or schema "
                  "mode.");
  }
  return Status::OK();
}

result<QueryResult> ExecutionSlot::ExecuteQuery(
    const std::string& query_string, const std::string& access_mode,
    const rapidjson::Value& parameters, int32_t num_threads) {
  if (execution_strategy_ != QueryExecutionStrategy::kDirect) {
    RETURN_ERROR(
        Status(StatusCode::ERR_NOT_SUPPORTED,
               "Direct query execution is only available in embedded mode."));
  }
  const auto requested_mode =
      access_mode.empty() ? AccessMode::kUnKnown : ParseAccessMode(access_mode);
  neug::QueryResponse response;
  RETURN_STATUS_ERROR_IF_NOT_OK(executeCore(query_string, requested_mode,
                                            parameters, num_threads, response));
  return QueryResult(std::move(response));
}

Status ExecutionSlot::executeCore(const std::string& query,
                                  AccessMode requested_mode,
                                  const rapidjson::Value& parameters,
                                  int32_t num_threads,
                                  QueryResponse& response) {
  const auto start = std::chrono::high_resolution_clock::now();
  const auto analysis = planner_->analyzeQuery(query);
  const auto access_mode = requested_mode == AccessMode::kUnKnown
                               ? analysis.access_mode
                               : requested_mode;
  std::shared_ptr<execution::CacheValue> prepared_query;

  // EXPLAIN CHECKPOINT is non-mutating; skip the checkpoint access-mode
  // validation so it works on read-only databases and with access_mode=read.
  if (NEUG_UNLIKELY(analysis.checkpoint() &&
                    analysis.explain_mode != physical::ExplainMode::EXPLAIN)) {
    RETURN_IF_NOT_OK(validateCheckpointRequest(access_mode));
  }

  auto execute_on_storage = [this, &query, access_mode, &analysis, &parameters,
                             num_threads, &response,
                             &prepared_query](const GraphStats& stats,
                                              auto& storage) -> Status {
    auto prepared = prepareQuery(stats, query, num_threads);
    if (NEUG_UNLIKELY(!prepared)) {
      return prepared.error();
    }
    prepared_query = std::move(prepared).value();

    RETURN_IF_NOT_OK(validateQueryAnalysis(analysis, *prepared_query));
    RETURN_IF_NOT_OK(
        validatePlan(access_mode, prepared_query->flags,
                     analysis.explain_mode == physical::ExplainMode::EXPLAIN));

    auto parsed_parameters =
        execution::parseJsonParameters(prepared_query->params_type, parameters);
    if (NEUG_UNLIKELY(!parsed_parameters)) {
      return parsed_parameters.error();
    }

    RETURN_IF_NOT_OK(executePreparedQuery(
        *prepared_query, parsed_parameters.value(), storage, response));
    return Status::OK();
  };

  Status status;
  // EXPLAIN is strategy-independent and must not acquire a write transaction,
  // including for EXPLAIN CHECKPOINT.
  if (NEUG_UNLIKELY(analysis.explain_mode == physical::ExplainMode::EXPLAIN)) {
    auto read_lease =
        ReadSnapshotLease::Acquire(version_manager_, snapshot_store_);
    StorageReadInterface storage(read_lease.view(), read_lease.timestamp());
    status = execute_on_storage(
        GraphStats(read_lease.view(), read_lease.planning_generation()),
        storage);
  } else if (NEUG_UNLIKELY(analysis.checkpoint())) {
    // PROFILE executes the checkpoint and is timed by executeCheckpoint().
    if (NEUG_UNLIKELY(!parameters.IsObject())) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Query parameters must be a JSON object.");
    }
    status =
        executeCheckpoint(analysis.explain_mode, checkpoint_coordinator_,
                          UpdateTimestampLease(version_manager_), response);
  } else if (NEUG_UNLIKELY(execution_strategy_ ==
                           QueryExecutionStrategy::kDirect)) {
    if (access_mode == AccessMode::kRead) {
      auto lease =
          ReadSnapshotLease::Acquire(version_manager_, snapshot_store_);
      StorageReadInterface storage(lease.view(), lease.timestamp());
      status = execute_on_storage(
          GraphStats(lease.view(), lease.planning_generation()), storage);
    } else if (access_mode == AccessMode::kInsert ||
               access_mode == AccessMode::kUpdate ||
               access_mode == AccessMode::kSchema) {
      InPlaceWriteScope write_scope(version_manager_, snapshot_store_);
      auto& slot = write_scope.Snapshot();
      StorageAPUpdateInterface storage(
          *slot.mutable_graph(), slot.mutable_view(), write_scope.Timestamp(),
          alloc_, [&write_scope]() { write_scope.MarkPlanningChanged(); });
      status = execute_on_storage(
          GraphStats(slot.view(), slot.planning_generation()), storage);
      markPlanningChangedIfNeeded(write_scope, prepared_query.get(), status);
    } else {
      return Status(
          StatusCode::ERR_NOT_SUPPORTED,
          "Access mode not supported in direct ExecutionSlot execution: " +
              std::to_string(static_cast<int>(access_mode)));
    }
  } else {
    auto execute_and_commit = [&execute_on_storage](auto& transaction,
                                                    auto& storage) -> Status {
      RETURN_IF_NOT_OK(execute_on_storage(transaction.statistic(), storage));
      if (NEUG_UNLIKELY(!transaction.Commit())) {
        return Status::InternalError("Transaction commit failed.");
      }
      return Status::OK();
    };

    if (access_mode == AccessMode::kRead) {
      auto transaction = GetReadTransaction();
      StorageReadInterface storage(transaction.view(), transaction.timestamp());
      status = execute_and_commit(transaction, storage);
    } else if (access_mode == AccessMode::kInsert) {
      auto transaction = GetInsertTransaction();
      StorageTPInsertInterface storage(transaction);
      status = execute_and_commit(transaction, storage);
    } else if (access_mode == AccessMode::kUpdate ||
               access_mode == AccessMode::kSchema) {
      auto transaction = GetUpdateTransaction();
      StorageTPUpdateInterface storage(transaction);
      status = execute_and_commit(transaction, storage);
    } else {
      return Status(StatusCode::ERR_NOT_SUPPORTED,
                    "Access mode not supported in transactional ExecutionSlot "
                    "execution: " +
                        std::to_string(static_cast<int>(access_mode)));
    }
  }

  if (NEUG_UNLIKELY(!status.ok())) {
    return status;
  }
  const auto end = std::chrono::high_resolution_clock::now();
  eval_duration_.fetch_add(
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count());
  ++query_num_;
  return Status::OK();
}

result<std::string> ExecutionSlot::ExecuteTransactionalRequest(
    const std::string& request) {
  std::string query;
  AccessMode requested_mode = AccessMode::kUnKnown;
  rapidjson::Document parameters_json;
  RETURN_STATUS_ERROR_IF_NOT_OK(RequestParser::ParseFromString(
      request, query, requested_mode, parameters_json));

  google::protobuf::Arena arena;
  auto* response =
      google::protobuf::Arena::CreateMessage<neug::QueryResponse>(&arena);
  RETURN_STATUS_ERROR_IF_NOT_OK(executeCore(
      query, requested_mode, parameters_json, /*num_threads=*/0, *response));
  return response->SerializeAsString();
}

std::string ExecutionSlot::GetSchema() const {
  auto lease = ReadSnapshotLease::Acquire(version_manager_, snapshot_store_);
  auto yaml = lease.view().schema().to_yaml();
  return get_json_string_from_yaml(yaml.value()).value();
}

void ExecutionSlot::ClearTemporarySchema() {
  CHECK(execution_strategy_ == QueryExecutionStrategy::kDirect);
  {
    auto lease = ReadSnapshotLease::Acquire(version_manager_, snapshot_store_);
    const auto& schema = lease.view().schema();
    if (schema.get_temporary_edge_triplet_keys().empty() &&
        schema.get_temporary_vertex_labels().empty()) {
      return;
    }
  }

  InPlaceWriteScope write_scope(version_manager_, snapshot_store_);
  auto& slot = write_scope.Snapshot();
  auto* graph = slot.mutable_graph();
  bool schema_changed = false;

  auto temporary_edges = graph->schema().get_temporary_edge_triplet_keys();
  for (auto key : temporary_edges) {
    auto [src, dst, edge] = graph->schema().parse_edge_label(key);
    try {
      const auto status = graph->DeleteEdgeType(src, dst, edge);
      if (status.ok()) {
        schema_changed = true;
        write_scope.MarkPlanningChanged();
      } else {
        LOG(WARNING) << "Failed to cleanup temp edge: " << status.ToString();
      }
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to cleanup temp edge: " << e.what();
    }
  }

  auto temporary_vertices = graph->schema().get_temporary_vertex_labels();
  for (auto label : temporary_vertices) {
    try {
      const auto status = graph->DeleteVertexType(label);
      if (status.ok()) {
        schema_changed = true;
        write_scope.MarkPlanningChanged();
      } else {
        LOG(WARNING) << "Failed to cleanup temp vertex: " << status.ToString();
      }
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to cleanup temp vertex: " << e.what();
    }
  }

  if (schema_changed) {
    slot.mutable_view().Rebuild(*graph);
  }
}

int ExecutionSlot::SlotId() const { return slot_id_; }

double ExecutionSlot::eval_duration() const {
  return static_cast<double>(eval_duration_.load()) / 1000000.0;
}

int64_t ExecutionSlot::query_num() const { return query_num_.load(); }

}  // namespace neug
