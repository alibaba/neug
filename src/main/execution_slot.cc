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

#include "neug/compiler/extension/extension_manager.h"
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/execution/execute/ops/admin/extension.h"
#include "neug/execution/utils/opr_timer.h"
#include "neug/generated/proto/response/response.pb.h"
#include "neug/main/checkpoint_coordinator.h"
#include "neug/main/query_request.h"
#include "neug/main/transaction_context.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/graph_stats.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/current_cow_write_transaction.h"
#include "neug/transaction/current_graph_write_guard.h"
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

Status executePreparedPipeline(execution::CacheValue& prepared_query,
                               ExplainMode explain_mode,
                               const execution::ParamsMap& parameters,
                               IStorageInterface& storage,
                               neug::QueryResponse& response) {
  response.mutable_schema()->CopyFrom(prepared_query.result_schema);

  if (explain_mode == ExplainMode::kExplain) {
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
  if (explain_mode == ExplainMode::kProfile) {
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

Status executeCheckpoint(ExplainMode explain_mode,
                         CheckpointCoordinator& checkpoint_coordinator,
                         UpdateTimestampLease timestamp_lease,
                         neug::QueryResponse& response) {
  execution::OprTimer checkpoint_timer;
  execution::TimerUnit checkpoint_timer_unit;
  const bool profile = explain_mode == ExplainMode::kProfile;
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
  if (analysis.checkpoint() != prepared_query.flags.checkpoint()) {
    return Status::InternalError(
        "Lightweight query analysis does not match the compiled plan.");
  }
  return Status::OK();
}

Status validateExplicitTransactionPlan(ExplainMode explain_mode,
                                       const physical::ExecutionFlag& flags,
                                       bool read_only) {
  if (explain_mode == ExplainMode::kExplain) {
    return Status::OK();
  }
  if (flags.procedure_call()) {
    return Status(StatusCode::ERR_NOT_SUPPORTED,
                  "Procedure calls are not supported in an explicit "
                  "transaction.");
  }
  if (read_only && !IsReadOnlyExecutionFlag(flags)) {
    return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                  "Write queries are not allowed in a read-only "
                  "transaction.");
  }
  if (!read_only && (flags.batch() || flags.copy_from() ||
                     flags.create_temp_table() || flags.checkpoint())) {
    return Status(StatusCode::ERR_NOT_SUPPORTED,
                  "Bulk, temporary schema, and maintenance operations are "
                  "not supported in an explicit transaction.");
  }
  return Status::OK();
}

}  // namespace

SnapshotReadTransaction ExecutionSlot::BeginSnapshotReadTransaction() const {
  return SnapshotReadTransaction(
      ReadSnapshotLease::Acquire(version_manager_, snapshot_store_));
}

MvccInsertTransaction ExecutionSlot::BeginMvccInsertTransaction() {
  uint32_t ts = version_manager_.acquire_insert_timestamp();
  SnapshotGuard guard(snapshot_store_);
  return MvccInsertTransaction(std::move(guard), alloc_, *wal_writer_,
                               version_manager_, ts);
}

SnapshotCowWriteTransaction ExecutionSlot::BeginSnapshotCowWriteTransaction() {
  UpdateTimestampLease timestamp_lease(version_manager_);
  auto [cow_graph, planning_generation] =
      snapshot_store_.CloneCurrentForUpdate();
  return SnapshotCowWriteTransaction(std::move(cow_graph), planning_generation,
                                     alloc_, *wal_writer_, snapshot_store_,
                                     std::move(timestamp_lease));
}

result<SnapshotCowWriteTransaction>
ExecutionSlot::TryBeginSnapshotCowWriteTransaction() {
  if (db_config_.mode == DBMode::READ_ONLY) {
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "Write transactions are not allowed on a read-only "
                        "database."));
  }
  auto timestamp_lease = UpdateTimestampLease::TryAcquire(version_manager_);
  if (!timestamp_lease) {
    RETURN_ERROR(Status(StatusCode::ERR_SERVICE_UNAVAILABLE,
                        "The TP update lease is unavailable."));
  }
  auto [cow_graph, planning_generation] =
      snapshot_store_.CloneCurrentForUpdate();
  return SnapshotCowWriteTransaction(std::move(cow_graph), planning_generation,
                                     alloc_, *wal_writer_, snapshot_store_,
                                     std::move(*timestamp_lease));
}

result<CurrentCowWriteTransaction>
ExecutionSlot::BeginCurrentCowWriteTransaction() {
  if (execution_strategy_ != QueryExecutionStrategy::kDirect) {
    RETURN_ERROR(Status(
        StatusCode::ERR_NOT_SUPPORTED,
        "Explicit Connection write transactions require embedded execution."));
  }
  if (db_config_.mode == DBMode::READ_ONLY) {
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "Write transactions are not allowed on a read-only "
                        "database."));
  }
  return CurrentCowWriteTransaction::Begin(
      CurrentGraphWriteGuard::Acquire(version_manager_, snapshot_store_),
      alloc_, snapshot_store_, *wal_writer_);
}

InPlaceCompactionTransaction
ExecutionSlot::BeginInPlaceCompactionTransaction() {
  timestamp_t ts = version_manager_.acquire_compact_timestamp();
  return InPlaceCompactionTransaction(snapshot_store_, *wal_writer_,
                                      version_manager_, ts);
}

result<std::shared_ptr<execution::CacheValue>> ExecutionSlot::prepareQuery(
    const GraphStats& stats, const std::string& query, int32_t num_threads,
    QueryCacheMode cache_mode) {
  if (num_threads == 0) {
    num_threads = db_config_.max_thread_num;
  }
  num_threads = std::min(num_threads, db_config_.max_thread_num);
  if (num_threads < 1) {
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "Number of threads must be greater than 0"));
  }

  if (cache_mode == QueryCacheMode::kBypassShared) {
    return pipeline_cache_.CompileUncached(stats, query);
  }
  return pipeline_cache_.Get(stats, query);
}

Status ExecutionSlot::validateAdminRequest(const AdminRequest& request,
                                           AccessMode access_mode) const {
  switch (request.type) {
  case AdminType::kCheckpoint:
    if (request.extension) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "CHECKPOINT admin request must not include extension "
                    "details");
    }
    break;
  case AdminType::kInstallExtension:
  case AdminType::kLoadExtension:
  case AdminType::kUninstallExtension:
    if (!request.extension) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Extension admin request is missing extension details");
    }
    break;
  default:
    return Status(StatusCode::ERR_NOT_SUPPORTED,
                  "Unsupported admin request type: " +
                      std::to_string(static_cast<uint8_t>(request.type)));
  }
  if (access_mode != AccessMode::kUpdate) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Admin requests only accept the default or update/u access "
                  "mode");
  }
  if (db_config_.mode == DBMode::READ_ONLY) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Admin requests are not allowed when the database is in "
                  "read-only mode");
  }
  return Status::OK();
}

Status ExecutionSlot::executeAdmin(const AdminRequest& request,
                                   ExplainMode explain_mode,
                                   QueryResponse& response) {
  if (request.type == AdminType::kCheckpoint) {
    return executeCheckpoint(explain_mode, checkpoint_coordinator_,
                             UpdateTimestampLease(version_manager_), response);
  }
  if (request.type != AdminType::kInstallExtension &&
      request.type != AdminType::kLoadExtension &&
      request.type != AdminType::kUninstallExtension) {
    return Status(StatusCode::ERR_NOT_SUPPORTED,
                  "Unsupported admin request type: " +
                      std::to_string(static_cast<uint8_t>(request.type)));
  }
  if (!request.extension) {
    return Status::InternalError(
        "Extension admin request is missing extension details");
  }
  const auto& info = *request.extension;
  execution::ops::checkDeprecatedExtension(info.name);
  if (request.type == AdminType::kInstallExtension) {
    RETURN_IF_NOT_OK(
        extension_manager_.InstallExtension(info.name, info.repository));
    response.set_row_count(0);
    return Status::OK();
  }
  if (request.type == AdminType::kUninstallExtension) {
    RETURN_IF_NOT_OK(extension_manager_.UninstallExtension(info.name));
    response.set_row_count(0);
    return Status::OK();
  }
  auto load_result = extension_manager_.LoadExtension(info.name);
  if (!load_result) {
    return load_result.error();
  }
  if (execution_strategy_ == QueryExecutionStrategy::kDirect) {
    auto txn = CurrentCowWriteTransaction::Begin(
        CurrentGraphWriteGuard::Acquire(version_manager_, snapshot_store_),
        alloc_, snapshot_store_, *wal_writer_);
    auto storage = txn.OpenStorage();
    auto activated = storage.ActivateIndexes();
    if (!activated) {
      txn.Abort();
      return activated.error();
    }
    RETURN_IF_NOT_OK(txn.Commit());
  } else {
    auto txn = BeginSnapshotCowWriteTransaction();
    auto storage = txn.OpenStorage();
    auto activated = storage.ActivateIndexes();
    if (!activated) {
      txn.Abort();
      return activated.error();
    }
    if (!txn.Commit()) {
      return Status(StatusCode::ERR_WAL_WRITE_FAIL,
                    "Failed to commit extension index activation");
    }
  }
  response.set_row_count(0);
  return Status::OK();
}

Status ExecutionSlot::validatePlan(AccessMode mode,
                                   const physical::ExecutionFlag& flags,
                                   bool is_explain) const {
  if (execution_strategy_ == QueryExecutionStrategy::kTransactional &&
      (flags.batch() || flags.copy_from() || flags.create_temp_table())) {
    return Status(StatusCode::ERR_NOT_SUPPORTED,
                  "COPY, batch, and temporary table operations are not "
                  "supported for TP service.");
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
  RETURN_STATUS_ERROR_IF_NOT_OK(executeAutoCommitQuery(
      query_string, requested_mode, parameters, num_threads, response));
  return QueryResult(std::move(response));
}

Status ExecutionSlot::executePreparedQuery(
    IStorageInterface& storage, const AnalyzedQuery& query,
    execution::CacheValue& prepared_query, QueryResponse& response) {
  RETURN_IF_NOT_OK(validateQueryAnalysis(query.analysis, prepared_query));
  RETURN_IF_NOT_OK(
      validatePlan(query.access_mode, prepared_query.flags,
                   query.analysis.explain_mode == ExplainMode::kExplain));
  auto parsed_parameters = execution::parseJsonParameters(
      prepared_query.params_type, query.parameters);
  if (NEUG_UNLIKELY(!parsed_parameters)) {
    return parsed_parameters.error();
  }
  return executePreparedPipeline(prepared_query, query.analysis.explain_mode,
                                 parsed_parameters.value(), storage, response);
}

result<QueryResult> ExecutionSlot::ExecuteQueryInTransaction(
    const std::string& query_string, AccessMode requested_mode,
    const rapidjson::Value& parameters, int32_t num_threads,
    TransactionContext& transaction_context) {
  CHECK(transaction_context.IsActive());
  try {
    const auto start = std::chrono::high_resolution_clock::now();
    const auto analysis = planner_->analyzeQuery(query_string);
    if (analysis.explain_mode != ExplainMode::kExplain &&
        analysis.is_copy_statement) {
      transaction_context.AbortAndMarkRollbackOnly();
      RETURN_ERROR(Status(StatusCode::ERR_NOT_SUPPORTED,
                          "COPY is not supported in an explicit transaction."));
    }
    const auto resolved_mode = requested_mode == AccessMode::kUnKnown
                                   ? analysis.access_mode
                                   : requested_mode;
    if (analysis.isAdmin() && analysis.explain_mode != ExplainMode::kExplain) {
      transaction_context.AbortAndMarkRollbackOnly();
      RETURN_ERROR(Status(StatusCode::ERR_NOT_SUPPORTED,
                          "Administrative operations are not supported in an "
                          "explicit transaction."));
    }
    if (transaction_context.IsReadOnly() &&
        analysis.explain_mode != ExplainMode::kExplain &&
        requested_mode != AccessMode::kUnKnown &&
        requested_mode != AccessMode::kRead) {
      transaction_context.AbortAndMarkRollbackOnly();
      RETURN_ERROR(Status(StatusCode::ERR_TX_STATE_CONFLICT,
                          "Write queries are not allowed in a read-only "
                          "transaction."));
    }

    const AnalyzedQuery query{query_string, analysis, resolved_mode, parameters,
                              num_threads};
    QueryResponse response;
    auto prepare_and_execute =
        [this, &query, &response](
            const GraphStats& stats, IStorageInterface& storage,
            QueryCacheMode cache_mode, bool read_only) -> Status {
      auto prepared =
          prepareQuery(stats, query.text, query.num_threads, cache_mode);
      if (NEUG_UNLIKELY(!prepared)) {
        return prepared.error();
      }
      auto prepared_query = std::move(prepared).value();
      RETURN_IF_NOT_OK(validateExplicitTransactionPlan(
          query.analysis.explain_mode, prepared_query->flags, read_only));
      return executePreparedQuery(storage, query, *prepared_query, response);
    };

    Status status;
    if (transaction_context.IsReadOnly()) {
      auto& transaction = transaction_context.ReadTransactionOwner();
      StorageReadInterface storage(transaction.view(), transaction.timestamp());
      status = prepare_and_execute(transaction.statistic(), storage,
                                   QueryCacheMode::kShared, true);
    } else {
      status = transaction_context.VisitCowWriteOwner(
          [&prepare_and_execute](auto& transaction) {
            auto storage = transaction.OpenStorage();
            const auto cache_mode = transaction.PlanningChanged()
                                        ? QueryCacheMode::kBypassShared
                                        : QueryCacheMode::kShared;
            return prepare_and_execute(transaction.statistic(), storage,
                                       cache_mode, false);
          });
    }
    if (!status.ok()) {
      transaction_context.AbortAndMarkRollbackOnly();
      RETURN_ERROR(status);
    }
    const auto end = std::chrono::high_resolution_clock::now();
    eval_duration_.fetch_add(
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count());
    ++query_num_;
    return QueryResult(std::move(response));
  } catch (...) {
    transaction_context.AbortAndMarkRollbackOnly();
    throw;
  }
}

Status ExecutionSlot::executeAutoCommitQuery(const std::string& query,
                                             AccessMode requested_mode,
                                             const rapidjson::Value& parameters,
                                             int32_t num_threads,
                                             QueryResponse& response) {
  const auto start = std::chrono::high_resolution_clock::now();
  const auto analysis = planner_->analyzeQuery(query);
  const auto access_mode = requested_mode == AccessMode::kUnKnown
                               ? analysis.access_mode
                               : requested_mode;
  const AnalyzedQuery analyzed_query{query, analysis, access_mode, parameters,
                                     num_threads};

  // EXPLAIN is non-mutating; skip Admin access-mode validation so it works on
  // read-only databases and with access_mode=read.
  if (NEUG_UNLIKELY(analysis.isAdmin() &&
                    analysis.explain_mode != ExplainMode::kExplain)) {
    RETURN_IF_NOT_OK(validateAdminRequest(*analysis.admin, access_mode));
  }

  auto execute_on_current_snapshot = [this, &analyzed_query,
                                      &response]() -> Status {
    auto lease = ReadSnapshotLease::Acquire(version_manager_, snapshot_store_);
    StorageReadInterface storage(lease.view(), lease.timestamp());
    auto prepared =
        prepareQuery(GraphStats(lease.view(), lease.planning_generation()),
                     analyzed_query.text, analyzed_query.num_threads);
    if (NEUG_UNLIKELY(!prepared)) {
      return prepared.error();
    }
    auto prepared_query = std::move(prepared).value();
    return executePreparedQuery(storage, analyzed_query, *prepared_query,
                                response);
  };

  Status status;
  if (NEUG_UNLIKELY(analysis.explain_mode == ExplainMode::kExplain)) {
    // EXPLAIN is strategy-independent and must not acquire a write transaction,
    // including for EXPLAIN CHECKPOINT.
    status = execute_on_current_snapshot();
  } else if (NEUG_UNLIKELY(analysis.isAdmin())) {
    if (NEUG_UNLIKELY(!parameters.IsObject())) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Query parameters must be a JSON object.");
    }
    status = executeAdmin(*analysis.admin, analysis.explain_mode, response);
  } else if (NEUG_UNLIKELY(execution_strategy_ ==
                           QueryExecutionStrategy::kDirect)) {
    if (access_mode == AccessMode::kRead) {
      status = execute_on_current_snapshot();
    } else if (access_mode == AccessMode::kInsert ||
               access_mode == AccessMode::kUpdate ||
               access_mode == AccessMode::kSchema) {
      auto guard =
          CurrentGraphWriteGuard::Acquire(version_manager_, snapshot_store_);
      // Classify under writer exclusion so the retained guard and prepared plan
      // both describe the private COW base used for execution.
      auto classification =
          prepareQuery(GraphStats(guard.Snapshot().view(),
                                  guard.Snapshot().planning_generation()),
                       query, num_threads);
      if (NEUG_UNLIKELY(!classification)) {
        return classification.error();
      }
      auto prepared_query = std::move(classification).value();
      const auto& flags = prepared_query->flags;
      if (flags.copy_from()) {
        auto transaction = CurrentCowWriteTransaction::Begin(
            std::move(guard), alloc_, snapshot_store_, *wal_writer_);
        auto storage = transaction.OpenBulkStorage();
        status = executePreparedQuery(storage, analyzed_query, *prepared_query,
                                      response);
        if (status.ok()) {
          // A normal COPY can target a table that is already temporary, so
          // planner syntax alone cannot select the commit protocol. Storage
          // marks the resolved target in the workspace.
          status = transaction.workspace_.HasTransientMutation()
                       ? transaction.CommitTransient()
                       : checkpoint_coordinator_.CommitCowWrite(transaction);
        } else {
          transaction.Abort();
        }
      } else {
        auto transaction = CurrentCowWriteTransaction::Begin(
            std::move(guard), alloc_, snapshot_store_, *wal_writer_);
        auto storage = transaction.OpenStorage();
        status = executePreparedQuery(storage, analyzed_query, *prepared_query,
                                      response);
        if (status.ok()) {
          status = transaction.workspace_.HasTransientMutation()
                       ? transaction.CommitTransient()
                       : transaction.Commit();
        } else {
          transaction.Abort();
        }
      }
    } else {
      return Status(
          StatusCode::ERR_NOT_SUPPORTED,
          "Access mode not supported in direct ExecutionSlot execution: " +
              std::to_string(static_cast<int>(access_mode)));
    }
  } else {
    auto execute_and_commit = [this, &analyzed_query, &response](
                                  auto& transaction, auto& storage) -> Status {
      auto prepared = prepareQuery(transaction.statistic(), analyzed_query.text,
                                   analyzed_query.num_threads);
      if (NEUG_UNLIKELY(!prepared)) {
        return prepared.error();
      }
      auto prepared_query = std::move(prepared).value();
      RETURN_IF_NOT_OK(executePreparedQuery(storage, analyzed_query,
                                            *prepared_query, response));
      if (NEUG_UNLIKELY(!transaction.Commit())) {
        return Status::InternalError("Transaction commit failed.");
      }
      return Status::OK();
    };

    if (access_mode == AccessMode::kRead) {
      auto transaction = BeginSnapshotReadTransaction();
      StorageReadInterface storage(transaction.view(), transaction.timestamp());
      status = execute_and_commit(transaction, storage);
    } else if (access_mode == AccessMode::kInsert) {
      auto transaction = BeginMvccInsertTransaction();
      StorageTPInsertInterface storage(transaction);
      status = execute_and_commit(transaction, storage);
    } else if (access_mode == AccessMode::kUpdate ||
               access_mode == AccessMode::kSchema) {
      auto transaction = BeginSnapshotCowWriteTransaction();
      auto storage = transaction.OpenStorage();
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
  RETURN_STATUS_ERROR_IF_NOT_OK(executeAutoCommitQuery(
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

  auto transaction = CurrentCowWriteTransaction::Begin(
      CurrentGraphWriteGuard::Acquire(version_manager_, snapshot_store_),
      alloc_, snapshot_store_, *wal_writer_);
  auto storage = transaction.OpenStorage();
  bool schema_changed = false;

  auto temporary_edges = transaction.schema().get_temporary_edge_triplet_keys();
  for (auto key : temporary_edges) {
    auto [src, dst, edge] = transaction.schema().parse_edge_label(key);
    try {
      const auto status = storage.DeleteEdgeType(src, dst, edge);
      if (status.ok()) {
        schema_changed = true;
      } else {
        LOG(WARNING) << "Failed to cleanup temp edge: " << status.ToString();
        transaction.Abort();
        return;
      }
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to cleanup temp edge: " << e.what();
      transaction.Abort();
      return;
    }
  }

  auto temporary_vertices = transaction.schema().get_temporary_vertex_labels();
  for (auto label : temporary_vertices) {
    try {
      const auto status = storage.DeleteVertexType(label);
      if (status.ok()) {
        schema_changed = true;
      } else {
        LOG(WARNING) << "Failed to cleanup temp vertex: " << status.ToString();
        transaction.Abort();
        return;
      }
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to cleanup temp vertex: " << e.what();
      transaction.Abort();
      return;
    }
  }

  if (!schema_changed) {
    transaction.Abort();
    return;
  }
  const auto status = transaction.CommitTransient();
  if (!status.ok()) {
    LOG(WARNING) << "Failed to publish temporary schema cleanup: "
                 << status.ToString();
  }
}

int ExecutionSlot::SlotId() const { return slot_id_; }

double ExecutionSlot::eval_duration() const {
  return static_cast<double>(eval_duration_.load()) / 1000000.0;
}

int64_t ExecutionSlot::query_num() const { return query_num_.load(); }

}  // namespace neug
