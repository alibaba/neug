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
#include "neug/main/query_request.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/graph_stats.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"
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

enum class LeaseKind {
  kRead,
  kUpdate,
};

class TimestampLease {
 public:
  TimestampLease(IVersionManager& version_manager, LeaseKind kind)
      : version_manager_(&version_manager), kind_(kind) {
    switch (kind_) {
    case LeaseKind::kRead:
      timestamp_ = version_manager_->acquire_read_timestamp();
      break;
    case LeaseKind::kUpdate:
      timestamp_ = version_manager_->acquire_update_timestamp();
      break;
    }
  }

  ~TimestampLease() { release(); }

  TimestampLease(const TimestampLease&) = delete;
  TimestampLease& operator=(const TimestampLease&) = delete;

  timestamp_t timestamp() const { return timestamp_; }

  void makeUpdateExclusive() {
    CHECK(kind_ == LeaseKind::kUpdate);
    version_manager_->begin_update_commit(timestamp_);
    version_manager_->drain_readers();
  }

 private:
  void release() {
    if (!active_) {
      return;
    }
    switch (kind_) {
    case LeaseKind::kRead:
      version_manager_->release_read_timestamp();
      break;
    case LeaseKind::kUpdate:
      version_manager_->release_update_timestamp(timestamp_);
      break;
    }
    active_ = false;
  }

  IVersionManager* version_manager_;
  LeaseKind kind_;
  timestamp_t timestamp_{INVALID_TIMESTAMP};
  bool active_{true};
};

bool invalidatesQueryCache(const physical::ExecutionFlag& flags) {
  return flags.schema() || flags.create_temp_table() || flags.batch() ||
         flags.insert() || flags.update();
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

}  // namespace

ReadTransaction ExecutionSlot::GetReadTransaction() const {
  uint32_t ts = version_manager_.acquire_read_timestamp();
  SnapshotGuard guard(snapshot_store_);
  return ReadTransaction(std::move(guard), version_manager_, ts);
}

InsertTransaction ExecutionSlot::GetInsertTransaction() {
  uint32_t ts = version_manager_.acquire_insert_timestamp();
  SnapshotGuard guard(snapshot_store_);
  return InsertTransaction(std::move(guard), alloc_, *wal_writer_,
                           version_manager_, ts);
}

UpdateTransaction ExecutionSlot::GetUpdateTransaction() {
  uint32_t ts = version_manager_.acquire_update_timestamp();
  auto cow_graph = snapshot_store_.CurrentSnapshot().Clone();
  return UpdateTransaction(std::move(cow_graph), alloc_, *wal_writer_,
                           version_manager_, snapshot_store_, pipeline_cache_,
                           ts);
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

Status ExecutionSlot::validatePlan(AccessMode mode,
                                   const physical::ExecutionFlag& flags) const {
  if (execution_strategy_ == QueryExecutionStrategy::kTransactional &&
      (flags.batch() || flags.create_temp_table())) {
    return Status(
        StatusCode::ERR_NOT_SUPPORTED,
        "Temporary table creation and batch operations are not supported "
        "for TP service.");
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
    return Status(
        StatusCode::ERR_INVALID_ARGUMENT,
        database_read_only
            ? "Database is in read-only mode; write operations are not allowed."
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
  auto status = executeCore(query_string, requested_mode, parameters,
                            num_threads, response);
  if (!status.ok()) {
    RETURN_ERROR(status);
  }
  return QueryResult(std::move(response));
}

Status ExecutionSlot::executeCore(const std::string& query,
                                  AccessMode requested_mode,
                                  const rapidjson::Value& parameters,
                                  int32_t num_threads,
                                  QueryResponse& response) {
  const auto start = std::chrono::high_resolution_clock::now();
  const auto access_mode = requested_mode == AccessMode::kUnKnown
                               ? planner_->analyzeMode(query)
                               : requested_mode;

  auto execute_on_storage = [this, &query, access_mode, &parameters,
                             num_threads, &response](const GraphStats& stats,
                                                     auto& storage) -> Status {
    auto prepared = prepareQuery(stats, query, num_threads);
    if (!prepared) {
      return prepared.error();
    }
    auto cache_value = std::move(prepared).value();
    auto status = validatePlan(access_mode, cache_value->flags);
    if (!status.ok()) {
      return status;
    }

    auto parsed_parameters =
        execution::parseJsonParameters(cache_value->params_type, parameters);
    if (!parsed_parameters) {
      return parsed_parameters.error();
    }

    status = executePreparedQuery(*cache_value, parsed_parameters.value(),
                                  storage, response);
    if (!status.ok()) {
      return status;
    }
    if (execution_strategy_ == QueryExecutionStrategy::kDirect &&
        invalidatesQueryCache(cache_value->flags)) {
      pipeline_cache_.clearGlobalCache();
    }
    return Status::OK();
  };

  Status status;
  if (execution_strategy_ == QueryExecutionStrategy::kDirect) {
    if (access_mode == AccessMode::kRead) {
      TimestampLease lease(version_manager_, LeaseKind::kRead);
      SnapshotGuard guard(snapshot_store_);
      StorageReadInterface storage(guard.get().view(), lease.timestamp());
      status =
          execute_on_storage(GraphStats(*guard.get().mutable_graph()), storage);
    } else if (access_mode == AccessMode::kInsert ||
               access_mode == AccessMode::kUpdate ||
               access_mode == AccessMode::kSchema) {
      TimestampLease lease(version_manager_, LeaseKind::kUpdate);
      lease.makeUpdateExclusive();
      SnapshotGuard guard(snapshot_store_);
      auto& slot = guard.get();
      StorageAPUpdateInterface storage(*slot.mutable_graph(),
                                       slot.mutable_view(), lease.timestamp(),
                                       alloc_);
      status = execute_on_storage(GraphStats(*slot.mutable_graph()), storage);
    } else {
      return Status(
          StatusCode::ERR_NOT_SUPPORTED,
          "Access mode not supported in direct ExecutionSlot execution: " +
              std::to_string(static_cast<int>(access_mode)));
    }
  } else {
    auto execute_and_commit = [&execute_on_storage](auto& transaction,
                                                    auto& storage) -> Status {
      auto transaction_status =
          execute_on_storage(transaction.statistic(), storage);
      if (!transaction_status.ok()) {
        return transaction_status;
      }
      if (!transaction.Commit()) {
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

  if (!status.ok()) {
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
  auto parse_result = RequestParser::ParseFromString(
      request, query, requested_mode, parameters_json);
  if (!parse_result.ok()) {
    RETURN_ERROR(parse_result);
  }

  google::protobuf::Arena arena;
  auto* response =
      google::protobuf::Arena::CreateMessage<neug::QueryResponse>(&arena);
  auto status = executeCore(query, requested_mode, parameters_json,
                            /*num_threads=*/0, *response);
  if (!status.ok()) {
    RETURN_ERROR(status);
  }
  return response->SerializeAsString();
}

std::string ExecutionSlot::GetSchema() const {
  TimestampLease lease(version_manager_, LeaseKind::kRead);
  SnapshotGuard guard(snapshot_store_);
  auto yaml = guard.get().mutable_graph()->schema().to_yaml();
  return get_json_string_from_yaml(yaml.value()).value();
}

void ExecutionSlot::ClearTemporarySchema() {
  CHECK(execution_strategy_ == QueryExecutionStrategy::kDirect);
  {
    TimestampLease lease(version_manager_, LeaseKind::kRead);
    SnapshotGuard guard(snapshot_store_);
    const auto& schema = guard.get().mutable_graph()->schema();
    if (schema.get_temporary_edge_triplet_keys().empty() &&
        schema.get_temporary_vertex_labels().empty()) {
      return;
    }
  }

  TimestampLease lease(version_manager_, LeaseKind::kUpdate);
  lease.makeUpdateExclusive();
  SnapshotGuard guard(snapshot_store_);
  auto& slot = guard.get();
  auto* graph = slot.mutable_graph();

  auto temporary_edges = graph->schema().get_temporary_edge_triplet_keys();
  for (auto key : temporary_edges) {
    auto [src, dst, edge] = graph->schema().parse_edge_label(key);
    try {
      graph->DeleteEdgeType(src, dst, edge);
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to cleanup temp edge: " << e.what();
    }
  }

  auto temporary_vertices = graph->schema().get_temporary_vertex_labels();
  for (auto label : temporary_vertices) {
    try {
      graph->DeleteVertexType(label);
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to cleanup temp vertex: " << e.what();
    }
  }

  if (!temporary_edges.empty() || !temporary_vertices.empty()) {
    slot.mutable_view().Rebuild(*graph);
    pipeline_cache_.clearGlobalCache();
  }
}

int ExecutionSlot::SlotId() const { return slot_id_; }

double ExecutionSlot::eval_duration() const {
  return static_cast<double>(eval_duration_.load()) / 1000000.0;
}

int64_t ExecutionSlot::query_num() const { return query_num_.load(); }

}  // namespace neug
