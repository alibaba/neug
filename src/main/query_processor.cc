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

#include <filesystem>

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/main/neug_db.h"
#include "neug/storages/checkpoint_session.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/pb_utils.h"

namespace neug {

namespace {

bool is_read_only(const physical::ExecutionFlag& flags) {
  return !(flags.insert() || flags.update() || flags.schema() ||
           flags.batch() || flags.create_temp_table() || flags.transaction() ||
           flags.checkpoint() || flags.procedure_call());
}

bool is_insert_only(const physical::ExecutionFlag& flags) {
  return flags.insert() &&
         !(flags.read() || flags.update() || flags.schema() || flags.batch() ||
           flags.create_temp_table() || flags.transaction() ||
           flags.checkpoint() || flags.procedure_call());
}

AccessMode resolve_access_mode(const std::shared_ptr<IGraphPlanner>& planner,
                               const std::string& query_string,
                               const std::string& user_access_mode,
                               const physical::ExecutionFlag& flags) {
  AccessMode requested_mode = AccessMode::kUnKnown;
  if (!user_access_mode.empty()) {
    requested_mode = ParseAccessMode(user_access_mode);
  }
  if (flags.checkpoint()) {
    if (requested_mode == AccessMode::kUnKnown ||
        requested_mode == AccessMode::kUpdate ||
        requested_mode == AccessMode::kCheckpoint) {
      return AccessMode::kCheckpoint;
    }
    return requested_mode;
  }
  if (requested_mode != AccessMode::kUnKnown) {
    return requested_mode;
  }
  return planner->analyzeMode(query_string);
}

Status validate_access_mode(AccessMode mode,
                            const physical::ExecutionFlag& flags,
                            bool is_read_only_db) {
  if (is_read_only_db && !is_read_only(flags)) {
    return neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Write queries are not supported in read-only mode");
  }
  if (flags.checkpoint()) {
    if (mode != AccessMode::kCheckpoint) {
      return neug::Status(
          neug::StatusCode::ERR_INVALID_ARGUMENT,
          "CHECKPOINT must be executed with checkpoint access mode.");
    }
    return Status::OK();
  }
  if (mode == AccessMode::kCheckpoint) {
    return neug::Status(
        neug::StatusCode::ERR_INVALID_ARGUMENT,
        "Checkpoint access mode only supports CHECKPOINT statements.");
  }
  if (mode == AccessMode::kRead && !is_read_only(flags)) {
    return neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                        "Read-only mode does not support write operations.");
  }
  if (mode == AccessMode::kInsert && !is_insert_only(flags)) {
    return neug::Status(
        neug::StatusCode::ERR_INVALID_ARGUMENT,
        "Insert-only mode does not support read or update operations.");
  }
  return Status::OK();
}

}  // namespace

result<std::pair<AccessMode, std::shared_ptr<execution::CacheValue>>>
QueryProcessor::check_and_retrieve_pipeline(const PropertyGraph& pg,
                                            const std::string& query_string,
                                            const std::string& user_access_mode,
                                            int32_t num_threads) {
  if (num_threads == 0) {
    num_threads = max_thread_num_;
  }
  if (num_threads > max_thread_num_) {
    num_threads = max_thread_num_;
  }
  if (num_threads < 1) {
    RETURN_ERROR(neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                              "Number of threads must be greater than 0"));
  }

  GS_AUTO(cache_value, global_query_cache_->Get(pg.schema(), query_string));
  assert(cache_value);
  const auto& flags = cache_value->flags;
  auto access_mode =
      resolve_access_mode(planner_, query_string, user_access_mode, flags);
  RETURN_STATUS_ERROR_IF_NOT_OK(
      validate_access_mode(access_mode, flags, is_read_only_));
  return std::make_pair(access_mode, cache_value);
}

result<QueryResult> QueryProcessor::execute(
    const std::string& query_string, const std::string& user_access_mode,
    const execution::ParamsMap& parameters, int32_t num_threads) {
  SnapshotGuard guard(snapshot_store_);
  GS_AUTO(access_mode_pipeline, check_and_retrieve_pipeline(
                                    *guard.get().mutable_graph(), query_string,
                                    user_access_mode, num_threads));
  if (need_exclusive_lock(access_mode_pipeline.first)) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (access_mode_pipeline.first == AccessMode::kCheckpoint) {
      // Re-pin after acquiring the exclusive lock. The guard created above
      // may have pinned a snapshot whose backing checkpoint files were
      // deleted by a prior checkpoint's CleanupPreviousCheckpoint.
      guard.release();
      SnapshotGuard checkpoint_guard(snapshot_store_);
      return execute_checkpoint(checkpoint_guard, query_string,
                                access_mode_pipeline.second, parameters,
                                num_threads);
    }
    return execute_internal(guard, query_string, access_mode_pipeline.second,
                            access_mode_pipeline.first, parameters,
                            num_threads);
  } else {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return execute_internal(guard, query_string, access_mode_pipeline.second,
                            access_mode_pipeline.first, parameters,
                            num_threads);
  }
}

result<QueryResult> QueryProcessor::execute(const std::string& query_string,
                                            const std::string& user_access_mode,
                                            const rapidjson::Value& parameters,
                                            int32_t num_threads) {
  SnapshotGuard guard(snapshot_store_);
  GS_AUTO(access_mode_pipeline, check_and_retrieve_pipeline(
                                    *guard.get().mutable_graph(), query_string,
                                    user_access_mode, num_threads));
  const auto& param_types = access_mode_pipeline.second->params_type;

  execution::ParamsMap params_map;
  if (parameters.IsObject()) {
    for (const auto& member : parameters.GetObject()) {
      std::string key = member.name.GetString();
      auto iter = param_types.find(key);
      if (iter == param_types.end()) {
        RETURN_ERROR(neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                                  "Unexpected parameter: " + key));
      }
      params_map.emplace(
          key, execution::Value::FromJson(member.value, iter->second));
    }
  }
  if (need_exclusive_lock(access_mode_pipeline.first)) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (access_mode_pipeline.first == AccessMode::kCheckpoint) {
      // Re-pin after acquiring the exclusive lock. The guard created above
      // may have pinned a snapshot whose backing checkpoint files were
      // deleted by a prior checkpoint's CleanupPreviousCheckpoint.
      guard.release();
      SnapshotGuard checkpoint_guard(snapshot_store_);
      return execute_checkpoint(checkpoint_guard, query_string,
                                access_mode_pipeline.second, params_map,
                                num_threads);
    }
    return execute_internal(guard, query_string, access_mode_pipeline.second,
                            access_mode_pipeline.first, params_map,
                            num_threads);
  } else {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return execute_internal(guard, query_string, access_mode_pipeline.second,
                            access_mode_pipeline.first, params_map,
                            num_threads);
  }
}

// The concurrency control is done outside this function.
result<QueryResult> QueryProcessor::execute_internal(
    SnapshotGuard& guard, const std::string& query_string,
    std::shared_ptr<execution::CacheValue> cache_value, AccessMode access_mode,
    const execution::ParamsMap& parameters, int32_t num_threads) {
  auto& slot = guard.get();
  auto& pg = *slot.mutable_graph();
  StorageAPUpdateInterface graph(pg, slot.mutable_view(), 0, allocator_);
  std::unique_ptr<execution::OprTimer> timer_ptr = nullptr;
  auto ctx_res = cache_value->pipeline.Execute(graph, execution::Context(),
                                               parameters, timer_ptr.get());
  if (!ctx_res) {
    LOG(ERROR) << "Error in executing query: " << query_string
               << ", error code: " << ctx_res.error().error_code()
               << ", message: " << ctx_res.error().error_message();
    RETURN_ERROR(ctx_res.error());
  }

  google::protobuf::Arena arena;
  neug::QueryResponse* response =
      google::protobuf::Arena::CreateMessage<neug::QueryResponse>(&arena);
  neug::execution::Sink::sink_results(ctx_res.value(), graph, response);
  response->mutable_schema()->CopyFrom(cache_value->result_schema);
  QueryResult ret = QueryResult::From(response->SerializeAsString());

  update_compiler_meta_if_needed(pg, cache_value->flags, access_mode);
  return ret;
}

result<QueryResult> QueryProcessor::execute_checkpoint(
    SnapshotGuard& guard, const std::string& query_string,
    std::shared_ptr<execution::CacheValue> cache_value,
    const execution::ParamsMap& parameters, int32_t num_threads) {
  try {
    (void) num_threads;
    auto checkpoint_graph = guard.get().mutable_graph()->Clone();
    GraphView checkpoint_view(*checkpoint_graph);
    auto checkpoint_session =
        CheckpointSession::Begin(checkpoint_mgr_, snapshot_store_,
                                 /*reopen=*/true, *checkpoint_graph);
    StorageAPUpdateInterface graph(*checkpoint_graph, checkpoint_view, 0,
                                   allocator_, &checkpoint_session);
    std::unique_ptr<execution::OprTimer> timer_ptr = nullptr;
    auto ctx_res = cache_value->pipeline.Execute(graph, execution::Context(),
                                                 parameters, timer_ptr.get());
    if (!ctx_res) {
      LOG(ERROR) << "Error in executing query: " << query_string
                 << ", error code: " << ctx_res.error().error_code()
                 << ", message: " << ctx_res.error().error_message();
      RETURN_ERROR(ctx_res.error());
    }

    guard.release();
    checkpoint_session.CommitAndCleanup();

    google::protobuf::Arena arena;
    neug::QueryResponse* response =
        google::protobuf::Arena::CreateMessage<neug::QueryResponse>(&arena);
    response->mutable_schema()->CopyFrom(cache_value->result_schema);
    return QueryResult::From(response->SerializeAsString());
  } catch (const exception::PermissionDeniedException& err) {
    LOG(ERROR) << "Error in executing query: " << query_string
               << ", message: " << err.what();
    RETURN_ERROR(Status(StatusCode::ERR_PERMISSION, err.what()));
  } catch (const exception::IOException& err) {
    LOG(ERROR) << "Error in executing query: " << query_string
               << ", message: " << err.what();
    RETURN_ERROR(Status(StatusCode::ERR_IO_ERROR, err.what()));
  } catch (const std::filesystem::filesystem_error& err) {
    LOG(ERROR) << "Error in executing query: " << query_string
               << ", message: " << err.what();
    RETURN_ERROR(Status(StatusCode::ERR_IO_ERROR, err.what()));
  } catch (const exception::Exception& err) {
    LOG(ERROR) << "Error in executing query: " << query_string
               << ", message: " << err.what();
    RETURN_ERROR(Status(StatusCode::ERR_INTERNAL_ERROR, err.what()));
  } catch (const std::exception& err) {
    LOG(ERROR) << "Error in executing query: " << query_string
               << ", message: " << err.what();
    RETURN_ERROR(Status(StatusCode::ERR_INTERNAL_ERROR, err.what()));
  }
}

bool QueryProcessor::need_exclusive_lock(AccessMode access_mode) {
  return access_mode != AccessMode::kRead;
}

void QueryProcessor::update_compiler_meta_if_needed(
    const PropertyGraph& pg, const physical::ExecutionFlag& flags,
    AccessMode mode) {
  YAML::Node schema_yaml;
  std::string statistics_json;
  bool need_update = false;
  if (flags.schema() || flags.create_temp_table() ||
      mode == AccessMode::kSchema) {
    schema_yaml = pg.schema().to_yaml().value();
    need_update = true;
  }
  if (flags.batch() || flags.insert() || flags.update()) {
    statistics_json = pg.get_statistics_json();
    need_update = true;
  }
  if (need_update) {
    global_query_cache_->clear(schema_yaml, statistics_json);
  }
}

void QueryProcessor::clear_cache() {
  SnapshotGuard guard(snapshot_store_);
  auto& pg = *guard.get().mutable_graph();
  physical::ExecutionFlag flags;
  flags.set_create_temp_table(true);
  update_compiler_meta_if_needed(pg, flags, AccessMode::kSchema);
}

}  // namespace neug
