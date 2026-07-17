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
#include "neug/main/checkpoint_coordinator.h"
#include "neug/storages/graph/graph_stats.h"
#include "neug/storages/graph/property_graph.h"

namespace neug {

namespace {

// Validate JSON parameters against the declared parameter types and
// build a typed ParamsMap; unknown keys are rejected.
result<execution::ParamsMap> build_params_map(
    const rapidjson::Value& parameters,
    const execution::ParamsMetaMap& params_type) {
  execution::ParamsMap params_map;
  if (parameters.IsObject()) {
    for (const auto& member : parameters.GetObject()) {
      std::string key = member.name.GetString();
      auto iter = params_type.find(key);
      if (iter == params_type.end()) {
        RETURN_ERROR(neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                                  "Unexpected parameter: " + key));
      }
      params_map.emplace(key, Value::FromJson(member.value, iter->second));
    }
  }
  return params_map;
}

}  // namespace

result<std::shared_ptr<execution::CacheValue>>
QueryProcessor::check_and_retrieve_pipeline(const PropertyGraph& pg,
                                            const std::string& query_string,
                                            AccessMode access_mode,
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

  GraphStats stats(pg);
  GS_AUTO(cache_value, global_query_cache_->Get(stats, query_string));
  assert(cache_value);
  const auto& flags = cache_value->flags;
  if (flags.checkpoint() && access_mode != AccessMode::kUpdate) {
    RETURN_ERROR(neug::Status(
        neug::StatusCode::ERR_INVALID_ARGUMENT,
        "CHECKPOINT only accepts the default or update/u access mode"));
  }
  // Explicit access_mode=read accepts read-only CALL (no procedure_call flag).
  // Unspecified mode: token-based analyzeMode still classifies CALL as update.
  if ((is_read_only_ || access_mode == AccessMode::kRead) &&
      !IsReadOnlyExecutionFlag(flags)) {
    RETURN_ERROR(
        neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                     "Write queries are not supported in read-only mode"));
  }
  return cache_value;
}

result<QueryResult> QueryProcessor::execute(
    const std::string& query_string, const std::string& user_access_mode,
    const execution::ParamsMap& parameters, int32_t num_threads) {
  const auto access_mode = resolve_access_mode(query_string, user_access_mode);
  if (need_exclusive_lock(access_mode)) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    SnapshotGuard guard(snapshot_store_);
    GS_AUTO(cache_value, check_and_retrieve_pipeline(
                             *guard.get().mutable_graph(), query_string,
                             access_mode, num_threads));
    return execute_internal(guard, query_string, cache_value, access_mode,
                            parameters, num_threads);
  } else {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    SnapshotGuard guard(snapshot_store_);
    GS_AUTO(cache_value, check_and_retrieve_pipeline(
                             *guard.get().mutable_graph(), query_string,
                             access_mode, num_threads));
    return execute_internal(guard, query_string, cache_value, access_mode,
                            parameters, num_threads);
  }
}

result<QueryResult> QueryProcessor::execute(const std::string& query_string,
                                            const std::string& user_access_mode,
                                            const rapidjson::Value& parameters,
                                            int32_t num_threads) {
  const auto access_mode = resolve_access_mode(query_string, user_access_mode);
  if (need_exclusive_lock(access_mode)) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    SnapshotGuard guard(snapshot_store_);
    GS_AUTO(cache_value, check_and_retrieve_pipeline(
                             *guard.get().mutable_graph(), query_string,
                             access_mode, num_threads));
    GS_AUTO(params_map, build_params_map(parameters, cache_value->params_type));
    return execute_internal(guard, query_string, cache_value, access_mode,
                            params_map, num_threads);
  } else {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    SnapshotGuard guard(snapshot_store_);
    GS_AUTO(cache_value, check_and_retrieve_pipeline(
                             *guard.get().mutable_graph(), query_string,
                             access_mode, num_threads));
    GS_AUTO(params_map, build_params_map(parameters, cache_value->params_type));
    return execute_internal(guard, query_string, cache_value, access_mode,
                            params_map, num_threads);
  }
}

// The concurrency control is done outside this function.
result<QueryResult> QueryProcessor::execute_internal(
    SnapshotGuard& guard, const std::string& query_string,
    std::shared_ptr<execution::CacheValue> cache_value, AccessMode access_mode,
    const execution::ParamsMap& parameters, int32_t num_threads) {
  auto& slot = guard.get();
  auto& pg = *slot.mutable_graph();
  // Use the recovered WAL watermark as the visibility timestamp so data
  // replayed from WAL (stamped with its original timestamp) stays visible;
  // it returns to 0 together with the per-record timestamps after the next
  // checkpoint.
  StorageAPUpdateInterface graph(
      pg, slot.mutable_view(),
      checkpoint_coordinator_.recovered_wal_timestamp(), allocator_);

  google::protobuf::Arena arena;
  neug::QueryResponse* response =
      google::protobuf::Arena::CreateMessage<neug::QueryResponse>(&arena);

  auto explain_mode = cache_value->explain_mode;

  // ================ EXPLAIN MODE =================
  if (explain_mode == physical::ExplainMode::EXPLAIN) {
    return execute_explain_mode(query_string, cache_value, parameters, graph,
                                access_mode, pg, arena);
  }

  if (cache_value->flags.checkpoint()) {
    // The ordinary pin was needed for compilation. Release it before
    // checkpoint maintenance; the AP exclusive query lock remains held.
    guard.release();

    const bool profile_checkpoint =
        explain_mode == physical::ExplainMode::PROFILE;
    execution::OprTimer checkpoint_timer;
    execution::TimerUnit checkpoint_timer_unit;
    if (profile_checkpoint) {
      checkpoint_timer.set_name("Checkpoint");
      checkpoint_timer_unit.start();
    }

    auto status = checkpoint_coordinator_.ExecuteApManual();
    if (!status.ok()) {
      RETURN_ERROR(status);
    }
    global_query_cache_->clear();

    response->set_row_count(0);
    response->mutable_schema()->CopyFrom(cache_value->result_schema);
    if (profile_checkpoint) {
      checkpoint_timer.record(checkpoint_timer_unit);
      *response->mutable_profile_result() =
          execution::OprTimer::ToProfileResult(&checkpoint_timer);
    }
    return QueryResult::From(response->SerializeAsString());
  }

  // ================ NORMAL/PROFILE EXECUTION =================
  std::unique_ptr<execution::OprTimer> timer_ptr = nullptr;
  if (explain_mode == physical::ExplainMode::PROFILE) {
    timer_ptr = std::make_unique<execution::OprTimer>();
  }

  auto ctx_res = cache_value->pipeline.Execute(graph, execution::Context(),
                                               parameters, timer_ptr.get());
  if (!ctx_res) {
    LOG(ERROR) << "Error in executing query: " << query_string
               << ", error code: " << ctx_res.error().error_code()
               << ", message: " << ctx_res.error().error_message();
    RETURN_ERROR(ctx_res.error());
  }

  neug::execution::Sink::sink_results(ctx_res.value(), graph, response);
  response->mutable_schema()->CopyFrom(cache_value->result_schema);

  if (explain_mode == physical::ExplainMode::PROFILE) {
    if (timer_ptr) {
      auto profile_result =
          execution::OprTimer::ToProfileResult(timer_ptr.get());
      response->set_allocated_profile_result(
          google::protobuf::Arena::CreateMessage<neug::ProfileResult>(&arena));
      *response->mutable_profile_result() = profile_result;
    }
  }

  QueryResult ret = QueryResult::From(response->SerializeAsString());

  update_compiler_meta_if_needed(pg, cache_value->flags, access_mode);
  return ret;
}

bool QueryProcessor::need_exclusive_lock(AccessMode access_mode) {
  if (access_mode == AccessMode::kRead) {
    return false;
  }
  return true;  // For Insert and Update operations
}

AccessMode QueryProcessor::resolve_access_mode(
    const std::string& query_string,
    const std::string& user_access_mode) const {
  auto mode = user_access_mode.empty() ? AccessMode::kUnKnown
                                       : ParseAccessMode(user_access_mode);
  return mode == AccessMode::kUnKnown ? planner_->analyzeMode(query_string)
                                      : mode;
}

void QueryProcessor::update_compiler_meta_if_needed(
    const PropertyGraph& pg, const physical::ExecutionFlag& flags,
    AccessMode mode) {
  bool need_update = false;
  if (flags.schema() || flags.create_temp_table() ||
      mode == AccessMode::kSchema) {
    need_update = true;
  }
  if (flags.batch() || flags.insert() || flags.update()) {
    need_update = true;
  }
  if (need_update) {
    global_query_cache_->clear();
  }
}

void QueryProcessor::clear_cache() {
  SnapshotGuard guard(snapshot_store_);
  auto& pg = *guard.get().mutable_graph();
  physical::ExecutionFlag flags;
  flags.set_create_temp_table(true);
  update_compiler_meta_if_needed(pg, flags, AccessMode::kSchema);
}

result<QueryResult> QueryProcessor::execute_explain_mode(
    const std::string& query_string,
    std::shared_ptr<execution::CacheValue> cache_value,
    const execution::ParamsMap& parameters, IStorageInterface& graph,
    AccessMode access_mode, const PropertyGraph& pg,
    google::protobuf::Arena& arena) {
  // 1. build explain tree, does not execute the query
  auto tree_result = cache_value->pipeline.explain_tree(graph, parameters);

  if (!tree_result) {
    LOG(ERROR) << "Error in building explain tree for query: " << query_string
               << ", error: " << tree_result.error().error_message();
    RETURN_ERROR(tree_result.error());
  }

  neug::QueryResponse* response =
      google::protobuf::Arena::CreateMessage<neug::QueryResponse>(&arena);

  // 2. build ProfileResult from the explain tree
  if (tree_result.value()) {
    auto profile_result =
        execution::OprTimer::ToProfileResult(tree_result.value().get());
    response->set_allocated_profile_result(
        google::protobuf::Arena::CreateMessage<neug::ProfileResult>(&arena));
    *response->mutable_profile_result() = profile_result;
  }

  // 3. fill in schema (even if there are no data rows)
  response->set_row_count(0);
  response->mutable_schema()->CopyFrom(cache_value->result_schema);

  QueryResult ret = QueryResult::From(response->SerializeAsString());
  update_compiler_meta_if_needed(pg, cache_value->flags, access_mode);
  return ret;
}

}  // namespace neug
