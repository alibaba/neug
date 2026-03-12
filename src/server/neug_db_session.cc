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
#include "neug/main/query_request.h"
#include "neug/main/query_result.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/update_transaction.h"
#include "neug/transaction/version_manager.h"
#include "neug/utils/access_mode.h"
#include "neug/utils/encoder.h"
#include "neug/utils/likely.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

neug::ReadTransaction NeugDBSession::GetReadTransaction() const {
  uint32_t ts = version_manager_->acquire_read_timestamp();
  return neug::ReadTransaction(graph_, *version_manager_, ts);
}

neug::InsertTransaction NeugDBSession::GetInsertTransaction() {
  uint32_t ts = version_manager_->acquire_insert_timestamp();
  return neug::InsertTransaction(graph_, alloc_, logger_, *version_manager_,
                                 ts);
}

neug::UpdateTransaction NeugDBSession::GetUpdateTransaction() {
  uint32_t ts = version_manager_->acquire_update_timestamp();
  return neug::UpdateTransaction(graph_, alloc_, logger_, *version_manager_,
                                 pipeline_cache_, ts);
}

const neug::PropertyGraph& NeugDBSession::graph() const { return graph_; }

neug::PropertyGraph& NeugDBSession::graph() { return graph_; }

const neug::Schema& NeugDBSession::schema() const { return graph_.schema(); }

inline bool is_read_only(const physical::ExecutionFlag flags) {
  return !(flags.insert() || flags.update() || flags.schema() ||
           flags.batch() || flags.create_temp_table() || flags.checkpoint() ||
           flags.procedure_call());
}

inline bool is_insert_only(const physical::ExecutionFlag flags) {
  return flags.insert() && !(flags.read() || flags.update() || flags.schema() ||
                             flags.batch() || flags.create_temp_table() ||
                             flags.checkpoint() || flags.procedure_call());
}

Status validate_flags(AccessMode mode, const physical::ExecutionFlag& flags) {
  if (mode == neug::AccessMode::kRead) {
    if (!is_read_only(flags)) {
      return neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                          "Read-only mode does not support write operations.");
    }
  } else if (mode == neug::AccessMode::kInsert) {
    if (!is_insert_only(flags)) {
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

template <typename Transaction>
inline neug::result<execution::Context> ExecutePipelineInTransaction(
    execution::LocalQueryCache& pipeline_cache, const Schema& schema,
    const std::string& query, AccessMode mode,
    const rapidjson::Document& param_json_obj, execution::OprTimer* timer,
    neug::MetaDatas& result_schema, Transaction& txn,
    IStorageInterface& storage_interface) {
  GS_AUTO(cache_value, pipeline_cache.Get(schema, query));
  assert(cache_value != nullptr);
  RETURN_STATUS_ERROR_IF_NOT_OK(validate_flags(mode, cache_value->flags));

  auto params_map =
      ParamsParser::ParseFromJsonObj(cache_value->params_type, param_json_obj);
  auto ctx_res = cache_value->pipeline.Execute(
      storage_interface, execution::Context(), params_map, timer);
  result_schema = cache_value->result_schema;
  if (!ctx_res) {
    txn.Abort();
    RETURN_ERROR(ctx_res.error());
  }
  if (!txn.Commit()) {
    LOG(ERROR) << "transaction commit failed.";
    RETURN_ERROR(neug::Status::InternalError("Transaction commit failed."));
  }
  return ctx_res;
}

neug::result<std::string> NeugDBSession::Eval(const std::string& req) {
  const auto start = std::chrono::high_resolution_clock::now();

  std::string query;
  AccessMode mode = AccessMode::kUnKnown;
  rapidjson::Document param_json_obj;
  auto parse_res =
      RequestParser::ParseFromString(req, query, mode, param_json_obj);
  if (!parse_res.ok()) {
    RETURN_ERROR(parse_res);
  }
  if (mode == AccessMode::kUnKnown) {
    mode = planner_->analyzeMode(query);
  }

  // Acquire different transaction on provided access_mode.;
  std::unique_ptr<neug::execution::OprTimer> timer = nullptr;
  google::protobuf::Arena arena;
  // Create a QueryResponse message on the arena to hold the results.
  neug::QueryResponse* response =
      google::protobuf::Arena::CreateMessage<neug::QueryResponse>(&arena);

  neug::MetaDatas result_schema;
  if (mode == neug::AccessMode::kRead) {
    auto read_txn = GetReadTransaction();
    neug::StorageReadInterface gri(read_txn.graph(), read_txn.timestamp());
    GS_AUTO(ctx, ExecutePipelineInTransaction(pipeline_cache_, schema(), query,
                                              mode, param_json_obj, timer.get(),
                                              result_schema, read_txn, gri));
    response->mutable_schema()->CopyFrom(result_schema);
    neug::execution::Sink::sink_results(ctx, gri, response);
  } else if (mode == AccessMode::kInsert) {
    auto insert_txn = GetInsertTransaction();
    neug::StorageTPInsertInterface gii(insert_txn);
    GS_AUTO(ctx, ExecutePipelineInTransaction(pipeline_cache_, schema(), query,
                                              mode, param_json_obj, timer.get(),
                                              result_schema, insert_txn, gii));
  } else if (mode == AccessMode::kUpdate ||
             mode == AccessMode::kSchema) {  // Update mode
    CHECK(planner_ != nullptr);
    auto update_txn = GetUpdateTransaction();
    neug::StorageTPUpdateInterface gui(update_txn);
    GS_AUTO(ctx, ExecutePipelineInTransaction(pipeline_cache_, schema(), query,
                                              mode, param_json_obj, timer.get(),
                                              result_schema, update_txn, gui));
    response->mutable_schema()->CopyFrom(result_schema);
    neug::execution::Sink::sink_results(ctx, gui, response);
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
  neug::timestamp_t ts = version_manager_->acquire_update_timestamp();
  return neug::CompactTransaction(graph_, logger_, *version_manager_,
                                  db_config_.compact_csr,
                                  db_config_.csr_reserve_ratio, ts);
}

double NeugDBSession::eval_duration() const {
  return static_cast<double>(eval_duration_.load()) / 1000000.0;
}

int64_t NeugDBSession::query_num() const { return query_num_.load(); }

}  // namespace neug