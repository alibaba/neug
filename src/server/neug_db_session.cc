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
#include "neug/main/query_request.h"
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

inline bool is_read_only(const physical::ExecutionFlag flags) {
  // Implementation to determine if the flags indicate a non-read-only operation
  return flags.read() && !(flags.insert() || flags.update() || flags.schema() ||
                           flags.checkpoint() || flags.procedure_call());
}

inline bool is_insert_only(const physical::ExecutionFlag flags) {
  // Implementation to determine if the flags indicate a non-insert-only
  // operation
  return flags.insert() && !(flags.read() || flags.update() || flags.schema() ||
                             flags.checkpoint() || flags.procedure_call());
}

result<std::tuple<physical::PhysicalPlan, std::string, runtime::ParamsMap>>
compile_plan_and_check_consistency(std::shared_ptr<IGraphPlanner> planner,
                                   const std::string& query,
                                   const rapidjson::Document& param_json_obj,
                                   AccessMode mode) {
  GS_AUTO(plan_and_schema, planner->compilePlan(query));
  const auto& flags = plan_and_schema.first.flag();
  if (flags.batch() || flags.create_temp_table()) {
    RETURN_ERROR(neug::Status(
        neug::StatusCode::ERR_INVALID_ARGUMENT,
        "Batch or temporary table creation is not supported in TP service"));
  }
  if (mode == neug::AccessMode::kRead) {
    if (!is_read_only(flags)) {
      RETURN_ERROR(
          neug::Status(neug::StatusCode::ERR_INVALID_ARGUMENT,
                       "Read-only mode does not support write operations."));
    }
  } else if (mode == neug::AccessMode::kInsert) {
    if (!is_insert_only(flags)) {
      RETURN_ERROR(neug::Status(
          neug::StatusCode::ERR_INVALID_ARGUMENT,
          "Insert-only mode does not support read or update operations."));
    }
  }
  auto params_type =
      runtime::PlanParser::parse_params_type(plan_and_schema.first);
  auto params_map = ParamsParser::ParseFromJsonObj(params_type, param_json_obj);
  return std::make_tuple(plan_and_schema.first, plan_and_schema.second,
                         params_map);
}

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
                                 ts);
}

const neug::PropertyGraph& NeugDBSession::graph() const { return graph_; }

neug::PropertyGraph& NeugDBSession::graph() { return graph_; }

const neug::Schema& NeugDBSession::schema() const { return graph_.schema(); }

neug::result<results::CollectiveResults> NeugDBSession::Eval(
    const std::string& req) {
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
  std::unique_ptr<neug::runtime::OprTimer> timer = nullptr;
  results::CollectiveResults ret;
  std::tuple<physical::PhysicalPlan, std::string, runtime::ParamsMap>
      plan_schema_param;
  std::string stats;
  YAML::Node schema_yaml_node;
  if (mode == AccessMode::kRead) {
    auto read_txn = GetReadTransaction();
    StorageReadInterface gri(read_txn.graph(), read_txn.timestamp());
    GS_ASSIGN(plan_schema_param, compile_plan_and_check_consistency(
                                     planner_, query, param_json_obj, mode));
    GS_AUTO(ctx, runtime::ParseAndExecuteQueryPipeline(
                     gri, std::get<0>(plan_schema_param),
                     std::get<2>(plan_schema_param), timer.get()));
    if (!read_txn.Commit()) {
      LOG(ERROR) << "Read transaction commit failed.";
      RETURN_ERROR(
          neug::Status::IntervalError("Read transaction commit failed."));
    }
    ret = runtime::Sink::sink(ctx, gri);
  } else if (mode == AccessMode::kInsert) {
    auto insert_txn = GetInsertTransaction();
    StorageTPInsertInterface gii(insert_txn);
    GS_ASSIGN(plan_schema_param, compile_plan_and_check_consistency(
                                     planner_, query, param_json_obj, mode));
    GS_AUTO(ctx, runtime::ParseAndExecuteQueryPipeline(
                     gii, std::get<0>(plan_schema_param),
                     std::get<2>(plan_schema_param), timer.get()));
    // We don't update statistics for insert mode currently, the reason is
    // that Update statistics for each insert cause massive performance
    // overhead.
    if (!insert_txn.Commit()) {
      LOG(ERROR) << "Insert transaction commit failed.";
      RETURN_ERROR(
          neug::Status::IntervalError("Insert transaction commit failed."));
    }
    // TODO(zhanglei,lexiao): enable sink for insert interface
  } else if (mode == AccessMode::kUpdate ||
             mode == AccessMode::kSchema) {  // Update mode
    auto update_txn = GetUpdateTransaction();
    neug::StorageTPUpdateInterface gui(update_txn);
    CHECK(planner_ != nullptr);
    GS_ASSIGN(plan_schema_param, compile_plan_and_check_consistency(
                                     planner_, query, param_json_obj, mode));
    // update_planner_meta() and update_planner_stats() are called inside the
    // transaction scope to ensure concurrency safety.
    GS_AUTO(ctx, runtime::ParseAndExecuteQueryPipeline(
                     gui, std::get<0>(plan_schema_param),
                     std::get<2>(plan_schema_param), timer.get()));
    if (!update_txn.Commit()) {
      LOG(ERROR) << "Update transaction commit failed.";
      RETURN_ERROR(
          neug::Status::IntervalError("Update transaction commit failed."));
    }
    ret = neug::runtime::Sink::sink(ctx, gui);
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Access mode not supported in NeugDBSession::Eval: " +
        std::to_string(static_cast<int>(mode)));
  }
  // Update planner meta and statistics after successful transaction commit.
  // Concurrency control is done inside planner.
  const auto& flags = std::get<0>(plan_schema_param).flag();
  if (flags.schema() || flags.procedure_call()) {  // create_temp_table should
                                                   // be rejected earlier
    schema_yaml_node = graph_.schema().to_yaml().value_or(YAML::Node());
  }
  if (!schema_yaml_node.IsNull()) {
    planner_->update_meta(schema_yaml_node);
  }
  // Only update schema, statistics will not changed.
  ret.set_result_schema(std::get<1>(plan_schema_param));

  const auto end = std::chrono::high_resolution_clock::now();
  eval_duration_.fetch_add(
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count());
  ++query_num_;
  return ret;
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
