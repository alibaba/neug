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
#include "src/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "src/utils/pb_utils.h"

namespace gs {

// Convert to a bool representing error_on_conflict.
bool conflict_action_to_bool(const physical::ConflictAction& action) {
  if (action == physical::ConflictAction::ON_CONFLICT_THROW) {
    return true;
  } else if (action == physical::ConflictAction::ON_CONFLICT_DO_NOTHING) {
    return false;
  } else {
    LOG(FATAL) << "invalid action: " << action;
  }
}

Result<results::CollectiveResults> QueryProcessor::execute(
    const physical::PhysicalPlan& plan, int32_t num_threads) {
  if (num_threads == 0) {
    num_threads = max_num_threads_;
  }
  if (num_threads > max_num_threads_) {
    num_threads = max_num_threads_;
  }
  LOG(INFO) << "Executing plan with " << num_threads
            << " threads, max_num_threads: " << max_num_threads_;
  if (num_threads < 1) {
    return Result<results::CollectiveResults>(
        gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                   "Number of threads must be greater than 0"));
  }
  VLOG(10) << "Executing plan: " << plan.DebugString();
  // TODO: Currently we get the read transaction with the thread id 0. Ideally,
  // we should be able to run queries with multiple threads.
  if (plan.has_ddl_plan()) {
    if (is_read_only_) {
      return Result<results::CollectiveResults>(
          gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                     "DDL queries are not supported in read-only mode"));
    }
    return execute_ddl(plan.ddl_plan(), num_threads);
  }

  if (!plan.has_query_plan()) {
    LOG(ERROR) << "No query plan found";
    return Result<results::CollectiveResults>(
        gs::Status(gs::StatusCode::OK, "Empty query plan"));
  }

  auto mode = plan.query_plan().mode();
  if (mode == physical::QueryPlan::READ_ONLY) {
    return execute_read_only(plan, num_threads);
  } else if (mode == physical::QueryPlan::READ_WRITE) {
    if (is_read_only_) {
      return Result<results::CollectiveResults>(
          gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                     "Read-write queries are not supported in read-only mode"));
    }
    return execute_read_write(plan, num_threads);
  } else if (mode == physical::QueryPlan::WRITE_ONLY) {
    if (is_read_only_) {
      return Result<results::CollectiveResults>(
          gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                     "Write-only queries are not supported in read-only mode"));
    }
    return execute_write_only(plan, num_threads);
  } else {
    LOG(ERROR) << "Unknown query plan mode: " << mode;
    return Result<results::CollectiveResults>(gs::Status(
        gs::StatusCode::ERR_INVALID_ARGUMENT, "Unknown query plan mode"));
  }
}

Result<results::CollectiveResults> QueryProcessor::execute_read_only(
    const physical::PhysicalPlan& plan, int32_t num_threads) {
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
          status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR,
                              "Error: " + std::to_string(err.error().value()) +
                                  ", Exception: " + err.exception()->what());
          return runtime::Context();
        },
        [&]() {
          status = gs::Status(gs::StatusCode::ERR_UNKNOWN, "Unknown error");
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

Result<results::CollectiveResults> QueryProcessor::execute_read_write(
    const physical::PhysicalPlan& plan, int32_t num_threads) {
  return Result<results::CollectiveResults>(
      gs::Status(gs::StatusCode::ERR_INVALID_ARGUMENT,
                 "Read-write queries are not supported yet"));
}

Result<results::CollectiveResults> QueryProcessor::execute_write_only(
    const physical::PhysicalPlan& plan, int32_t num_threads) {
  auto txn = db_.GetUpdateTransaction();
  runtime::GraphUpdateInterface gii(txn);
  runtime::Context ctx;
  gs::Status status = gs::Status::OK();
  {
    ctx = bl::try_handle_all(
        [this, &gii, &plan]() -> bl::result<runtime::Context> {
          return runtime::PlanParser::get()
              .parse_update_pipeline(gii.schema(), plan)
              .value()
              .Execute(gii, runtime::Context(), {}, timer_);
        },
        [&status](const gs::Status& err) {
          status = err;
          return runtime::Context();
        },
        [&](const bl::error_info& err) {
          status = gs::Status(gs::StatusCode::ERR_INTERNAL_ERROR,
                              "Error: " + std::to_string(err.error().value()) +
                                  ", Exception: " + err.exception()->what());
          return runtime::Context();
        },
        [&]() {
          status = gs::Status(gs::StatusCode::ERR_UNKNOWN, "Unknown error");
          return runtime::Context();
        });
  }
  if (!status.ok()) {
    LOG(ERROR) << "Error: " << status.ToString();
    // We encode the error message to the output, so that the client can
    // get the error message.
    return Result<results::CollectiveResults>(status);
  }
  // No results for write-only queries
  return Result<results::CollectiveResults>(Status::OK());
}

Result<results::CollectiveResults> QueryProcessor::execute_add_vertex_property(
    const physical::AddVertexPropertySchema& add_vertex_property_schema) {
  auto& graph_ = db_.graph();
  auto vertex_type_name = add_vertex_property_schema.vertex_type().name();
  auto tuple_res =
      property_defs_to_tuple(add_vertex_property_schema.properties());
  if (!tuple_res.ok()) {
    return tuple_res.status();
  }
  return graph_.add_vertex_properties(
      vertex_type_name, tuple_res.value(),
      conflict_action_to_bool(add_vertex_property_schema.conflict_action()));
}

Result<results::CollectiveResults> QueryProcessor::execute_add_edge_property(
    const physical::AddEdgePropertySchema& add_edge_property_schema) {
  auto& graph_ = db_.graph();
  auto edge_type_name = add_edge_property_schema.edge_type().type_name().name();
  auto src_type_name =
      add_edge_property_schema.edge_type().src_type_name().name();
  auto dst_type_name =
      add_edge_property_schema.edge_type().dst_type_name().name();
  auto tuple_res =
      property_defs_to_tuple(add_edge_property_schema.properties());
  if (!tuple_res.ok()) {
    return tuple_res.status();
  }

  return graph_.add_edge_properties(
      src_type_name, dst_type_name, edge_type_name, tuple_res.value(),
      conflict_action_to_bool(add_edge_property_schema.conflict_action()));
}

Result<results::CollectiveResults> QueryProcessor::execute_drop_vertex_property(
    const physical::DropVertexPropertySchema& drop_vertex_property_schema) {
  auto& graph_ = db_.graph();
  auto vertex_type_name = drop_vertex_property_schema.vertex_type().name();
  std::vector<std::string> property_names;
  for (const auto& prop : drop_vertex_property_schema.properties()) {
    property_names.push_back(prop);
  }
  return graph_.delete_vertex_properties(
      vertex_type_name, property_names,
      conflict_action_to_bool(drop_vertex_property_schema.conflict_action()));
}

Result<results::CollectiveResults> QueryProcessor::execute_drop_edge_property(
    const physical::DropEdgePropertySchema& drop_edge_property_schema) {
  auto& graph_ = db_.graph();
  auto edge_type_name =
      drop_edge_property_schema.edge_type().type_name().name();
  auto src_type_name =
      drop_edge_property_schema.edge_type().src_type_name().name();
  auto dst_type_name =
      drop_edge_property_schema.edge_type().dst_type_name().name();
  std::vector<std::string> property_names;
  for (const auto& prop : drop_edge_property_schema.properties()) {
    property_names.push_back(prop);
  }
  return graph_.delete_edge_properties(
      src_type_name, dst_type_name, edge_type_name, property_names,
      conflict_action_to_bool(drop_edge_property_schema.conflict_action()));
}

Result<results::CollectiveResults>
QueryProcessor::execute_rename_vertex_property(
    const physical::RenameVertexPropertySchema& rename_vertex_property_schema) {
  auto& graph_ = db_.graph();
  auto vertex_type_name = rename_vertex_property_schema.vertex_type().name();
  std::vector<std::tuple<std::string, std::string>> rename_pairs;
  for (const auto& rename : rename_vertex_property_schema.mappings()) {
    rename_pairs.emplace_back(rename.first, rename.second);
  }
  return graph_.rename_vertex_properties(
      vertex_type_name, rename_pairs,
      conflict_action_to_bool(rename_vertex_property_schema.conflict_action()));
}

Result<results::CollectiveResults> QueryProcessor::execute_rename_edge_property(
    const physical::RenameEdgePropertySchema& rename_edge_property_schema) {
  auto& graph_ = db_.graph();
  auto edge_type_name =
      rename_edge_property_schema.edge_type().type_name().name();
  auto src_type_name =
      rename_edge_property_schema.edge_type().src_type_name().name();
  auto dst_type_name =
      rename_edge_property_schema.edge_type().dst_type_name().name();
  std::vector<std::tuple<std::string, std::string>> rename_pairs;
  for (const auto& rename : rename_edge_property_schema.mappings()) {
    rename_pairs.emplace_back(rename.first, rename.second);
  }
  return graph_.rename_edge_properties(
      src_type_name, dst_type_name, edge_type_name, rename_pairs,
      conflict_action_to_bool(rename_edge_property_schema.conflict_action()));
}

Result<results::CollectiveResults> QueryProcessor::execute_ddl(
    const physical::DDLPlan& ddl_plan, int32_t num_threads) {
  auto& graph_ = db_.graph();
  if (ddl_plan.has_create_vertex_schema()) {
    auto& create_vertex = ddl_plan.create_vertex_schema();
    VLOG(10) << "Got create vertex request: " << create_vertex.DebugString();
    auto vertex_type_name = create_vertex.vertex_type().name();
    auto tuple_res = property_defs_to_tuple(create_vertex.properties());
    if (!tuple_res.ok()) {
      return tuple_res.status();
    }
    if (create_vertex.primary_key_size() == 0) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Primary key is required for vertex type creation");
    }
    if (create_vertex.primary_key_size() > 1) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Only one primary key is supported");
    }
    std::vector<std::string> pks{create_vertex.primary_key(0)};
    return graph_.create_vertex_type(vertex_type_name, tuple_res.value(), pks);
  } else if (ddl_plan.has_create_edge_schema()) {
    auto& create_edge = ddl_plan.create_edge_schema();
    VLOG(10) << "Got create edge request: " << create_edge.DebugString();
    auto edge_type_name = create_edge.edge_type().type_name().name();
    auto src_vertex_type_name = create_edge.edge_type().src_type_name().name();
    auto dst_vertex_type_name = create_edge.edge_type().dst_type_name().name();
    auto tuple_res = property_defs_to_tuple(create_edge.properties());
    if (!tuple_res.ok()) {
      return tuple_res.status();
    }
    if (create_edge.primary_key_size() != 0) {
      LOG(ERROR) << "Primary key is not supported for edge type creation";
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Primary key is not supported for edge type creation");
    }
    EdgeStrategy oe_stragety, ie_stragety;
    if (!multiplicity_to_storage_strategy(create_edge.multiplicity(),
                                          oe_stragety, ie_stragety)) {
      LOG(ERROR) << "Invalid edge multiplicity: " << create_edge.multiplicity();
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Invalid edge multiplicity: " + create_edge.multiplicity());
    }

    return graph_.create_edge_type(
        src_vertex_type_name, dst_vertex_type_name, edge_type_name,
        tuple_res.value(),
        conflict_action_to_bool(create_edge.conflict_action()), oe_stragety,
        ie_stragety);
  } else if (ddl_plan.has_add_vertex_property_schema()) {
    return execute_add_vertex_property(ddl_plan.add_vertex_property_schema());
  } else if (ddl_plan.has_add_edge_property_schema()) {
    return execute_add_edge_property(ddl_plan.add_edge_property_schema());
  } else if (ddl_plan.has_drop_vertex_property_schema()) {
    return execute_drop_vertex_property(ddl_plan.drop_vertex_property_schema());
  } else if (ddl_plan.has_drop_edge_property_schema()) {
    return execute_drop_edge_property(ddl_plan.drop_edge_property_schema());
  } else if (ddl_plan.has_rename_vertex_property_schema()) {
    return execute_rename_vertex_property(
        ddl_plan.rename_vertex_property_schema());
  } else if (ddl_plan.has_rename_edge_property_schema()) {
    return execute_rename_edge_property(ddl_plan.rename_edge_property_schema());
  } else {
    LOG(ERROR) << "Unknown DDL plan: " << ddl_plan.DebugString();
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Unknown DDL plan: " + ddl_plan.DebugString());
  }
}

}  // namespace gs
