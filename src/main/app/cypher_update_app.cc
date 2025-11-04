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

#include "neug/main/app/cypher_update_app.h"

#include <glog/logging.h>
#include <stddef.h>

#include <map>
#include <ostream>
#include <string_view>
#include <utility>

#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/execution/execute/plan_parser.h"
#include "neug/main/app/cypher_app_utils.h"
#include "neug/main/neug_db.h"
#include "neug/main/neug_db_session.h"
#include "neug/transaction/update_transaction.h"
#include "neug/utils/app_utils.h"
#include "neug/utils/pb_utils.h"

namespace gs {

result<results::CollectiveResults> CypherUpdateApp::execute_add_vertex_property(
    NeugDBSession& graph,
    const physical::AddVertexPropertySchema& add_vertex_property_schema) {
  auto& graph_ = graph.graph();
  auto vertex_type_name = add_vertex_property_schema.vertex_type().name();
  auto tuple_res =
      property_defs_to_tuple(add_vertex_property_schema.properties());
  if (!tuple_res) {
    RETURN_ERROR(tuple_res.error());
  }
  auto ret = graph_.AddVertexProperties(
      vertex_type_name, tuple_res.value(),
      conflict_action_to_bool(add_vertex_property_schema.conflict_action()));
  if (ret.ok()) {
    return results::CollectiveResults();
  } else {
    RETURN_ERROR(ret);
  }
}

result<results::CollectiveResults> CypherUpdateApp::execute_add_edge_property(
    NeugDBSession& graph,
    const physical::AddEdgePropertySchema& add_edge_property_schema) {
  auto& graph_ = graph.graph();
  auto edge_type_name = add_edge_property_schema.edge_type().type_name().name();
  auto src_type_name =
      add_edge_property_schema.edge_type().src_type_name().name();
  auto dst_type_name =
      add_edge_property_schema.edge_type().dst_type_name().name();
  auto tuple_res =
      property_defs_to_tuple(add_edge_property_schema.properties());
  if (!tuple_res) {
    RETURN_ERROR(tuple_res.error());
  }

  auto ret = graph_.AddEdgeProperties(
      src_type_name, dst_type_name, edge_type_name, tuple_res.value(),
      conflict_action_to_bool(add_edge_property_schema.conflict_action()));
  if (ret.ok()) {
    return results::CollectiveResults();
  } else {
    RETURN_ERROR(ret);
  }
}

result<results::CollectiveResults>
CypherUpdateApp::execute_drop_vertex_property(
    NeugDBSession& graph,
    const physical::DropVertexPropertySchema& drop_vertex_property_schema) {
  auto& graph_ = graph.graph();
  auto vertex_type_name = drop_vertex_property_schema.vertex_type().name();
  std::vector<std::string> property_names;
  for (const auto& prop : drop_vertex_property_schema.properties()) {
    property_names.push_back(prop);
  }
  auto ret = graph_.DeleteVertexProperties(
      vertex_type_name, property_names,
      conflict_action_to_bool(drop_vertex_property_schema.conflict_action()));
  if (ret.ok()) {
    return results::CollectiveResults();
  } else {
    RETURN_ERROR(ret);
  }
}

result<results::CollectiveResults> CypherUpdateApp::execute_drop_edge_property(
    NeugDBSession& graph,
    const physical::DropEdgePropertySchema& drop_edge_property_schema) {
  auto& graph_ = graph.graph();
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
  auto ret = graph_.DeleteEdgeProperties(
      src_type_name, dst_type_name, edge_type_name, property_names,
      conflict_action_to_bool(drop_edge_property_schema.conflict_action()));
  if (ret.ok()) {
    return results::CollectiveResults();
  } else {
    RETURN_ERROR(ret);
  }
}

result<results::CollectiveResults>
CypherUpdateApp::execute_rename_vertex_property(
    NeugDBSession& graph,
    const physical::RenameVertexPropertySchema& rename_vertex_property_schema) {
  auto& graph_ = graph.graph();
  auto vertex_type_name = rename_vertex_property_schema.vertex_type().name();
  std::vector<std::tuple<std::string, std::string>> rename_pairs;
  for (const auto& rename : rename_vertex_property_schema.mappings()) {
    rename_pairs.emplace_back(rename.first, rename.second);
  }
  auto ret = graph_.RenameVertexProperties(
      vertex_type_name, rename_pairs,
      conflict_action_to_bool(rename_vertex_property_schema.conflict_action()));
  if (ret.ok()) {
    return results::CollectiveResults();
  } else {
    RETURN_ERROR(ret);
  }
}

result<results::CollectiveResults> CypherUpdateApp::execute_rename_vertex_type(
    NeugDBSession& graph,
    const physical::RenameVertexTypeSchema& rename_vertex_type_schema) {
  THROW_NOT_IMPLEMENTED_EXCEPTION("Rename vertex type is not implemented yet");
}

result<results::CollectiveResults> CypherUpdateApp::execute_rename_edge_type(
    NeugDBSession& graph,
    const physical::RenameEdgeTypeSchema& rename_edge_type_schema) {
  THROW_NOT_IMPLEMENTED_EXCEPTION("Rename edge type is not implemented yet");
}

result<results::CollectiveResults>
CypherUpdateApp::execute_rename_edge_property(
    NeugDBSession& graph,
    const physical::RenameEdgePropertySchema& rename_edge_property_schema) {
  auto& graph_ = graph.graph();
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
  auto ret = graph_.RenameEdgeProperties(
      src_type_name, dst_type_name, edge_type_name, rename_pairs,
      conflict_action_to_bool(rename_edge_property_schema.conflict_action()));
  if (ret.ok()) {
    return results::CollectiveResults();
  } else {
    RETURN_ERROR(ret);
  }
}

result<results::CollectiveResults> CypherUpdateApp::execute_drop_vertex_schema(
    NeugDBSession& graph,
    const physical::DropVertexSchema& drop_vertex_schema) {
  auto& graph_ = graph.graph();
  auto vertex_type_name = drop_vertex_schema.vertex_type().name();
  // Todo(NENG): Always drop vertex type with detach mode
  auto ret = graph_.DeleteVertexType(
      vertex_type_name, true,
      conflict_action_to_bool(drop_vertex_schema.conflict_action()));
  if (ret.ok()) {
    return results::CollectiveResults();
  } else {
    RETURN_ERROR(ret);
  }
}

result<results::CollectiveResults> CypherUpdateApp::execute_drop_edge_schema(
    NeugDBSession& graph, const physical::DropEdgeSchema& drop_edge_schema) {
  auto& graph_ = graph.graph();
  auto edge_type_name = drop_edge_schema.edge_type().type_name().name();
  auto src_type_name = drop_edge_schema.edge_type().src_type_name().name();
  auto dst_type_name = drop_edge_schema.edge_type().dst_type_name().name();
  auto status = graph_.DeleteEdgeType(
      src_type_name, dst_type_name, edge_type_name,
      conflict_action_to_bool(drop_edge_schema.conflict_action()));
  if (!status.ok()) {
    RETURN_ERROR(status);
  }
  return results::CollectiveResults();
}

result<results::CollectiveResults> CypherUpdateApp::execute_ddl(
    NeugDBSession& graph, const physical::DDLPlan& ddl_plan) {
  auto& graph_ = graph.graph();
  try {
    if (ddl_plan.has_create_vertex_schema()) {
      auto& create_vertex = ddl_plan.create_vertex_schema();
      VLOG(10) << "Got create vertex request: " << create_vertex.DebugString();
      auto vertex_type_name = create_vertex.vertex_type().name();
      auto tuple_res = property_defs_to_tuple(create_vertex.properties());
      if (!tuple_res) {
        RETURN_ERROR(tuple_res.error());
      }
      if (create_vertex.primary_key_size() == 0) {
        RETURN_ERROR(
            Status(StatusCode::ERR_INVALID_ARGUMENT,
                   "Must specify a primary key for vertex type creation"));
      }
      if (create_vertex.primary_key_size() > 1) {
        RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                            "Only one primary key is supported"));
      }
      std::vector<std::string> pks{create_vertex.primary_key(0)};
      auto res = graph_.CreateVertexType(
          vertex_type_name, tuple_res.value(), pks,
          conflict_action_to_bool(create_vertex.conflict_action()));
      if (!res.ok()) {
        LOG(ERROR) << "Fail to create vertex type: " << vertex_type_name
                   << ", reason: " << res.ToString();
        RETURN_ERROR(res);
      }
      return results::CollectiveResults();
    } else if (ddl_plan.has_create_edge_schema()) {
      auto& create_edges = ddl_plan.create_edge_schema();
      auto tuple_res = property_defs_to_tuple(create_edges.properties());
      if (!tuple_res) {
        RETURN_ERROR(tuple_res.error());
      }
      if (create_edges.primary_key_size() != 0) {
        LOG(ERROR) << "Primary key is not supported for edge type creation";
        RETURN_ERROR(
            Status(StatusCode::ERR_INVALID_ARGUMENT,
                   "Primary key is not supported for edge type creation"));
      }
      bool conflict_action =
          conflict_action_to_bool(create_edges.conflict_action());
      using property_def_t =
          std::vector<std::tuple<PropertyType, std::string, Property>>;
      using create_edge_value_t =
          std::tuple<std::string, std::string, std::string, property_def_t,
                     bool, EdgeStrategy, EdgeStrategy>;
      std::vector<create_edge_value_t> create_edge_defs;
      for (int32_t i = 0; i < create_edges.type_info_size(); ++i) {
        const auto& create_edge = create_edges.type_info(i);
        auto multiplicity = create_edges.type_info(i).multiplicity();
        auto edge_type_name = create_edge.edge_type().type_name().name();
        auto src_vertex_type_name =
            create_edge.edge_type().src_type_name().name();
        auto dst_vertex_type_name =
            create_edge.edge_type().dst_type_name().name();
        EdgeStrategy oe_stragety, ie_stragety;
        if (!multiplicity_to_storage_strategy(multiplicity, oe_stragety,
                                              ie_stragety)) {
          LOG(ERROR) << "Invalid edge multiplicity: " << multiplicity;
          RETURN_ERROR(Status(
              StatusCode::ERR_INVALID_ARGUMENT,
              "Invalid edge multiplicity: " +
                  physical::CreateEdgeSchema_Multiplicity_Name(multiplicity)));
        }

        if (graph_.schema().exist(src_vertex_type_name, dst_vertex_type_name,
                                  edge_type_name)) {
          if (conflict_action) {
            RETURN_ERROR(Status(
                StatusCode::ERR_INVALID_ARGUMENT,
                "Edge triplet already exists: " + src_vertex_type_name + ", " +
                    dst_vertex_type_name + ", " + edge_type_name));
          }
        } else {
          create_edge_defs.emplace_back(
              src_vertex_type_name, dst_vertex_type_name, edge_type_name,
              tuple_res.value(), conflict_action, oe_stragety, ie_stragety);
        }
      }
      int32_t succeed_index = 0;
      int32_t defs_size = create_edge_defs.size();
      while (succeed_index < defs_size) {
        const auto& create_edge_def = create_edge_defs[succeed_index];
        auto status = graph_.create_edge_type(
            std::get<0>(create_edge_def), std::get<1>(create_edge_def),
            std::get<2>(create_edge_def), std::get<3>(create_edge_def),
            std::get<4>(create_edge_def), std::get<5>(create_edge_def),
            std::get<6>(create_edge_def));
        if (!status.ok()) {
          LOG(ERROR) << "Fail to insert edge triplet: "
                     << std::get<0>(create_edge_def) << ", "
                     << std::get<1>(create_edge_def) << ", "
                     << std::get<2>(create_edge_def)
                     << ", reason: " << status.ToString();
          break;
        }
        succeed_index += 1;
      }
      while (succeed_index >= 0 && succeed_index < defs_size) {
        const auto& create_edge_def = create_edge_defs[succeed_index];
        // Drop the created edge types.
        if (!graph_
                 .DeleteEdgeType(std::get<0>(create_edge_def),
                                 std::get<1>(create_edge_def),
                                 std::get<2>(create_edge_def), false)
                 .ok()) {
          LOG(ERROR) << "Fail to revert created edge type in CreateEdgeSchema "
                        "request";
        }
        succeed_index -= 1;
      }
      return results::CollectiveResults();

    } else if (ddl_plan.has_add_vertex_property_schema()) {
      return execute_add_vertex_property(graph,
                                         ddl_plan.add_vertex_property_schema());
    } else if (ddl_plan.has_add_edge_property_schema()) {
      return execute_add_edge_property(graph,
                                       ddl_plan.add_edge_property_schema());
    } else if (ddl_plan.has_drop_vertex_property_schema()) {
      return execute_drop_vertex_property(
          graph, ddl_plan.drop_vertex_property_schema());
    } else if (ddl_plan.has_drop_edge_property_schema()) {
      return execute_drop_edge_property(graph,
                                        ddl_plan.drop_edge_property_schema());
    } else if (ddl_plan.has_rename_vertex_property_schema()) {
      return execute_rename_vertex_property(
          graph, ddl_plan.rename_vertex_property_schema());
    } else if (ddl_plan.has_rename_edge_property_schema()) {
      return execute_rename_edge_property(
          graph, ddl_plan.rename_edge_property_schema());
    } else if (ddl_plan.has_drop_vertex_schema()) {
      return execute_drop_vertex_schema(graph, ddl_plan.drop_vertex_schema());
    } else if (ddl_plan.has_drop_edge_schema()) {
      return execute_drop_edge_schema(graph, ddl_plan.drop_edge_schema());
    } else if (ddl_plan.has_rename_vertex_type_schema()) {
      return execute_rename_vertex_type(graph,
                                        ddl_plan.rename_vertex_type_schema());
    } else if (ddl_plan.has_rename_edge_type_schema()) {
      return execute_rename_edge_type(graph,
                                      ddl_plan.rename_edge_type_schema());
    } else {
      LOG(ERROR) << "Unknown DDL plan: " << ddl_plan.DebugString();
      RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                          "Unknown DDL plan: " + ddl_plan.DebugString()));
    }
  } catch (const exception::InvalidArgumentException& e) {
    LOG(ERROR) << "Invalid argument: " << e.what();
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT, e.what()));
  } catch (const exception::InternalException& e) {
    LOG(ERROR) << "Internal error: " << e.what();
    RETURN_ERROR(Status(StatusCode::ERR_INTERNAL_ERROR, e.what()));
  } catch (const exception::ConnectionException& e) {
    LOG(ERROR) << "Connection error: " << e.what();
    RETURN_ERROR(Status(StatusCode::ERR_CONNECTION_ERROR, e.what()));
  } catch (const exception::ConversionException& e) {
    LOG(ERROR) << "Conversion error: " << e.what();
    RETURN_ERROR(Status(StatusCode::ERR_TYPE_CONVERSION, e.what()));
  } catch (const exception::BinderException& e) {
    LOG(ERROR) << "Binder error: " << e.what();
    RETURN_ERROR(Status(StatusCode::ERR_COMPILATION, e.what()));
  } catch (const exception::RuntimeError& e) {
    LOG(ERROR) << "Runtime error: " << e.what();
    RETURN_ERROR(Status(StatusCode::ERR_INTERNAL_ERROR, e.what()));
  } catch (const exception::CopyException& e) {
    LOG(ERROR) << "Copy error: " << e.what();
    RETURN_ERROR(Status(StatusCode::ERR_INTERNAL_ERROR, e.what()));
  } catch (const exception::IndexException& e) {
    LOG(ERROR) << "Index error: " << e.what();
    RETURN_ERROR(Status(StatusCode::ERR_INDEX_ERROR, e.what()));
  } catch (const exception::ExtensionException& e) {
    LOG(ERROR) << "Extension error: " << e.what();
    RETURN_ERROR(Status(StatusCode::ERR_EXTENSION, e.what()));
  } catch (const exception::QueryExecutionError& e) {
    LOG(ERROR) << "Query execution error: " << e.what();
    RETURN_ERROR(Status(StatusCode::ERR_QUERY_EXECUTION, e.what()));
  } catch (const exception::SchemaMismatchException& e) {
    LOG(ERROR) << "Schema mismatch error: " << e.what();
    RETURN_ERROR(Status(StatusCode::ERR_SCHEMA_MISMATCH, e.what()));
  } catch (const exception::Exception& e) {
    LOG(ERROR) << "Exception: " << e.what();
    RETURN_ERROR(Status(StatusCode::ERR_INTERNAL_ERROR, e.what()));
  } catch (const std::exception& e) {
    LOG(ERROR) << "Unknown error: " << e.what();
    RETURN_ERROR(Status(StatusCode::ERR_INTERNAL_ERROR, e.what()));
  } catch (...) {
    LOG(ERROR) << "Unknown error occurred during DDL execution";
    RETURN_ERROR(Status(StatusCode::ERR_UNKNOWN,
                        "Unknown error occurred during DDL "
                        "execution"));
  }
}

result<results::CollectiveResults> CypherUpdateApp::execute_update_query(
    NeugDBSession& graph, const physical::PhysicalPlan& plan,
    runtime::OprTimer* timer, bool insert_with_resize) {
  auto txn = graph.GetUpdateTransaction();
  txn.set_insert_vertex_with_resize(insert_with_resize);
  runtime::GraphUpdateInterface gii(txn);
  auto ctx = runtime::ParseAndExecuteUpdatePipeline(gii, plan, timer);

  if (!ctx) {
    LOG(ERROR) << "Error: " << ctx.error().ToString();
    txn.Abort();
    // We encode the error message to the output, so that the client can
    // get the error message.
    RETURN_ERROR(ctx.error());
  }
  auto res = runtime::Sink::sink(ctx.value(), gii);
  if (!txn.Commit()) {
    LOG(ERROR) << "Commit failed";
    // If commit fails, we return an error.
    RETURN_ERROR(Status(StatusCode::ERR_INTERNAL_ERROR, "Commit failed"));
  }
  return result<results::CollectiveResults>(std::move(res));
}

bool CypherUpdateApp::Query(NeugDBSession& graph, Decoder& input,
                            Encoder& output) {
  std::string_view r_bytes = input.get_bytes();
  uint8_t type = static_cast<uint8_t>(r_bytes.back());
  std::string_view bytes = std::string_view(r_bytes.data(), r_bytes.size() - 1);
  if (type == Schema::ADHOC_UPDATE_PLUGIN_ID) {
    physical::PhysicalPlan plan;
    if (!plan.ParseFromString(std::string(bytes))) {
      LOG(ERROR) << "Parse plan failed...";
      return false;
    }

    VLOG(1) << "plan: " << plan.DebugString();
    if (plan.has_ddl_plan()) {
      auto result = execute_ddl(graph, plan.ddl_plan());
      if (!result) {
        output.put_string(result.error().ToString());
        return false;
      }
      // TODO(zhanglei): Currently we dump schema to disk after each DDL
      // operation, to ensure the consistency.
      // The better way is to write the schema change to WAL, and apply them
      // after the transaction is committed.
      graph.graph().DumpSchema();
      auto res = result.value().SerializeAsString();
      output.put_bytes(res.data(), res.size());
      return true;
    }

    // TODO(lexiao,zhanglei): Currently we resize the vertex property column
    // is space is not enough.
    //  This may infect the performance of the update query.
    std::unique_ptr<runtime::OprTimer> timer =
        std::make_unique<runtime::OprTimer>();
    auto res = execute_update_query(graph, plan, timer.get(), true);

    if (!res) {
      LOG(ERROR) << "Execute update query failed: " << res.error().ToString();
      output.put_string(res.error().ToString());
      return false;
    }
    auto collective_results = res.value().SerializeAsString();
    output.put_bytes(collective_results.data(), collective_results.size());
    return true;
  } else {
    LOG(ERROR) << "Unsupported write type: " << static_cast<int>(type);
    output.put_string("Unsupported write type");
    return false;
  }
}
AppWrapper CypherUpdateAppFactory::CreateApp(const NeugDB& db) {
  return AppWrapper(new CypherUpdateApp(db), NULL);
}
}  // namespace gs
