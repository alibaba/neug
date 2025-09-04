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

#ifndef ENGINES_GRAPH_DB_CYPHER_UPDATE_APP_H_
#define ENGINES_GRAPH_DB_CYPHER_UPDATE_APP_H_
#include <string>
#include <unordered_map>

#include "neug/execution/execute/pipeline.h"
#include "neug/execution/utils/opr_timer.h"
#include "neug/main/app/app_base.h"
#include "neug/main/neug_db_session.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/physical.pb.h"
#else
#include "neug/utils/proto/plan/physical.pb.h"
#endif

namespace gs {
class Decoder;
class Encoder;
class NeugDB;
class NeugDBSession;

class CypherUpdateApp : public WriteAppBase {
 public:
  CypherUpdateApp(const NeugDB& db) {}

  AppType type() const override { return AppType::kCypherAdhoc; }

  bool Query(NeugDBSession& graph, Decoder& input, Encoder& output) override;

  static Result<results::CollectiveResults> execute_ddl(
      NeugDBSession& graph, const physical::DDLPlan& ddl_plan);

  static Result<results::CollectiveResults> execute_update_query(
      NeugDBSession& graph, const physical::PhysicalPlan& plan,
      runtime::OprTimer* timer_, bool insert_with_resize = false);

  static Result<results::CollectiveResults> execute_add_vertex_property(
      NeugDBSession& graph,
      const physical::AddVertexPropertySchema& add_vertex_property_schema);

  static Result<results::CollectiveResults> execute_add_edge_property(
      NeugDBSession& graph,
      const physical::AddEdgePropertySchema& add_edge_property_schema);

  static Result<results::CollectiveResults> execute_drop_vertex_property(
      NeugDBSession& graph,
      const physical::DropVertexPropertySchema& drop_vertex_property_schema);

  static Result<results::CollectiveResults> execute_drop_edge_property(
      NeugDBSession& graph,
      const physical::DropEdgePropertySchema& drop_edge_property_schema);

  static Result<results::CollectiveResults> execute_rename_vertex_property(
      NeugDBSession& graph, const physical::RenameVertexPropertySchema&
                                rename_vertex_property_schema);
  static Result<results::CollectiveResults> execute_rename_edge_property(
      NeugDBSession& graph,
      const physical::RenameEdgePropertySchema& rename_edge_property_schema);
  static Result<results::CollectiveResults> execute_drop_vertex_schema(
      NeugDBSession& graph,
      const physical::DropVertexSchema& drop_vertex_schema);
  static Result<results::CollectiveResults> execute_drop_edge_schema(
      NeugDBSession& graph, const physical::DropEdgeSchema& drop_edge_schema);

 private:
  std::unordered_map<std::string, physical::PhysicalPlan> plan_cache_;
  std::unordered_map<std::string, runtime::InsertPipeline> pipeline_cache_;
};

class CypherUpdateAppFactory : public AppFactoryBase {
 public:
  CypherUpdateAppFactory() = default;
  ~CypherUpdateAppFactory() = default;

  AppWrapper CreateApp(const NeugDB& db) override;
};

}  // namespace gs
#endif  // ENGINES_GRAPH_DB_CYPHER_WRITE_APP_H_