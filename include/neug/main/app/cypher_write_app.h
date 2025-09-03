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

#ifndef ENGINES_GRAPH_DB_CYPHER_WRITE_APP_H_
#define ENGINES_GRAPH_DB_CYPHER_WRITE_APP_H_
#include <string>
#include <unordered_map>

#include "neug/execution/runtime/execute/pipeline.h"
#include "neug/execution/runtime/utils/opr_timer.h"
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
class GraphDBSession;

class CypherWriteApp : public WriteAppBase {
 public:
  CypherWriteApp(const NeugDB& db) : db_(db) {}

  AppType type() const override { return AppType::kCypherAdhoc; }

  bool Query(GraphDBSession& graph, Decoder& input, Encoder& output) override;

 private:
  const NeugDB& db_;
  std::unordered_map<std::string, physical::PhysicalPlan> plan_cache_;
  std::unordered_map<std::string, runtime::InsertPipeline> pipeline_cache_;
};

class CypherWriteAppFactory : public AppFactoryBase {
 public:
  CypherWriteAppFactory() = default;
  ~CypherWriteAppFactory() = default;

  AppWrapper CreateApp(const NeugDB& db) override;
};

}  // namespace gs
#endif  // ENGINES_GRAPH_DB_CYPHER_WRITE_APP_H_