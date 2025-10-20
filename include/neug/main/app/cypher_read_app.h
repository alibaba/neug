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

#ifndef INCLUDE_NEUG_MAIN_APP_CYPHER_READ_APP_H_
#define INCLUDE_NEUG_MAIN_APP_CYPHER_READ_APP_H_

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

class CypherReadApp : public ReadAppBase {
 public:
  explicit CypherReadApp(const NeugDB& db) {}

  AppType type() const override { return AppType::kCypherAdhoc; }

  bool Query(const NeugDBSession& graph, Decoder& input,
             Encoder& output) override;

 private:
  std::unordered_map<std::string, physical::PhysicalPlan> plan_cache_;
  std::unordered_map<std::string, runtime::ReadPipeline> pipeline_cache_;
};

class CypherReadAppFactory : public AppFactoryBase {
 public:
  CypherReadAppFactory() = default;
  ~CypherReadAppFactory() = default;

  AppWrapper CreateApp(const NeugDB& db) override;
};

}  // namespace gs

#endif  // INCLUDE_NEUG_MAIN_APP_CYPHER_READ_APP_H_
