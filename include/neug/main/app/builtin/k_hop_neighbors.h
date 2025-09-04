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

#ifndef ENGINES_GRAPH_DB_APP_BUILDIN_K_HOP_NEIGHBORS_
#define ENGINES_GRAPH_DB_APP_BUILDIN_K_HOP_NEIGHBORS_
#include <stdint.h>
#include <string>
#include <tuple>

#include "neug/main/app/app_base.h"
#include "neug/main/app/cypher_proc_app_base.h"
#include "neug/main/neug_db_session.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/results.pb.h"
#else
#include "neug/utils/proto/plan/results.pb.h"
#endif

namespace gs {
class NeugDB;
class NeugDBSession;

class KNeighbors : public CypherReadProcAppBase<std::string, int64_t, int32_t> {
 public:
  KNeighbors() {}
  results::CollectiveResults Query(const NeugDBSession& sess,
                                   std::string label_name, int64_t vertex_id,
                                   int32_t hop_range) override;
};

class KNeighborsFactory : public AppFactoryBase {
 public:
  KNeighborsFactory() = default;
  ~KNeighborsFactory() = default;

  AppWrapper CreateApp(const NeugDB& db) override;
};

}  // namespace gs

#endif  // ENGINES_GRAPH_DB_APP_BUILDIN_K_HOP_NEIGHBORS_