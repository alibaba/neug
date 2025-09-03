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

#ifndef ENGINES_GRAPH_DB_APP_BUILDIN_SHORTEST_PATH_AMONG_THREE_H_
#define ENGINES_GRAPH_DB_APP_BUILDIN_SHORTEST_PATH_AMONG_THREE_H_
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "neug/main/app/app_base.h"
#include "neug/main/app/cypher_proc_app_base.h"
#include "neug/main/neug_db_session.h"
#include "neug/utils/property/types.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/results.pb.h"
#else
#include "neug/utils/proto/plan/results.pb.h"
#endif

namespace gs {
class NeugDB;
class GraphDBSession;
class ReadTransaction;

class ShortestPathAmongThree
    : public CypherReadProcAppBase<std::string, std::string, std::string,
                                   std::string, std::string, std::string> {
 public:
  ShortestPathAmongThree() {}
  results::CollectiveResults Query(const GraphDBSession& sess,
                                   std::string label_name1, std::string oid1,
                                   std::string label_name2, std::string oid2,
                                   std::string label_name3, std::string oid3);

 private:
  bool ShortestPath(const gs::ReadTransaction& txn, label_t v1_l,
                    vid_t v1_index, label_t v2_l, vid_t v2_index,
                    std::vector<std::pair<label_t, vid_t>>& result_);
  std::vector<std::pair<label_t, vid_t>> ConnectPath(
      const std::vector<std::pair<label_t, vid_t>>& path1,
      const std::vector<std::pair<label_t, vid_t>>& path2,
      const std::vector<std::pair<label_t, vid_t>>& path3);
};

class ShortestPathAmongThreeFactory : public AppFactoryBase {
 public:
  ShortestPathAmongThreeFactory() = default;
  ~ShortestPathAmongThreeFactory() = default;

  AppWrapper CreateApp(const NeugDB& db) override;
};

}  // namespace gs

#endif