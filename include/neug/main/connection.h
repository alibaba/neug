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

#ifndef TOOLS_PYTHON_BIND_SRC_CONNECTION_H_
#define TOOLS_PYTHON_BIND_SRC_CONNECTION_H_

#include <atomic>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "neug/compiler/planner/graph_planner.h"
#include "neug/main/query_processor.h"
#include "neug/main/query_result.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/generated/proto/plan/results.pb.h"
#else
#include "neug/utils/proto/plan/physical.pb.h"
#include "neug/utils/proto/plan/results.pb.h"
#endif
#include "neug/utils/result.h"

namespace gs {

class NeugDB;

/**
 * This the the internal connection class, which is used to execute the query.
 * The connection is not thread safe, so the user should create a new connection
 * for each thread.
 */
class Connection {
 public:
  Connection(NeugDB& db, std::shared_ptr<IGraphPlanner> planner,
             std::shared_ptr<QueryProcessor> query_processor)
      : db_(db),
        planner_(planner),
        query_processor_(query_processor),
        is_closed_(false) {}
  ~Connection() { Close(); }

  /**
   * @brief call query_impl and convert results::CollectiveResults to
   * QueryResult.
   */
  Result<QueryResult> Query(const std::string& query_string);

  const Schema& GetSchema() const;

  void Close();

  bool IsClosed() const { return is_closed_.load(); }

 private:
  /**
   * @brief Execute the query and return the result.
   * @note The query process is divided into two parts:
   * 1. Parse the query string and generate the execution plan.
   * 2. Execute the execution plan using runtime engine.
   */
  Result<results::CollectiveResults> query_impl(
      const std::string& query_string);

  NeugDB& db_;

  std::shared_ptr<IGraphPlanner> planner_;
  std::shared_ptr<QueryProcessor> query_processor_;

  std::atomic<bool> is_closed_{false};
};
}  // namespace gs

#endif  // TOOLS_PYTHON_BIND_SRC_CONNECTION_H_