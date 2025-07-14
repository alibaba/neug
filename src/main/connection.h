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

#include "src/engines/graph_db/database/graph_db.h"
#include "src/main/query_processor.h"
#include "src/main/query_result.h"
#include "src/planner/graph_planner.h"
#include "src/proto_generated_gie/physical.pb.h"
#include "src/proto_generated_gie/results.pb.h"
#include "src/utils/result.h"

namespace gs {

class NeugDB;

/**
 * This the the internal connection class, which is used to execute the query.
 * The connection is not thread safe, so the user should create a new connection
 * for each thread.
 */
class Connection {
 public:
  Connection(GraphDB& db, std::shared_ptr<IGraphPlanner> planner,
             std::shared_ptr<QueryProcessor> query_processor)
      : db_(db),
        planner_(planner),
        query_processor_(query_processor),
        is_closed_(false) {}
  ~Connection() { close(); }

  /**
   * @brief call query_impl and convert results::CollectiveResults to
   * QueryResult.
   */
  Result<QueryResult> query(const std::string& query_string);

  const Schema& get_schema() const {
    if (is_closed()) {
      LOG(ERROR) << "Connection is closed, cannot get schema.";
      throw std::runtime_error("Connection is closed, cannot get schema.");
    }
    return db_.schema();
  }

  void close() {
    if (is_closed_.load(std::memory_order_relaxed)) {
      LOG(WARNING) << "Connection is already closed.";
      return;
    }
    LOG(INFO) << "Closing connection.";
    is_closed_.store(true);
    // Any necessary cleanup can be done here.
  }

  bool is_closed() const { return is_closed_.load(); }

 private:
  /**
   * @brief Execute the query and return the result.
   * @note The query process is divided into two parts:
   * 1. Parse the query string and generate the execution plan.
   * 2. Execute the execution plan using runtime engine.
   */
  Result<results::CollectiveResults> query_impl(
      const std::string& query_string);

  // TODO: Make sure
  Plan createDDLPlan(const std::string& query_string);

  Plan createDMLPlan(const std::string& query_string);

  Plan createDDLPlanWithGopt(const std::string& query_string);

  Plan createDMLPlanWithGopt(const std::string& query_string);

  GraphDB& db_;

  std::shared_ptr<IGraphPlanner> planner_;
  std::shared_ptr<QueryProcessor> query_processor_;

  std::atomic<bool> is_closed_{false};
};
}  // namespace gs

#endif  // TOOLS_PYTHON_BIND_SRC_CONNECTION_H_