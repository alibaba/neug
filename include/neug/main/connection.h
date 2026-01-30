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
#pragma once

#include <glog/logging.h>

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "neug/compiler/planner/graph_planner.h"
#include "neug/execution/common/types/value.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/generated/proto/plan/results.pb.h"
#include "neug/main/query_processor.h"
#include "neug/main/query_result.h"
#include "neug/utils/result.h"

namespace neug {

class NeugDB;

/**
 * @brief Internal connection class for executing queries against the graph
 * database.
 *
 * Connection provides the core interface for query execution within a NeuG
 * database. It maintains a reference to the property graph, query planner, and
 * query processor to handle the complete query execution pipeline.
 *
 * **Thread Safety:** This class is NOT thread-safe. Each thread should create
 * and manage its own Connection instance.
 *
 * **Lifecycle:**
 * - Created with references to graph, planner, and query processor
 * - Used to execute queries via Query() method
 * - Must be explicitly closed or will be closed in destructor
 *
 * @note This is an internal C++ API class. External users should use the Python
 *       binding classes for database interactions.
 *
 * @since v0.1.0
 */
class Connection {
 public:
  Connection(PropertyGraph& graph,
             std::shared_ptr<QueryProcessor> query_processor)
      : graph_(graph), query_processor_(query_processor), is_closed_(false) {}
  ~Connection() { Close(); }

  /**
   * @brief Execute a query string and return the results.
   *
   * Processes the query string through the planner and query processor, then
   * converts the internal CollectiveResults to a QueryResult for user
   * consumption.
   *
   * @param query_string The query string to execute
   * @param access_mode The access mode of the query. It could be `read(r)`,
   * `insert(i)`, `update(u)` (include deletion). User should specify the
   * correct access mode for the query to ensure the correctness of the
   * database. If the access mode is not specified, it will be set to `update`
   * by default.
   * @param parameters The parameters to be used in the query. The parameters
   * should be a dictionary, where the keys are the parameter names, and the
   * values are the parameter values. If no parameters are needed, it can be set
   * to an empty map.
   * @return Result<QueryResult> containing either the query results or an error
   * status
   *
   * @throws Logs error and returns error status if connection is closed
   *
   * Implementation: Calls query_impl() internally, then converts
   * CollectiveResults to QueryResult using QueryResult::From().
   *
   * @since v0.1.0
   */
  result<QueryResult> Query(const std::string& query_string,
                            const std::string& access_mode = "update",
                            const runtime::ParamsMap& parameters = {});

  /**
   * @brief Get the database schema.
   *
   * Returns a reference to the schema from the underlying property graph.
   *
   * @return const Schema& Reference to the database schema
   *
   * @throws std::runtime_error if the connection is closed
   *
   * Implementation: Checks if connection is closed, then returns
   * graph_.schema().
   *
   * @since v0.1.0
   */
  std::string GetSchema() const;

  /**
   * @brief Close the connection and mark it as closed.
   *
   * Sets the is_closed_ atomic flag to true. This method is idempotent and
   * safe to call multiple times.
   *
   * Implementation: Uses atomic store operation to set is_closed_ flag.
   * Logs warning if already closed, otherwise logs info message.
   *
   * @since v0.1.0
   */
  void Close();

  /**
   * @brief Check if the connection is closed.
   *
   * @return true if the connection has been closed, false otherwise
   *
   * Implementation: Uses atomic load operation on is_closed_ flag.
   *
   * @since v0.1.0
   */
  bool IsClosed() const { return is_closed_.load(); }

 private:
  PropertyGraph& graph_;

  std::shared_ptr<QueryProcessor> query_processor_;

  std::atomic<bool> is_closed_{false};
};

}  // namespace neug
