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
#include <stddef.h>
#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "neug/compiler/planner/graph_planner.h"
#include "neug/execution/execute/query_cache.h"
#include "neug/main/query_result.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/update_transaction.h"
#include "neug/utils/access_mode.h"
#include "neug/utils/allocators.h"
#include "neug/utils/property/column.h"
#include "neug/utils/result.h"
#include "neug/utils/service_utils.h"

namespace neug {

class NeugDB;
class IWalWriter;
class ColumnBase;
class Encoder;
class PropertyGraph;
class RefColumnBase;
class Schema;
class AppManager;

/**
 * @brief Database session for executing queries in high-throughput scenarios.
 *
 * NeugDBSession provides a session-based interface for interacting with the
 * NeuG database. Each session maintains its own transaction context and
 * application state, enabling concurrent access while ensuring data
 * consistency.
 *
 * Sessions are typically acquired from a SessionPool via NeugDBService, not
 * created directly. This is the server-side equivalent of Python's Session
 * class for client connections.
 *
 * **Usage Example:**
 * @code{.cpp}
 * // Acquire session from service
 * auto guard = service.AcquireSession();
 *
 * // Execute read query
 * std::string query = R"({
 *   "query": "MATCH (n:Person) RETURN n.name LIMIT 10",
 *   "access_mode": "read"
 * })";
 * auto result = guard->Eval(query);
 *
 * // Execute write query with parameters
 * std::string insert_query = R"({
 *   "query": "CREATE (n:Person {name: $name})",
 *   "access_mode": "insert",
 *   "parameters": {"name": "Alice"}
 * })";
 * auto write_result = guard->Eval(insert_query);
 * @endcode
 *
 * **Transaction Types:**
 * - `ReadTransaction`: Read-only snapshot access
 * - `InsertTransaction`: Add new vertices and edges
 * - `UpdateTransaction`: Modify existing graph elements
 * - `CompactTransaction`: Background compaction operations
 *
 * **Thread Safety:** Each session is tied to a specific thread and should
 * not be shared across threads. Sessions are managed by SessionPool to
 * ensure thread-local access.
 *
 * @see NeugDBService for HTTP service wrapper
 * @see SessionPool for session management
 * @since v0.1.0
 */
class NeugDBSession {
 public:
  static constexpr int32_t MAX_RETRY = 3;
  NeugDBSession(PropertyGraph& graph, std::shared_ptr<IGraphPlanner> planner,
                std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
                std::shared_ptr<IVersionManager> vm, Allocator& alloc,
                IWalWriter& logger, const NeugDBConfig& config_, int thread_id)
      : graph_(graph),
        planner_(planner),
        pipeline_cache_(global_query_cache),
        version_manager_(vm),
        alloc_(alloc),
        logger_(logger),
        db_config_(config_),
        thread_id_(thread_id),
        eval_duration_(0),
        query_num_(0) {}
  ~NeugDBSession() {}

  ReadTransaction GetReadTransaction() const;

  InsertTransaction GetInsertTransaction();

  UpdateTransaction GetUpdateTransaction();

  CompactTransaction GetCompactTransaction();

  const PropertyGraph& graph() const;
  PropertyGraph& graph();
  const Schema& schema() const;

  /**
   * @brief Execute a Cypher query within the session.
   *
   * Executes a query specified as a JSON string containing the Cypher query,
   * access mode, and optional parameters. This is the primary method for
   * query execution in high-throughput service scenarios.
   *
   * **JSON Format:**
   * @code{.json}
   * {
   *   "query": "MATCH (n:Person) RETURN n.name",
   *   "access_mode": "read",
   *   "parameters": {
   *     "param1": "value1",
   *     "list_param": [1, 2, 3],
   *     "map_param": {"key": "value"}
   *   }
   * }
   * @endcode
   *
   * **Access Modes:**
   * - `"read"` or `"r"`: Read-only query (MATCH without mutations)
   * - `"insert"` or `"i"`: Insert-only operations (CREATE)
   * - `"update"` or `"u"`: Update/delete operations (SET, DELETE, MERGE)
   * - `"schema"` or `"s"`: Schema modification operations (CREATE/DROP labels)
   *
   * **Usage Example:**
   * @code{.cpp}
   * auto guard = service.AcquireSession();
   *
   * // Simple read query
   * auto result = guard->Eval(R"({"query": "MATCH (n) RETURN count(n)"})");
   * if (result.has_value()) {
   *   // Process result
   * }
   *
   * // Parameterized query
   * std::string query = R"({
   *   "query": "MATCH (n:Person {age: $age}) RETURN n",
   *   "access_mode": "read",
   *   "parameters": {"age": 30}
   * })";
   * auto param_result = guard->Eval(query);
   * @endcode
   *
   * @param query JSON string containing query, access_mode, and parameters
   * @return Result containing QueryResult on success, or error status
   */
  result<std::string> Eval(const std::string& query);

  int SessionId() const;

  double eval_duration() const;

  int64_t query_num() const;

 private:
  PropertyGraph& graph_;
  std::shared_ptr<IGraphPlanner> planner_;
  execution::LocalQueryCache pipeline_cache_;
  std::shared_ptr<IVersionManager> version_manager_;
  Allocator& alloc_;
  IWalWriter& logger_;
  const NeugDBConfig& db_config_;
  int thread_id_;

  std::atomic<int64_t> eval_duration_;
  std::atomic<int64_t> query_num_;
};

}  // namespace neug
