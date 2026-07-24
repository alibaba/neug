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
#include <cstdint>
#include <memory>
#include <string>

#include "neug/compiler/planner/graph_planner.h"
#include "neug/config.h"
#include "neug/execution/execute/query_cache.h"
#include "neug/storages/allocators.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/update_transaction.h"
#include "neug/utils/result.h"

namespace neug {

class GraphSnapshotStore;
class IWalWriter;
class ColumnBase;
class Encoder;
class PropertyGraph;
class RefColumnBase;
class AppManager;
class IVersionManager;

/**
 * @brief Database session for executing queries in high-throughput scenarios.
 *
 * Session is a passive core execution context. It owns session-local query
 * state and borrows database-wide transaction, storage, allocator, and WAL
 * resources.
 *
 * Service mode typically acquires sessions from a SessionPool. Other drivers
 * may create and drive sessions without introducing their thread runtime into
 * this class.
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
 * **Concurrency:** A session must not be used concurrently. It is not bound to
 * a physical pthread or bthread worker and may resume on another physical
 * worker after a cooperative yield while retaining the same logical session,
 * allocator, and WAL resources.
 *
 * **Lifetime:** All borrowed constructor dependencies must outlive the Session
 * and every transaction created from it. A Session must be destroyed before
 * its owning NeugDB is prepared for serving, closed, or destroyed.
 *
 * @see NeugDBService for HTTP service wrapper
 * @see SessionPool for session management
 * @since v0.1.0
 */
class Session {
 public:
  static constexpr int32_t MAX_RETRY = 3;
  Session(GraphSnapshotStore& snapshot_store,
          std::shared_ptr<IGraphPlanner> planner,
          std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
          IVersionManager& vm, Allocator& alloc, IWalWriter& wal_writer,
          const NeugDBConfig& config_, int session_id)
      : snapshot_store_(snapshot_store),
        planner_(planner),
        pipeline_cache_(global_query_cache),
        version_manager_(vm),
        alloc_(alloc),
        wal_writer_(wal_writer),
        db_config_(config_),
        session_id_(session_id),
        eval_duration_(0),
        query_num_(0) {}
  ~Session() {}

  ReadTransaction GetReadTransaction() const;

  InsertTransaction GetInsertTransaction();

  UpdateTransaction GetUpdateTransaction();

  CompactTransaction GetCompactTransaction();

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
  GraphSnapshotStore& snapshot_store_;
  std::shared_ptr<IGraphPlanner> planner_;
  execution::LocalQueryCache pipeline_cache_;
  IVersionManager& version_manager_;
  Allocator& alloc_;
  IWalWriter& wal_writer_;
  const NeugDBConfig& db_config_;
  int session_id_;

  std::atomic<int64_t> eval_duration_;
  std::atomic<int64_t> query_num_;
};

}  // namespace neug
