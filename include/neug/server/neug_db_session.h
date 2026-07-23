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
class CheckpointCoordinator;
class IVersionManager;

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
  NeugDBSession(GraphSnapshotStore& snapshot_store,
                std::shared_ptr<IGraphPlanner> planner,
                std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
                std::shared_ptr<IVersionManager> vm, Allocator& alloc,
                IWalWriter& wal_writer,
                CheckpointCoordinator& checkpoint_coordinator,
                const NeugDBConfig& config_, int thread_id)
      : snapshot_store_(snapshot_store),
        planner_(planner),
        pipeline_cache_(global_query_cache),
        version_manager_(vm),
        alloc_(alloc),
        wal_writer_(wal_writer),
        checkpoint_coordinator_(checkpoint_coordinator),
        db_config_(config_),
        thread_id_(thread_id),
        eval_duration_(0),
        query_num_(0) {}
  ~NeugDBSession() {}

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

  // Invalidate the global query cache through this session's local cache,
  // mirroring how update transactions invalidate it on schema change. Any
  // session can initiate this; every other session discards its local cache
  // lazily on the next version check.
  void InvalidateGlobalQueryCache();

 private:
  UpdateTimestampGuard getUpdateTimestampGuard();
  UpdateTransaction createUpdateTransaction(
      UpdateTimestampGuard timestamp_guard);

  GraphSnapshotStore& snapshot_store_;
  std::shared_ptr<IGraphPlanner> planner_;
  execution::LocalQueryCache pipeline_cache_;
  std::shared_ptr<IVersionManager> version_manager_;
  Allocator& alloc_;
  IWalWriter& wal_writer_;
  // A checkpoint is a database-wide operation initiated by one session. The
  // session drives the coordinator directly; service-owned runtime state
  // (WAL rotation, cache invalidation) is installed through the hooks
  // registered on the coordinator at service startup.
  CheckpointCoordinator& checkpoint_coordinator_;
  const NeugDBConfig& db_config_;
  int thread_id_;

  std::atomic<int64_t> eval_duration_;
  std::atomic<int64_t> query_num_;
};

}  // namespace neug
