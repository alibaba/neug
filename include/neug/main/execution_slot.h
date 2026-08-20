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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include <rapidjson/document.h>

#include "neug/compiler/planner/graph_planner.h"
#include "neug/config.h"
#include "neug/execution/execute/query_cache.h"
#include "neug/main/query_result.h"
#include "neug/storages/allocators.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/timestamp_lease.h"
#include "neug/transaction/update_transaction.h"
#include "neug/utils/access_mode.h"
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
class NeugDB;
class ExecutionSlot;
class TpExecutionSlotPool;
class ExtensionManager;

enum class QueryExecutionStrategy : uint8_t {
  kDirect,
  kTransactional,
};

/**
 * @brief Move-only RAII handle for exclusive use of a TP ExecutionSlot.
 *
 * TpExecutionSlotPool injects a noexcept release operation so this handle can
 * return the slot without exposing bthread synchronization to ExecutionSlot.
 */
class ExecutionSlotLease {
 public:
  ExecutionSlotLease() = default;
  ~ExecutionSlotLease();

  ExecutionSlotLease(ExecutionSlotLease&& other) noexcept;
  ExecutionSlotLease& operator=(ExecutionSlotLease&& other) noexcept;

  ExecutionSlotLease(const ExecutionSlotLease&) = delete;
  ExecutionSlotLease& operator=(const ExecutionSlotLease&) = delete;

  ExecutionSlot* get() const { return slot_; }
  ExecutionSlot* operator->() const { return slot_; }
  ExecutionSlot& operator*() const { return *slot_; }
  explicit operator bool() const { return slot_ != nullptr; }

 private:
  using Releaser = void (*)(void*, size_t) noexcept;

  friend class TpExecutionSlotPool;

  ExecutionSlotLease(ExecutionSlot* slot, void* owner, size_t slot_id,
                     Releaser releaser)
      : slot_(slot), owner_(owner), slot_id_(slot_id), releaser_(releaser) {}

  void reset() noexcept;

  ExecutionSlot* slot_{nullptr};
  void* owner_{nullptr};
  size_t slot_id_{0};
  Releaser releaser_{nullptr};
};

/**
 * @brief Database execution slot for high-throughput query execution.
 *
 * ExecutionSlot is a passive core execution context. It owns slot-local query
 * state and borrows database-wide transaction, storage, allocator, and WAL
 * resources.
 *
 * Embedded connections exclusively own one ExecutionSlot. Service mode owns a
 * fixed set through TpExecutionSlotPool and leases them per request.
 *
 * **Usage Example:**
 * @code{.cpp}
 * // Lease execution slot from service
 * auto lease = service.AcquireExecutionSlot();
 *
 * // Execute read query
 * std::string query = R"({
 *   "query": "MATCH (n:Person) RETURN n.name LIMIT 10",
 *   "access_mode": "read"
 * })";
 * auto result = lease->ExecuteTransactionalRequest(query);
 *
 * // Execute write query with parameters
 * std::string insert_query = R"({
 *   "query": "CREATE (n:Person {name: $name})",
 *   "access_mode": "insert",
 *   "parameters": {"name": "Alice"}
 * })";
 * auto write_result = lease->ExecuteTransactionalRequest(insert_query);
 * @endcode
 *
 * **Transaction Types:**
 * - `ReadTransaction`: Read-only snapshot access
 * - `InsertTransaction`: Add new vertices and edges
 * - `UpdateTransaction`: Modify existing graph elements
 * - `CompactTransaction`: Background compaction operations
 *
 * **Concurrency:** An execution slot must not be used concurrently. It is not
 * bound to a physical pthread or bthread worker and may resume on another
 * physical worker after a cooperative yield while retaining the same
 * allocator, cache, and WAL resources.
 *
 * **Lifetime:** All borrowed constructor dependencies must outlive the
 * ExecutionSlot and every transaction created from it. Connection and
 * TpExecutionSlotPool release their slots before NeugDB destroys those shared
 * dependencies.
 *
 * @see NeugDBService for HTTP service wrapper
 * @see TpExecutionSlotPool for execution-slot management
 * @since v0.1.0
 */
class ExecutionSlot {
 public:
  ~ExecutionSlot() {}

  ReadTransaction GetReadTransaction() const;

  InsertTransaction GetInsertTransaction();

  UpdateTransaction GetUpdateTransaction();

  CompactTransaction GetCompactTransaction();

  /**
   * @brief Execute a serialized Cypher request in a transaction.
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
   * auto lease = service.AcquireExecutionSlot();
   *
   * // Simple read query
   * auto result = lease->ExecuteTransactionalRequest(
   *     R"({"query": "MATCH (n) RETURN count(n)"})");
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
   * auto param_result = lease->ExecuteTransactionalRequest(query);
   * @endcode
   *
   * @param request JSON string containing query, access_mode, and parameters
   * @return Serialized QueryResponse on success, or error status
   */
  result<std::string> ExecuteTransactionalRequest(const std::string& request);

  result<QueryResult> ExecuteQuery(const std::string& query_string,
                                   const std::string& access_mode,
                                   const rapidjson::Value& parameters =
                                       rapidjson::Value{rapidjson::kObjectType},
                                   int32_t num_threads = 0);

  std::string GetSchema() const;

  void ClearTemporarySchema();

  int SlotId() const;

  double eval_duration() const;

  int64_t query_num() const;

 private:
  friend class NeugDB;
  friend class TpExecutionSlotPool;

  ExecutionSlot(GraphSnapshotStore& snapshot_store,
                std::shared_ptr<IGraphPlanner> planner,
                std::shared_ptr<execution::GlobalQueryCache> global_query_cache,
                IVersionManager& vm, Allocator& alloc,
                QueryExecutionStrategy execution_strategy,
                IWalWriter* wal_writer,
                CheckpointCoordinator& checkpoint_coordinator,
                ExtensionManager& extension_manager,
                const NeugDBConfig& config_, int slot_id)
      : snapshot_store_(snapshot_store),
        planner_(planner),
        pipeline_cache_(global_query_cache),
        version_manager_(vm),
        alloc_(alloc),
        execution_strategy_(execution_strategy),
        wal_writer_(wal_writer),
        checkpoint_coordinator_(checkpoint_coordinator),
        extension_manager_(extension_manager),
        db_config_(config_),
        slot_id_(slot_id),
        eval_duration_(0),
        query_num_(0) {
    CHECK(execution_strategy_ == QueryExecutionStrategy::kDirect ||
          execution_strategy_ == QueryExecutionStrategy::kTransactional);
    CHECK_EQ(execution_strategy_ == QueryExecutionStrategy::kTransactional,
             wal_writer_ != nullptr);
  }

  result<std::shared_ptr<execution::CacheValue>> prepareQuery(
      const GraphStats& stats, const std::string& query, int32_t num_threads);

  Status validatePlan(AccessMode mode, const physical::ExecutionFlag& flags,
                      bool is_explain) const;

  Status validateAdminRequest(const AdminRequest& request,
                              AccessMode access_mode) const;
  Status executeAdmin(const AdminRequest& request, ExplainMode explain_mode,
                      QueryResponse& response);

  Status executeCore(const std::string& query, AccessMode requested_mode,
                     const rapidjson::Value& parameters, int32_t num_threads,
                     QueryResponse& response);

  GraphSnapshotStore& snapshot_store_;
  std::shared_ptr<IGraphPlanner> planner_;
  execution::LocalQueryCache pipeline_cache_;
  IVersionManager& version_manager_;
  Allocator& alloc_;
  const QueryExecutionStrategy execution_strategy_;
  IWalWriter* const wal_writer_;
  CheckpointCoordinator& checkpoint_coordinator_;
  ExtensionManager& extension_manager_;
  const NeugDBConfig& db_config_;
  int slot_id_;

  std::atomic<int64_t> eval_duration_;
  std::atomic<int64_t> query_num_;
};

}  // namespace neug
