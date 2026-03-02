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

#include <stddef.h>
#include <stdint.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "neug/config.h"
#include "neug/execution/execute/query_cache.h"
#include "neug/generated/proto/plan/cypher_ddl.pb.h"
#include "neug/generated/proto/plan/cypher_dml.pb.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/main/connection.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/transaction/compact_transaction.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/update_transaction.h"
#include "neug/utils/allocators.h"
#include "neug/utils/mmap_array.h"
#include "neug/utils/property/types.h"
#include "neug/version.h"

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

namespace neug {
class NeugDBService;
class AppManager;
class Connection;
class ConnectionManager;
class FileLock;
class IGraphPlanner;
class IWalParser;
class NeugDBSession;
class QueryProcessor;
class Schema;

/**
 * @brief Core database engine for NeuG graph database system.
 *
 * NeugDB serves as the **primary entry point** for all NeuG graph database
 * operations. It provides a complete lifecycle management API including
 * database initialization, query execution, and graceful shutdown.
 *
 * **Usage Example:**
 * @code{.cpp}
 * // Create and open database
 * neug::NeugDB db;
 * db.Open("/path/to/data", 4);  // 4 threads
 *
 * // Create connection and execute query
 * auto conn = db.Connect();
 * auto result = conn->Query("MATCH (n:Person) RETURN n LIMIT 10");
 *
 * // Process results
 * for (auto& record : result.value()) {
 *   std::cout << record.ToString() << std::endl;
 * }
 *
 * // Close database (persists data)
 * db.Close();
 * @endcode
 *
 * **Key Components:**
 * - PropertyGraph: Underlying graph data storage engine
 * - QueryProcessor: Cypher query compilation and execution
 * - ConnectionManager: Client connection pool management
 * - IGraphPlanner: Query optimization (GOPT or Greedy planner)
 *
 * **Database Modes:**
 * - `DBMode::READ_ONLY`: Read-only access for analytics workloads
 * - `DBMode::READ_WRITE`: Full transactional read/write access
 *
 * **Thread Safety:** This class is thread-safe. Multiple connections can
 * execute queries concurrently. The ConnectionManager handles thread
 * synchronization internally.
 *
 * **Resource Management:**
 * - File locking prevents concurrent database access from multiple processes
 * - Automatic WAL (Write-Ahead Log) for crash recovery
 * - Configurable checkpoint and compaction on close
 *
 * @note For query execution, obtain a Connection via Connect() method.
 * @note Always call Close() before destroying the NeugDB instance to ensure
 *       data persistence.
 *
 * @see Connection For executing queries against the database
 * @see PropertyGraph For direct graph storage access
 * @see NeugDBConfig For configuration options
 *
 * @since v0.1.0
 */
class NeugDB {
 public:
  NeugDB();
  ~NeugDB();

  /**
   * @brief Open the database from persistent storage.
   *
   * Initializes and opens the NeuG database from the specified data directory.
   * This method loads the graph schema, vertex/edge data, and initializes
   * the query processor and planner.
   *
   * **Data Directory Structure:**
   * The data_dir should contain:
   * - `graph.yaml`: Schema definition file
   * - `snapshot/`: Vertex and edge data files
   * - `wal/`: Write-ahead log files (optional, for recovery)
   *
   * **Usage Example:**
   * @code{.cpp}
   * neug::NeugDB db;
   *
   * // Simple open with defaults
   * db.Open("/path/to/graph");
   *
   * // Open with custom settings (8 threads, read-write mode, GOPT planner)
   * db.Open("/path/to/graph", 8, neug::DBMode::READ_WRITE, "gopt");
   * @endcode
   *
   * @param data_dir Path to the graph data directory
   * @param max_num_threads Maximum threads for concurrent operations.
   *        If 0, uses hardware concurrency (number of CPU cores)
   * @param mode Database access mode (READ_ONLY or READ_WRITE)
   * @param planner_kind Query planner type: "gopt" (Graph Optimizer) or
   * "greedy"
   * @param enable_auto_compaction Enable background auto-compaction thread
   * @param compact_csr Compact CSR structures during auto-compaction
   * @param compact_on_close Perform compaction when closing database
   * @param checkpoint_on_close Create checkpoint (persist data) when closing
   *
   * @return true if database opened successfully, false otherwise
   *
   * @note This overload is primarily designed for Python bindings.
   * @note For C++ usage, prefer the config-based Open(NeugDBConfig&) overload.
   *
   * @see NeugDBConfig For detailed configuration options
   * @see Close For proper database shutdown
   *
   * @since v0.1.0
   */
  bool Open(const std::string& data_dir, int32_t max_num_threads = 0,
            const DBMode mode = DBMode::READ_WRITE,
            const std::string& planner_kind = "gopt",
            bool enable_auto_compaction = false, bool compact_csr = true,
            bool compact_on_close = true, bool checkpoint_on_close = true);

  /**
   * @brief Open the database with a configuration object.
   *
   * Opens the database using a NeugDBConfig structure that provides
   * comprehensive configuration options.
   *
   * **Usage Example:**
   * @code{.cpp}
   * neug::NeugDBConfig config;
   * config.data_dir = "/path/to/graph";
   * config.thread_num = 8;
   * config.mode = neug::DBMode::READ_WRITE;
   * config.memory_level = 2;  // Use hugepages
   * config.enable_auto_compaction = true;
   *
   * neug::NeugDB db;
   * db.Open(config);
   * @endcode
   *
   * @param config Configuration object with all database settings
   *
   * @return true if database opened successfully, false otherwise
   *
   * @see NeugDBConfig For all available configuration options
   *
   * @since v0.1.0
   */
  bool Open(const NeugDBConfig& config);

  /**
   * @brief Close the database and release all resources.
   *
   * Performs a graceful shutdown of the database. Depending on configuration:
   * - Creates checkpoint if checkpoint_on_close is enabled
   * - Performs compaction if compact_on_close is enabled
   * - Closes all open connections
   * - Releases file locks
   *
   * **Important:** Always call Close() before destroying the NeugDB instance
   * to ensure data integrity and proper resource cleanup.
   *
   * **Usage Example:**
   * @code{.cpp}
   * neug::NeugDB db;
   * db.Open("/path/to/data");
   *
   * // ... perform operations ...
   *
   * db.Close();  // Persist data and cleanup
   * @endcode
   *
   * @note This method is idempotent - calling it multiple times is safe.
   * @note After closing, the database cannot be reopened. Create a new
   *       NeugDB instance to open the database again.
   *
   * @since v0.1.0
   */
  void Close();

  /**
   * @brief Check if the database is closed.
   * @return true if the database is closed.
   */
  inline bool IsClosed() const { return closed_.load(); }

  /**
   * @brief Create a new connection to the database for query execution.
   *
   * Creates and returns a Connection object that can be used to execute
   * Cypher queries against the database. The connection shares the query
   * planner and processor with other connections from the same database.
   *
   * **Usage Example:**
   * @code{.cpp}
   * auto conn = db.Connect();
   * auto result = conn->Query("MATCH (n) RETURN count(n)");
   * if (result.has_value()) {
   *     std::cout << "Query succeeded" << std::endl;
   * }
   * conn->Close();  // Optional: auto-closed on destruction
   * @endcode
   *
   * @return std::shared_ptr<Connection> A shared pointer to the new Connection
   *
   * @note In READ_ONLY mode, multiple connections can be created.
   * @note In READ_WRITE mode, only one write connection is allowed.
   * @note Connections share the planner instance for efficiency.
   *
   * @throws std::runtime_error if database is not open or closed
   *
   * @see Connection::Query For executing Cypher queries
   * @see Connection::Close For closing the connection
   *
   * @since v0.1.0
   */
  std::shared_ptr<Connection> Connect();

  /**
   * @brief Remove a connection from the database.
   * @param conn The connection to be removed.
   * @note This method is used to remove a connection when it is closed, to
   * remove the handle from the database.
   * @note This method is not thread-safe, so it should be called only when
   * the connection is closed. And should be only called internally.
   */
  void RemoveConnection(std::shared_ptr<Connection> conn);

  /**
   * @brief Remove all connection from the database.
   * @note This method is used to remove all connection when tp svc created, to
   * remove the handle from the database.
   */
  void CloseAllConnection();

  inline const PropertyGraph& graph() const { return graph_; }
  inline PropertyGraph& graph() { return graph_; }

  inline const Schema& schema() const { return graph_.schema(); }

  std::string work_dir() const { return work_dir_; }

  inline const NeugDBConfig& config() const { return config_; }

  inline std::shared_ptr<IGraphPlanner> GetPlanner() const { return planner_; }

  inline std::shared_ptr<execution::GlobalQueryCache> GetQueryCache() const {
    return global_query_cache_;
  }

  inline const char* Version() const { return TOSTRING(NEUG_VERSION_STRING); }

 private:
  void preprocessConfig();
  void initAllocators();
  void openGraphAndSchema();
  void ingestWals();
  void ingestWals(IWalParser& parser, const std::string& work_dir);
  void initPlannerAndQueryProcessor();
  /**
   * @brief Create a checkpoint of the current graph.
   * @param force_compaction Whether to force compaction before creating the
   * checkpoint.
   * @note force_compaction will override the config_.compact_on_close setting.
   * force_compaction should be set to true after the database is recovered from
   * wals to remove the tombstone type/data, otherwise the graph size will keep
   * growing.
   */
  void createCheckpoint(bool force_compaction = false, bool reopen = true);

  friend class NeugDBSession;
  friend class neug::NeugDBService;

  timestamp_t last_compaction_ts_;
  timestamp_t last_ts_;
  // Configuration and settings
  std::atomic<bool> closed_;
  bool is_pure_memory_;
  int thread_num_;
  NeugDBConfig config_;
  std::string work_dir_;
  std::unique_ptr<FileLock> file_lock_;

  // The property graph and transaction controls
  PropertyGraph graph_;
  std::shared_ptr<IGraphPlanner> planner_;
  std::shared_ptr<QueryProcessor> query_processor_;
  std::unique_ptr<ConnectionManager> connection_manager_;
  std::shared_ptr<execution::GlobalQueryCache> global_query_cache_;

  std::mutex mutex_;
  std::vector<std::shared_ptr<Allocator>>
      allocators_;  // Allocators for each thread
};

}  // namespace neug
