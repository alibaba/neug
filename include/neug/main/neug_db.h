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

namespace server {
class NeugDBService;
}  // namespace server

namespace gs {
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
 * NeugDB serves as the main entry point and central management system for the
 * NeuG graph database. It coordinates all major components including storage,
 * transactions, query processing, and application management.
 *
 * **Key Components:**
 * - PropertyGraph for graph data storage and schema management
 * - QueryProcessor for Cypher query execution
 * - ConnectionManager for client connection handling
 *
 * **Database Modes:**
 * - READ_ONLY: Read-only access for query operations
 * - READ_WRITE: Full read/write access with transaction support
 *
 * **Thread Safety:** This class is thread-safe. Multiple sessions can
 * access the database concurrently through the connection manager.
 *
 * **Resource Management:**
 * - Handles file locking to prevent concurrent database access
 * - Provides graceful shutdown with optional data dumping
 *
 * @note This is the primary interface for database lifecycle management.
 *       For query execution, use NeugDBSession obtained through
 * CreateSession().
 *
 * @since v0.1.0
 */
class NeugDB {
 public:
  NeugDB();
  ~NeugDB();

  /**
   * @brief Load the graph from data directory.
   * @param data_dir The directory of graph data.
   * @param max_num_threads The maximum number of threads for graph db
   * concurrency. If it is 0, it will be set to the number of hardware cores.
   * @param mode The mode of opening graph db, could be "read_only" or
   * "read_write".
   * @param planner_kind The kind of graph planner, could be "gopt" or
   * "greedy"
   * @param warmup Whether to warm up the graph in memory.
   * @param enable_auto_compaction Whether to enable auto compaction thread.
   * @param compact_csr Whether to compact the csr when doing auto compaction.
   * @param compact_on_close Whether to compact the graph when closing the
   * graph db.
   * @param checkpoint_on_close Whether to dump the graph when closing the
   * graph db.
   * @return true if successed.
   *
   * @note This function is mainly for python binding.
   */
  bool Open(const std::string& data_dir, int32_t max_num_threads = 0,
            const DBMode mode = DBMode::READ_WRITE,
            const std::string& planner_kind = "gopt", bool warmup = false,
            bool enable_auto_compaction = false, bool compact_csr = true,
            bool compact_on_close = true, bool checkpoint_on_close = true);

  /**
   * @brief Load the graph from data directory.
   * @return true if successed.
   *
   */
  bool Open(const NeugDBConfig& config);

  /**
   * @brief Close the current opened graph.
   */
  void Close();

  /**
   * @brief Check if the database is closed.
   * @return true if the database is closed.
   */
  inline bool IsClosed() const { return closed_.load(); }

  /**
   * @brief Open a connection to the database.
   * @return A Connection object that can be used to interact with the
   * database.
   * @note We the mode is read-only, this method could be called multiple
   * times. But if the mode is read-write, this method should be called only
   * once.
   * @note Each connection will hold a shared pointer, which means it will
   * share the planner with other connections in the same database.
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

  inline const PropertyGraph& graph() const { return graph_; }
  inline PropertyGraph& graph() { return graph_; }

  inline const Schema& schema() const { return graph_.schema(); }

  std::string work_dir() const { return work_dir_; }

  inline const NeugDBConfig& config() const { return config_; }

  inline std::shared_ptr<IGraphPlanner> GetPlanner() const { return planner_; }

  inline const char* Version() const { return TOSTRING(NEUG_VERSION_STRING); }

  std::shared_ptr<AppManager> GetAppManager() const { return app_manager_; }

 private:
  void preprocessConfig();
  void initAllocators();
  void openGraphAndSchema();
  void ingestWals();
  void ingestWals(IWalParser& parser, const std::string& work_dir);
  void initPlannerAndQueryProcessor();
  void initAppManager();
  void createCheckpoint();

  bool registerApp(const std::string& path, uint8_t index = 0);

  friend class NeugDBSession;
  friend class server::NeugDBService;

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
  std::shared_ptr<AppManager> app_manager_;
  std::shared_ptr<IGraphPlanner> planner_;
  std::shared_ptr<QueryProcessor> query_processor_;
  std::unique_ptr<ConnectionManager> connection_manager_;

  std::mutex mutex_;
  std::vector<std::shared_ptr<Allocator>>
      allocators_;  // Allocators for each thread
};

}  // namespace gs
