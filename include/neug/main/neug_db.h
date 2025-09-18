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

#ifndef ENGINES_GRAPH_DB_DATABASE_GRAPH_DB_H_
#define ENGINES_GRAPH_DB_DATABASE_GRAPH_DB_H_

#include <dlfcn.h>
#include <stddef.h>
#include <stdint.h>
#include <array>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "neug/compiler/planner/gopt_planner.h"
#include "neug/compiler/planner/graph_planner.h"
#include "neug/config.h"

#include "neug/main/connection_manager.h"
#include "neug/main/file_lock.h"
#include "neug/main/neug_db.h"
#include "neug/main/query_processor.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/loader/loader_factory.h"
#include "neug/storages/loader/loading_config.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/transaction_manager.h"
#include "neug/transaction/update_transaction.h"
#include "neug/transaction/version_manager.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/mmap_array.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/cypher_ddl.pb.h"
#include "neug/generated/proto/plan/cypher_dml.pb.h"
#include "neug/generated/proto/plan/physical.pb.h"
#else
#include "neug/utils/proto/plan/cypher_ddl.pb.h"
#include "neug/utils/proto/plan/cypher_dml.pb.h"
#include "neug/utils/proto/plan/physical.pb.h"
#endif
#include "neug/version.h"

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

namespace gs {

class NeugDB;
class NeugDBSession;
struct SessionLocalContext;
class ColumnBase;
class Encoder;
class IWalParser;
class RefColumnBase;

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
   * @param compact_on_close Whether to compact the graph when closing the graph
   * db.
   * @param checkpoint_on_close Whether to dump the graph when closing the graph
   * db.
   * @return true if successed.
   *
   * @note This function is mainly for python binding.
   */
  bool Open(const std::string& data_dir, int32_t max_num_threads = 0,
            const DBMode mode = DBMode::READ_WRITE,
            const std::string& planner_kind = "gopt", bool warmup = false,
            bool enable_auto_compaction = false, bool compact_csr = true,
            bool compact_on_close = false, bool checkpoint_on_close = true);

  /**
   * @brief Load the graph from data directory.
   * @return true if successed.
   *
   */
  bool Open(const NeugDBConfig& config);

  /**
   * @brief Load the graph from data directory with a given schema.
   * @param schema The schema of the graph. Could be empty if the graph
   * already exists in data_dir.
   * @param config The configuration for opening the graph db.
   * @return true if successed.
   */
  bool Open(const Schema& schema, const NeugDBConfig& config);

  /**
   * @brief Close the current opened graph.
   */
  void Close();

  /**
   * @brief Open a connection to the database.
   * @return A Connection object that can be used to interact with the database.
   * @note We the mode is read-only, this method could be called multiple times.
   * But if the mode is read-write, this method should be called only once.
   * @note Each connection will hold a shared pointer, which means it will share
   * the planner with other connections in the same database.
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

  /** @brief Create a transaction to read vertices and edges.
   *
   * @return graph_dir The directory of graph data.
   */
  ReadTransaction GetReadTransaction(int thread_id = 0);

  /** @brief Create a transaction to insert vertices and edges with a default
   * allocator.
   *
   * @return InsertTransaction
   */
  InsertTransaction GetInsertTransaction(int thread_id = 0);

  /** @brief Create a transaction to update vertices and edges.
   *
   * @param alloc Allocator to allocate memory for graph.
   * @return UpdateTransaction
   */
  UpdateTransaction GetUpdateTransaction(int thread_id = 0);

  /** @brief Create a transaction to compact the graph.
   *
   * @return CompactTransaction
   */
  CompactTransaction GetCompactTransaction(int thread_id = 0);

  inline const PropertyGraph& graph() const { return graph_; }
  inline PropertyGraph& graph() { return graph_; }

  inline const Schema& schema() const { return graph_.schema(); }

  NeugDBSession& GetSession(int thread_id);
  const NeugDBSession& GetSession(int thread_id) const;

  int SessionNum() const;

  std::string work_dir() const { return work_dir_; }

  inline const NeugDBConfig& config() const { return config_; }

  inline std::shared_ptr<IGraphPlanner> GetPlanner() const { return planner_; }

  inline const char* Version() const { return TOSTRING(NEUG_VERSION_STRING); }

  /**
   * @brief Switch the graph db to TP mode.
   * This method should be called before starting the server to ensure that
   * the version manager is appropriate for transactional processing workloads.
   */
  void SwitchToTPMode();

  /**
   * @brief Switch the graph db to AP mode.
   * This method should be called before starting the server to ensure that
   * the version manager is appropriate for analytical processing workloads.
   */
  void SwitchToAPMode();

 private:
  void preprocessConfig();
  void openGraphAndSchema();
  void ingestWals();
  void ingestWals(IWalParser& parser, const std::string& work_dir,
                  MemoryStrategy allocator_strategy, int thread_num);
  void startMonitorIfNeeded();
  void startAutoCompactionIfNeeded();
  void startCompactThreadIfNeeded();
  void initVersionManager();
  void initTransactionManager();
  void initPlannerAndQueryProcessor();
  void initAppManager();
  void createCheckpoint();

  bool registerApp(const std::string& path, uint8_t index = 0);

  size_t getExecutedQueryNum() const;

  friend class NeugDBSession;

  timestamp_t last_compaction_ts_;
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
  std::shared_ptr<IVersionManager> version_manager_;
  std::unique_ptr<TransactionManager> txn_manager_;
  std::shared_ptr<IGraphPlanner> planner_;
  std::shared_ptr<QueryProcessor> query_processor_;
  std::unique_ptr<ConnectionManager> connection_manager_;

  std::thread monitor_thread_;
  std::thread compact_thread_;
  bool monitor_thread_running_ = false;
  bool compact_thread_running_ = false;

  std::mutex mutex_;
};

}  // namespace gs

#endif  // ENGINES_GRAPH_DB_DATABASE_GRAPH_DB_H_
