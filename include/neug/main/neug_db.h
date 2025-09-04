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
#include "neug/main/app/app_base.h"
#include "neug/main/connection.h"
#include "neug/main/file_lock.h"
#include "neug/main/neug_db.h"
#include "neug/main/query_processor.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/loader/loader_factory.h"
#include "neug/storages/loader/loading_config.h"
#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/single_edge_insert_transaction.h"
#include "neug/transaction/single_vertex_insert_transaction.h"
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

struct NeugDBConfig {
  NeugDBConfig()
      : mode(DBMode::READ_WRITE),
        thread_num(1),
        warmup(false),
        enable_monitoring(false),
        enable_auto_compaction(false),
        memory_level(1),
        wal_uri("") {}
  NeugDBConfig(const Schema& schema_, const std::string& data_dir_,
               const std::string& compiler_path_ = "", int thread_num_ = 1)
      : mode(DBMode::READ_WRITE),
        schema(schema_),
        data_dir(data_dir_),
        compiler_path(compiler_path_),
        thread_num(thread_num_),
        warmup(false),
        enable_monitoring(false),
        enable_auto_compaction(false),
        memory_level(1),
        wal_uri(""),
        dump_on_close(false) {}

  // Create without schema
  NeugDBConfig(const std::string& data_dir_,
               const std::string& compiler_path_ = "", int thread_num_ = 1)
      : mode(DBMode::READ_WRITE),
        data_dir(data_dir_),
        compiler_path(compiler_path_),
        thread_num(thread_num_),
        warmup(false),
        enable_monitoring(false),
        enable_auto_compaction(false),
        memory_level(1),
        wal_uri(""),
        dump_on_close(false) {}

  DBMode mode;
  Schema schema;
  std::string data_dir;
  std::string compiler_path;
  std::string planner_kind;  // currently only gopt
  int thread_num;
  bool warmup;
  bool enable_monitoring;
  bool enable_auto_compaction;

  /*
    0 - sync with disk;
    1 - mmap virtual memory;
    2 - preferring hugepages;
    3 - force hugepages;
  */
  int memory_level;
  std::string wal_uri;  // Indicate the where shall we store the wal files.
  // could be file://{GRAPH_DATA_DIR}/wal or other scheme
  // that interactive supports
  bool dump_on_close;  // whether dump the graph when
                       // closing the graph db.
};

class NeugDB {
 public:
  NeugDB();
  ~NeugDB();

  /**
   * @brief Load the graph from data directory.
   * @param data_dir The directory of graph data.
   * @param schema The schema of the graph. Could be empty if the graph
   * already exists in data_dir.
   * @param max_num_threads The maximum number of threads for graph db
   * concurrency. If it is 0, it will be set to the number of hardware cores.
   * @param mode The mode of opening graph db, could be "read_only" or
   * "read_write".
   * @param planner_kind The kind of graph planner, could be "gopt" or "greedy"
   * @return true if successed.
   *
   * @note This function is mainly for python binding.
   */
  bool Open(const std::string& data_dir, const Schema& schema,
            int32_t max_num_threads = 0, const DBMode mode = DBMode::READ_WRITE,
            const std::string& planner_kind = "gopt", bool warmup = false,
            bool enable_auto_compaction = false, bool dump_on_close = false);

  bool Open(const std::string& data_dir, int32_t max_num_threads = 0,
            const DBMode mode = DBMode::READ_WRITE,
            const std::string& planner_kind = "gopt", bool warmup = false,
            bool enable_auto_compaction = false, bool dump_on_close = false);

  bool Open(const NeugDBConfig& config);

  /**
   * @brief Close the current opened graph.
   */
  void Close();

  /**
   * @brief Open a connection to the database.
   * @return A Connection object that can be used to interact with the database.
   *
   * @note We the mode is read-only, this method could be called multiple times.
   * But if the mode is read-write, this method should be called only once.
   *
   * @note Each connection will hold a shared pointer, which means it will share
   * the planner with other connections in the same database.
   */
  std::shared_ptr<Connection> connect();

  /**
   * @brief Remove a connection from the database.
   * @param conn The connection to be removed.
   * @note This method is used to remove a connection when it is closed, to
   * remove the handle from the database.
   * @note This method is not thread-safe, so it should be called only when the
   * connection is closed. And should be only called internally.
   */
  void remove_connection(std::shared_ptr<Connection> conn);

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

  /** @brief Create a transaction to insert a single vertex.
   *
   * @param alloc Allocator to allocate memory for graph.
   * @return SingleVertexInsertTransaction
   */
  SingleVertexInsertTransaction GetSingleVertexInsertTransaction(
      int thread_id = 0);

  /** @brief Create a transaction to insert a single edge.
   *
   * @param alloc Allocator to allocate memory for graph.
   * @return
   */
  SingleEdgeInsertTransaction GetSingleEdgeInsertTransaction(int thread_id = 0);

  /** @brief Create a transaction to update vertices and edges.
   *
   * @param alloc Allocator to allocate memory for graph.
   * @return UpdateTransaction
   */
  UpdateTransaction GetUpdateTransaction(int thread_id = 0);

  inline const PropertyGraph& graph() const { return graph_; }
  inline PropertyGraph& graph() { return graph_; }

  inline const Schema& schema() const { return graph_.schema(); }

  AppWrapper CreateApp(uint8_t app_type, int thread_id);

  void GetAppInfo(Encoder& result);

  NeugDBSession& GetSession(int thread_id);
  const NeugDBSession& GetSession(int thread_id) const;

  int SessionNum() const;

  void UpdateCompactionTimestamp(timestamp_t ts);
  timestamp_t GetLastCompactionTimestamp() const;

  std::string work_dir() const { return work_dir_; }

  inline const NeugDBConfig& config() const { return config_; }

  inline std::shared_ptr<IGraphPlanner> GetPlanner() const { return planner_; }

  inline const char* Version() const { return TOSTRING(NEUG_VERSION_STRING); }

 private:
  void preprocessConfig();
  void openGraphAndSchema();
  void startMonitorIfNeeded();
  void startAutoCompactionIfNeeded();
  void startCompactThreadIfNeeded();
  void openWalAndCreateContexts();
  void initPlannerAndQueryProcessor();
  void outputCypherProfiles(const std::string& prefix);

  bool registerApp(const std::string& path, uint8_t index = 0);

  void ingestWals(IWalParser& parser, const std::string& work_dir,
                  int thread_num);

  void initApps(
      const std::unordered_map<std::string, std::pair<std::string, uint8_t>>&
          plugins);

  void showAppMetrics() const;

  size_t getExecutedQueryNum() const;

  friend class NeugDBSession;

  // Configuration and settings
  std::atomic<bool> closed_;
  bool is_pure_memory_;
  int thread_num_;
  NeugDBConfig config_;
  std::string work_dir_;
  timestamp_t last_compaction_ts_;

  // The property graph and transaction controls
  PropertyGraph graph_;
  SessionLocalContext* contexts_;
  VersionManager version_manager_;
  std::unique_ptr<FileLock> file_lock_;
  std::shared_ptr<IGraphPlanner> planner_;
  std::shared_ptr<QueryProcessor> query_processor_;
  std::shared_ptr<Connection> read_write_connection_;
  std::vector<std::shared_ptr<Connection>> read_only_connections_;
  std::mutex connection_mutex_;

  std::array<std::string, 256> app_paths_;
  std::array<std::shared_ptr<AppFactoryBase>, 256> app_factories_;

  std::thread monitor_thread_;
  std::thread compact_thread_;
  bool monitor_thread_running_ = false;
  bool compact_thread_running_ = false;
};

}  // namespace gs

#endif  // ENGINES_GRAPH_DB_DATABASE_GRAPH_DB_H_
