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

#ifndef SRC_MAIN_NEUG_DB_H_
#define SRC_MAIN_NEUG_DB_H_

#include <chrono>
#include <csignal>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include "neug/engines/graph_db/database/graph_db.h"
#include "neug/main/connection.h"
#include "neug/main/file_lock.h"
#include "neug/main/query_processor.h"
#include "neug/planner/dummy_graph_planner.h"
#include "neug/planner/gopt_planner.h"
#include "neug/planner/graph_planner.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/http_handler_manager.h"
#ifdef BUILD_HTTP_SERVER
#include "neug/engines/brpc_server/brpc_http_hdl_mgr.h"
#endif  // BUILD_HTTP_SERVER

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

namespace gs {

void signal_handler(int signal);

void setup_signal_handler();

class NeugDB {
 public:
  NeugDB(const std::string& data_dir, int32_t max_num_threads = 0,
         const std::string& mode = "read_write",
         const std::string& planner_kind = "gopt",
         const std::string& planner_config_path = "")
      : file_lock_(data_dir), planner_config_path_(planner_config_path) {
    if (max_num_threads == 0) {
      max_num_threads = std::thread::hardware_concurrency();
    }
    std::string db_dir = data_dir;
    if (db_dir.empty() || db_dir == ":memory" || db_dir == ":memory:") {
      std::string db_dir_prefix;
      char* prefix_env = std::getenv("NEUG_DB_TMP_DIR");
      if (prefix_env) {
        db_dir_prefix = std::string(prefix_env);
      } else {
        db_dir_prefix = "/tmp";
      }
      std::stringstream ss;
      auto now = std::chrono::system_clock::now();
      auto duration = now.time_since_epoch();
      ss << "neug_db_"
         << std::chrono::duration_cast<std::chrono::milliseconds>(duration)
                .count();
      db_dir = db_dir_prefix + "/" + ss.str();
      is_pure_memory_ = true;
      file_lock_ = FileLock(db_dir);
      LOG(INFO) << "Creating temp NeugDB with: " << db_dir << " in " << mode
                << " mode, "
                << " planner: " << planner_kind;
    } else {
      is_pure_memory_ = false;
      LOG(INFO) << "Creating NeugDB with: " << db_dir << " in " << mode
                << " mode, "
                << " planner: " << planner_kind;
    }
    config_.data_dir = db_dir;
    config_.thread_num = max_num_threads;
    config_.memory_level = 1;
    ensure_directory_exists(db_dir);

    if (mode == "read" || mode == "r" || mode == "read-only" ||
        mode == "read_only") {
      mode_ = DBMode::READ_ONLY;
    } else if (mode == "read_write" || mode == "rw" || mode == "w" ||
               mode == "wr" || mode == "write" || mode == "readwrite" ||
               mode == "read-write") {
      mode_ = DBMode::READ_WRITE;
    } else {
      THROW_INVALID_ARGUMENT_EXCEPTION("Invalid mode: " + mode);
    }

    std::string message;
    if (!file_lock_.lock(message, mode_)) {
      THROW_IO_EXCEPTION("Failed to lock directory: " + db_dir + "," + message);
    }

    auto res = db_.Open(config_);
    if (!res.ok()) {
      THROW_IO_EXCEPTION("Failed to open database: " +
                         res.status().error_message());
    }
    LOG(INFO) << "Database opened successfully in " << mode << " mode.";
    planner_ = create_planner(planner_kind, planner_config_path);
    planner_->update_meta(db_.schema().to_yaml().value());
    planner_->update_statistics(db_.get_statistics_json());

    query_processor_ = std::make_shared<QueryProcessor>(
        db_, max_num_threads, mode_ == DBMode::READ_ONLY);
  }

  void close();

  ~NeugDB() { close(); }

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

  /**
   * @brief Start the database server.
   * @param port The port to listen on, default is 10000.
   * @param host The host to bind to, default is "localhost".
   * @return A string containing the URL of the server.
   * @note This method will block until the server is stopped. It will launch
   * the brpc service serving both rpc and http requests.
   *
   */
  std::string serve(int port = 10000, const std::string& host = "localhost");

  /**
   * @brief Stop the database server.
   * @note This method will stop the server and release resources.
   */
  void stop_serving();

  inline GraphDB& db() { return db_; }

 private:
  std::shared_ptr<IGraphPlanner> create_planner(
      const std::string& planner_kind, const std::string& planner_config_path) {
    if (planner_kind == "dummy") {
      return std::make_shared<DummyGraphPlanner>();
    } else if (planner_kind == "gopt") {
      // Gopt planner is the default planner, so we don't need to create it.
      return std::make_shared<GOptPlanner>(planner_config_path);
    } else {
      THROW_INVALID_ARGUMENT_EXCEPTION("Invalid planner kind: " + planner_kind);
    }
  }

  GraphDBConfig config_;
  GraphDB db_;
  DBMode mode_;
  FileLock file_lock_;
  bool is_pure_memory_;

  std::string planner_config_path_;
  std::shared_ptr<IGraphPlanner> planner_;
  std::shared_ptr<QueryProcessor> query_processor_;

  std::shared_ptr<Connection> read_write_connection_;
  std::vector<std::shared_ptr<Connection>> read_only_connections_;

  std::mutex connection_mutex_;

  // For serving the database
  server::ServiceConfig service_config_;
  std::unique_ptr<server::IHttpHandlerManager> hdl_mgr_;
};
}  // namespace gs

#endif  // SRC_MAIN_NEUG_DB_H_