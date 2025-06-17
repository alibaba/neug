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

#ifndef SRC_MAIN_NEXG_DB_H_
#define SRC_MAIN_NEXG_DB_H_

#include <csignal>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include "src/engines/graph_db/database/graph_db.h"
#include "src/main/connection.h"
#include "src/main/file_lock.h"
#include "src/main/query_processor.h"
#include "src/planner/dummy_graph_planner.h"
#include "src/planner/gopt_planner.h"
#include "src/planner/graph_planner.h"
#ifdef BUILD_JNI_PLANNER
#include "src/planner/jni_graph_planner.h"
#endif  // BUILD_JNI_PLANNER
#include "src/utils/file_utils.h"

namespace gs {

void signal_handler(int signal);

void setup_signal_handler();

class NexgDB {
 public:
  NexgDB(const std::string& data_dir, int32_t max_num_threads,
         const std::string& mode, const std::string& planner_kind,
         const std::string& jni_planner_class_path = "",
         const std::string& planner_config_path = "")
      : file_lock_(data_dir) {
    LOG(INFO) << "Creating NexgDB with: " << data_dir << " in " << mode
              << " mode, "
              << " planner: " << planner_kind;
    if (max_num_threads == 0) {
      max_num_threads = std::thread::hardware_concurrency();
    }
    config_.data_dir = data_dir;
    config_.thread_num = max_num_threads;
    config_.memory_level = 0;
    ensure_directory_exists(data_dir);

    if (mode == "read" || mode == "r") {
      mode_ = DBMode::READ_ONLY;
    } else if (mode == "read_write" || mode == "rw" || mode == "w" ||
               mode == "wr" || mode == "write") {
      mode_ = DBMode::READ_WRITE;
    } else {
      throw std::invalid_argument("Invalid mode: " + mode);
    }

    std::string message;
    if (!file_lock_.lock(message, mode_)) {
      throw std::runtime_error("Failed to lock directory: " + data_dir + "," +
                               message);
    }

    auto res = db_.Open(config_);
    if (!res.ok()) {
      throw std::runtime_error("Failed to open database: " +
                               res.status().error_message());
    }
    LOG(INFO) << "Database opened successfully in " << mode << " mode.";
    planner_ = create_planner(planner_kind, jni_planner_class_path,
                              planner_config_path);

    query_processor_ = std::make_shared<QueryProcessor>(
        db_, max_num_threads, mode_ == DBMode::READ_ONLY);
  }

  void close();

  ~NexgDB() { close(); }

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

 private:
  std::shared_ptr<IGraphPlanner> create_planner(
      const std::string& planner_kind,
      const std::string& jni_planner_class_path,
      const std::string& planner_config_path) {
    if (planner_kind == "jni") {
#ifdef BUILD_JNI_PLANNER
      static std::shared_ptr<IGraphPlanner> jni_planner;
      if (jni_planner) {
        VLOG(10) << "Using existing JNI Graph Planner.";
      } else {
        VLOG(10) << "Creating new JNI Graph Planner with class path: "
                 << jni_planner_class_path;
        jni_planner = std::make_shared<JavaGraphPlanner>(
            planner_config_path, jni_planner_class_path);
      }
      return jni_planner;
#else
      throw std::runtime_error(
          "JNI planner is not built. Please build with BUILD_JNI_PLANNER=ON.");
#endif  // BUILD_JNI_PLANNER
    } else if (planner_kind == "dummy") {
      return std::make_shared<DummyGraphPlanner>();
    } else if (planner_kind == "gopt") {
      // Gopt planner is the default planner, so we don't need to create it.
      return std::make_shared<GOptPlanner>(planner_config_path);
    } else {
      throw std::invalid_argument("Invalid planner kind: " + planner_kind);
    }
  }

  GraphDBConfig config_;
  GraphDB db_;
  DBMode mode_;
  FileLock file_lock_;

  std::shared_ptr<IGraphPlanner> planner_;
  std::shared_ptr<QueryProcessor> query_processor_;

  std::shared_ptr<Connection> read_write_connection_;
  std::vector<std::shared_ptr<Connection>> read_only_connections_;

  std::mutex connection_mutex_;
};
}  // namespace gs

#endif  // SRC_MAIN_NEXG_DB_H_