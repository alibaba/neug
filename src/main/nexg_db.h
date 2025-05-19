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

#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include "src/engines/graph_db/database/graph_db.h"
#include "src/main/connection.h"
#include "src/main/query_processor.h"
#include "src/planner/dummy_graph_planner.h"
#include "src/planner/graph_planner.h"
#include "src/planner/jni_graph_planner.h"
#include "src/utils/file_utils.h"

namespace gs {

enum class DBMode { READ_ONLY, READ_WRITE };

class NexgDB {
 public:
  static constexpr const char* LOCK_FILE_NAME = "nexgdb.lock";

  NexgDB(const std::string& data_dir, int32_t max_num_threads,
         const std::string& mode, const std::string& planner_kind,
         const std::string& jni_planner_class_path,
         const std::string& planner_config_path,
         const std::string& resource_path) {
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

    if (mode == "read_only" || mode == "r") {
      mode_ = DBMode::READ_ONLY;
    } else if (mode == "read_write" || mode == "rw" || mode == "w" ||
               mode == "wr") {
      mode_ = DBMode::READ_WRITE;
      std::string message;
      if (!try_to_lock_directory(message)) {
        throw std::runtime_error("Failed to lock directory: " + data_dir + "," +
                                 message);
      }
    } else {
      throw std::invalid_argument("Invalid mode: " + mode);
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
    resource_path_ = resource_path;
  }

  ~NexgDB() {
    db_.Close();
    std::remove(get_lock_file_path().c_str());
  }

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
  bool try_to_lock_directory(std::string& error_msg) {
    std::string lock_file_path = get_lock_file_path();
    // If the lock file already exists, it means another process is using the
    // database.
    if (std::filesystem::exists(lock_file_path)) {
      LOG(ERROR) << "Lock file already exists: " << lock_file_path;
      // Read the lock file's content
      std::ifstream lock_file(lock_file_path);
      if (!lock_file.is_open()) {
        LOG(ERROR) << "Failed to open lock file: " << lock_file_path;
        return false;
      }
      std::getline(lock_file, error_msg);
      return false;
    }

    // Create the lock file
    std::ofstream lock_file(lock_file_path);
    if (!lock_file.is_open()) {
      LOG(ERROR) << "Failed to create lock file: " << lock_file_path;
      return false;
    }
    // Write the current process ID to the lock file
    lock_file << "Locked by process ID: " << getpid() << "\n";
    lock_file.close();
    VLOG(10) << "Successfully locked directory: " << config_.data_dir
             << ", lock file: " << lock_file_path;
    return true;
  }

  std::string get_lock_file_path() {
    return config_.data_dir + "/" + LOCK_FILE_NAME;
  }

  std::shared_ptr<IGraphPlanner> create_planner(
      const std::string& planner_kind,
      const std::string& jni_planner_class_path,
      const std::string& planner_config_path) {
    if (planner_kind == "jni") {
      return std::make_shared<JavaGraphPlanner>(planner_config_path,
                                                jni_planner_class_path);
    } else if (planner_kind == "dummy") {
      return std::make_shared<DummyGraphPlanner>();
    } else {
      throw std::invalid_argument("Invalid planner kind: " + planner_kind);
    }
  }

  GraphDBConfig config_;
  GraphDB db_;
  DBMode mode_;

  std::shared_ptr<IGraphPlanner> planner_;
  std::shared_ptr<QueryProcessor> query_processor_;

  std::shared_ptr<Connection> read_write_connection_;
  std::vector<std::shared_ptr<Connection>> read_only_connections_;

  std::mutex connection_mutex_;

  std::string resource_path_;
};
}  // namespace gs

#endif  // SRC_MAIN_NEXG_DB_H_