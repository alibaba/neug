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

#include "src/main/nexg_db.h"

namespace gs {

void signal_handler(int signal) {
  LOG(INFO) << "Received signal " << signal << ", exiting...";
  // support SIGKILL, SIGINT, SIGTERM
  if (signal == SIGKILL || signal == SIGINT || signal == SIGTERM ||
      signal == SIGSEGV || signal == SIGABRT) {
    LOG(ERROR) << "Received signal " << signal << ", Remove all filelocks";
    // remove all files in work_dir
    gs::FileLock::CleanupAllLocks();
    exit(signal);
  } else {
    LOG(ERROR) << "Received unexpected signal " << signal << ", exiting...";
    exit(1);
  }
}

void setup_signal_handler() {
  // Register handlers for SIGKILL, SIGINT, SIGTERM, SIGSEGV, SIGABRT
  // LOG(FATAL) cause SIGABRT
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);
  std::signal(SIGKILL, signal_handler);
  std::signal(SIGSEGV, signal_handler);
  std::signal(SIGABRT, signal_handler);
}

void NexgDB::close() {
  LOG(INFO) << "Closing NexgDB.";
  if (mode_ == DBMode::READ_WRITE) {
    file_lock_.unlock();
  }
  db_.Close();
}

std::shared_ptr<Connection> NexgDB::connect() {
  if (mode_ == DBMode::READ_ONLY) {
    auto conn = std::make_shared<Connection>(db_, planner_, query_processor_,
                                             resource_path_);
    read_only_connections_.push_back(conn);
    return conn;
  } else if (mode_ == DBMode::READ_WRITE) {
    std::unique_lock<std::mutex> lock(connection_mutex_);
    if (read_write_connection_) {
      LOG(ERROR) << "There is already a read-write connection constructed.";
      throw std::runtime_error(
          "There is already a read-write connection constructed.");
    }
    read_write_connection_ = std::make_shared<Connection>(
        db_, planner_, query_processor_, resource_path_);
    return read_write_connection_;
  } else {
    throw std::runtime_error("Invalid mode.");
  }
}
}  // namespace gs
