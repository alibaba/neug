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

#include "src/main/neug_db.h"
#ifdef NEUG_BACKTRACE
#include <cpptrace/cpptrace.hpp>
#endif

namespace gs {

void signal_handler(int signal) {
  LOG(INFO) << "Received signal " << signal << ", exiting...";
  // support SIGKILL, SIGINT, SIGTERM
  if (signal == SIGKILL || signal == SIGINT || signal == SIGTERM ||
      signal == SIGSEGV || signal == SIGABRT) {
    LOG(ERROR) << "Received signal " << signal << ", Remove all filelocks";
    // remove all files in work_dir
    gs::FileLock::CleanupAllLocks();
#ifdef NEUG_BACKTRACE
    cpptrace::generate_trace(1 /*skip this function's frame*/).print();
#endif
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
  std::signal(SIGFPE, signal_handler);
}

void NeugDB::close() {
  LOG(INFO) << "Closing NeugDB.";
  if (mode_ == DBMode::READ_WRITE) {
    file_lock_.unlock();
  }
  db_.Close();
  for (auto& conn : read_only_connections_) {
    if (conn) {
      LOG(INFO) << "Closing read-only connection.";
      conn->close();
    }
  }
  VLOG(10) << "Close all read-only connections.";
  if (read_write_connection_) {
    read_write_connection_->close();
  }
  if (is_pure_memory_) {
    std::filesystem::remove_all(config_.data_dir);
  }
  VLOG(10) << "Close all connections.";
}

std::shared_ptr<Connection> NeugDB::connect() {
  if (mode_ == DBMode::READ_ONLY) {
    auto conn = std::make_shared<Connection>(db_, planner_, query_processor_);
    read_only_connections_.push_back(conn);
    return conn;
  } else if (mode_ == DBMode::READ_WRITE) {
    std::unique_lock<std::mutex> lock(connection_mutex_);
    if (read_write_connection_) {
      LOG(ERROR) << "There is already a read-write connection constructed.";
      throw std::runtime_error(
          "There is already a read-write connection constructed.");
    }
    read_write_connection_ =
        std::make_shared<Connection>(db_, planner_, query_processor_);
    return read_write_connection_;
  } else {
    throw std::runtime_error("Invalid mode.");
  }
}

std::string NeugDB::serve(int port, const std::string& host) {
#ifdef BUILD_HTTP_SERVER
  if (hdl_mgr_) {
    LOG(WARNING) << "HTTP service is already started.";
    return "";
  }
  service_config_.query_port = port;
  service_config_.host_str = host;
  service_config_.engine_config_path = planner_config_path_;
  if (is_pure_memory_) {
    throw std::runtime_error(
        "Cannot serve a pure memory database. Please use a persistent "
        "database.");
  }
  std::shared_ptr<IGraphPlanner> planner =
      std::make_shared<GOptPlanner>(planner_config_path_);
  planner->update_meta(db_.schema().to_yaml().value(),
                       db_.get_statistics_json());
  hdl_mgr_ = std::make_unique<server::BrpcHttpHandlerManager>(db_, planner);
  hdl_mgr_->Init(service_config_);
  LOG(INFO) << "Starting HTTP service on " << host << ":" << port;
  return hdl_mgr_->Start();
#else
  LOG(ERROR) << "HTTP server is not built. Please build with HTTP server "
                "support.";
  throw std::runtime_error(
      "HTTP server is not built. Please build with HTTP server support.");
#endif
}

void NeugDB::stop_serving() {
#ifdef BUILD_HTTP_SERVER
  if (hdl_mgr_) {
    LOG(INFO) << "Stopping HTTP service.";
    hdl_mgr_->Stop();
  } else {
    LOG(WARNING) << "HTTP service is not running.";
  }
#else
  LOG(ERROR) << "HTTP server is not built. Please build with HTTP server "
                "support.";
#endif
}
}  // namespace gs
