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

#include "py_database.h"
#include "neug/config.h"

namespace neug {

void PyDatabase::initialize(pybind11::handle& m) {
  pybind11::class_<PyDatabase, std::shared_ptr<PyDatabase>>(
      m, "PyDatabase",
      "PyDatabase is the python binds for the actual c++ implementation of "
      "the database: NeugDB.\n")
      .def(pybind11::init([](pybind11::kwargs kwargs) {
        std::string database_path =
            kwargs.contains("database_path")
                ? kwargs["database_path"].cast<std::string>()
                : "";
        int32_t max_thread_num = kwargs.contains("max_thread_num")
                                     ? kwargs["max_thread_num"].cast<int32_t>()
                                     : 0;
        std::string mode =
            kwargs.contains("mode") ? kwargs["mode"].cast<std::string>() : "r";
        std::string planner = kwargs.contains("planner")
                                  ? kwargs["planner"].cast<std::string>()
                                  : "gopt";
        bool checkpoint_on_close =
            kwargs.contains("checkpoint_on_close")
                ? kwargs["checkpoint_on_close"].cast<bool>()
                : true;
        std::string buffer_strategy =
            kwargs.contains("buffer_strategy")
                ? kwargs["buffer_strategy"].cast<std::string>()
                : "M_FULL";
        return std::make_shared<PyDatabase>(database_path, max_thread_num, mode,
                                            planner, checkpoint_on_close,
                                            buffer_strategy);
      }))  // "Creating a PyDatabase. Holds a shared pointer to the C++ "
           // "NeugDB object.\n"
      .def("connect", &PyDatabase::connect,
           "Connect to the database and "
           "return a PyConnection object.\n\n"
           "Returns:\n"
           "    PyConnection: A connection to the database.\n")
      .def("close", &PyDatabase::close,
           "Close the database connection and "
           "release resources.\n")
      .def("serve", &PyDatabase::serve,
           "Start the database server.\n\n"
           "Args:\n"
           "    port (int): The port to listen on, default is 10000.\n"
           "    host (str): The host to bind to, default is 'localhost'.\n"
           "    num_thread (int): The number of threads to use, default is 0, "
           "which means use all hardware threads.\n"
           "    blocking (bool): Whether to block the function until the "
           "server shuts down.\n"
           "Returns:\n"
           "    uri (str): A string containing the URL of the server.\n")
      .def("stop_serving", &PyDatabase::stop_serving,
           "Stop the database server.\n\n"
           "Returns:\n"
           "    None: This method will stop the server if it is running.\n");
}

PyConnection PyDatabase::connect() {
  if (!database) {
    THROW_RUNTIME_ERROR("Database is not initialized.");
  }
  return PyConnection(*database, database->Connect());
}

std::string PyDatabase::serve(int port, const std::string& host,
                              int32_t num_thread, bool blocking) {
#ifdef BUILD_HTTP_SERVER
  if (!database) {
    THROW_RUNTIME_ERROR("Database is not initialized.");
  }
  if (service_) {
    THROW_RUNTIME_ERROR("Server is already running.");
  }
  /**
   * Attention here: We utilize the NeugDBService to start the server, based on
   * database. But we need to make some changes to the NeugDB to make it works
   * well for service mode, where concurrent queries will be processed.
   *
   * But before all, we need to close the database, dump the data to disk, and
   * then reload the database with TPVersionManager and multiple contexts. By
   * doing this, we make sure all changes made during AP mode is persisted.
   */

  std::lock_guard<std::recursive_mutex> lock(mtx_);

  database->Close();
  database->Open(database->config());
  neug::ServiceConfig config;
  config.query_port = port;
  config.host_str = host;
  config.shard_num =
      (num_thread == 0) ? std::thread::hardware_concurrency() : num_thread;
#ifdef __APPLE__
  if (host == "localhost") {
    config.host_str = "127.0.0.1";
  }
#endif

  service_ = std::make_unique<neug::NeugDBService>(*database, config);
  if (blocking) {
    service_->run_and_wait_for_exit();
    return "";
  }
  return service_->Start();
#else
  THROW_RUNTIME_ERROR("HTTP server is not enabled in this build.");
#endif
}

void PyDatabase::stop_serving() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  VLOG(1) << "Stopping server if running.";
#ifdef BUILD_HTTP_SERVER
  if (!service_) {
    return;
  }
  if (service_) {
    service_->Stop();
    service_.reset();
  }
#endif
}

void PyDatabase::close() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  stop_serving();
  if (database) {
    VLOG(1) << "Closing database.";
    database->Close();
    database.reset();
  }
}

MemoryLevel PyDatabase::parse_buffer_strategy(const std::string& level) {
  if (level == "InMemory" || level == "inmemory" || level == "in_memory" ||
      level == "M_FULL") {
    return MemoryLevel::kInMemory;
  } else if (level == "SyncToFile" || level == "synctofile" ||
             level == "sync_to_file" || level == "M_LAZY") {
    return MemoryLevel::kSyncToFile;
  } else if (level == "HugePagePreferred" || level == "hugepagepreferred" ||
             level == "huge_page_preferred" || level == "M_HUGE") {
    return MemoryLevel::kHugePagePreferred;
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid memory level: " + level);
  }
}

}  // namespace neug