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

namespace gs {

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
        return std::make_shared<PyDatabase>(database_path, max_thread_num, mode,
                                            planner);
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
  return PyConnection(*database, database->connect());
}

std::string PyDatabase::serve(int port, const std::string& host) {
  if (!database) {
    THROW_RUNTIME_ERROR("Database is not initialized.");
  }
  if (service_) {
    THROW_RUNTIME_ERROR("Server is already running.");
  }
  service_ = std::make_unique<server::NeugDBService>(*database);
  server::ServiceConfig config;
  config.query_port = port;
  config.host_str = host;
  service_->init(config);
  return service_->Start();
}

void PyDatabase::stop_serving() {
  if (!database) {
    THROW_RUNTIME_ERROR("Database is not initialized.");
  }
  if (!service_) {
    THROW_RUNTIME_ERROR("Server is not running.");
  }
  if (service_) {
    service_->Stop();
    service_.reset();
  }
}

void PyDatabase::close() {
  if (database) {
    database->Close();
    database.reset();
  }
  if (service_) {
    service_->Stop();
    service_.reset();
  }
}

}  // namespace gs