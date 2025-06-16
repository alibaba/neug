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
      "the database: NexgDB.\n")
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
        std::string jni_planner_jar_path =
            kwargs.contains("jni_planner_jar_path")
                ? kwargs["jni_planner_jar_path"].cast<std::string>()
                : "";
        std::string planner_config_path =
            kwargs.contains("planner_config_path")
                ? kwargs["planner_config_path"].cast<std::string>()
                : "";
        return std::make_shared<PyDatabase>(database_path, max_thread_num, mode,
                                            planner, jni_planner_jar_path,
                                            planner_config_path);
      }))  // "Creating a PyDatabase. Holds a shared pointer to the C++ "
           // "NexgDB object.\n"
      .def("connect", &PyDatabase::connect,
           "Connect to the database and "
           "return a PyConnection object.\n\n"
           "Returns:\n"
           "    PyConnection: A connection to the database.\n")
      .def("close", &PyDatabase::close,
           "Close the database connection and "
           "release resources.\n");
}

PyConnection PyDatabase::connect() {
  if (!database) {
    throw std::runtime_error("Database is not initialized.");
  }
  return PyConnection(database->connect());
}

void PyDatabase::close() {
  if (database) {
    database->close();
    database.reset();
  }
}

}  // namespace gs