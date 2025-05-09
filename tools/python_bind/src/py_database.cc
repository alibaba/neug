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
  pybind11::class_<PyDatabase, std::shared_ptr<PyDatabase>>(m, "PyDatabase")
      .def(pybind11::init<const std::string&, int32_t, const std::string&,
                          const std::string&, const std::string&,
                          const std::string&, const std::string&>(),
           pybind11::arg("database_path"), pybind11::arg("max_thread_num"),
           pybind11::arg("mode"), pybind11::arg("planner"),
           pybind11::arg("jni_planner_jar_path"),
           pybind11::arg("planner_config_path"), pybind11::arg("resource_path"))
      .def("connect", &PyDatabase::connect)
      .def("close", &PyDatabase::close);
}

PyConnection PyDatabase::connect() { return PyConnection(database->connect()); }

void PyDatabase::close() { database.reset(); }

}  // namespace gs