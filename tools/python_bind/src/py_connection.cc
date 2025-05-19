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

#include "py_connection.h"
#include <datetime.h>
#include <memory>

namespace gs {

void PyConnection::initialize(pybind11::handle& m) {
  pybind11::class_<PyConnection, std::shared_ptr<PyConnection>>(m,
                                                                "PyConnection")
      .def(pybind11::init<std::shared_ptr<Connection>>(), pybind11::arg("conn"))
      .def("close", &PyConnection::close)
      .def("execute", &PyConnection::execute, pybind11::arg("statement"));
  PyDateTime_IMPORT;
}

PyConnection::PyConnection(std::shared_ptr<Connection> conn) : conn_(conn) {
  if (!conn_) {
    throw std::runtime_error("Connection is null");
  }
}

void PyConnection::close() { conn_.reset(); }

std::unique_ptr<PyQueryResult> PyConnection::execute(
    const std::string& statement) {
  // TODO: currently we assume all statements are graph queries, we need to
  // support the DDL, DML later
  return std::make_unique<PyQueryResult>(conn_->get_schema(),
                                         conn_->query(statement));
}

}  // namespace gs