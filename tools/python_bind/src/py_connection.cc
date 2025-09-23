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
#include "neug/main/neug_db.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/yaml_utils.h"

namespace gs {

void PyConnection::initialize(pybind11::handle& m) {
  pybind11::class_<PyConnection, std::shared_ptr<PyConnection>>(
      m, "PyConnection",
      "PyConnection is the python binds for the actual c++ implementation "
      "of the connection to the database, gs::Connection.\n")
      .def(pybind11::init<NeugDB&, std::shared_ptr<Connection>>(),
           pybind11::arg("db"), pybind11::arg("conn"),
           "Creating a PyConnection. Holds a shared pointer to the C++ "
           "Connection object.\n")
      .def("close", &PyConnection::close,
           "Close the connection to the database.\n")
      .def("execute", &PyConnection::execute, pybind11::arg("query_string"),
           pybind11::arg("format") = "proto",
           "Execute a query_string on the database. Which is passed to the "
           "query "
           "processor.\n\n"
           "Args:\n"
           "    query_string (str): The query string to execute.\n"
           "    format (str): Output format of query result.\n\n"
           "Returns:\n"
           "    PyQueryResult: The result of the query execution.\n")
      .def("get_schema", &PyConnection::get_schema,
           "Get graph schema of database.\n");
  PyDateTime_IMPORT;
}

PyConnection::PyConnection(NeugDB& db, std::shared_ptr<Connection> conn)
    : db_(db), conn_(conn) {
  if (!conn_) {
    THROW_RUNTIME_ERROR("Connection is null");
  }
}

void PyConnection::close() {
  if (conn_) {
    db_.RemoveConnection(conn_);
    conn_.reset();
  }
}

std::unique_ptr<PyQueryResult> PyConnection::execute(
    const std::string& query_string, const std::string& format) {
  auto query_result = conn_->Query(query_string);
  if (!query_result) {
    return std::make_unique<PyQueryResult>(query_result.error());
  }
  if (format == "json") {
    std::string result_res =
        proto_to_bolt_response(query_result.value().get_result());
    return std::make_unique<PyQueryResult>(result_res, "json");
  } else {
    return std::make_unique<PyQueryResult>(std::move(query_result.value()));
  }
}

std::string PyConnection::get_schema() const {
  const auto& schema = db_.schema();
  auto yaml = schema.to_yaml();
  return gs::get_json_string_from_yaml(yaml.value()).value();
}

}  // namespace gs