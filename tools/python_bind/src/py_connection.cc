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
#include "neug/execution/common/params_map.h"
#include "neug/main/neug_db.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/yaml_utils.h"
#include "py_query_request.h"

namespace neug {

void PyConnection::initialize(pybind11::handle& m) {
  pybind11::class_<PyConnection, std::shared_ptr<PyConnection>>(
      m, "PyConnection",
      "PyConnection is the python binds for the actual c++ implementation "
      "of the connection to the database, neug::Connection.\n")
      .def(pybind11::init<NeugDB&, std::shared_ptr<Connection>>(),
           pybind11::arg("db"), pybind11::arg("conn"),
           "Creating a PyConnection. Holds a shared pointer to the C++ "
           "Connection object.\n")
      .def("close", &PyConnection::close,
           "Close the connection to the database.\n")
      .def("execute", &PyConnection::execute, pybind11::arg("query_string"),
           pybind11::arg("access_mode") = "",
           pybind11::arg("parameters") = pybind11::dict(),
           "Execute a query_string on the database. Which is passed to the "
           "query "
           "processor.\n\n"
           "Args:\n"
           "    query_string (str): The query string to execute.\n"
           "    access_mode (str): The access mode of the query. It could be "
           "`read(r)`, "
           "`insert(i)`, `update(u)` (include deletion). User should specify "
           "the "
           "correct access mode for the query to ensure the correctness of the "
           "database. If the access mode is not specified, it will be set to "
           "`update` by default.\n"
           "    parameters (dict[str, Any], optional): The parameters to be "
           "used "
           "in the query. The parameters should be a dictionary, where the "
           "keys are the parameter names, and the values are the parameter "
           "values. If no parameters are needed, it can be set to None.\n"
           "\n"
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
    const std::string& query_string, const std::string& access_mode,
    const pybind11::dict& parameters) {
  rapidjson::Document params_json;
  for (auto item : parameters) {
    std::string key = pybind11::cast<std::string>(item.first);
    pybind11::object value =
        pybind11::reinterpret_borrow<pybind11::object>(item.second);
    PyParameterSerializer::SerializeParameter(params_json, key, value);
  }
  auto query_result = conn_->Query(query_string, access_mode, params_json);
  if (!query_result) {
    return std::make_unique<PyQueryResult>(query_result.error());
  }
  return std::make_unique<PyQueryResult>(std::move(query_result.value()));
}

std::string PyConnection::get_schema() const { return conn_->GetSchema(); }

}  // namespace neug