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

#ifndef TOOLS_PYTHON_BIND_SRC_PY_CONNECTION_H_
#define TOOLS_PYTHON_BIND_SRC_PY_CONNECTION_H_

#include <memory>
#include <string>

#include "neug/execution/common/types/value.h"
#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "py_query_result.h"
#include "pybind11/include/pybind11/pybind11.h"

namespace neug {

class PyConnection : public std::enable_shared_from_this<PyConnection> {
 public:
  static void initialize(pybind11::handle& m);

  // TODO: Add more parameter? thread_num, etc.
  explicit PyConnection(NeugDB& db, std::shared_ptr<Connection> conn);

  void close();

  PyConnection(const PyConnection& other)
      : db_(other.db_), conn_(other.conn_) {}

  PyConnection(PyConnection&& other) = delete;
  ~PyConnection() = default;

  /**
   * The execution of query could be splitted into two parts:
   * 1. parse the query string and generate the execution plan.
   * 2. Execute the execution plan using runtime engine.
   */
  std::unique_ptr<PyQueryResult> execute(
      const std::string& query_string, const std::string& access_mode = "",
      const pybind11::dict& parameters = pybind11::dict());

  std::string get_schema() const;

 private:
  NeugDB& db_;
  std::shared_ptr<Connection> conn_;
};

}  // namespace neug

#endif  // TOOLS_PYTHON_BIND_SRC_PY_CONNECTION_H_