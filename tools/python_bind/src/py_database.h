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

#ifndef TOOLS_PYTHON_BIND_SRC_PY_DATABASE_H_
#define TOOLS_PYTHON_BIND_SRC_PY_DATABASE_H_

#include "third_party/pybind11/include/pybind11/pybind11.h"
#include "third_party/pybind11/include/pybind11/stl.h"

#include "py_connection.h"
#include "src/main/neug_db.h"

namespace gs {

class PyDatabase : public std::enable_shared_from_this<PyDatabase> {
 public:
  static void initialize(pybind11::handle& m);

  explicit PyDatabase(const std::string& databasePath, int32_t max_thread_num,
                      const std::string& mode, const std::string& planner,
                      const std::string& planner_config_path) {
    database = std::make_unique<NeugDB>(databasePath, max_thread_num, mode,
                                        planner, planner_config_path);
  }

  ~PyDatabase() { close(); }

  PyConnection connect();

  void close();

 private:
  std::unique_ptr<NeugDB> database;
};

}  // namespace gs
#endif  // TOOLS_PYTHON_BIND_SRC_PY_DATABASE_H_