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

#include "pybind11/include/pybind11/pybind11.h"
#include "pybind11/include/pybind11/stl.h"

#include "neug/main/neug_db.h"
#include "py_connection.h"

namespace gs {

class PyDatabase : public std::enable_shared_from_this<PyDatabase> {
 public:
  static void initialize(pybind11::handle& m);

  explicit PyDatabase(const std::string& databasePath, int32_t max_thread_num,
                      const std::string& mode, const std::string& planner,
                      const std::string& planner_config_path)
      : planner_config_path_(planner_config_path) {
    database = std::make_unique<NeugDB>(databasePath, max_thread_num, mode,
                                        planner, planner_config_path);
  }

  ~PyDatabase() { close(); }

  PyConnection connect();

  /**
   * @brief Start the database server.
   * @param port The port to listen on, default is 10000.
   * @param host The host to bind to, default is "localhost".
   * @return A string containing the URL of the server.
   * @note This method will block until the server is stopped.
   */
  std::string serve(int port = 10000, const std::string& host = "localhost");

  /**
   * @brief Stop the database server.
   * @note This method will stop the server if it is running.
   */
  void stop_serving();

  void close();

 private:
  std::string planner_config_path_;
  std::unique_ptr<NeugDB> database;
};

}  // namespace gs
#endif  // TOOLS_PYTHON_BIND_SRC_PY_DATABASE_H_