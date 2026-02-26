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

#include <memory>
#include <mutex>
#include <string>

#include "neug/main/neug_db.h"
#ifdef BUILD_HTTP_SERVER
#include "neug/server/neug_db_service.h"
#endif
#include "py_connection.h"

namespace neug {

class PyDatabase : public std::enable_shared_from_this<PyDatabase> {
 public:
  static void initialize(pybind11::handle& m);

  explicit PyDatabase(const std::string& data_dir, int32_t max_thread_num,
                      const std::string& mode, const std::string& planner,
                      bool checkpoint_on_close = true) {
    db_dir_ = data_dir;
    neug::DBMode mode_;
    if (mode == "read" || mode == "r" || mode == "read-only" ||
        mode == "read_only") {
      mode_ = DBMode::READ_ONLY;
    } else if (mode == "read_write" || mode == "rw" || mode == "w" ||
               mode == "wr" || mode == "write" || mode == "readwrite" ||
               mode == "read-write") {
      mode_ = DBMode::READ_WRITE;
    } else {
      THROW_INVALID_ARGUMENT_EXCEPTION("Invalid mode: " + mode);
    }
    database = std::make_unique<NeugDB>();
    NeugDBConfig config(db_dir_, max_thread_num);
    config.mode = mode_;
    config.planner_kind = planner;
    config.checkpoint_on_close = checkpoint_on_close;
    database->Open(config);
  }

  ~PyDatabase() { close(); }

  PyConnection connect();

  /**
   * @brief Start the database server.
   * @param port The port to listen on, default is 10000.
   * @param host The host to bind to, default is "localhost".
   * @param num_thread The number of threads to use, default is 0, which means
   * use all hardware threads.
   * @param blocking Whether to block the function until the server shuts down.
   * @return A string containing the URL of the server.
   * @note This method will block until the server is stopped.
   */
  std::string serve(int port = 10000, const std::string& host = "localhost",
                    int32_t num_thread = 0, bool blocking = false);

  /**
   * @brief Stop the database server.
   * @note This method will stop the server if it is running.
   */
  void stop_serving();

  void close();

 private:
  std::recursive_mutex mtx_;
  std::string db_dir_;
  std::unique_ptr<NeugDB> database;
#ifdef BUILD_HTTP_SERVER
  std::unique_ptr<NeugDBService> service_;
#endif
};

}  // namespace neug
#endif  // TOOLS_PYTHON_BIND_SRC_PY_DATABASE_H_