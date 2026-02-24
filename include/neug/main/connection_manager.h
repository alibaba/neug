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
#pragma once

#include <stddef.h>
#include <memory>
#include <mutex>
#include <vector>

namespace neug {

class PropertyGraph;
class Connection;
class IGraphPlanner;
class QueryProcessor;
struct NeugDBConfig;

class ConnectionManager {
 public:
  ConnectionManager(PropertyGraph& graph,
                    std::shared_ptr<IGraphPlanner> planner,
                    std::shared_ptr<QueryProcessor> query_processor,
                    const NeugDBConfig& config)
      : graph_(graph),
        planner_(planner),
        query_processor_(query_processor),
        config_(config) {}
  ~ConnectionManager() { Close(); }

  std::shared_ptr<Connection> CreateConnection();

  /**
   * @brief Close all connections managed by the connection manager.
   */
  void Close();

  /**
   * @brief Remove a connection from the database.
   * @param conn The connection to be removed.
   * @note This method is used to remove a connection when it is closed, to
   * remove the handle from the database.
   * @note This method is not thread-safe, so it should be called only when the
   * connection is closed. And should be only called internally.
   */
  void RemoveConnection(std::shared_ptr<Connection> conn);

  size_t ConnectionNum() const {
    return read_only_connections_.size() + (read_write_connection_ ? 1 : 0);
  }

 private:
  PropertyGraph& graph_;
  std::shared_ptr<IGraphPlanner> planner_;
  std::shared_ptr<QueryProcessor> query_processor_;
  const NeugDBConfig& config_;

  std::shared_ptr<Connection> read_write_connection_;
  std::vector<std::shared_ptr<Connection>> read_only_connections_;
  std::mutex connection_mutex_;
};

}  // namespace neug
