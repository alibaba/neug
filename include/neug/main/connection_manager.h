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

class Connection;
class NeugDB;
struct NeugDBConfig;

class ConnectionManager {
 private:
  struct ConnectionRegistry {
    std::shared_ptr<Connection> read_write_connection;
    std::vector<std::shared_ptr<Connection>> read_only_connections;
    mutable std::mutex mutex;
  };

 public:
  ConnectionManager(NeugDB& db, const NeugDBConfig& config)
      : db_(db),
        config_(config),
        connection_registry_(std::make_shared<ConnectionRegistry>()) {}
  ~ConnectionManager() { Close(); }

  std::shared_ptr<Connection> CreateConnection();

  /**
   * @brief Close all connections managed by the connection manager.
   * @note The caller must ensure no managed Connection operation is in
   * progress.
   */
  void Close();

  /** @brief Return the number of currently open managed connections. */
  size_t ConnectionNum() const;

  /**
   * @brief Check whether any managed connection is still open.
   */
  bool HasOpenConnections() const;

 private:
  static void UnregisterConnection(
      const std::weak_ptr<ConnectionRegistry>& connection_registry,
      Connection* conn);

  NeugDB& db_;
  const NeugDBConfig& config_;
  std::shared_ptr<ConnectionRegistry> connection_registry_;
};

}  // namespace neug
