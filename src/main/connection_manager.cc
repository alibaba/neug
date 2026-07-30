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

#include "neug/main/connection_manager.h"

#include <glog/logging.h>
#include <ostream>
#include "neug/config.h"
#include "neug/main/connection.h"
#include "neug/main/execution_slot.h"
#include "neug/main/neug_db.h"
#include "neug/utils/exception/exception.h"

namespace neug {

void ConnectionManager::Close() {
  auto connection_registry = connection_registry_;
  std::vector<std::shared_ptr<Connection>> connections;
  {
    std::lock_guard<std::mutex> lock(connection_registry->mutex);
    connections.reserve(connection_registry->read_only_connections.size() +
                        (connection_registry->read_write_connection ? 1 : 0));
    if (connection_registry->read_write_connection) {
      connections.emplace_back(
          std::move(connection_registry->read_write_connection));
    }
    for (auto& conn : connection_registry->read_only_connections) {
      connections.emplace_back(std::move(conn));
    }
    connection_registry->read_only_connections.clear();
  }

  // Close callbacks unregister themselves, so the manager mutex must not be
  // held while invoking Connection::Close().
  for (auto& conn : connections) {
    conn->Close();
  }
}

size_t ConnectionManager::ConnectionNum() const {
  auto connection_registry = connection_registry_;
  std::lock_guard<std::mutex> lock(connection_registry->mutex);
  size_t connection_num = 0;
  if (connection_registry->read_write_connection &&
      !connection_registry->read_write_connection->IsClosed()) {
    ++connection_num;
  }
  for (const auto& conn : connection_registry->read_only_connections) {
    if (conn && !conn->IsClosed()) {
      ++connection_num;
    }
  }
  return connection_num;
}

bool ConnectionManager::HasOpenConnections() const {
  return ConnectionNum() != 0;
}

std::shared_ptr<Connection> ConnectionManager::CreateConnection() {
  auto connection_registry = connection_registry_;
  std::lock_guard<std::mutex> lock(connection_registry->mutex);
  const std::weak_ptr<ConnectionRegistry> weak_connection_registry =
      connection_registry;
  if (config_.mode == DBMode::READ_ONLY) {
    auto conn = std::make_shared<Connection>(
        db_.createExecutionSlot(/*slot_id=*/0),
        [weak_connection_registry](Connection* connection) {
          UnregisterConnection(weak_connection_registry, connection);
        });
    connection_registry->read_only_connections.push_back(conn);
    return conn;
  } else if (config_.mode == DBMode::READ_WRITE) {
    if (connection_registry->read_write_connection) {
      LOG(ERROR) << "There is already a read-write connection constructed.";
      THROW_TX_STATE_CONFLICT(
          "There is already a read-write connection constructed.");
    }
    connection_registry->read_write_connection = std::make_shared<Connection>(
        db_.createExecutionSlot(/*slot_id=*/0),
        [weak_connection_registry](Connection* connection) {
          UnregisterConnection(weak_connection_registry, connection);
        });
    return connection_registry->read_write_connection;
  } else {
    THROW_RUNTIME_ERROR("Invalid mode.");
  }
}

void ConnectionManager::UnregisterConnection(
    const std::weak_ptr<ConnectionRegistry>& weak_connection_registry,
    Connection* conn) {
  auto connection_registry = weak_connection_registry.lock();
  if (!connection_registry) {
    return;
  }

  std::lock_guard<std::mutex> lock(connection_registry->mutex);
  if (connection_registry->read_write_connection.get() == conn) {
    connection_registry->read_write_connection.reset();
    VLOG(10) << "Unregistered the read-write connection.";
    return;
  }
  for (auto it = connection_registry->read_only_connections.begin();
       it != connection_registry->read_only_connections.end(); ++it) {
    if (it->get() == conn) {
      connection_registry->read_only_connections.erase(it);
      VLOG(10) << "Unregistered a read-only connection.";
      return;
    }
  }
}

}  // namespace neug
