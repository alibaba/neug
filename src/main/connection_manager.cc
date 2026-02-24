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
#include "neug/utils/exception/exception.h"

namespace neug {

void ConnectionManager::ConnectionManager::Close() {
  std::lock_guard<std::mutex> lock(connection_mutex_);
  if (read_write_connection_) {
    read_write_connection_->Close();
    read_write_connection_.reset();
  }
  for (auto& conn : read_only_connections_) {
    conn->Close();
  }
  read_only_connections_.clear();
}

std::shared_ptr<Connection> ConnectionManager::CreateConnection() {
  std::lock_guard<std::mutex> lock(connection_mutex_);
  if (config_.mode == DBMode::READ_ONLY) {
    auto conn = std::make_shared<Connection>(graph_, query_processor_);
    read_only_connections_.push_back(conn);
    return conn;
  } else if (config_.mode == DBMode::READ_WRITE) {
    if (read_write_connection_) {
      LOG(ERROR) << "There is already a read-write connection constructed.";
      THROW_TX_STATE_CONFLICT(
          "There is already a read-write connection constructed.");
    }
    read_write_connection_ =
        std::make_shared<Connection>(graph_, query_processor_);
    return read_write_connection_;
  } else {
    THROW_RUNTIME_ERROR("Invalid mode.");
  }
}

void ConnectionManager::RemoveConnection(std::shared_ptr<Connection> conn) {
  std::lock_guard<std::mutex> lock(connection_mutex_);
  if (config_.mode == DBMode::READ_ONLY) {
    for (auto it = read_only_connections_.begin();
         it != read_only_connections_.end(); ++it) {
      if (*it == conn) {
        read_only_connections_.erase(it);
        VLOG(10) << "Removed a read-only connection.";
        return;
      }
    }
    LOG(ERROR) << "Connection not found in read-only connections.";
  } else if (config_.mode == DBMode::READ_WRITE) {
    if (read_write_connection_ == conn) {
      read_write_connection_.reset();
      VLOG(10) << "Removed the read-write connection.";
      return;
    } else {
      LOG(ERROR) << "Connection not found in read-write connection.";
    }
  } else {
    THROW_RUNTIME_ERROR("Invalid mode.");
  }
  LOG(ERROR) << "Connection not found.";
}

}  // namespace neug
