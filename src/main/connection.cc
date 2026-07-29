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

#include "neug/main/connection.h"

#include "neug/main/execution_slot.h"

namespace neug {

Connection::Connection(std::unique_ptr<ExecutionSlot> execution_slot,
                       CloseCallback on_close)
    : execution_slot_(std::move(execution_slot)),
      on_close_(std::move(on_close)) {
  CHECK(execution_slot_ != nullptr);
}

Connection::~Connection() { Close(); }

std::string Connection::GetSchema() const {
  if (IsClosed()) {
    LOG(ERROR) << "Connection is closed, cannot get schema.";
    THROW_RUNTIME_ERROR("Connection is closed, cannot get schema.");
  }
  return execution_slot_->GetSchema();
}

void Connection::Close() {
  if (is_closed_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Connection is already closed.";
    return;
  }
  LOG(INFO) << "Closing connection.";

  // Clean up all temporary schemas created through embedded execution.
  // This is safe to do globally because LOAD AS is only supported in
  // READ_WRITE mode, and ConnectionManager enforces that at most ONE
  // read-write connection exists at a time. Therefore, all temporary
  // labels in the schema must belong to this connection.
  execution_slot_->ClearTemporarySchema();
  execution_slot_.reset();
  is_closed_.store(true, std::memory_order_release);

  auto on_close = std::move(on_close_);
  if (on_close) {
    on_close(this);
  }
}

result<QueryResult> Connection::Query(const std::string& query_string,
                                      const std::string& access_mode,
                                      const execution::ParamsMap& parameters) {
  VLOG(1) << "Query: " << query_string;
  if (IsClosed()) {
    LOG(ERROR) << "Connection is closed, cannot execute query.";
    RETURN_ERROR(
        Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed."));
  }
  return execution_slot_->ExecuteQuery(query_string, access_mode, parameters);
}

result<QueryResult> Connection::Query(const std::string& query_string,
                                      const std::string& access_mode,
                                      const rapidjson::Value& parameters_json) {
  VLOG(1) << "Query: " << query_string;
  if (IsClosed()) {
    LOG(ERROR) << "Connection is closed, cannot execute query.";
    RETURN_ERROR(
        Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed."));
  }
  return execution_slot_->ExecuteQuery(query_string, access_mode,
                                       parameters_json);
}

}  // namespace neug
