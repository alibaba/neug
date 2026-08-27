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

#include <exception>

#include "neug/main/execution_slot.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/yaml_utils.h"

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
  if (transaction_context_.IsRollbackOnly()) {
    THROW_TX_STATE_CONFLICT(
        "Transaction is rollback-only; Rollback() is required before "
        "GetSchema.");
  }
  if (transaction_context_.IsActive()) {
    auto yaml = transaction_context_.schema().to_yaml();
    return get_json_string_from_yaml(yaml.value()).value();
  }
  return execution_slot_->GetSchema();
}

void Connection::Close() {
  if (is_closed_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Connection is already closed.";
    return;
  }
  LOG(INFO) << "Closing connection.";

  transaction_context_.Rollback();

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

Status Connection::BeginTransaction(TransactionMode mode) {
  if (IsClosed()) {
    return Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed.");
  }
  if (transaction_context_.HasActiveTransaction()) {
    return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                  "An explicit transaction is already active.");
  }

  try {
    switch (mode) {
    case TransactionMode::kReadOnly:
      transaction_context_.Begin(
          execution_slot_->BeginSnapshotReadTransaction());
      return Status::OK();
    case TransactionMode::kReadWrite: {
      auto transaction = execution_slot_->BeginCurrentCowWriteTransaction();
      if (!transaction) {
        return transaction.error();
      }
      transaction_context_.Begin(std::move(transaction).value());
      return Status::OK();
    }
    }
  } catch (const std::exception& e) {
    return Status::InternalError(
        std::string("Failed to begin explicit transaction: ") + e.what());
  } catch (...) {
    return Status::InternalError("Failed to begin explicit transaction");
  }
  return Status(StatusCode::ERR_INVALID_ARGUMENT,
                "Unsupported explicit transaction mode.");
}

Status Connection::Commit() {
  if (IsClosed()) {
    return Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed.");
  }
  if (transaction_context_.IsRollbackOnly()) {
    return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                  "Transaction is rollback-only; Rollback() is required.");
  }
  if (!transaction_context_.IsActive()) {
    return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                  "No explicit transaction is active.");
  }
  return transaction_context_.Commit();
}

Status Connection::Rollback() {
  if (IsClosed()) {
    return Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed.");
  }
  if (!transaction_context_.HasActiveTransaction()) {
    return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                  "No explicit transaction is active.");
  }
  transaction_context_.Rollback();
  return Status::OK();
}

result<QueryResult> Connection::Query(const std::string& query_string,
                                      const std::string& access_mode,
                                      const rapidjson::Value& parameters) {
  VLOG(1) << "Query: " << query_string;
  if (IsClosed()) {
    LOG(ERROR) << "Connection is closed, cannot execute query.";
    RETURN_ERROR(
        Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed."));
  }
  if (transaction_context_.IsRollbackOnly()) {
    RETURN_ERROR(
        Status(StatusCode::ERR_TX_STATE_CONFLICT,
               "Transaction is rollback-only; Rollback() is required."));
  }
  if (transaction_context_.IsActive()) {
    return execution_slot_->ExecuteQueryInTransaction(
        query_string, access_mode, parameters, /*num_threads=*/0,
        transaction_context_);
  }
  return execution_slot_->ExecuteQuery(query_string, access_mode, parameters);
}

}  // namespace neug
