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

#include <glog/logging.h>

#include <atomic>
#include <functional>
#include <memory>
#include <string>

#include <rapidjson/document.h>

#include "neug/main/query_result.h"
#include "neug/main/transaction_context.h"
#include "neug/utils/api.h"
#include "neug/utils/result.h"

namespace neug {

class ExecutionSlot;

/**
 * @brief Database connection for executing Cypher queries.
 *
 * Connection is the primary embedded-mode interface for interacting with a
 * NeuG database. It provides methods to execute Cypher queries, retrieve
 * schema information, manage a programmatic AP explicit transaction, and
 * manage the connection lifecycle.
 *
 * **Usage Example:**
 * @code{.cpp}
 * // Get connection from database
 * auto conn = db.Connect();
 *
 * // Execute a read query
 * auto result = conn->Query("MATCH (n:Person) RETURN n.name LIMIT 10", "read");
 * for (auto& record : result.value()) {
 *   // Process record...
 * }
 *
 * // Execute an insert query
 * conn->Query("CREATE (p:Person {name: 'Alice', age: 30})", "insert");
 *
 * // Close connection when done
 * conn->Close();
 * @endcode
 *
 * **Access Modes:**
 * - `"read"` or `"r"`: Read-only queries (MATCH, RETURN)
 * - `"insert"` or `"i"`: Insert-only operations (CREATE)
 * - `"update"` or `"u"`: Update/delete operations (SET, DELETE, MERGE)
 * - `"schema"` or `"s"`: Schema modification operations (CREATE/DROP labels)
 *
 * **Thread Safety:** This class is NOT thread-safe. A Connection and its owned
 * ExecutionSlot must be used by only one thread at a time. Use a separate
 * Connection per thread.
 *
 * **Lifecycle:**
 * - Created via NeugDB::Connect()
 * - Execute queries via Query() method
 * - Close via Close(), which automatically unregisters the connection
 * - Automatically closed and unregistered in the destructor
 *
 * @note Each connection exclusively owns one execution slot.
 * @note For best performance, reuse connections for multiple queries.
 *
 * @see NeugDB::Connect For creating connections
 * @see QueryResult For processing query results
 *
 * @since v0.1.0
 */
class NEUG_API Connection {
 public:
  using CloseCallback = std::function<void(Connection*)>;

  explicit Connection(std::unique_ptr<ExecutionSlot> execution_slot,
                      CloseCallback on_close = {});
  ~Connection();

  /**
   * @brief Execute a Cypher query and return results.
   *
   * Compiles and executes a Cypher query string against the database.
   * The query is processed through the planner for optimization, then
   * executed by the connection-owned execution slot.
   *
   * **Usage Example:**
   * @code{.cpp}
   * // Simple read query
   * auto result = conn->Query("MATCH (n:Person) RETURN n.name", "read");
   *
   * // Query with parameters
   * rapidjson::Document params(rapidjson::kObjectType);
   * params.AddMember("min_age", 18, params.GetAllocator());
   * result = conn->Query("MATCH (p:Person) WHERE p.age > $min_age RETURN p",
   * "read", params);
   *
   * // Process results
   * if (result.has_value()) {
   *   for (auto& record : result.value()) {
   *     // Access columns via record.entries()
   *   }
   * } else {
   *   std::cerr << "Query failed: " << result.error().message() << std::endl;
   * }
   * @endcode
   *
   * @param query_string The Cypher query to execute
   * @param access_mode Query access mode:
   *        - `"read"` or `"r"`: Read-only operations
   *        - `"insert"` or `"i"`: Insert-only operations (CREATE)
   *        - `"update"` or `"u"`: Update/delete operations
   *        - `"schema"` or `"s"`: Schema modification operations
   *        - empty string: Infer access mode from query text
   * @param parameters Named parameters for parameterized queries.
   *        Keys are parameter names (without `$`), values are parameter values.
   *
   * @return result<QueryResult> containing either:
   *         - QueryResult with query results on success
   *         - Error status with message on failure
   *
   * @note Use parameterized queries for dynamic values to prevent injection.
   * @note Specifying correct access_mode ensures proper transaction handling.
   * @note Within an active explicit transaction, this query uses the
   * connection-owned pinned read view or private COW write view. Cypher
   * BEGIN/COMMIT/ROLLBACK statements are not supported; use the programmatic
   * control methods below.
   *
   * @see QueryResult For iterating over results
   *
   * @since v0.1.0
   */
  result<QueryResult> Query(const std::string& query_string,
                            const std::string& access_mode = "",
                            const rapidjson::Value& parameters =
                                rapidjson::Value{rapidjson::kObjectType});

  /**
   * @brief Begin a Connection-owned embedded AP explicit transaction.
   *
   * A read-only transaction pins one published read view across Query() calls.
   * A read-write transaction owns one private COW view; successful writes are
   * visible to later queries on this Connection and are published together by
   * Commit(). Read-write AP transactions hold exclusive AP admission until a
   * terminal operation.
   *
   * @return Status::OK on success. Otherwise:
   *         - ERR_CONNECTION_CLOSED if this Connection is closed
   *         - ERR_TX_STATE_CONFLICT if a transaction is already active
   *         - ERR_INVALID_ARGUMENT if the requested mode is invalid or a
   *           read-write transaction is requested on a read-only database
   *         - ERR_NOT_SUPPORTED if the execution mode does not support
   *           embedded explicit transactions
   *
   * @note This API is not a Cypher BEGIN statement and does not support nested
   * transactions or read-to-write upgrades.
   */
  Status BeginTransaction(TransactionMode mode = TransactionMode::kReadWrite);
  /**
   * @brief Commit the active explicit transaction.
   *
   * A read-write transaction appends and publishes its accumulated logical
   * redo once. A read-only transaction only releases its pinned read view.
   *
   * @return A transaction-state error if no transaction is active or it is
   * rollback-only. A failed commit leaves the Connection rollback-only; call
   * Rollback() before reusing it.
   */
  Status Commit();
  /**
   * @brief Abort the active or rollback-only explicit transaction.
   *
   * Discards the private COW view, if any, and returns the Connection to idle.
   */
  Status Rollback();
  /**
   * @brief Return whether this Connection has an unfinished explicit
   * transaction.
   *
   * Returns true for both active and rollback-only states. Only a successful
   * Commit() or Rollback() returns the Connection to idle.
   */
  bool HasActiveTransaction() const noexcept {
    return transaction_context_.HasActiveTransaction();
  }

  /**
   * @brief Get the database schema as a YAML string.
   *
   * Returns the complete graph schema definition in YAML format,
   * including all vertex types, edge types, and their properties.
   *
   * **Usage Example:**
   * @code{.cpp}
   * std::string schema_yaml = conn->GetSchema();
   * std::cout << "Schema:\n" << schema_yaml << std::endl;
   * @endcode
   *
   * @return std::string YAML-formatted schema definition
   *
   * @throws std::runtime_error if the connection is closed
   * @throws TxStateConflictException if the active transaction is rollback-only
   *
   * @note During an active explicit transaction this returns that transaction's
   * pinned read schema or private COW schema, rather than the published schema.
   *
   * @see Schema For programmatic schema access
   *
   * @since v0.1.0
   */
  std::string GetSchema() const;

  /**
   * @brief Close the connection and release resources.
   *
   * Marks the connection as closed and releases any held resources.
   * After closing, any Query() calls will fail.
   *
   * **Usage Example:**
   * @code{.cpp}
   * conn->Close();
   * // conn->Query(...) will now return an error
   * @endcode
   *
   * @note Sequential repeated calls are idempotent. Concurrent calls are not
   * safe.
   * @note Closing automatically unregisters this connection from its database.
   * @note The connection is also automatically closed in the destructor.
   * @note An active or rollback-only explicit transaction is rolled back before
   * temporary schema cleanup and execution-slot destruction.
   *
   * @since v0.1.0
   */
  void Close();

  /**
   * @brief Check if the connection is closed.
   *
   * @return true if the connection has been closed, false if still active
   *
   * @since v0.1.0
   */
  bool IsClosed() const { return is_closed_.load(); }

 private:
  std::unique_ptr<ExecutionSlot> execution_slot_;
  CloseCallback on_close_;
  TransactionContext transaction_context_;

  std::atomic<bool> is_closed_{false};
};

}  // namespace neug
