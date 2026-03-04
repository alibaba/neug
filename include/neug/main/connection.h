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
#include <memory>
#include <string>
#include <vector>

#include "neug/compiler/planner/graph_planner.h"
#include "neug/execution/common/types/value.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/main/query_processor.h"
#include "neug/main/query_result.h"
#include "neug/utils/result.h"

namespace neug {

class NeugDB;

/**
 * @brief Database connection for executing Cypher queries.
 *
 * Connection is the primary interface for interacting with a NeuG database.
 * It provides methods to execute Cypher queries, retrieve schema information,
 * and manage the connection lifecycle.
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
 * **Thread Safety:** This class is NOT thread-safe. Each thread should use
 * its own Connection instance. Use NeugDB::Connect() to create connections.
 *
 * **Lifecycle:**
 * - Created via NeugDB::Connect()
 * - Execute queries via Query() method
 * - Close via Close() or automatic cleanup in destructor
 *
 * @note Connections hold references to shared resources (planner, query
 * processor).
 * @note For best performance, reuse connections for multiple queries.
 *
 * @see NeugDB::Connect For creating connections
 * @see QueryResult For processing query results
 *
 * @since v0.1.0
 */
class Connection {
 public:
  Connection(PropertyGraph& graph,
             std::shared_ptr<QueryProcessor> query_processor)
      : graph_(graph), query_processor_(query_processor), is_closed_(false) {}
  ~Connection() { Close(); }

  /**
   * @brief Execute a Cypher query and return results.
   *
   * Compiles and executes a Cypher query string against the database.
   * The query is processed through the planner for optimization, then
   * executed by the query processor.
   *
   * **Usage Example:**
   * @code{.cpp}
   * // Simple read query
   * auto result = conn->Query("MATCH (n:Person) RETURN n.name", "read");
   *
   * // Query with parameters
   * neug::execution::ParamsMap params;
   * params["min_age"] = neug::execution::Value(18);
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
   *        - `"update"` or `"u"`: Update/delete operations (default)
   *        - `"schema"` or `"s"`: Schema modification operations
   * @param parameters Named parameters for parameterized queries.
   *        Keys are parameter names (without `$`), values are parameter values.
   *
   * @return result<QueryResult> containing either:
   *         - QueryResult with query results on success
   *         - Error status with message on failure
   *
   * @note Use parameterized queries for dynamic values to prevent injection.
   * @note Specifying correct access_mode ensures proper transaction handling.
   *
   * @see QueryResult For iterating over results
   *
   * @since v0.1.0
   */
  result<QueryResult> Query(const std::string& query_string,
                            const std::string& access_mode = "update",
                            const execution::ParamsMap& parameters = {});

  /**
   * @brief Execute a Cypher query with JSON parameters.
   * The parameter values are provided as a JSON object.
   */
  result<QueryResult> Query(const std::string& query_string,
                            const std::string& access_mode,
                            const rapidjson::Value& parameters_json);

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
   * @note This method is idempotent - calling it multiple times is safe.
   * @note The connection is also automatically closed in the destructor.
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
  PropertyGraph& graph_;

  std::shared_ptr<QueryProcessor> query_processor_;

  std::atomic<bool> is_closed_{false};
};

}  // namespace neug
