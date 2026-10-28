/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <string>
#include <string_view>

#include "neug/main/query_result.h"
#include "neug/main/transaction_mode.h"
#include "neug/server/service_transaction.h"
#include "neug/utils/result.h"

namespace neug {

struct QueryRequest;

/**
 * @brief Application boundary between TP operations and transport adapters.
 *
 * Transport adapters own endpoint mapping, request parsing, and query result
 * encoding. Schema and status retain their existing JSON payloads.
 */
class ITpOperations {
 public:
  virtual ~ITpOperations() = default;

  /**
   * @brief Executes a standalone query with service-managed transactions.
   * @param request Parsed query, parameters, and requested access mode.
   * @return Typed query result, or a query execution error.
   */
  virtual result<QueryResult> ExecuteQuery(const QueryRequest& request) = 0;

  /** @brief Returns the graph schema as JSON, or a schema retrieval error. */
  virtual result<std::string> GetSchema() = 0;

  /** @brief Returns the service health and version as a JSON payload. */
  virtual result<std::string> GetServiceStatus() = 0;

  /**
   * @brief Opens an explicit transaction with a fixed access policy.
   * @param mode Read-only snapshot or read-write private COW transaction.
   * @return Transaction identifier and optional expiry, or an admission error.
   */
  virtual result<ServiceTransactionInfo> BeginTransaction(
      TransactionMode mode) = 0;

  /**
   * @brief Executes a query without committing the explicit transaction.
   * @param transaction_id Identifier returned by BeginTransaction().
   * @param request Parsed query, parameters, and requested access mode.
   * @return Typed query result, or a transaction/query error. Encoding failures
   * in the transport affect only the response, not the transaction state.
   */
  virtual result<QueryResult> ExecuteInTransaction(
      std::string_view transaction_id, const QueryRequest& request) = 0;

  /**
   * @brief Commits an explicit transaction and releases it on success.
   * @param transaction_id Identifier returned by BeginTransaction().
   * @return OK on success, or a lookup, transaction-state, or commit error.
   */
  virtual Status CommitTransaction(std::string_view transaction_id) = 0;

  /**
   * @brief Rolls back an explicit transaction and releases its resources.
   * @param transaction_id Identifier returned by BeginTransaction().
   * @return OK on success, or a transaction lookup/state error.
   */
  virtual Status RollbackTransaction(std::string_view transaction_id) = 0;
};

}  // namespace neug
