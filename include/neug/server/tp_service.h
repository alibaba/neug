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

#include <chrono>
#include <optional>
#include <string>
#include <string_view>

#include "neug/main/query_result.h"
#include "neug/main/transaction_mode.h"
#include "neug/utils/result.h"

namespace neug {

struct QueryRequest;

struct BeginTransactionResult {
  std::string transaction_id;
  // Advisory expiry time for client display. The runtime enforces expiry with
  // a steady clock, so this value may drift when the system clock changes.
  // Nullopt means that expiry is disabled.
  std::optional<std::chrono::system_clock::time_point> expires_at;
};

/** Protocol-neutral application boundary for NeuG TP service operations. */
class ITpService {
 public:
  virtual ~ITpService() = default;

  virtual result<QueryResult> ExecuteQuery(const QueryRequest& request) = 0;
  virtual result<std::string> GetSchema() = 0;
  virtual result<std::string> GetServiceStatus() = 0;

  virtual result<BeginTransactionResult> BeginTransaction(
      TransactionMode mode) = 0;
  virtual result<std::string> ExecuteInTransaction(
      std::string_view transaction_id, const QueryRequest& request) = 0;
  virtual Status CommitTransaction(std::string_view transaction_id) = 0;
  virtual Status RollbackTransaction(std::string_view transaction_id) = 0;
};

}  // namespace neug
