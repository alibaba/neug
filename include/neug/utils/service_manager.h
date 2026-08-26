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

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "neug/utils/result.h"
#include "neug/utils/service_utils.h"

namespace neug {
class Status;
}  // namespace neug

namespace neug {

/**
 * @brief Configuration for NeuG HTTP service.
 *
 * ServiceConfig contains settings for the HTTP server that handles remote
 * Cypher query execution. Use this to configure the service endpoint before
 * starting NeugDBService.
 *
 * **Usage Example:**
 * @code{.cpp}
 * neug::ServiceConfig config;
 * config.query_port = 8080;       // Listen on port 8080
 * config.host_str = "0.0.0.0";    // Accept connections from any interface
 * config.auto_compaction = true;
 *
 * neug::NeugDBService service(db, config);
 * service.Start();
 * @endcode
 *
 * @see NeugDBService for HTTP service management
 * @since v0.1.0
 */
struct ServiceConfig {
  /// Default thread count policy: 0 means auto-select from database
  /// max_thread_num.
  static constexpr const uint32_t DEFAULT_THREAD_NUM = 0;
  /// Default HTTP port for query endpoint
  static constexpr const uint32_t DEFAULT_QUERY_PORT = 10000;
  /// 0 scales the session limit with the TP execution slot count.
  static constexpr const uint32_t DEFAULT_MAX_EXPLICIT_TRANSACTIONS = 0;
  /// Default absolute lifetime for an explicit HTTP transaction.
  static constexpr const uint64_t DEFAULT_EXPLICIT_TRANSACTION_TIMEOUT_MS =
      60000;

  /// HTTP port for the query endpoint (default: 10000)
  uint32_t query_port;
  /// Service thread count. 0 means auto-select from database max_thread_num. If
  /// set, values above the database max_thread_num are clamped to that limit
  /// and a warning is logged.
  uint32_t thread_num;
  /// Host address to bind (default: "127.0.0.1", use "0.0.0.0" for all
  /// interfaces)
  std::string host_str;
  /// Enable background auto-compaction thread while serving
  bool auto_compaction;
  /// Maximum active explicit HTTP transactions. 0 follows the slot count.
  uint32_t max_explicit_transactions;
  /// Absolute lifetime of an explicit HTTP transaction, in milliseconds.
  uint64_t explicit_transaction_timeout_ms;

  /**
   * @brief Constructs ServiceConfig with default values.
   *
   * Default configuration:
   * - query_port: 10000
   * - thread_num: 0 (auto-select from database max_thread_num)
   * - host_str: "127.0.0.1" (localhost only)
   * - auto_compaction: true
   * - max_explicit_transactions: 0 (one session per TP slot)
   * - explicit_transaction_timeout_ms: 60000
   */
  ServiceConfig()
      : query_port(DEFAULT_QUERY_PORT),
        thread_num(DEFAULT_THREAD_NUM),
        host_str("127.0.0.1"),
        auto_compaction(true),
        max_explicit_transactions(DEFAULT_MAX_EXPLICIT_TRANSACTIONS),
        explicit_transaction_timeout_ms(
            DEFAULT_EXPLICIT_TRANSACTION_TIMEOUT_MS) {}
};

class IServiceManager {
 public:
  virtual ~IServiceManager() = default;
  virtual void Init(const ServiceConfig& config) = 0;
  virtual std::string Start() = 0;
  virtual void Stop() = 0;
  virtual void RunAndWaitForExit() = 0;
  virtual bool IsRunning() const = 0;
};
}  // namespace neug
