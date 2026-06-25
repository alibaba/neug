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

#include <algorithm>
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
 * ServiceConfig contains settings for the HTTP server that handles
 * remote Cypher query execution. Use this to configure the service
 * endpoint before starting NeugDBService.
 *
 * **Usage Example:**
 * @code{.cpp}
 * neug::ServiceConfig config;
 * config.query_port = 8080;       // Listen on port 8080
 * config.host_str = "0.0.0.0";    // Accept connections from any interface
 *
 * neug::NeugDBService service(db, config);
 * service.Start();
 * @endcode
 *
 * @see NeugDBService for HTTP service management
 * @since v0.1.0
 */
struct ServiceConfig {
  /// Default number of shards for distributed processing
  static constexpr const uint32_t DEFAULT_SHARD_NUM = 1;
  /// Default HTTP port for query endpoint
  static constexpr const uint32_t DEFAULT_QUERY_PORT = 10000;

  /// HTTP port for the query endpoint (default: 10000)
  uint32_t query_port;
  /// Number of shards for distributed processing (default: 1)
  uint32_t shard_num;
  /// Host address to bind (default: "127.0.0.1", use "0.0.0.0" for all
  /// interfaces)
  std::string host_str;

  /**
   * @brief Constructs ServiceConfig with default values.
   *
   * Default configuration:
   * - query_port: 10000
   * - shard_num: 1
   * - host_str: "127.0.0.1" (localhost only)
   */
  ServiceConfig()
      : query_port(DEFAULT_QUERY_PORT),
        shard_num(DEFAULT_SHARD_NUM),
        host_str("127.0.0.1") {}
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
