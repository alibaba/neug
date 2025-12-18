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

#include "neug/utils/service_utils.h"

namespace gs {}
namespace server {

struct ServiceConfig {
  static constexpr const uint32_t DEFAULT_SHARD_NUM = 1;
  static constexpr const uint32_t DEFAULT_QUERY_PORT = 10000;

  // Those has default value
  uint32_t query_port;
  uint32_t shard_num;

  // Those has not default value
  std::string host_str;

  ServiceConfig()
      : query_port(DEFAULT_QUERY_PORT),
        shard_num(DEFAULT_SHARD_NUM),
        host_str("127.0.0.1") {}
};

class IHttpHandlerManager {
 public:
  virtual ~IHttpHandlerManager() = default;
  virtual void Init(const ServiceConfig& config) = 0;
  virtual std::string Start() = 0;
  virtual void Stop() = 0;
  virtual void RunAndWaitForExit() = 0;
  virtual bool IsRunning() const = 0;
};
}  // namespace server
