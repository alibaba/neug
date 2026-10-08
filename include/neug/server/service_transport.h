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

#include <string>

namespace neug {

/**
 * @brief Network lifecycle boundary for a TP service transport.
 *
 * NeugDBService serializes lifecycle calls. Request handlers borrow
 * ITpOperations; they must not outlive it. The interface does not prescribe
 * HTTP or RPC.
 */
class IServiceTransport {
 public:
  virtual ~IServiceTransport() = default;

  /**
   * @brief Starts listening and returns the actual endpoint.
   * On return the transport can accept requests. On failure it must leave no
   * listener or active request callbacks. Restart after StopAndJoin is allowed.
   */
  virtual std::string Start() = 0;

  /**
   * @brief Stops accepting requests and waits for all callbacks to finish.
   * Idempotent, including before Start or after a failed Start. On return no
   * handler may call the business service until the next successful Start.
   */
  virtual void StopAndJoin() noexcept = 0;
};

}  // namespace neug
