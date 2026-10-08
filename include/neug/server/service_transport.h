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

#include <memory>
#include <string>

#include "neug/transaction/runtime_wait.h"

namespace neug {
class IExecutionSlotSynchronizer;

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

  /** Stops accepting new requests without waiting for active callbacks. */
  virtual void StopAccepting() noexcept = 0;

  /** Waits for active callbacks after StopAccepting(). Idempotent. */
  virtual void Join() noexcept = 0;

  /** Scheduler wait used while this transport runs request callbacks. */
  virtual RuntimeWaitFn RuntimeWait() const noexcept {
    return &NativeRuntimeWait;
  }

  /** Creates a waiter compatible with this transport's callback scheduler. */
  virtual std::unique_ptr<IExecutionSlotSynchronizer> CreateSlotSynchronizer()
      const;

  /** Whether this transport's process-wide shutdown has been requested. */
  virtual bool IsExitRequested() const noexcept { return false; }

  /** Stops the transport and waits until handlers can no longer call the
   * service. */
  void StopAndJoin() noexcept {
    StopAccepting();
    Join();
  }
};

}  // namespace neug
