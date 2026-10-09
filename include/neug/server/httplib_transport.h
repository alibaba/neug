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

// Include httplib.h before any project headers on Windows. httplib.h pulls in
// winsock2.h; if a project header (e.g. neug/main/neug_db.h) includes windows.h
// first, the older winsock.h gets loaded and causes redefinition errors with
// winsock2.h.
#include "httplib.h"

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "neug/server/service_transport.h"

namespace neug {
class ITpOperations;

/**
 * @brief cpp-httplib based transport used where BRPC is unavailable (Windows).
 *
 * Owns an httplib::Server and its request handlers; lifecycle calls are
 * serialized by NeugDBService. Endpoints mirror BrpcHttpHandler so clients
 * observe the same HTTP surface on both platforms. The transport runs its
 * callbacks on ordinary native threads, so the inherited RuntimeWait() and
 * CreateSlotSynchronizer() defaults (native wait, std primitives) already
 * match its scheduler.
 */
class HttplibTransport final : public IServiceTransport {
 public:
  HttplibTransport(ITpOperations& service, std::string host, uint32_t port,
                   uint32_t thread_num);
  ~HttplibTransport() override;

  std::string Start() override;
  void StopAccepting() noexcept override;
  void Join() noexcept override;

 private:
  void RegisterHandlers();
  void HandleFinishTransaction(const httplib::Request& req,
                               httplib::Response& res, bool commit);

  ITpOperations& tp_operations_;
  std::string host_;
  uint32_t port_;
  uint32_t thread_num_;
  std::unique_ptr<httplib::Server> server_;
  std::thread listen_thread_;
  // Set once a listener has been started; Join() drains it exactly once.
  std::atomic<bool> join_pending_{false};
};

}  // namespace neug
