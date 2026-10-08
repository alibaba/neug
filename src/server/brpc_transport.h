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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "neug/server/service_transport.h"

namespace brpc {
class Server;
}
namespace google::protobuf {
class Service;
}

namespace neug {
class ITpOperations;

/** Owns a BRPC server and its protocol handlers; lifecycle calls are serialized
 * by NeugDBService. HTTP is the currently registered protocol. */
class BrpcTransport final : public IServiceTransport {
 public:
  BrpcTransport(ITpOperations& service, std::string host, uint32_t port,
                int database_thread_num = 0);
  ~BrpcTransport() override;

  std::string Start() override;
  void StopAccepting() noexcept override;
  void Join() noexcept override;
  RuntimeWaitFn RuntimeWait() const noexcept override;
  std::unique_ptr<IExecutionSlotSynchronizer> CreateSlotSynchronizer()
      const override;
  bool IsExitRequested() const noexcept override;

 private:
  std::string host_;
  uint32_t port_;
  int database_thread_num_;
  // Destroy the server before the handlers it borrows.
  std::vector<std::unique_ptr<google::protobuf::Service>> handlers_;
  std::unique_ptr<brpc::Server> server_;
  bool join_pending_{false};
};

}  // namespace neug
