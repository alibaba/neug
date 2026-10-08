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
#include "brpc_transport.h"

#include <brpc/server.h>
#include <utility>

#include "brpc_http_handler.h"
#include "neug/utils/exception/exception.h"

namespace neug {

BrpcTransport::BrpcTransport(ITpOperations& service, std::string host,
                             uint32_t port)
    : host_(std::move(host)),
      port_(port),
      server_(std::make_unique<brpc::Server>()) {
#ifdef ENABLE_HTTP_PROTOCOL
  auto handler = std::make_unique<BrpcHttpHandler>(service);
  brpc::ServiceOptions options;
  options.ownership = brpc::SERVER_DOESNT_OWN_SERVICE;
  options.restful_mappings = BrpcHttpHandler::Routes();
  // Establish ownership before handing BRPC a borrowed pointer.
  handlers_.emplace_back(std::move(handler));
  if (server_->AddService(handlers_.back().get(), options) != 0) {
    THROW_RUNTIME_ERROR("Failed to register HTTP service with BRPC");
  }
#endif
  if (handlers_.empty()) {
    THROW_NOT_SUPPORTED_EXCEPTION("No BRPC service protocols are enabled.");
  }
}

BrpcTransport::~BrpcTransport() { StopAndJoin(); }

std::string BrpcTransport::Start() {
  const auto address = host_ + ":" + std::to_string(port_);
  brpc::ServerOptions options;
  options.idle_timeout_sec = 60;
  // The runtime owns process-wide bthread capacity. Query concurrency is
  // enforced by its execution slot pool, not by BRPC worker count.
  options.num_threads = 0;
  if (server_->Start(address.c_str(), &options) != 0) {
    THROW_RUNTIME_ERROR("Failed to start BRPC server on " + address);
  }
  try {
    const auto endpoint = "http://" + host_ + ":" +
                          std::to_string(server_->listen_address().port);
    LOG(INFO) << "BRPC server started at " << endpoint;
    return endpoint;
  } catch (...) {
    StopAndJoin();
    throw;
  }
}

void BrpcTransport::StopAndJoin() noexcept {
  if (server_->IsRunning()) {
    server_->Stop(0);
    server_->Join();
  }
}

}  // namespace neug
