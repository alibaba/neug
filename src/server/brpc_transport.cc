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

#include <brpc/controller.h>
#include <brpc/server.h>
#include <bthread/bthread.h>
#include <algorithm>
#include <utility>

#include "brpc_http_handler.h"
#include "default_service_transport.h"
#include "neug/server/bthread_runtime_wait.h"
#include "neug/server/service_config.h"
#include "neug/server/tp_execution_slot_pool.h"
#include "neug/utils/exception/exception.h"

namespace neug {

namespace {
class BthreadSlotSynchronizer final : public IExecutionSlotSynchronizer {
 public:
  BthreadSlotSynchronizer() {
    CHECK_EQ(bthread_mutex_init(&mutex_, nullptr), 0);
    CHECK_EQ(bthread_cond_init(&cond_, nullptr), 0);
  }
  ~BthreadSlotSynchronizer() override {
    bthread_cond_destroy(&cond_);
    bthread_mutex_destroy(&mutex_);
  }
  void lock() noexcept override { bthread_mutex_lock(&mutex_); }
  void unlock() noexcept override { bthread_mutex_unlock(&mutex_); }
  void Wait() noexcept override { bthread_cond_wait(&cond_, &mutex_); }
  void NotifyOne() noexcept override { bthread_cond_signal(&cond_); }

 private:
  bthread_mutex_t mutex_;
  bthread_cond_t cond_;
};
}  // namespace

std::unique_ptr<IServiceTransport> CreateDefaultServiceTransport(
    ITpOperations& service, const ServiceConfig& config,
    int database_thread_num) {
  return std::make_unique<BrpcTransport>(
      service, config.host_str, config.query_port, database_thread_num);
}

BrpcTransport::BrpcTransport(ITpOperations& service, std::string host,
                             uint32_t port, int database_thread_num)
    : host_(std::move(host)),
      port_(port),
      database_thread_num_(database_thread_num),
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
  if (database_thread_num_ > 0) {
    // BRPC worker capacity follows the database; execution slots enforce
    // service-local query concurrency.
    bthread_setconcurrency(
        std::max(database_thread_num_, BTHREAD_MIN_CONCURRENCY));
  }
  const auto address = host_ + ":" + std::to_string(port_);
  brpc::ServerOptions options;
  options.idle_timeout_sec = 60;
  // The backend configures process-wide bthread capacity separately. Query
  // concurrency is enforced by the execution-slot pool.
  options.num_threads = 0;
  if (server_->Start(address.c_str(), &options) != 0) {
    THROW_RUNTIME_ERROR("Failed to start BRPC server on " + address);
  }
  join_pending_ = true;
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

void BrpcTransport::StopAccepting() noexcept {
  if (join_pending_ && server_->IsRunning()) {
    server_->Stop(0);
  }
}

void BrpcTransport::Join() noexcept {
  if (join_pending_) {
    StopAccepting();
    server_->Join();
    join_pending_ = false;
  }
}

RuntimeWaitFn BrpcTransport::RuntimeWait() const noexcept {
  return &BthreadRuntimeWait;
}

std::unique_ptr<IExecutionSlotSynchronizer>
BrpcTransport::CreateSlotSynchronizer() const {
  return std::make_unique<BthreadSlotSynchronizer>();
}

bool BrpcTransport::IsExitRequested() const noexcept {
  return brpc::IsAskedToQuit();
}

}  // namespace neug
