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

#include "neug/server/neug_db_service.h"

#include <brpc/controller.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>

#include "../main/service_mode_lease.h"
#include "brpc_transport.h"
#include "neug/main/neug_db.h"
#include "neug/server/service_transport.h"
#include "neug/utils/exception/exception.h"
#include "tp_service_runtime.h"

namespace neug {

class NeugDBService::Impl {
 public:
  Impl(NeugDB& db, const ServiceConfig& config, const TransportFactory& factory)
      : mode_lease_(db.enterServiceMode()),
        db_(db),
        runtime_(db_, config),
        transport_(factory ? factory(runtime_)
                           : std::make_unique<BrpcTransport>(
                                 runtime_, runtime_.config().host_str,
                                 runtime_.config().query_port)) {
    if (!transport_) {
      THROW_RUNTIME_ERROR("Service transport factory returned null");
    }
  }

  ~Impl() { StopResources(); }

  std::string Start() {
    std::lock_guard<std::mutex> lock(mutex_);
    return StartLocked();
  }

  void Stop() { StopImpl(/*report_not_running=*/true); }

  void RunAndWaitForExit(const std::function<void()>& before_wait) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      StartLocked();
      run_and_wait_active_ = true;
    }

    struct WaitGuard {
      std::mutex& mutex;
      bool& active;
      ~WaitGuard() {
        std::lock_guard<std::mutex> lock(mutex);
        active = false;
      }
    } guard{mutex_, run_and_wait_active_};

    try {
      if (before_wait) {
        before_wait();
      }
      std::unique_lock<std::mutex> lock(mutex_);
      // Preserve BRPC's process-wide quit-signal handling.
      while (IsRunning() && !brpc::IsAskedToQuit()) {
        stopped_cv_.wait_for(lock, std::chrono::milliseconds(100));
      }
    } catch (...) {
      StopImpl(/*report_not_running=*/false);
      throw;
    }
    StopImpl(/*report_not_running=*/false);
  }

  bool IsRunning() const { return running_.load(std::memory_order_relaxed); }

  NeugDB& db() const { return db_; }

  TpServiceRuntime& runtime() { return runtime_; }

  const TpServiceRuntime& runtime() const { return runtime_; }

 private:
  // Caller holds mutex_, except during destruction when no callers remain.
  // Unconditional cleanup also covers startup failure and resources acquired
  // before the service was started.
  void StopResources() {
    runtime_.CloseAdmission();
    transport_->StopAndJoin();
    runtime_.Drain();
    runtime_.StopCompaction();
  }

  std::string StartLocked() {
    if (IsRunning() || run_and_wait_active_) {
      THROW_RUNTIME_ERROR("NeugDB service has already been started!");
    }
    runtime_.StartCompaction();
    try {
      runtime_.OpenAdmission();
      auto ret = transport_->Start();
      running_.store(true, std::memory_order_relaxed);
      return ret;
    } catch (...) {
      StopResources();
      throw;
    }
  }

  void StopImpl(bool report_not_running) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!IsRunning()) {
      if (report_not_running) {
        std::cerr << "NeugDB service has not been started!" << std::endl;
      }
      return;
    }
    StopResources();
    running_.store(false, std::memory_order_relaxed);
    stopped_cv_.notify_all();
  }
  // Declaration order is intentional: the lease is acquired first and
  // released last, after every runtime resource has been destroyed.
  NeugDB::ServiceModeLease mode_lease_;
  NeugDB& db_;
  TpServiceRuntime runtime_;
  std::unique_ptr<IServiceTransport> transport_;

  std::atomic<bool> running_{false};
  std::mutex mutex_;
  std::condition_variable stopped_cv_;
  // Prevents a new start from attaching to the server generation owned by an
  // in-flight RunAndWaitForExit call after another thread has stopped it.
  bool run_and_wait_active_{false};
};

NeugDBService::NeugDBService(NeugDB& db, const ServiceConfig& config)
    : NeugDBService(db, config, {}) {}

NeugDBService::NeugDBService(NeugDB& db, const ServiceConfig& config,
                             const TransportFactory& factory)
    : impl_(std::make_unique<Impl>(db, config, factory)) {}

NeugDBService::~NeugDBService() = default;

NeugDB& NeugDBService::db() { return impl_->db(); }

std::string NeugDBService::Start() { return impl_->Start(); }

void NeugDBService::Stop() { impl_->Stop(); }

const ServiceConfig& NeugDBService::GetServiceConfig() const {
  return impl_->runtime().config();
}

ExecutionSlotLease NeugDBService::AcquireExecutionSlot() {
  return impl_->runtime().AcquireExecutionSlot();
}

bool NeugDBService::IsRunning() const { return impl_->IsRunning(); }

result<std::string> NeugDBService::service_status() {
  if (!IsRunning()) {
    return result<std::string>("NeugDB service has not been started!");
  }
  return result<std::string>("NeugDB service is running ...");
}

void NeugDBService::run_and_wait_for_exit() { runAndWaitForExitWithHook({}); }

void NeugDBService::runAndWaitForExitWithHook(
    const std::function<void()>& before_wait) {
  impl_->RunAndWaitForExit(before_wait);
}

size_t NeugDBService::getExecutedQueryNum() const {
  return impl_->runtime().getExecutedQueryNum();
}

size_t NeugDBService::ExecutionSlotNum() const {
  return impl_->runtime().ExecutionSlotNum();
}

}  // namespace neug
