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

#include <atomic>
#include <iostream>
#include <mutex>

#include "../main/service_mode_lease.h"
#include "neug/main/neug_db.h"
#include "neug/server/brpc_service_mgr.h"
#include "neug/utils/exception/exception.h"
#include "tp_service_runtime.h"

namespace neug {

class NeugDBService::Impl {
 public:
  Impl(NeugDB& db, const ServiceConfig& config)
      : mode_lease_(db.enterServiceMode()),
        db_(db),
        runtime_(db_, config),
        handler_(runtime_, runtime_.max_thread_num()) {
    handler_.Init(runtime_.config());
  }

  ~Impl() {
    runtime_.CloseAdmission();
    handler_.Stop();
    runtime_.Drain();
    runtime_.StopCompaction();
  }

  std::string Start() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (IsRunning()) {
      THROW_RUNTIME_ERROR("NeugDB service has already been started!");
    }
    runtime_.StartCompaction();
    try {
      runtime_.OpenAdmission();
      auto ret = handler_.Start();
      running_.store(true, std::memory_order_relaxed);
      return ret;
    } catch (...) {
      runtime_.CloseAdmission();
      runtime_.StopCompaction();
      throw;
    }
  }

  void Stop() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!IsRunning()) {
      std::cerr << "NeugDB service has not been started!" << std::endl;
      return;
    }
    runtime_.CloseAdmission();
    handler_.Stop();
    runtime_.Drain();
    running_.store(false, std::memory_order_relaxed);
    runtime_.StopCompaction();
  }

  void RunAndWaitForExit() {
    if (IsRunning()) {
      THROW_RUNTIME_ERROR("NeugDB service has already been started!");
    }
    runtime_.StartCompaction();
    running_.store(true, std::memory_order_relaxed);
    try {
      runtime_.OpenAdmission();
      handler_.RunAndWaitForExit();
      runtime_.Drain();
      running_.store(false, std::memory_order_relaxed);
    } catch (...) {
      runtime_.CloseAdmission();
      handler_.Stop();
      runtime_.Drain();
      running_.store(false, std::memory_order_relaxed);
      runtime_.StopCompaction();
      throw;
    }
    runtime_.StopCompaction();
  }

  bool IsRunning() const { return running_.load(std::memory_order_relaxed); }

  NeugDB& db() const { return db_; }

  TpServiceRuntime& runtime() { return runtime_; }

  const TpServiceRuntime& runtime() const { return runtime_; }

 private:
  // Declaration order is intentional: the lease is acquired first and
  // released last, after every runtime resource has been destroyed.
  NeugDB::ServiceModeLease mode_lease_;
  NeugDB& db_;
  TpServiceRuntime runtime_;
  BrpcServiceManager handler_;

  std::atomic<bool> running_{false};
  std::mutex mutex_;
};

NeugDBService::NeugDBService(NeugDB& db, const ServiceConfig& config)
    : impl_(std::make_unique<Impl>(db, config)) {}

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

void NeugDBService::run_and_wait_for_exit() { impl_->RunAndWaitForExit(); }

size_t NeugDBService::getExecutedQueryNum() const {
  return impl_->runtime().getExecutedQueryNum();
}

size_t NeugDBService::ExecutionSlotNum() const {
  return impl_->runtime().ExecutionSlotNum();
}

}  // namespace neug
