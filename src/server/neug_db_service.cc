/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/server/neug_db_service.h"

#include <condition_variable>
#include <exception>
#include <mutex>
#include <utility>

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
    try {
      StopImpl();
    } catch (const std::exception& e) {
      LOG(ERROR) << "Failed to stop NeugDB service during destruction: "
                 << e.what();
    } catch (...) {
      LOG(ERROR) << "Failed to stop NeugDB service during destruction";
    }
  }

  std::string Start() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (state_ != State::kIdle) {
        THROW_RUNTIME_ERROR("NeugDB service has already been started!");
      }
      state_ = State::kStarting;
    }

    try {
      runtime_.StartCompaction();
      runtime_.OpenAdmission();
      auto endpoint = handler_.Start();
      {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ = State::kRunning;
      }
      state_changed_.notify_all();
      return endpoint;
    } catch (...) {
      auto start_error = std::current_exception();
      try {
        cleanupRuntime();
      } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to clean up a failed service start: " << e.what();
      } catch (...) {
        LOG(ERROR) << "Failed to clean up a failed service start";
      }
      {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ = State::kIdle;
      }
      state_changed_.notify_all();
      std::rethrow_exception(start_error);
    }
  }

  void Stop() { StopImpl(); }

  void RunAndWaitForExit() {
    Start();
    try {
      handler_.WaitForExit();
    } catch (...) {
      auto wait_error = std::current_exception();
      try {
        StopImpl();
      } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to clean up service after wait failure: "
                   << e.what();
      } catch (...) {
        LOG(ERROR) << "Failed to clean up service after wait failure";
      }
      std::rethrow_exception(wait_error);
    }
    StopImpl();
  }

  bool IsRunning() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return state_ == State::kRunning;
  }

  NeugDB& db() const { return db_; }

  TpServiceRuntime& runtime() { return runtime_; }

  const TpServiceRuntime& runtime() const { return runtime_; }

 private:
  enum class State { kIdle, kStarting, kRunning, kStopping };

  template <typename Operation>
  static void TryCleanup(std::exception_ptr& first_error,
                         Operation&& operation) noexcept {
    try {
      operation();
    } catch (...) {
      if (!first_error) {
        first_error = std::current_exception();
      }
    }
  }

  void cleanupRuntime() {
    std::exception_ptr first_error;
    TryCleanup(first_error, [this] { runtime_.CloseAdmission(); });
    TryCleanup(first_error, [this] { handler_.Stop(); });
    TryCleanup(first_error, [this] { runtime_.Drain(); });
    TryCleanup(first_error, [this] { runtime_.StopCompaction(); });
    if (first_error) {
      std::rethrow_exception(first_error);
    }
  }

  void StopImpl() {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      state_changed_.wait(lock,
                          [this]() { return state_ != State::kStarting; });
      if (state_ == State::kIdle) {
        return;
      }
      if (state_ == State::kStopping) {
        state_changed_.wait(lock, [this]() { return state_ == State::kIdle; });
        return;
      }
      state_ = State::kStopping;
    }

    std::exception_ptr cleanup_error;
    try {
      cleanupRuntime();
    } catch (...) { cleanup_error = std::current_exception(); }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      state_ = State::kIdle;
    }
    state_changed_.notify_all();
    if (cleanup_error) {
      std::rethrow_exception(cleanup_error);
    }
  }

  // Declaration order is intentional: the lease is acquired first and
  // released last, after every runtime resource has been destroyed.
  NeugDB::ServiceModeLease mode_lease_;
  NeugDB& db_;
  TpServiceRuntime runtime_;
  BrpcServiceManager handler_;

  mutable std::mutex mutex_;
  std::condition_variable state_changed_;
  State state_{State::kIdle};
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
