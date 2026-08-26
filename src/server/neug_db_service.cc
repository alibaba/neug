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

#include <glog/logging.h>

#include <algorithm>

#include <bthread/bthread.h>

#include "neug/main/checkpoint_coordinator.h"
#include "neug/server/brpc_service_mgr.h"
#include "neug/server/bthread_runtime_wait.h"
#include "neug/transaction/version_manager.h"
#include "service_transaction_manager.h"

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

namespace neug {

namespace {
constexpr auto kCompactInterval = std::chrono::seconds(30);
constexpr size_t kCompactQueryThreshold = 100000;
}  // namespace

NeugDBService::NeugDBService(neug::NeugDB& db, const ServiceConfig& config)
    : db_(db), db_config_(db_.config()) {
  db_.registerService(this);
  try {
    installBthreadRuntimeWait();
    init(config);
  } catch (...) {
    hdl_mgr_.reset();
    transaction_manager_.reset();
    execution_slot_pool_.reset();
    restoreNativeRuntimeWait();
    db_.unregisterService(this);
    throw;
  }
}

void NeugDBService::installBthreadRuntimeWait() {
  CHECK(!bthread_runtime_wait_installed_);
  if (!db_.version_manager_->try_set_runtime_wait_if_quiescent(
          &BthreadRuntimeWait)) {
    THROW_RUNTIME_ERROR(
        "Cannot install bthread runtime wait while transactions are "
        "active.");
  }
  bthread_runtime_wait_installed_ = true;
}

void NeugDBService::restoreNativeRuntimeWait() noexcept {
  if (!bthread_runtime_wait_installed_) {
    return;
  }
  CHECK(db_.version_manager_->try_set_runtime_wait_if_quiescent(
      &NativeRuntimeWait))
      << "All service transactions must be quiescent before restoring native "
         "runtime wait";
  bthread_runtime_wait_installed_ = false;
}

void NeugDBService::init(const ServiceConfig& config) {
  if (db_.IsClosed()) {
    THROW_RUNTIME_ERROR("NeugDB instance is not ready for serving!");
  }
  if (hdl_mgr_) {
    LOG(ERROR) << "NeugDB service has already been initialized!";
    return;
  }
  if (running_.load(std::memory_order_relaxed)) {
    LOG(ERROR) << "NeugDB service is already running!";
    return;
  }
  ServiceConfig effective_config = config;
  if (effective_config.thread_num > 0 &&
      effective_config.thread_num >
          static_cast<uint32_t>(db_config_.max_thread_num)) {
    LOG(WARNING) << "Service thread_num (" << effective_config.thread_num
                 << ") exceeds database max_thread_num ("
                 << db_config_.max_thread_num << "); clamping to "
                 << db_config_.max_thread_num << ".";
    effective_config.thread_num =
        static_cast<uint32_t>(db_config_.max_thread_num);
  }

  bthread_setconcurrency(
      std::max(db_config_.max_thread_num, BTHREAD_MIN_CONCURRENCY));

  execution_slot_pool_ = std::make_unique<neug::TpExecutionSlotPool>(
      db_.graph_snapshot_store(), db_.GetPlanner(), db_.GetQueryCache(),
      *db_.version_manager_, *db_.checkpoint_coordinator_,
      db_.extension_manager(), db_.allocators_, *db_.wal_writers_, db_config_);

  transaction_manager_ = std::make_unique<ServiceTransactionManager>(
      *execution_slot_pool_, effective_config.max_explicit_transactions,
      effective_config.explicit_transaction_timeout_ms);
  hdl_mgr_ = std::make_unique<BrpcServiceManager>(db_, *execution_slot_pool_,
                                                  *transaction_manager_);
  hdl_mgr_->Init(effective_config);
  service_config_ = effective_config;
}

NeugDBService::~NeugDBService() {
  if (transaction_manager_) {
    transaction_manager_->CloseAdmission();
  }
  if (hdl_mgr_) {
    hdl_mgr_->Stop();
  }
  if (transaction_manager_) {
    transaction_manager_->Close();
  }
  // An auto-compaction task can be waiting for an explicit write session's
  // update lease. Roll back those sessions before joining the compact thread.
  stopCompactThread();
  hdl_mgr_.reset();
  transaction_manager_.reset();
  execution_slot_pool_.reset();
  restoreNativeRuntimeWait();
  db_.unregisterService(this);
}

const ServiceConfig& NeugDBService::GetServiceConfig() const {
  return service_config_;
}

neug::ExecutionSlotLease NeugDBService::AcquireExecutionSlot() {
  return execution_slot_pool_->AcquireExecutionSlot();
}

bool NeugDBService::IsRunning() const {
  return running_.load(std::memory_order_relaxed);
}

neug::result<std::string> NeugDBService::service_status() {
  if (!hdl_mgr_ || !execution_slot_pool_) {
    return neug::result<std::string>(
        "NeugDB service has not been initialized!");
  }
  if (!IsRunning()) {
    return neug::result<std::string>("NeugDB service has not been started!");
  }
  return neug::result<std::string>("NeugDB service is running ...");
}

void NeugDBService::run_and_wait_for_exit() {
  if (IsRunning()) {
    THROW_RUNTIME_ERROR("NeugDB service has already been started!");
  }
  if (!hdl_mgr_) {
    THROW_RUNTIME_ERROR("Query handler has not been inited!");
  }
  startCompactThread();
  running_.store(true, std::memory_order_relaxed);
  try {
    transaction_manager_->Open();
    hdl_mgr_->RunAndWaitForExit();
    transaction_manager_->Close();
    running_.store(false, std::memory_order_relaxed);
  } catch (...) {
    transaction_manager_->CloseAdmission();
    hdl_mgr_->Stop();
    transaction_manager_->Close();
    running_.store(false, std::memory_order_relaxed);
    stopCompactThread();
    throw;
  }
  stopCompactThread();
}

void NeugDBService::Stop() {
  std::unique_lock<std::mutex> lock(mtx_);
  if (!IsRunning()) {
    std::cerr << "NeugDB service has not been started!" << std::endl;
    return;
  }
  if (hdl_mgr_) {
    transaction_manager_->CloseAdmission();
    hdl_mgr_->Stop();
    transaction_manager_->Close();
    running_.store(false, std::memory_order_relaxed);
    stopCompactThread();
    return;
  } else {
    THROW_RUNTIME_ERROR("Query handler has not been inited!");
  }
}

std::string NeugDBService::Start() {
  std::unique_lock<std::mutex> lock(mtx_);
  if (IsRunning()) {
    THROW_RUNTIME_ERROR("NeugDB service has already been started!");
  }
  if (hdl_mgr_) {
    startCompactThread();
    try {
      transaction_manager_->Open();
      auto ret = hdl_mgr_->Start();
      running_.store(true, std::memory_order_relaxed);
      return ret;
    } catch (...) {
      transaction_manager_->CloseAdmission();
      stopCompactThread();
      throw;
    }
  } else {
    THROW_RUNTIME_ERROR("Query handler has not been inited!");
  }
}

size_t NeugDBService::getExecutedQueryNum() const {
  return execution_slot_pool_->getExecutedQueryNum();
}

void NeugDBService::stopCompactThread() {
  compact_thread_running_.store(false, std::memory_order_relaxed);
  compact_cv_.notify_all();
  if (compact_thread_.joinable()) {
    compact_thread_.join();
  }
}

void NeugDBService::startCompactThread() {
  if (!service_config_.auto_compaction) {
    return;
  }
  stopCompactThread();
  compact_thread_running_.store(true, std::memory_order_relaxed);
  try {
    compact_thread_ = std::thread([this]() {
      size_t last_compaction_at = 0;
      while (compact_thread_running_.load(std::memory_order_relaxed)) {
        size_t query_num_before = getExecutedQueryNum();
        {
          std::unique_lock<std::mutex> lock(compact_mtx_);
          if (compact_cv_.wait_for(lock, kCompactInterval, [this] {
                return !compact_thread_running_.load(std::memory_order_relaxed);
              })) {
            break;
          }
        }
        if (!compact_thread_running_.load(std::memory_order_relaxed)) {
          break;
        }
        try {
          size_t query_num_after = getExecutedQueryNum();
          if (query_num_before == query_num_after &&
              (query_num_after >
               (last_compaction_at + kCompactQueryThreshold))) {
            VLOG(10) << "Trigger auto compaction";
            last_compaction_at = query_num_after;
            auto slot_lease = AcquireExecutionSlot();
            auto txn = slot_lease->BeginInPlaceCompactionTransaction();
            txn.Commit();
            VLOG(10) << "Finish compaction";
          }
        } catch (const std::exception& e) {
          LOG(WARNING) << "Auto compaction failed: " << e.what();
        } catch (...) {
          LOG(WARNING) << "Auto compaction failed with unknown error";
        }
      }
    });
  } catch (...) {
    compact_thread_running_.store(false, std::memory_order_relaxed);
    throw;
  }
}

}  // namespace neug
