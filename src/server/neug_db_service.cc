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
#include "neug/server/brpc_service_mgr.h"

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

namespace neug {

namespace {
constexpr auto kCompactInterval = std::chrono::seconds(30);
constexpr size_t kCompactQueryThreshold = 100000;
}  // namespace

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
  if (config.thread_num > 0 &&
      config.thread_num > static_cast<uint32_t>(db_config_.max_thread_num)) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Invalid service thread_num: " + std::to_string(config.thread_num) +
        ". Must be less than or equal to database max_thread_num: " +
        std::to_string(db_config_.max_thread_num) + ".");
  }

  version_manager_ = std::make_shared<neug::VersionManager>();
  version_manager_->init_ts(
      db_.last_ts_,
      db_config_.max_thread_num);  // We assume versions start from 1.

  session_pool_ = std::make_unique<neug::SessionPool>(
      db_, db_.GetPlanner(), db_.GetQueryCache(), version_manager_,
      db_.allocators_, db_config_);

  hdl_mgr_ = std::make_unique<BrpcServiceManager>(db_, *session_pool_);
  hdl_mgr_->Init(config);
  service_config_ = config;
}

NeugDBService::~NeugDBService() {
  stopCompactThread();
  if (hdl_mgr_) {
    hdl_mgr_->Stop();
    hdl_mgr_.reset();
  }
}

const ServiceConfig& NeugDBService::GetServiceConfig() const {
  return service_config_;
}

neug::SessionGuard NeugDBService::AcquireSession() {
  return session_pool_->AcquireSession();
}

bool NeugDBService::IsRunning() const {
  return running_.load(std::memory_order_relaxed);
}

neug::result<std::string> NeugDBService::service_status() {
  if (!hdl_mgr_ || !session_pool_) {
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
    hdl_mgr_->RunAndWaitForExit();
    running_.store(false, std::memory_order_relaxed);
  } catch (...) {
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
    hdl_mgr_->Stop();
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
      auto ret = hdl_mgr_->Start();
      running_.store(true, std::memory_order_relaxed);
      return ret;
    } catch (...) {
      stopCompactThread();
      throw;
    }
  } else {
    THROW_RUNTIME_ERROR("Query handler has not been inited!");
  }
}

size_t NeugDBService::getExecutedQueryNum() const {
  return session_pool_->getExecutedQueryNum();
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
            auto session_guard = AcquireSession();
            auto txn = session_guard->GetCompactTransaction();
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
