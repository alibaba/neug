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
#include "neug/server/brpc_http_hdl_mgr.h"

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

namespace server {

void NeugDBService::init(const ServiceConfig& config) {
  if (db_.IsClosed()) {
    THROW_RUNTIME_ERROR("NeugDB instance is not ready for serving!");
  }
  if (initialized_.load(std::memory_order_relaxed)) {
    std::cerr << "NeugDB service has been already initialized!" << std::endl;
    return;
  }

  if (hdl_mgr_) {
    LOG(ERROR) << "NeugDB service has already been initialized!";
    return;
  }

  version_manager_ = std::make_shared<gs::TPVersionManager>();
  version_manager_->init_ts(
      db_.last_ts_, db_config_.thread_num);  // We assume versions start from 1.

  txn_manager_ = std::make_unique<gs::TransactionManager>(
      version_manager_, db_.graph(), db_.allocators_, db_config_,
      db_.work_dir(), db_config_.thread_num);

  hdl_mgr_ = std::make_unique<BrpcHttpHandlerManager>(db_, *txn_manager_);
  hdl_mgr_->Init(config);

  initialized_.store(true);
  service_config_ = config;
}

NeugDBService::~NeugDBService() {
  if (hdl_mgr_) {
    hdl_mgr_->Stop();
    hdl_mgr_.reset();
  }
  if (compact_thread_running_) {
    compact_thread_running_ = false;
    compact_thread_.join();
  }
}

const ServiceConfig& NeugDBService::GetServiceConfig() const {
  return service_config_;
}

gs::ReadTransaction NeugDBService::GetReadTransaction(int thread_id) {
  return txn_manager_->GetReadTransaction(thread_id);
}

gs::InsertTransaction NeugDBService::GetInsertTransaction(int thread_id) {
  return txn_manager_->GetInsertTransaction(thread_id);
}

gs::UpdateTransaction NeugDBService::GetUpdateTransaction(int thread_id) {
  return txn_manager_->GetUpdateTransaction(thread_id);
}

gs::CompactTransaction NeugDBService::GetCompactTransaction(int thread_id) {
  return txn_manager_->GetCompactTransaction(thread_id);
}

NeugDBSession& NeugDBService::GetSession(int thread_id) {
  return txn_manager_->GetSession(thread_id);
}

const NeugDBSession& NeugDBService::GetSession(int thread_id) const {
  return txn_manager_->GetSession(thread_id);
}

bool NeugDBService::IsInitialized() const {
  return initialized_.load(std::memory_order_relaxed);
}

bool NeugDBService::IsRunning() const {
  return running_.load(std::memory_order_relaxed);
}

gs::result<std::string> NeugDBService::service_status() {
  if (!IsInitialized()) {
    return gs::result<std::string>("NeugDB service has not been inited!");
  }
  if (!IsRunning()) {
    return gs::result<std::string>("NeugDB service has not been started!");
  }
  return gs::result<std::string>("NeugDB service is running ...");
}

void NeugDBService::run_and_wait_for_exit() {
  if (!IsInitialized()) {
    THROW_RUNTIME_ERROR("NeugDB service has not been inited!");
  }
  if (IsRunning()) {
    THROW_RUNTIME_ERROR("NeugDB service has already been started!");
  }
  if (hdl_mgr_) {
    hdl_mgr_->RunAndWaitForExit();
  } else {
    THROW_RUNTIME_ERROR("Query handler has not been inited!");
  }
  return;
}

void NeugDBService::Stop() {
  if (!IsInitialized()) {
    std::cerr << "NeugDB service has not been inited!" << std::endl;
    return;
  }
  if (!IsRunning()) {
    std::cerr << "NeugDB service has not been started!" << std::endl;
    return;
  }
  std::unique_lock<std::mutex> lock(mtx_);
  if (hdl_mgr_) {
    hdl_mgr_->Stop();
    return;
  } else {
    THROW_RUNTIME_ERROR("Query handler has not been inited!");
  }
}

std::string NeugDBService::Start() {
  if (!IsInitialized()) {
    THROW_RUNTIME_ERROR("NeugDB service has not been inited!");
  }
  if (IsRunning()) {
    THROW_RUNTIME_ERROR("NeugDB service has already been started!");
  }
  std::unique_lock<std::mutex> lock(mtx_);
  if (hdl_mgr_) {
    return hdl_mgr_->Start();
  } else {
    THROW_RUNTIME_ERROR("Query handler has not been inited!");
  }
}

size_t NeugDBService::getExecutedQueryNum() const {
  return txn_manager_->getExecutedQueryNum();
}

void NeugDBService::startCompactThreadIfNeeded() {
  if (db_config_.enable_auto_compaction) {
    if (compact_thread_running_) {
      compact_thread_running_ = false;
      compact_thread_.join();
    }
    compact_thread_running_ = true;
    compact_thread_ = std::thread([&]() {
      size_t last_compaction_at = 0;
      while (compact_thread_running_) {
        size_t query_num_before = getExecutedQueryNum();
        sleep(30);
        if (!compact_thread_running_) {
          break;
        }
        size_t query_num_after = getExecutedQueryNum();
        if (query_num_before == query_num_after &&
            (query_num_after > (last_compaction_at + 100000))) {
          VLOG(10) << "Trigger auto compaction";
          last_compaction_at = query_num_after;
          auto txn = txn_manager_->GetCompactTransaction(0);
          txn.Commit();
          VLOG(10) << "Finish compaction";
        }
      }
    });
  }
}

}  // namespace server
