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

#include "neug/server/brpc_http_hdl_mgr.h"

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

namespace server {

void NeugDBService::init(const ServiceConfig& config) {
  if (initialized_.load(std::memory_order_relaxed)) {
    std::cerr << "NeugDB service has been already initialized!" << std::endl;
    return;
  }

  if (hdl_mgr_) {
    LOG(ERROR) << "NeugDB service has already been initialized!";
    return;
  }

  hdl_mgr_ = std::make_unique<BrpcHttpHandlerManager>(db_);
  hdl_mgr_->Init(config);

  initialized_.store(true);
  service_config_ = config;
}

NeugDBService::~NeugDBService() {
  if (hdl_mgr_) {
    hdl_mgr_->Stop();
    hdl_mgr_.reset();
  }
}

const ServiceConfig& NeugDBService::GetServiceConfig() const {
  return service_config_;
}

bool NeugDBService::IsInitialized() const {
  return initialized_.load(std::memory_order_relaxed);
}

bool NeugDBService::IsRunning() const {
  return running_.load(std::memory_order_relaxed);
}

gs::Result<std::string> NeugDBService::service_status() {
  if (!IsInitialized()) {
    return gs::Result<std::string>(gs::StatusCode::OK,
                                   "NeugDB service has not been inited!", "");
  }
  if (!IsRunning()) {
    return gs::Result<std::string>(gs::StatusCode::OK,
                                   "NeugDB service has not been started!", "");
  }
  return gs::Result<std::string>(std::string("NeugDB service is running ..."));
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

}  // namespace server
