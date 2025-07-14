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
#include "src/engines/graph_db_service.h"
#include "src/engines/http_server/options.h"

#ifdef HTTP_SERVER_TYPE_HIACTOR
#include "src/engines/http_server/hiactor_http_hdl_mgr.h"
#elif defined(HTTP_SERVER_TYPE_BRPC)
#include "src/engines/brpc_server/brpc_http_hdl_mgr.h"
#else
#error "HTTP_SERVER_TYPE must be defined as either HIACTOR or BRPC"
#endif

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#define HTTP_SERVER_TYPE_VAL HTTP_SERVER_TYPE

namespace server {

const std::string GraphDBService::DEFAULT_GRAPH_NAME = "modern_graph";
const std::string GraphDBService::DEFAULT_INTERACTIVE_HOME = "/opt/src/";
const std::string GraphDBService::COMPILER_SERVER_CLASS_NAME =
    "com.alibaba.graphscope.GraphServer";

GraphDBService& GraphDBService::get() {
  static GraphDBService instance;
  return instance;
}

std::shared_ptr<gs::IGraphMetaStore> GraphDBService::get_metadata_store()
    const {
  return metadata_store_;
}

void GraphDBService::init(const ServiceConfig& config) {
  if (initialized_.load(std::memory_order_relaxed)) {
    std::cerr << "High QPS service has been already initialized!" << std::endl;
    return;
  }

  if (hdl_mgr_) {
    LOG(ERROR) << "High QPS service has already been initialized!";
    return;
  }
  planner_ = std::make_shared<gs::GOptPlanner>(config.engine_config_path);

  if (strcmp(HTTP_SERVER_TYPE_VAL, "hiactor") == 0) {
#ifdef HTTP_SERVER_TYPE_HIACTOR
    hdl_mgr_ = std::make_unique<HiactorHttpHandlerManager>();
#else
    LOG(FATAL) << "HTTP server type is not set to HIACTOR, but the code is "
                  "compiled with HIACTOR support.";
#endif
  } else if (strcmp(HTTP_SERVER_TYPE_VAL, "brpc") == 0) {
#ifdef HTTP_SERVER_TYPE_BRPC
    hdl_mgr_ = std::make_unique<BrpcHttpHandlerManager>(db_, planner_);
#else
    LOG(FATAL) << "HTTP server type is not set to BRPC, but the code is "
                  "compiled with BRPC support.";
#endif
  } else {
    LOG(FATAL) << "Unsupported HTTP server type: " << HTTP_SERVER_TYPE_VAL;
  }
  hdl_mgr_->Init(config);

  initialized_.store(true);
  service_config_ = config;
}

GraphDBService::~GraphDBService() {
  if (hdl_mgr_) {
    hdl_mgr_->Stop();
    hdl_mgr_.reset();
  }
}

const ServiceConfig& GraphDBService::get_service_config() const {
  return service_config_;
}

bool GraphDBService::is_initialized() const {
  return initialized_.load(std::memory_order_relaxed);
}

bool GraphDBService::is_running() const {
  return running_.load(std::memory_order_relaxed);
}

uint16_t GraphDBService::get_query_port() const {
  return service_config_.query_port;
}

uint64_t GraphDBService::get_start_time() const {
  return start_time_.load(std::memory_order_relaxed);
}

void GraphDBService::reset_start_time() {
  start_time_.store(gs::GetCurrentTimeStamp());
}

gs::Result<std::string> GraphDBService::service_status() {
  if (!is_initialized()) {
    return gs::Result<std::string>(gs::StatusCode::OK,
                                   "High QPS service has not been inited!", "");
  }
  if (!is_running()) {
    return gs::Result<std::string>(
        gs::StatusCode::OK, "High QPS service has not been started!", "");
  }
  return gs::Result<std::string>(
      std::string("High QPS service is running ..."));
}

void GraphDBService::run_and_wait_for_exit() {
  if (!is_initialized()) {
    std::cerr << "High QPS service has not been inited!" << std::endl;
    return;
  }
  if (is_running()) {
    std::cerr << "High QPS service has already been started!" << std::endl;
    return;
  }
  if (hdl_mgr_) {
    hdl_mgr_->RunAndWaitForExit();
  } else {
    LOG(ERROR) << "High QPS service handler manager is not initialized!";
  }
  return;
}

bool GraphDBService::is_actors_running() const {
  return hdl_mgr_ && hdl_mgr_->IsRunning();
}

void GraphDBService::stop_query_actors() {
  std::unique_lock<std::mutex> lock(mtx_);
  if (hdl_mgr_) {
    hdl_mgr_->Stop();
    return;
  } else {
    std::cerr << "Query handler has not been inited!" << std::endl;
    return;
  }
}

void GraphDBService::start_query_actors() {
  std::unique_lock<std::mutex> lock(mtx_);
  if (hdl_mgr_) {
    hdl_mgr_->Start();
  } else {
    std::cerr << "Query handler has not been inited!" << std::endl;
    return;
  }
}

}  // namespace server
