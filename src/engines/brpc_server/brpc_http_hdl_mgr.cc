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

#include "src/engines/brpc_server/brpc_http_hdl_mgr.h"

namespace server {

void cleanup(void* ptr) { delete (int*) ptr; }

BrpcHttpHandlerManager::BrpcHttpHandlerManager() {
  brpc_server_ = std::make_unique<brpc::Server>();
}

BrpcHttpHandlerManager::~BrpcHttpHandlerManager() {}

void BrpcHttpHandlerManager::Init(const ServiceConfig& config) {
  service_config_ = config;

  if (brpc_server_->AddService(&cypher_query_service_,
                               brpc::SERVER_DOESNT_OWN_SERVICE,
                               "/v1/graph/current/query => CypherQuery,"
                               "/v1/graph/*/query => CypherQuery") == -1) {
    LOG(ERROR) << "Failed to add CypherQueryService";
  }
}

void BrpcHttpHandlerManager::Start() {
  LOG(INFO) << "Starting brpc server";
  butil::EndPoint endpoint;
  std::string ip_port =
      std::string("0.0.0.0") + ":" + std::to_string(service_config_.query_port);
  if (butil::str2endpoint(ip_port.c_str(), &endpoint) != 0) {
    LOG(ERROR) << "Failed to parse endpoint from port "
               << service_config_.query_port;
    return;
  }
  brpc::ServerOptions options = get_server_options();
  if (brpc_server_->Start(endpoint, &options) != 0) {
    LOG(ERROR) << "Failed to start brpc server on port "
               << service_config_.query_port;
    return;
  }
  LOG(INFO) << "Brpc server started on port " << service_config_.query_port;
}

void BrpcHttpHandlerManager::RunAndWaitForExit() {
  Start();
  LOG(INFO) << "Brpc server is running, waiting for exit...";
  brpc_server_->RunUntilAskedToQuit();
}

void BrpcHttpHandlerManager::Stop() {
  LOG(INFO) << "Stopping brpc server";
  brpc_server_->Stop(0);
  LOG(INFO) << "Brpc server stopped";
}

brpc::ServerOptions BrpcHttpHandlerManager::get_server_options() const {
  brpc::ServerOptions options;
  options.idle_timeout_sec = 60;  // 1 minute
  return options;
}
}  // namespace server
