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

#include "neug/server/brpc_http_hdl_mgr.h"

#include "neug/compiler/planner/graph_planner.h"
#include "neug/generated/proto/plan/error.pb.h"

namespace server {

void cleanup(void* ptr) { delete reinterpret_cast<int*>(ptr); }

int32_t status_code_to_http_code(gs::StatusCode code) {
  switch (code) {
  case gs::StatusCode::OK:
    return brpc::HTTP_STATUS_OK;
  case gs::StatusCode::ERR_PERMISSION:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  case gs::StatusCode::ERR_DATABASE_LOCKED:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  case gs::StatusCode::ERR_NOT_SUPPORTED:
    return brpc::HTTP_STATUS_NOT_IMPLEMENTED;
  case gs::StatusCode::ERR_NOT_IMPLEMENTED:
    return brpc::HTTP_STATUS_NOT_IMPLEMENTED;
  case gs::StatusCode::ERR_QUERY_SYNTAX:
    return brpc::HTTP_STATUS_BAD_REQUEST;
  case gs::StatusCode::ERR_NOT_INITIALIZED:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  case gs::StatusCode::ERR_QUERY_EXECUTION:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  case gs::StatusCode::ERR_INTERNAL_ERROR:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  case gs::StatusCode::ERR_NOT_FOUND:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  case gs::StatusCode::ERR_INVALID_ARGUMENT:
    return brpc::HTTP_STATUS_BAD_REQUEST;
  case gs::StatusCode::ERR_COMPILATION:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  default:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  }
}

BrpcHttpHandlerManager::BrpcHttpHandlerManager(gs::NeugDB& graph_db,
                                               gs::TransactionManager& txn_mgr)
    : svc_(graph_db, txn_mgr) {
  brpc_server_ = std::make_unique<brpc::Server>();
}

BrpcHttpHandlerManager::~BrpcHttpHandlerManager() {}

void BrpcHttpHandlerManager::Init(const ServiceConfig& config) {
  service_config_ = config;

  // The API start with Internval marks the legacy API for submitting queries
  if (brpc_server_->AddService(&svc_, brpc::SERVER_DOESNT_OWN_SERVICE,
                               "/cypher => CypherQuery,"
                               "/service_status => ServiceStatus,"
                               "/schema => Schema") == -1) {
    LOG(ERROR) << "Failed to add CypherQueryService";
  }
}

std::string BrpcHttpHandlerManager::Start() {
  LOG(INFO) << "Starting brpc server";
  butil::EndPoint endpoint;
  std::string ip_port = service_config_.host_str + ":" +
                        std::to_string(service_config_.query_port);
  brpc::ServerOptions options = get_server_options();
  if (brpc_server_->Start(ip_port.c_str(), &options) != 0) {
    THROW_RUNTIME_ERROR("Failed to start brpc server on " + ip_port);
  }
  LOG(INFO) << "Brpc server started on : " << service_config_.host_str << ":"
            << service_config_.query_port;
  std::stringstream ss;
  ss << "http://" << service_config_.host_str << ":"
     << service_config_.query_port;
  return ss.str();
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
  options.num_threads = service_config_.shard_num;
  return options;
}
}  // namespace server
