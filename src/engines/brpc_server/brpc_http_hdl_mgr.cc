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

#include "src/planner/graph_planner.h"

namespace server {

void cleanup(void* ptr) { delete (int*) ptr; }

bool has_update_opr_in_plan(const physical::PhysicalPlan& plan) {
  for (auto i = 0; i < plan.query_plan().plan_size(); ++i) {
    auto& opr = plan.query_plan().plan(i).opr();
    if (opr.has_source() || opr.has_load_vertex() || opr.has_load_edge() ||
        opr.has_create_vertex() || opr.has_create_edge() ||
        opr.has_set_vertex() || opr.has_set_edge() || opr.has_delete_vertex() ||
        opr.has_delete_edge()) {
      return true;
    }
  }
  return false;
}

BrpcHttpHandlerManager::BrpcHttpHandlerManager(
    gs::GraphDB& graph_db, std::shared_ptr<gs::IGraphPlanner> planner)
    : graph_db_(graph_db), svc_(graph_db, planner) {
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
  std::string ip_port =
      std::string("0.0.0.0") + ":" + std::to_string(service_config_.query_port);
  if (butil::str2endpoint(ip_port.c_str(), &endpoint) != 0) {
    throw std::runtime_error("Failed to parse endpoint: " + ip_port);
  }
  brpc::ServerOptions options = get_server_options();
  if (brpc_server_->Start(endpoint, &options) != 0) {
    throw std::runtime_error("Failed to start brpc server on " + ip_port);
  }
  LOG(INFO) << "Brpc server started on : " << butil::my_hostname() << ":"
            << service_config_.query_port;
  std::stringstream ss;
  ss << "http://" << butil::my_hostname() << ":" << service_config_.query_port;
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
  return options;
}
}  // namespace server
