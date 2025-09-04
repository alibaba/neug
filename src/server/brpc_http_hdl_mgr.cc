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
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/error.pb.h"
#else
#include "neug/utils/proto/plan/error.pb.h"
#endif

namespace server {

void cleanup(void* ptr) { delete (int*) ptr; }

bool append_plugin_id(const physical::PhysicalPlan& physical_plan,
                      std::string& plan_proto_str, bool& update_schema,
                      bool& update_statistics) {
  // There are two possible cases:
  //  1. Query plan contains operator that mutate the graph(schema or data)
  //  2. Query plan that only execute read-only operations
  // We need to determine which plugin to use based on the plan type.
  // ALSO, if the query mutate the graph, we should also update the schema
  // and statistics for the compiler.
  if (physical_plan.has_query_plan()) {
    if (physical_plan.query_plan().mode() ==
        physical::QueryPlan::Mode::QueryPlan_Mode_READ_ONLY) {
      plan_proto_str.append(1, *gs::Schema::ADHOC_READ_PLUGIN_ID_STR);
      plan_proto_str.append(
          1, static_cast<char>(gs::NeugDBSession::InputFormat::kCypherString));
    } else {
      plan_proto_str.append(1, *gs::Schema::ADHOC_UPDATE_PLUGIN_ID_STR);
      plan_proto_str.append(
          1, static_cast<char>(gs::NeugDBSession::InputFormat::kCypherString));
      update_statistics = true;
    }
    return true;
  } else if (physical_plan.has_ddl_plan()) {
    plan_proto_str.append(1, *gs::Schema::ADHOC_UPDATE_PLUGIN_ID_STR);
    plan_proto_str.append(
        1, static_cast<char>(gs::NeugDBSession::InputFormat::kCypherString));
    update_schema = true;
    update_statistics = true;
    return true;
  } else {
    return false;
  }
}

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

BrpcHttpHandlerManager::BrpcHttpHandlerManager(
    gs::NeugDB& graph_db, std::shared_ptr<gs::IGraphPlanner> planner)
    : svc_(graph_db, planner) {
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
    THROW_RUNTIME_ERROR("Failed to parse endpoint: " + ip_port);
  }
  brpc::ServerOptions options = get_server_options();
  if (brpc_server_->Start(endpoint, &options) != 0) {
    THROW_RUNTIME_ERROR("Failed to start brpc server on " + ip_port);
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
  options.num_threads = service_config_.shard_num;
  return options;
}
}  // namespace server
