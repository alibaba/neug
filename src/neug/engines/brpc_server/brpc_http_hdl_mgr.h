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

#ifndef ENGINES_BRPC_SERVER_BRPC_HTTP_HDL_MGR_H_
#define ENGINES_BRPC_SERVER_BRPC_HTTP_HDL_MGR_H_

#include <brpc/server.h>
#include <json2pb/pb_to_json.h>
#include <memory>
#include <string_view>
#include "neug/engines/graph_db/database/graph_db.h"
#include "neug/engines/graph_db/database/graph_db_session.h"
#include "neug/engines/graph_db_service.h"
#include "neug/planner/graph_planner.h"
#include "neug/storages/metadata/graph_meta_store.h"
#include "neug/storages/rt_mutable_graph/schema.h"
#include "neug/utils/http_handler_manager.h"
#include "neug/utils/leaf_utils.h"
#include "neug/utils/pb_utils.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/http_service/http_svc.pb.h"
#include "neug/generated/proto/plan/results.pb.h"
#else
#include "neug/utils/proto/http_service/http_svc.pb.h"
#include "neug/utils/proto/plan/results.pb.h"
#endif

#include "neug/utils/yaml_utils.h"

namespace server {

void cleanup(void* ptr);

bool append_plugin_id(const physical::PhysicalPlan& physical_plan,
                      std::string& plan_proto_str, bool& update_schema,
                      bool& update_statistics);

int32_t status_code_to_http_code(gs::StatusCode code);

class HttpServiceImpl : public HttpService {
 public:
  HttpServiceImpl(gs::GraphDB& graph_db,
                  std::shared_ptr<gs::IGraphPlanner> planner)
      : graph_db_(graph_db), planner_(planner) {
    pthread_key_create(&thread_id_key, cleanup);
  }
  virtual ~HttpServiceImpl() {}
  void CypherQuery(google::protobuf::RpcController* cntl_base,
                   const HttpRequest* request, HttpResponse* response,
                   google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    int* id_ptr = (int*) pthread_getspecific(thread_id_key);
#ifdef __GLIBC__
    if (__glibc_unlikely(!id_ptr)) {
#else
    if (!id_ptr) {
#endif
      id_ptr = new int;
      *id_ptr = thread_id_key_count.fetch_add(1);
      pthread_setspecific(thread_id_key, id_ptr);
    }
    int id = *id_ptr;

    cntl->http_response().set_content_type("text/plain");
    cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    auto req = cntl->request_attachment().to_string();
    if (req.empty()) {
      LOG(ERROR) << "Eval request is empty";
      cntl->SetFailed(brpc::HTTP_STATUS_BAD_REQUEST, "%s",
                      "Eval request is empty");
      return;
    }
    if (!planner_) {
      LOG(ERROR) << "Planner is not set";
      cntl->SetFailed(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR, "%s",
                      "Planner is not set");
      return;
    }
    gs::Status status;
    auto plan_res = planner_->compilePlan(req);

    if (!plan_res.ok()) {
      LOG(ERROR) << "Plan compilation failed: "
                 << plan_res.status().error_message();
      const char* error_msg = plan_res.status().error_message().c_str();
      cntl->SetFailed(status_code_to_http_code(plan_res.status().error_code()),
                      "%s", error_msg);
      cntl->response_attachment().append(plan_res.status().error_message());
      return;
    }
    const auto& physical_plan = plan_res.value().first;

    VLOG(10) << "got plan: " << physical_plan.DebugString();
    std::string plan_proto_str;
    physical_plan.SerializeToString(&plan_proto_str);
    bool update_schema = false, update_statistics = false;
    if (!append_plugin_id(physical_plan, plan_proto_str, update_schema,
                          update_statistics)) {
      cntl->SetFailed(
          status_code_to_http_code(gs::StatusCode::ERR_NOT_SUPPORTED), "%s",
          "Unsupported plan type, only query and DDL plans are supported");
      return;
    }

    auto result = graph_db_.GetSession(id).Eval(plan_proto_str);
    const auto& result_buffer = result.value();
    // TODO(zhanglei): Remove encoder/decoder in de/serialization.
    gs::Decoder decoder(result_buffer.data(), result_buffer.size());
    if (!result.ok()) {
      LOG(ERROR) << "Eval failed: " << result.status().error_message();
      const char* error_msg = result.status().error_message().c_str();
      cntl->SetFailed(status_code_to_http_code(result.status().error_code()),
                      "%s", error_msg);
      cntl->response_attachment().append(result.status().error_message());
      return;
    }

    VLOG(10) << "Query executed successfully, updating planner's schema and "
                "statistics";
    auto yaml_node = graph_db_.schema().to_yaml();
    if (!yaml_node.ok()) {
      LOG(ERROR) << "Failed to convert schema to YAML: "
                 << yaml_node.status().error_message();
      // If schema updating fails, we should not proceed on.
      THROW_RUNTIME_ERROR("Failed to convert schema to YAML: " +
                          yaml_node.status().error_message());
    }
    if (update_schema) {
      planner_->update_meta(yaml_node.value());
    }
    if (update_statistics) {
      planner_->update_statistics(graph_db_.get_statistics_json());
    }

    std::string_view actual_res = decoder.get_bytes();
    LOG(INFO) << "Actual response size: " << actual_res.size();
    {
      // TODO(zhanglei): Currently we append the result_schema to the parsed
      // collectiveResults. This introduces additional cost. We need to avoid
      // unnecessary serialization and deserialization
      results::CollectiveResults final_res;
      final_res.ParseFromArray(actual_res.data(), actual_res.size());
      final_res.set_result_schema(plan_res.value().second);
      auto final_res_str = final_res.SerializeAsString();
      cntl->response_attachment().append(final_res_str.data(),
                                         final_res_str.size());
      LOG(INFO) << "Eval success: " << final_res_str.size() << " bytes";
    }
  }

  void ServiceStatus(google::protobuf::RpcController* cntl_base,
                     const google::protobuf::Empty*, HttpResponse* response,
                     google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    cntl->http_response().set_content_type("text/plain");
    cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    cntl->response_attachment().append(
        "{\"status\": \"OK\", "
        "\"version\": \"" NEUG_VERSION "\"}");
    LOG(INFO) << "Status request processed successfully";
  }

  void Schema(google::protobuf::RpcController* cntl_base,
              const google::protobuf::Empty*, HttpResponse* response,
              google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    cntl->http_response().set_content_type("text/plain");
    cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    const auto& schema = graph_db_.schema();
    auto yaml = schema.to_yaml();
    if (!yaml.ok()) {
      LOG(ERROR) << "Failed to convert schema to YAML: "
                 << yaml.status().error_message();
      cntl->SetFailed(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR, "%s",
                      "Failed to convert schema to YAML");
      return;
    }
    auto schema_str_res = gs::get_json_string_from_yaml(yaml.value());
    if (!schema_str_res.ok()) {
      LOG(ERROR) << "Failed to convert schema YAML to JSON: "
                 << schema_str_res.status().error_message();
      cntl->SetFailed(
          status_code_to_http_code(schema_str_res.status().error_code()), "%s",
          "Failed to convert schema YAML to JSON");
      return;
    }
    // Append schema string to response attachment
    cntl->response_attachment().append(schema_str_res.value().data(),
                                       schema_str_res.value().size());
    LOG(INFO) << "Schema request processed successfully";
  }

 private:
  gs::GraphDB& graph_db_;
  std::shared_ptr<gs::IGraphPlanner> planner_;
  pthread_key_t thread_id_key;
  std::atomic<int> thread_id_key_count{0};
};

class BrpcHttpHandlerManager : public IHttpHandlerManager {
 public:
  BrpcHttpHandlerManager(gs::GraphDB& graph_db,
                         std::shared_ptr<gs::IGraphPlanner> planner);

  ~BrpcHttpHandlerManager();
  void Init(const ServiceConfig& config) override;
  std::string Start() override;
  void Stop() override;
  void RunAndWaitForExit() override;
  bool IsRunning() const override { return brpc_server_->IsRunning(); }

 private:
  brpc::ServerOptions get_server_options() const;

  gs::GraphDB& graph_db_;
  ServiceConfig service_config_;
  HttpServiceImpl svc_;
  std::unique_ptr<brpc::Server> brpc_server_;
  std::shared_ptr<gs::IGraphMetaStore> metadata_store_;
};
}  // namespace server

#endif  // ENGINES_BRPC_SERVER_BRPC_HTTP_HDL_MGR_H_