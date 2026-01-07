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
#pragma once

#include <brpc/server.h>
#include <json2pb/pb_to_json.h>
#include <rapidjson/document.h>
#include <memory>
#include <string>
#include <string_view>

#include "neug/compiler/planner/graph_planner.h"
#include "neug/generated/proto/http_service/http_svc.pb.h"
#include "neug/generated/proto/plan/results.pb.h"
#include "neug/main/neug_db.h"
#include "neug/server/neug_db_service.h"
#include "neug/server/neug_db_session.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/access_mode.h"
#include "neug/utils/app_utils.h"
#include "neug/utils/http_handler_manager.h"
#include "neug/utils/likely.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/result.h"
#include "neug/utils/yaml_utils.h"

namespace server {

void cleanup(void* ptr);

int32_t status_code_to_http_code(gs::StatusCode code);

class HttpServiceImpl : public HttpService {
 public:
  explicit HttpServiceImpl(gs::NeugDB& graph_db,
                           gs::TransactionManager& txn_mgr)
      : graph_db_(graph_db), txn_mgr_(txn_mgr) {
    planner_ = graph_db_.GetPlanner();
    pthread_key_create(&thread_id_key, cleanup);
  }
  virtual ~HttpServiceImpl() {}
  void CypherQuery(google::protobuf::RpcController* cntl_base,
                   const HttpRequest* request, HttpResponse* response,
                   google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

    std::string final_res_str;
    if (!do_cypher_query(cntl, request, final_res_str)) {
      return;
    }
    // TODO(zhanglei): Currently we append the result_schema to the parsed
    // collectiveResults. This introduces additional cost. We need to avoid
    // unnecessary serialization and deserialization
    cntl->response_attachment().append(final_res_str.data(),
                                       final_res_str.size());
    LOG(INFO) << "Eval success: " << final_res_str.size() << " bytes";
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
    if (!yaml) {
      LOG(ERROR) << "Failed to convert schema to YAML: "
                 << yaml.error().error_message();
      cntl->SetFailed(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR, "%s",
                      "Failed to convert schema to YAML");
      return;
    }
    auto schema_str_res = gs::get_json_string_from_yaml(yaml.value());
    if (!schema_str_res) {
      LOG(ERROR) << "Failed to convert schema YAML to JSON: "
                 << schema_str_res.error().error_message();
      cntl->SetFailed(
          status_code_to_http_code(schema_str_res.error().error_code()), "%s",
          "Failed to convert schema YAML to JSON");
      return;
    }
    // Append schema string to response attachment
    cntl->response_attachment().append(schema_str_res.value().data(),
                                       schema_str_res.value().size());
    LOG(INFO) << "Schema request processed successfully";
  }

 private:
  bool do_cypher_query(brpc::Controller* cntl, const HttpRequest* request,
                       std::string& final_res_str) {
    int* id_ptr = reinterpret_cast<int*>(pthread_getspecific(thread_id_key));
    if (NEUG_UNLIKELY(!id_ptr)) {
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
      return false;
    }
    if (!planner_) {
      LOG(ERROR) << "Planner is not set";
      cntl->SetFailed(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR, "%s",
                      "Planner is not set");
      return false;
    }

    auto result = txn_mgr_.GetSession(id).Eval(req);
    if (!result) {
      std::string error_msg = result.error().ToString();
      LOG(ERROR) << "Eval failed: " << error_msg;
      cntl->SetFailed(status_code_to_http_code(result.error().error_code()),
                      "%s", error_msg.c_str());
      cntl->response_attachment().append(error_msg);
      return false;
    }

    const results::CollectiveResults& final_res = result.value();
    final_res_str = final_res.SerializeAsString();
    return true;
  }

  gs::NeugDB& graph_db_;
  gs::TransactionManager& txn_mgr_;
  std::shared_ptr<gs::IGraphPlanner> planner_;
  pthread_key_t thread_id_key;
  std::atomic<int> thread_id_key_count{0};
};

class BrpcHttpHandlerManager : public IHttpHandlerManager {
 public:
  explicit BrpcHttpHandlerManager(gs::NeugDB& graph_db,
                                  gs::TransactionManager& txn_mgr);

  ~BrpcHttpHandlerManager();
  void Init(const ServiceConfig& config) override;
  std::string Start() override;
  void Stop() override;
  void RunAndWaitForExit() override;
  bool IsRunning() const override { return brpc_server_->IsRunning(); }

 private:
  brpc::ServerOptions get_server_options() const;

  ServiceConfig service_config_;
  HttpServiceImpl svc_;
  std::unique_ptr<brpc::Server> brpc_server_;
};

}  // namespace server
