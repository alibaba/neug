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

#ifndef ENGINES_BRPC_SERVER_HIACTOR_HTTP_HDL_MGR_H_
#define ENGINES_BRPC_SERVER_HIACTOR_HTTP_HDL_MGR_H_

#include <brpc/server.h>
#include <json2pb/pb_to_json.h>
#include <memory>
#include <string_view>
#include "src/engines/graph_db/database/graph_db.h"
#include "src/engines/graph_db/database/graph_db_session.h"
#include "src/engines/graph_db_service.h"
#include "src/http_service/http_svc.pb.h"
#include "src/planner/graph_planner.h"
#include "src/proto_generated_gie/results.pb.h"
#include "src/storages/metadata/graph_meta_store.h"
#include "src/storages/rt_mutable_graph/schema.h"
#include "src/utils/http_handler_manager.h"
#include "src/utils/yaml_utils.h"

namespace server {

void cleanup(void* ptr);

bool has_update_opr_in_plan(const physical::PhysicalPlan& plan);

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
      cntl->SetFailed(brpc::HTTP_STATUS_BAD_REQUEST, "Eval request is empty");
      return;
    }
    // TODO(zhanglei): we could tell whether to call read_app or write_app from
    // planner.
    if (!planner_) {
      LOG(ERROR) << "Planner is not set";
      cntl->SetFailed(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR,
                      "Planner is not set");
      return;
    }
    auto plan = planner_->compilePlan(
        req, gs::read_yaml_file_to_string(graph_db_.get_schema_yaml_path()),
        graph_db_.get_statistics_json());
    if (plan.error_code != gs::StatusCode::OK) {
      LOG(ERROR) << "Plan compilation failed: " << plan.full_message;
      cntl->SetFailed(plan.full_message);
      cntl->http_response().set_status_code(
          brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
      return;
    }
    VLOG(10) << "got plan: " << plan.physical_plan.DebugString();
    std::string plan_proto_str;
    plan.physical_plan.SerializeToString(&plan_proto_str);
    if (plan.physical_plan.has_query_plan()) {
      if (has_update_opr_in_plan(plan.physical_plan)) {
        VLOG(10) << "Update operation detected, using update plugin";
        plan_proto_str.append(1, *gs::Schema::ADHOC_UPDATE_PLUGIN_ID_STR);
        plan_proto_str.append(
            1,
            static_cast<char>(gs::GraphDBSession::InputFormat::kCypherString));
      } else {
        VLOG(10) << "Read operation detected, using read plugin";
        plan_proto_str.append(1, *gs::Schema::ADHOC_READ_PLUGIN_ID_STR);
        plan_proto_str.append(
            1,
            static_cast<char>(gs::GraphDBSession::InputFormat::kCypherString));
      }
    } else if (plan.physical_plan.has_ddl_plan()) {
      VLOG(10) << "DDL plan detected, using DDL plugin";
      plan_proto_str.append(1, *gs::Schema::ADHOC_UPDATE_PLUGIN_ID_STR);
      plan_proto_str.append(
          1, static_cast<char>(gs::GraphDBSession::InputFormat::kCypherString));
    } else {
      cntl->SetFailed(
          "Unsupported plan type, only query and DDL plans are supported");
      cntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
      return;
    }

    auto result = graph_db_.GetSession(id).Eval(plan_proto_str);
    const auto& result_buffer = result.value();
    // TODO(zhanglei): Remove encoder/decoder in de/serialization.
    gs::Decoder decoder(result_buffer.data(), result_buffer.size());
    if (!result.ok()) {
      LOG(ERROR) << "Eval failed: " << result.status().error_message();
      cntl->SetFailed(result.status().error_message() + ", " +
                      std::string(decoder.get_string()));
      // When query fails, the error message is put in string format
      cntl->http_response().set_status_code(
          brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
      return;
    }

    std::string_view actual_res = decoder.get_bytes();
    LOG(INFO) << "Actual response size: " << actual_res.size();
    {
      // TODO(zhanglei): Currently we append the result_schema to the parsed
      // collectiveResults. This introduces additional cost. We need to avoid
      // unnecessary serialization and deserialization
      results::CollectiveResults final_res;
      final_res.ParseFromArray(actual_res.data(), actual_res.size());
      final_res.set_result_schema(plan.result_schema);
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
      cntl->SetFailed(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR,
                      "Failed to convert schema to YAML");
      return;
    }
    auto schema_str_res = gs::get_json_string_from_yaml(yaml.value());
    if (!schema_str_res.ok()) {
      LOG(ERROR) << "Failed to convert schema YAML to JSON: "
                 << schema_str_res.status().error_message();
      cntl->SetFailed(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR,
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

#endif  // ENGINES_BRPC_SERVER_HIACTOR_HTTP_HDL_MGR_H_