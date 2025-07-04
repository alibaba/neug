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
#include "src/engines/graph_db/database/graph_db_session.h"
#include "src/engines/graph_db_service.h"
#include "src/http_service/http_svc.pb.h"
#include "src/storages/metadata/graph_meta_store.h"
#include "src/utils/http_handler_manager.h"

namespace server {

void cleanup(void* ptr);

class HttpServiceImpl : public HttpService {
 public:
  HttpServiceImpl() { pthread_key_create(&thread_id_key, cleanup); }
  virtual ~HttpServiceImpl() {}
  void CypherQuery(google::protobuf::RpcController* cntl_base,
                   const HttpRequest* request, HttpResponse* response,
                   google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    // optional: set a callback function which is called after response is sent
    // and before cntl/req/res is destructed.
    cntl->set_after_rpc_resp_fn(
        std::bind(&HttpServiceImpl::CallAfterRpc, std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3));

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
    auto result = GraphDBService::get().graph_db().GetSession(id).Eval(req);
    if (!result.ok()) {
      LOG(ERROR) << "Eval failed: " << result.status().error_message();
      return;
    }
    const auto& result_buffer = result.value();
    cntl->response_attachment().append(result_buffer.data(),
                                       result_buffer.size());
    LOG(INFO) << "Eval success: " << result_buffer.size() << " bytes";
  }

  // optional
  static void CallAfterRpc(brpc::Controller* cntl,
                           const google::protobuf::Message* req,
                           const google::protobuf::Message* res) {
    // at this time res is already sent to client, but cntl/req/res is not
    // destructed
    std::string req_str;
    std::string res_str;
    json2pb::ProtoMessageToJson(*req, &req_str, NULL);
    json2pb::ProtoMessageToJson(*res, &res_str, NULL);
    LOG(INFO) << "req:" << req_str << " res:" << res_str;
  }

 private:
  pthread_key_t thread_id_key;
  std::atomic<int> thread_id_key_count{0};
};

class BrpcHttpHandlerManager : public IHttpHandlerManager {
 public:
  BrpcHttpHandlerManager();
  ~BrpcHttpHandlerManager();
  void Init(const ServiceConfig& config) override;
  void Start() override;
  void Stop() override;
  void RunAndWaitForExit() override;
  bool IsRunning() const override { return brpc_server_->IsRunning(); }

 private:
  brpc::ServerOptions get_server_options() const;

  ServiceConfig service_config_;
  HttpServiceImpl cypher_query_service_;
  std::unique_ptr<brpc::Server> brpc_server_;
  std::shared_ptr<gs::IGraphMetaStore> metadata_store_;
};
}  // namespace server

#endif  // ENGINES_BRPC_SERVER_HIACTOR_HTTP_HDL_MGR_H_