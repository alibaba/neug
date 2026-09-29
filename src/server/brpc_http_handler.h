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

#include "neug/generated/proto/http_service/http_svc.pb.h"

namespace neug {
class ITpService;

/** Converts HTTP requests and responses around the TP business interface. */
class BrpcHttpHandler : public neug::HttpService {
 public:
  explicit BrpcHttpHandler(ITpService& tp_service) : tp_service_(tp_service) {}
  ~BrpcHttpHandler() override = default;

  static const char* Routes();

  void PostCypherQuery(google::protobuf::RpcController* cntl_base,
                       const HttpRequest* request, HttpResponse* response,
                       google::protobuf::Closure* done) override;

  void GetSchema(google::protobuf::RpcController* cntl_base,
                 const google::protobuf::Empty*, HttpResponse* response,
                 google::protobuf::Closure* done) override;

  void GetServiceStatus(google::protobuf::RpcController* cntl_base,
                        const google::protobuf::Empty*, HttpResponse* response,
                        google::protobuf::Closure* done) override;

  void BeginTransaction(google::protobuf::RpcController* cntl_base,
                        const HttpRequest* request, HttpResponse* response,
                        google::protobuf::Closure* done) override;
  void ExecuteTransactionQuery(google::protobuf::RpcController* cntl_base,
                               const HttpRequest* request,
                               HttpResponse* response,
                               google::protobuf::Closure* done) override;
  void CommitTransaction(google::protobuf::RpcController* cntl_base,
                         const HttpRequest* request, HttpResponse* response,
                         google::protobuf::Closure* done) override;
  void RollbackTransaction(google::protobuf::RpcController* cntl_base,
                           const HttpRequest* request, HttpResponse* response,
                           google::protobuf::Closure* done) override;

 private:
  ITpService& tp_service_;
};

}  // namespace neug
