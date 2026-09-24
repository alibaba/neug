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
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "neug/generated/proto/http_service/http_svc.pb.h"
#include "neug/server/tp_service.h"
#include "neug/utils/result.h"
#include "neug/utils/service_manager.h"

namespace neug {

int32_t status_code_to_http_code(neug::StatusCode code);

/**
 * @brief The protocol entry for BRPC service manager. Holding function pointers
 * for parsing query requests and sending query responses.
 */
struct BrpcServiceProtocol {
  using ParseQueryRequestFunc = bool (*)(brpc::Controller* cntl, void* request,
                                         std::string& query_request);
  using SendQueryResponseFunc = void (*)(brpc::Controller* cntl,
                                         neug::result<std::string>& response);
  using SendSchemaResponseFunc = void (*)(brpc::Controller* cntl,
                                          neug::result<std::string>& response);
  using SendServiceStatusResponseFunc =
      void (*)(brpc::Controller* cntl, neug::result<std::string>& response);

  ParseQueryRequestFunc parse_query_request{nullptr};
  SendQueryResponseFunc send_query_response{nullptr};
  SendSchemaResponseFunc send_schema_response{nullptr};
  SendServiceStatusResponseFunc send_service_status_response{nullptr};
  const char* name{"unknown"};
};

struct BrpcServiceProtocolEntry {
  BrpcServiceProtocol protocol;
  bool valid{false};
};

/**
 * @brief The singleton manager for service protocols.
 */
class BrpcServiceProtocolManager {
 public:
  static BrpcServiceProtocolManager& Get();
  bool RegisterProtocol(brpc::ProtocolType type,
                        const BrpcServiceProtocol& protocol);
  const BrpcServiceProtocol& GetProtocol(brpc::ProtocolType type);

  // Friend function to seal the protocol registration after initialization
  friend void SealProtocolRegistration();

 private:
  BrpcServiceProtocolManager() = default;
  ~BrpcServiceProtocolManager() = default;

  std::mutex mutex_;
  std::vector<BrpcServiceProtocolEntry> protocols_;
  // Sealed flag: once set to true, no more registrations allowed
  // and GetProtocol can skip locking for better performance
  std::atomic<bool> sealed_{false};
};

/**
 * @brief Seal the protocol registration. Should be called only once after
 * all protocols are registered during initialization. After sealing,
 * RegisterProtocol will fail and GetProtocol can skip locking.
 */
void SealProtocolRegistration();

// Helper functions for protocol registration and retrieval
inline bool RegisterServiceProtocol(brpc::ProtocolType type,
                                    const BrpcServiceProtocol& protocol) {
  return BrpcServiceProtocolManager::Get().RegisterProtocol(type, protocol);
}

inline const BrpcServiceProtocol& GetServiceProtocol(brpc::ProtocolType type) {
  return BrpcServiceProtocolManager::Get().GetProtocol(type);
}

void InitializeBrpcServiceProtocols();

class HttpServiceImpl : public neug::HttpService {
 public:
  explicit HttpServiceImpl(ITpService& tp_service)
      : tp_service_(tp_service),
        protocol_(GetServiceProtocol(brpc::PROTOCOL_HTTP)) {}
  ~HttpServiceImpl() override = default;

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
  const BrpcServiceProtocol& protocol_;
};

class BrpcServiceManager : public IServiceManager {
 public:
  explicit BrpcServiceManager(ITpService& tp_service,
                              uint32_t database_max_thread_num);

  ~BrpcServiceManager() override = default;
  void Init(const ServiceConfig& config) override;
  std::string Start() override;
  void Stop() override;
  void WaitForExit();
  void RunAndWaitForExit() override;
  bool IsRunning() const override { return brpc_server_->IsRunning(); }

 private:
  ITpService& tp_service_;
  const uint32_t database_max_thread_num_;
  uint32_t resolve_num_threads() const;
  brpc::ServerOptions get_server_options() const;

  ServiceConfig service_config_;
  std::unique_ptr<HttpServiceImpl> http_service_;
  std::unique_ptr<brpc::Server> brpc_server_;
};

}  // namespace neug
