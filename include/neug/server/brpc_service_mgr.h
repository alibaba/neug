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
#include <mutex>
#include <string>
#include <string_view>

#include "neug/compiler/planner/graph_planner.h"
#include "neug/generated/proto/http_service/http_svc.pb.h"
#include "neug/main/neug_db.h"
#include "neug/server/neug_db_service.h"
#include "neug/server/neug_db_session.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/encoder.h"
#include "neug/utils/likely.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/result.h"
#include "neug/utils/service_manager.h"
#include "neug/utils/yaml_utils.h"

#include "bthread/bthread.h"

namespace neug {

int32_t status_code_to_http_code(neug::StatusCode code);

/**
 * @brief The protocol entry for BRPC service manager. Holding function pointers
 * for parsing query requests and sending query responses.
 */
struct BrpcServiceProtocol {
  BrpcServiceProtocol()
      : parse_query_request(nullptr),
        send_query_response(nullptr),
        name("unknown") {}

  BrpcServiceProtocol(const BrpcServiceProtocol& other)
      : parse_query_request(other.parse_query_request),
        send_query_response(other.send_query_response),
        name(other.name) {}

  typedef bool (*ParseQueryRequestFunc)(brpc::Controller* cntl, void* request,
                                        std::string& query_request);
  ParseQueryRequestFunc parse_query_request;

  typedef void (*SendQueryResponseFunc)(brpc::Controller* cntl,
                                        neug::result<std::string>& response);
  SendQueryResponseFunc send_query_response;

  typedef void (*SendSchemaResponseFunc)(brpc::Controller* cntl,
                                         neug::result<std::string>& response);
  SendSchemaResponseFunc send_schema_response;

  typedef void (*SendServiceStatusResponseFunc)(
      brpc::Controller* cntl, neug::result<std::string>& response);
  SendServiceStatusResponseFunc send_service_status_response;

  const char* name;
};

struct BrpcServiceProtocolEntry {
  BrpcServiceProtocol protocol;
  bool valid;
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

/**
 * @brief The unified service implementation for BRPC server.
 */
class UnifiedServiceImpl {
 public:
  explicit UnifiedServiceImpl(neug::NeugDB& neug_db, SessionPool& session_pool)
      : neug_db_(neug_db),
        session_pool_(session_pool),
        planner_(neug_db_.GetPlanner()) {}

  virtual ~UnifiedServiceImpl() {}

  neug::result<std::string> GetSchemaImpl(brpc::Controller* cntl_base);

  neug::result<std::string> GetServiceStatusImpl(brpc::Controller* cntl_base);

 protected:
  neug::NeugDB& neug_db_;
  SessionPool& session_pool_;
  std::shared_ptr<neug::IGraphPlanner> planner_;
};

class HttpServiceImpl : public UnifiedServiceImpl, public neug::HttpService {
 public:
  explicit HttpServiceImpl(neug::NeugDB& neug_db, SessionPool& session_pool)
      : UnifiedServiceImpl(neug_db, session_pool),
        protocol_(GetServiceProtocol(brpc::PROTOCOL_HTTP)) {}
  virtual ~HttpServiceImpl() {}

  void PostCypherQuery(google::protobuf::RpcController* cntl_base,
                       const HttpRequest* request, HttpResponse* response,
                       google::protobuf::Closure* done);

  void GetSchema(google::protobuf::RpcController* cntl_base,
                 const google::protobuf::Empty*, HttpResponse* response,
                 google::protobuf::Closure* done);

  void GetServiceStatus(google::protobuf::RpcController* cntl_base,
                        const google::protobuf::Empty*, HttpResponse* response,
                        google::protobuf::Closure* done);

 private:
  const BrpcServiceProtocol& protocol_;
};

class BrpcServiceManager : public IServiceManager {
 public:
  explicit BrpcServiceManager(neug::NeugDB& neug_db, SessionPool& session_pool);

  ~BrpcServiceManager();
  void Init(const ServiceConfig& config) override;
  std::string Start() override;
  void Stop() override;
  void RunAndWaitForExit() override;
  bool IsRunning() const override { return brpc_server_->IsRunning(); }

 private:
  neug::NeugDB& neug_db_;
  SessionPool& session_pool_;
  brpc::ServerOptions get_server_options() const;

  ServiceConfig service_config_;
  std::vector<std::unique_ptr<UnifiedServiceImpl>> services_;
  std::unique_ptr<brpc::Server> brpc_server_;
};

}  // namespace neug
