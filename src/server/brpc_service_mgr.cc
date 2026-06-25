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

#include "neug/server/brpc_service_mgr.h"

#include "neug/compiler/planner/graph_planner.h"
#include "neug/generated/proto/plan/error.pb.h"

namespace neug {

static pthread_once_t brpc_service_protocol_init_once = PTHREAD_ONCE_INIT;

int32_t status_code_to_http_code(neug::StatusCode code) {
  switch (code) {
  case neug::StatusCode::OK:
    return brpc::HTTP_STATUS_OK;
  case neug::StatusCode::ERR_PERMISSION:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  case neug::StatusCode::ERR_DATABASE_LOCKED:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  case neug::StatusCode::ERR_NOT_SUPPORTED:
    return brpc::HTTP_STATUS_NOT_IMPLEMENTED;
  case neug::StatusCode::ERR_NOT_IMPLEMENTED:
    return brpc::HTTP_STATUS_NOT_IMPLEMENTED;
  case neug::StatusCode::ERR_QUERY_SYNTAX:
    return brpc::HTTP_STATUS_BAD_REQUEST;
  case neug::StatusCode::ERR_NOT_INITIALIZED:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  case neug::StatusCode::ERR_QUERY_EXECUTION:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  case neug::StatusCode::ERR_INTERNAL_ERROR:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  case neug::StatusCode::ERR_NOT_FOUND:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  case neug::StatusCode::ERR_INVALID_ARGUMENT:
    return brpc::HTTP_STATUS_BAD_REQUEST;
  case neug::StatusCode::ERR_COMPILATION:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  default:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  }
}

BrpcServiceProtocolManager& BrpcServiceProtocolManager::Get() {
  static BrpcServiceProtocolManager instance;
  return instance;
}
//////////////////Http Protocol Implementation///////////////////////////
bool ParseHttpQueryRequest(brpc::Controller* cntl, void* request,
                           std::string& query_request) {
  auto req = cntl->request_attachment().to_string();
  if (req.empty()) {
    LOG(ERROR) << "Eval request is empty";
    cntl->SetFailed(brpc::HTTP_STATUS_BAD_REQUEST, "%s",
                    "Eval request is empty");
    return false;
  }
  query_request = req;
  return true;
}

void SendHttpQueryResponse(brpc::Controller* cntl,
                           neug::result<std::string>& response) {
  if (response) {
    cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    const auto& results = response.value();
    cntl->response_attachment().append(results.data(), results.size());
  } else {
    const auto& status = response.error();
    LOG(ERROR) << "Query failed: " << status.ToString();
    auto http_code = status_code_to_http_code(status.error_code());
    cntl->SetFailed(http_code, "%s", status.ToString().c_str());
  }
  return;
}

void SendHttpStringResponse(brpc::Controller* cntl,
                            neug::result<std::string>& schema) {
  if (schema) {
    cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    cntl->response_attachment().append(schema.value().data(),
                                       schema.value().size());
  } else {
    const auto& error = schema.error();
    LOG(ERROR) << "Error " << error.ToString();
    cntl->SetFailed(status_code_to_http_code(error.error_code()), "%s",
                    error.ToString().c_str());
  }
}

bool BrpcServiceProtocolManager::RegisterProtocol(
    brpc::ProtocolType type, const BrpcServiceProtocol& protocol) {
  // Check if sealed first (lock-free fast path)
  if (NEUG_UNLIKELY(sealed_.load(std::memory_order_acquire))) {
    LOG(ERROR) << "Cannot register protocol after sealing. Protocol "
               << static_cast<int>(type) << " registration rejected.";
    return false;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  // Double-check after acquiring lock
  if (sealed_.load(std::memory_order_relaxed)) {
    LOG(ERROR) << "Cannot register protocol after sealing. Protocol "
               << static_cast<int>(type) << " registration rejected.";
    return false;
  }

  if (static_cast<size_t>(type) >= protocols_.size()) {
    protocols_.resize(static_cast<size_t>(type) + 1);
  }
  if (protocols_[static_cast<size_t>(type)].valid) {
    LOG(WARNING) << "Brpc service protocol " << static_cast<int>(type)
                 << " already registered";
    return false;
  }
  protocols_[static_cast<size_t>(type)].protocol = protocol;
  protocols_[static_cast<size_t>(type)].valid = true;
  return true;
}

const BrpcServiceProtocol& BrpcServiceProtocolManager::GetProtocol(
    brpc::ProtocolType type) {
  // Fast path: after sealing, no locking needed (lock-free read)
  // The acquire memory order ensures we see all writes from RegisterProtocol
  if (NEUG_LIKELY(sealed_.load(std::memory_order_acquire))) {
    if (static_cast<size_t>(type) >= protocols_.size() ||
        !protocols_[static_cast<size_t>(type)].valid) {
      THROW_NOT_FOUND_EXCEPTION("Brpc service protocol " +
                                std::to_string(static_cast<int>(type)) +
                                " not found");
    }
    assert(protocols_[static_cast<size_t>(type)].valid);
    return protocols_[static_cast<size_t>(type)].protocol;
  }

  // Slow path: before sealing, need to acquire lock to prevent races with
  // resize
  std::unique_lock<std::mutex> lock(mutex_);
  if (static_cast<size_t>(type) >= protocols_.size() ||
      !protocols_[static_cast<size_t>(type)].valid) {
    THROW_NOT_FOUND_EXCEPTION("Brpc service protocol " +
                              std::to_string(static_cast<int>(type)) +
                              " not found");
  }
  return protocols_[static_cast<size_t>(type)].protocol;
}

void SealProtocolRegistration() {
  auto& mgr = BrpcServiceProtocolManager::Get();
  std::lock_guard<std::mutex> lock(mgr.mutex_);

  if (mgr.sealed_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "Protocol registration already sealed";
    return;
  }

  // Use release memory order to ensure all protocol registrations
  // are visible to other threads when they see sealed_ == true
  mgr.sealed_.store(true, std::memory_order_release);
  LOG(INFO) << "Protocol registration sealed.";
}

// Should only be called once
void InitializeBrpcServiceProtocols() {
  // Register HTTP protocol
#ifdef ENABLE_HTTP_PROTOCOL
  BrpcServiceProtocol http_protocol;
  http_protocol.name = "http";
  http_protocol.parse_query_request = ParseHttpQueryRequest;
  http_protocol.send_query_response = SendHttpQueryResponse;
  http_protocol.send_schema_response = SendHttpStringResponse;
  http_protocol.send_service_status_response = SendHttpStringResponse;
  RegisterServiceProtocol(brpc::PROTOCOL_HTTP, http_protocol);
#endif

  // Seal the registration to prevent further modifications
  SealProtocolRegistration();
}

neug::result<std::string> UnifiedServiceImpl::GetSchemaImpl(
    brpc::Controller* cntl) {
  const auto& schema = neug_db_.schema();
  auto yaml = schema.to_yaml();
  if (!yaml) {
    RETURN_ERROR(yaml.error());
  }
  return neug::get_json_string_from_yaml(yaml.value());
}

neug::result<std::string> UnifiedServiceImpl::GetServiceStatusImpl(
    brpc::Controller* cntl) {
  // Implement the logic to get service status here
  // For now, return a placeholder string
  return std::string("{\"status\": \"OK\", \"version\": \"" NEUG_VERSION "\"}");
}

void HttpServiceImpl::PostCypherQuery(
    google::protobuf::RpcController* cntl_base, const HttpRequest* request,
    HttpResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
  std::string query_request;
  // 1. Parse query request
  if (!protocol_.parse_query_request(cntl, (void*) request, query_request)) {
    cntl->SetFailed(brpc::HTTP_STATUS_BAD_REQUEST, "%s",
                    "Failed to parse query request");
    return;
  }
  if (query_request.empty()) {
    LOG(ERROR) << "Cypher query is empty";
    cntl->SetFailed(brpc::HTTP_STATUS_BAD_REQUEST, "%s",
                    "Cypher query is empty");
    return;
  }

  // 2. Execute query
  auto session_guard = session_pool_.AcquireSession();
  auto result = session_guard->Eval(query_request);

  // 3. Send Query Response
  protocol_.send_query_response(cntl, result);
  VLOG(10) << "Query executed successfully, updating planner's schema and "
              "statistics";
  return;
}

void HttpServiceImpl::GetSchema(google::protobuf::RpcController* cntl_base,
                                const google::protobuf::Empty*,
                                HttpResponse* response,
                                google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
  // No need to parse request for Schema
  auto ret = GetSchemaImpl(cntl);

  protocol_.send_schema_response(cntl, ret);
  return;
}

void HttpServiceImpl::GetServiceStatus(
    google::protobuf::RpcController* cntl_base, const google::protobuf::Empty*,
    HttpResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
  // No need to parse request for ServiceStatus
  auto ret = GetServiceStatusImpl(cntl);

  protocol_.send_service_status_response(cntl, ret);
  return;
}

BrpcServiceManager::BrpcServiceManager(neug::NeugDB& neug_db,
                                       SessionPool& session_pool)
    : neug_db_(neug_db), session_pool_(session_pool) {
  brpc_server_ = std::make_unique<brpc::Server>();
}

BrpcServiceManager::~BrpcServiceManager() {}

void BrpcServiceManager::Init(const ServiceConfig& config) {
  // Initialize Brpc service protocols
  if (pthread_once(&brpc_service_protocol_init_once,
                   InitializeBrpcServiceProtocols) != 0) {
    THROW_RUNTIME_ERROR("Failed to initialize BRPC service protocols");
  }
  service_config_ = config;

  // Enable progressive read to avoid blocking IO bthreads
  brpc::ServiceOptions svc_options;
  svc_options.ownership = brpc::SERVER_DOESNT_OWN_SERVICE;
  svc_options.restful_mappings =
      "/cypher => PostCypherQuery,"
      "/service_status => GetServiceStatus,"
      "/schema => GetSchema";

#ifdef ENABLE_HTTP_PROTOCOL
  auto http_svc = std::make_unique<HttpServiceImpl>(neug_db_, session_pool_);
  if (brpc_server_->AddService(http_svc.get(), svc_options) == -1) {
    LOG(ERROR) << "Failed to add http service to brpc server";
  }
  services_.emplace_back(std::move(http_svc));
#endif
  if (services_.empty()) {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "No brpc protocols are enabled. Please enable at least one protocol.");
  }
}

std::string BrpcServiceManager::Start() {
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

void BrpcServiceManager::RunAndWaitForExit() {
  Start();
  LOG(INFO) << "Brpc server is running, waiting for exit...";
  brpc_server_->RunUntilAskedToQuit();
}

void BrpcServiceManager::Stop() {
  LOG(INFO) << "Stopping brpc server";
  brpc_server_->Stop(0);
  LOG(INFO) << "Brpc server stopped";
}

brpc::ServerOptions BrpcServiceManager::get_server_options() const {
  brpc::ServerOptions options;
  options.idle_timeout_sec = 60;  // 1 minute
  options.num_threads = service_config_.shard_num;

  return options;
}
}  // namespace neug
