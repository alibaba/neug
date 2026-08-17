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

#include <algorithm>

#include <rapidjson/document.h>

#include "neug/compiler/planner/graph_planner.h"
#include "neug/generated/proto/plan/error.pb.h"
#include "neug/server/service_transaction_manager.h"

namespace neug {

static pthread_once_t brpc_service_protocol_init_once = PTHREAD_ONCE_INIT;

namespace {

constexpr char kTransactionIdHeader[] = "X-Transaction-Id";
constexpr char kNoStoreHeader[] = "Cache-Control";

void SetHttpError(brpc::Controller* cntl, const Status& status) {
  const auto http_code = status_code_to_http_code(status.error_code());
  cntl->SetFailed(http_code, "%s", status.ToString().c_str());
  cntl->http_response().set_status_code(http_code);
}

void SetTransactionResponseHeaders(brpc::Controller* cntl) {
  cntl->http_response().SetHeader(kNoStoreHeader, "no-store");
}

bool RequirePost(brpc::Controller* cntl) {
  if (cntl->http_request().method() == brpc::HTTP_METHOD_POST) {
    return true;
  }
  cntl->SetFailed(brpc::HTTP_STATUS_METHOD_NOT_ALLOWED, "%s",
                  "This transaction endpoint requires POST");
  cntl->http_response().set_status_code(brpc::HTTP_STATUS_METHOD_NOT_ALLOWED);
  return false;
}

bool RequireGet(brpc::Controller* cntl) {
  if (cntl->http_request().method() == brpc::HTTP_METHOD_GET) {
    return true;
  }
  cntl->SetFailed(brpc::HTTP_STATUS_METHOD_NOT_ALLOWED, "%s",
                  "This transaction endpoint requires GET");
  cntl->http_response().set_status_code(brpc::HTTP_STATUS_METHOD_NOT_ALLOWED);
  return false;
}

bool IsValidTransactionId(const std::string& transaction_id) {
  return transaction_id.size() == 22 &&
         std::all_of(transaction_id.begin(), transaction_id.end(), [](char c) {
           return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
                  (c >= '0' && c <= '9') || c == '-' || c == '_';
         });
}

result<std::string> GetTransactionId(brpc::Controller* cntl) {
  const auto* transaction_id =
      cntl->http_request().GetHeader(kTransactionIdHeader);
  if (transaction_id == nullptr || !IsValidTransactionId(*transaction_id)) {
    RETURN_ERROR(Status(
        StatusCode::ERR_INVALID_ARGUMENT,
        "A valid X-Transaction-Id header is required for this endpoint."));
  }
  return *transaction_id;
}

result<TransactionMode> ParseTransactionMode(brpc::Controller* cntl) {
  const auto body = cntl->request_attachment().to_string();
  rapidjson::Document document;
  if (body.empty() ||
      document.Parse(body.data(), body.size()).HasParseError() ||
      !document.IsObject() || !document.HasMember("mode") ||
      !document["mode"].IsString()) {
    RETURN_ERROR(
        Status(StatusCode::ERR_INVALID_ARGUMENT,
               "Transaction body must be an object with a string mode field."));
  }

  const std::string_view mode(document["mode"].GetString(),
                              document["mode"].GetStringLength());
  if (mode == "read_only") {
    return TransactionMode::kReadOnly;
  }
  if (mode == "read_write") {
    return TransactionMode::kReadWrite;
  }
  RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                      "Transaction mode must be read_only or read_write."));
}

}  // namespace

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
    return brpc::HTTP_STATUS_NOT_FOUND;
  case neug::StatusCode::ERR_NO_CHECKPOINT:
    return brpc::HTTP_STATUS_NOT_FOUND;
  case neug::StatusCode::ERR_INVALID_ARGUMENT:
    return brpc::HTTP_STATUS_BAD_REQUEST;
  case neug::StatusCode::ERR_COMPILATION:
    return brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR;
  case neug::StatusCode::ERR_SERVICE_UNAVAILABLE:
    return brpc::HTTP_STATUS_SERVICE_UNAVAILABLE;
  case neug::StatusCode::ERR_TX_STATE_CONFLICT:
  case neug::StatusCode::ERR_TX_TIMEOUT:
    return brpc::HTTP_STATUS_CONFLICT;
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
    LOG(ERROR) << "Query request is empty";
    cntl->SetFailed(brpc::HTTP_STATUS_BAD_REQUEST, "%s",
                    "Query request is empty");
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
    SetHttpError(cntl, status);
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
    SetHttpError(cntl, error);
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
  (void) cntl;
  auto slot_lease = service_.AcquireExecutionSlot();
  auto read_txn = slot_lease->BeginReadTransaction();
  auto yaml = read_txn.schema().to_yaml();
  if (!yaml) {
    read_txn.Abort();
    RETURN_ERROR(yaml.error());
  }
  auto json = get_json_string_from_yaml(yaml.value());
  if (!json) {
    read_txn.Abort();
    RETURN_ERROR(json.error());
  }
  read_txn.Commit();
  return json;
}

neug::result<std::string> UnifiedServiceImpl::GetServiceStatusImpl(
    brpc::Controller* cntl) {
  (void) cntl;
  // Implement the logic to get service status here
  // For now, return a placeholder string
  return std::string("{\"status\": \"OK\", \"version\": \"" NEUG_VERSION "\"}");
}

void HttpServiceImpl::PostCypherQuery(
    google::protobuf::RpcController* cntl_base, const HttpRequest* request,
    HttpResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
  if (cntl->http_request().GetHeader(kTransactionIdHeader) != nullptr) {
    SetTransactionResponseHeaders(cntl);
    SetHttpError(cntl, Status(StatusCode::ERR_INVALID_ARGUMENT,
                              "X-Transaction-Id is not accepted by /cypher."));
    return;
  }
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
  auto slot_lease = service_.AcquireExecutionSlot();
  auto result = slot_lease->ExecuteTransactionalRequest(query_request);

  // 3. Send Query Response
  protocol_.send_query_response(cntl, result);
  VLOG(10) << "Query executed successfully, updating planner's schema and "
              "statistics";
  return;
}

void HttpServiceImpl::BeginTransaction(
    google::protobuf::RpcController* cntl_base, const HttpRequest*,
    HttpResponse*, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(cntl_base);
  SetTransactionResponseHeaders(cntl);
  if (!RequirePost(cntl)) {
    return;
  }
  if (cntl->http_request().GetHeader(kTransactionIdHeader) != nullptr) {
    SetHttpError(
        cntl,
        Status(
            StatusCode::ERR_TX_STATE_CONFLICT,
            "X-Transaction-Id is not accepted when beginning a transaction."));
    return;
  }

  auto mode = ParseTransactionMode(cntl);
  if (!mode) {
    SetHttpError(cntl, mode.error());
    return;
  }
  auto transaction_id = service_.transactionManager().BeginTransaction(*mode);
  if (!transaction_id) {
    SetHttpError(cntl, transaction_id.error());
    return;
  }
  cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
  cntl->http_response().SetHeader(kTransactionIdHeader, *transaction_id);
}

void HttpServiceImpl::PostTransactionQuery(
    google::protobuf::RpcController* cntl_base, const HttpRequest* request,
    HttpResponse*, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(cntl_base);
  SetTransactionResponseHeaders(cntl);
  if (!RequirePost(cntl)) {
    return;
  }
  auto transaction_id = GetTransactionId(cntl);
  if (!transaction_id) {
    SetHttpError(cntl, transaction_id.error());
    return;
  }

  std::string query_request;
  if (!protocol_.parse_query_request(cntl, (void*) request, query_request)) {
    return;
  }
  auto result = service_.transactionManager().ExecuteRequest(*transaction_id,
                                                             query_request);
  protocol_.send_query_response(cntl, result);
}

void HttpServiceImpl::RollbackTransaction(
    google::protobuf::RpcController* cntl_base, const HttpRequest*,
    HttpResponse*, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(cntl_base);
  SetTransactionResponseHeaders(cntl);
  if (!RequirePost(cntl)) {
    return;
  }
  auto transaction_id = GetTransactionId(cntl);
  if (!transaction_id) {
    SetHttpError(cntl, transaction_id.error());
    return;
  }
  if (cntl->request_attachment().size() != 0) {
    SetHttpError(cntl, Status(StatusCode::ERR_INVALID_ARGUMENT,
                              "Transaction rollback requires an empty body."));
    return;
  }

  auto status = service_.transactionManager().Rollback(*transaction_id);
  if (!status.ok()) {
    SetHttpError(cntl, status);
    return;
  }
  cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
}

void HttpServiceImpl::CommitTransaction(
    google::protobuf::RpcController* cntl_base, const HttpRequest*,
    HttpResponse*, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(cntl_base);
  SetTransactionResponseHeaders(cntl);
  if (!RequirePost(cntl)) {
    return;
  }
  auto transaction_id = GetTransactionId(cntl);
  if (!transaction_id) {
    SetHttpError(cntl, transaction_id.error());
    return;
  }
  if (cntl->request_attachment().size() != 0) {
    SetHttpError(cntl, Status(StatusCode::ERR_INVALID_ARGUMENT,
                              "Transaction commit requires an empty body."));
    return;
  }

  auto status = service_.transactionManager().Commit(*transaction_id);
  if (!status.ok()) {
    SetHttpError(cntl, status);
    return;
  }
  cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
}

void HttpServiceImpl::GetTransactionSchema(
    google::protobuf::RpcController* cntl_base, const google::protobuf::Empty*,
    HttpResponse*, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(cntl_base);
  SetTransactionResponseHeaders(cntl);
  if (!RequireGet(cntl)) {
    return;
  }
  auto transaction_id = GetTransactionId(cntl);
  if (!transaction_id) {
    SetHttpError(cntl, transaction_id.error());
    return;
  }
  auto schema = service_.transactionManager().GetSchema(*transaction_id);
  protocol_.send_schema_response(cntl, schema);
}

void HttpServiceImpl::GetSchema(google::protobuf::RpcController* cntl_base,
                                const google::protobuf::Empty*,
                                HttpResponse* response,
                                google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
  if (cntl->http_request().GetHeader(kTransactionIdHeader) != nullptr) {
    SetTransactionResponseHeaders(cntl);
    SetHttpError(cntl, Status(StatusCode::ERR_INVALID_ARGUMENT,
                              "X-Transaction-Id is not accepted by /schema."));
    return;
  }
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
  if (cntl->http_request().GetHeader(kTransactionIdHeader) != nullptr) {
    SetTransactionResponseHeaders(cntl);
    SetHttpError(
        cntl, Status(StatusCode::ERR_INVALID_ARGUMENT,
                     "X-Transaction-Id is not accepted by /service_status."));
    return;
  }
  // No need to parse request for ServiceStatus
  auto ret = GetServiceStatusImpl(cntl);

  protocol_.send_service_status_response(cntl, ret);
  return;
}

BrpcServiceManager::BrpcServiceManager(NeugDBService& service)
    : service_(service) {
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
      "/transactions => BeginTransaction,"
      "/transactions/query => PostTransactionQuery,"
      "/transactions/commit => CommitTransaction,"
      "/transactions/rollback => RollbackTransaction,"
      "/transactions/schema => GetTransactionSchema,"
      "/service_status => GetServiceStatus,"
      "/schema => GetSchema";

#ifdef ENABLE_HTTP_PROTOCOL
  auto http_svc = std::make_unique<HttpServiceImpl>(service_);
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
  std::string ip_port = service_config_.host_str + ":" +
                        std::to_string(service_config_.query_port);
  brpc::ServerOptions options = get_server_options();
  LOG(INFO) << "Service config: db_max_thread_num="
            << service_.db().config().max_thread_num
            << ", configured_thread_num=" << service_config_.thread_num
            << ", resolved_num_threads=" << options.num_threads;
  if (brpc_server_->Start(ip_port.c_str(), &options) != 0) {
    THROW_RUNTIME_ERROR("Failed to start brpc server on " + ip_port);
  }
  const auto actual_port = brpc_server_->listen_address().port;
  LOG(INFO) << "Brpc server started on : " << service_config_.host_str << ":"
            << actual_port;
  std::stringstream ss;
  ss << "http://" << service_config_.host_str << ":" << actual_port;
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
  brpc_server_->Join();
  LOG(INFO) << "Brpc server stopped";
}

uint32_t BrpcServiceManager::resolve_num_threads() const {
  if (service_config_.thread_num != 0) {
    return service_config_.thread_num;
  }
  const auto max_thread_num = service_.db().config().max_thread_num;
  if (max_thread_num <= 0) {
    return 1;
  }
  return static_cast<uint32_t>(max_thread_num);
}

brpc::ServerOptions BrpcServiceManager::get_server_options() const {
  brpc::ServerOptions options;
  options.idle_timeout_sec = 60;  // 1 minute
  options.num_threads = resolve_num_threads();

  return options;
}
}  // namespace neug
