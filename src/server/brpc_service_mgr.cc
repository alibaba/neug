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

#include <chrono>
#include <cstdio>
#include <ctime>
#include <utility>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "service_transaction_manager.h"

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
    return brpc::HTTP_STATUS_CONFLICT;
  case neug::StatusCode::ERR_TX_TIMEOUT:
  case neug::StatusCode::ERR_TX_NOT_FOUND:
    return brpc::HTTP_STATUS_GONE;
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
    auto http_code = status_code_to_http_code(status.error_code());
    cntl->SetFailed(http_code, "%s", status.ToString().c_str());
    // brpc treats SetFailed's integer as an RPC error code and maps unknown
    // values (including HTTP 501) back to 500. Override the HTTP status after
    // SetFailed, as required by brpc::Controller's contract.
    cntl->http_response().set_status_code(http_code);
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
    auto http_code = status_code_to_http_code(error.error_code());
    cntl->SetFailed(http_code, "%s", error.ToString().c_str());
    cntl->http_response().set_status_code(http_code);
  }
}

namespace {

result<std::string_view> TransactionIdFromPath(brpc::Controller* cntl) {
  const auto& transaction_id = cntl->http_request().unresolved_path();
  if (transaction_id.empty() || transaction_id.find('/') != std::string::npos) {
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "A transaction ID is required in the request path."));
  }
  return std::string_view(transaction_id);
}

std::string FormatExpiresAt(std::chrono::system_clock::time_point expires_at) {
  const auto epoch_milliseconds =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          expires_at.time_since_epoch());
  const auto epoch_seconds =
      std::chrono::duration_cast<std::chrono::seconds>(epoch_milliseconds);
  const auto milliseconds = epoch_milliseconds - epoch_seconds;
  const auto time = static_cast<std::time_t>(epoch_seconds.count());
  std::tm utc_time{};
#ifdef _WIN32
  gmtime_s(&utc_time, &time);
#else
  gmtime_r(&time, &utc_time);
#endif
  char buffer[32];
  std::snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
                utc_time.tm_year + 1900, utc_time.tm_mon + 1, utc_time.tm_mday,
                utc_time.tm_hour, utc_time.tm_min, utc_time.tm_sec,
                static_cast<int>(milliseconds.count()));
  return buffer;
}

std::string SerializeBeginResponse(
    const ServiceTransactionManager::BeginResult& transaction,
    TransactionMode mode) {
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  writer.StartObject();
  writer.Key("transaction_id");
  writer.String(
      transaction.transaction_id.data(),
      static_cast<rapidjson::SizeType>(transaction.transaction_id.size()));
  writer.Key("mode");
  writer.String(mode == TransactionMode::kReadOnly ? "read_only"
                                                   : "read_write");
  writer.Key("expires_at");
  if (transaction.expires_at) {
    const auto expires_at = FormatExpiresAt(*transaction.expires_at);
    writer.String(expires_at.data(),
                  static_cast<rapidjson::SizeType>(expires_at.size()));
  } else {
    writer.Null();
  }
  writer.EndObject();
  return std::string(buffer.GetString(), buffer.GetSize());
}

result<TransactionMode> ParseTransactionMode(brpc::Controller* cntl) {
  const auto body = cntl->request_attachment().to_string();
  rapidjson::Document document;
  document.Parse(body.data(), body.size());
  if (document.HasParseError() || !document.IsObject() ||
      !document.HasMember("mode") || !document["mode"].IsString()) {
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "Transaction begin requires a JSON mode."));
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

Status RequireEmptyBody(brpc::Controller* cntl) {
  if (!cntl->request_attachment().empty()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "This transaction operation does not accept a request body.");
  }
  return Status::OK();
}

void MarkTransactionResponse(brpc::Controller* cntl) {
  cntl->http_response().SetHeader("Cache-Control", "no-store");
}

bool RequireHttpMethod(brpc::Controller* cntl, brpc::HttpMethod expected,
                       const char* expected_name) {
  if (cntl->http_request().method() == expected) {
    return true;
  }
  cntl->SetFailed(brpc::HTTP_STATUS_METHOD_NOT_ALLOWED,
                  "This transaction endpoint requires %s.", expected_name);
  cntl->http_response().set_status_code(brpc::HTTP_STATUS_METHOD_NOT_ALLOWED);
  cntl->http_response().SetHeader("Allow", expected_name);
  return false;
}

template <typename Operation>
void FinishTransaction(brpc::Controller* cntl,
                       const BrpcServiceProtocol& protocol,
                       Operation&& operation) {
  MarkTransactionResponse(cntl);
  if (!RequireHttpMethod(cntl, brpc::HTTP_METHOD_POST, "POST")) {
    return;
  }
  auto transaction_id = TransactionIdFromPath(cntl);
  Status status =
      transaction_id ? RequireEmptyBody(cntl) : transaction_id.error();
  if (status.ok()) {
    status = operation(transaction_id.value());
  }
  result<std::string> response =
      status.ok() ? result<std::string>("") : tl::unexpected(status);
  protocol.send_query_response(cntl, response);
}

}  // namespace

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
  auto slot_lease = execution_slot_pool_.AcquireExecutionSlot();
  auto read_txn = slot_lease->BeginSnapshotReadTransaction();
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
  auto slot_lease = execution_slot_pool_.AcquireExecutionSlot();
  auto result = slot_lease->ExecuteTransactionalRequest(query_request);

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

void HttpServiceImpl::BeginTransaction(
    google::protobuf::RpcController* cntl_base, const HttpRequest*,
    HttpResponse*, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(cntl_base);
  MarkTransactionResponse(cntl);
  if (!RequireHttpMethod(cntl, brpc::HTTP_METHOD_POST, "POST")) {
    return;
  }
  auto mode = ParseTransactionMode(cntl);
  if (!mode) {
    result<std::string> error = tl::unexpected(mode.error());
    protocol_.send_query_response(cntl, error);
    return;
  }
  auto transaction = transaction_manager_.Begin(mode.value());
  if (!transaction) {
    result<std::string> error = tl::unexpected(transaction.error());
    protocol_.send_query_response(cntl, error);
    return;
  }
  result<std::string> response =
      SerializeBeginResponse(transaction.value(), mode.value());
  protocol_.send_query_response(cntl, response);
  cntl->http_response().set_status_code(brpc::HTTP_STATUS_CREATED);
  cntl->http_response().set_content_type("application/json");
  cntl->http_response().SetHeader(
      "Location", "/transactions/" + transaction->transaction_id);
}

void HttpServiceImpl::ExecuteTransactionQuery(
    google::protobuf::RpcController* cntl_base, const HttpRequest*,
    HttpResponse*, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(cntl_base);
  MarkTransactionResponse(cntl);
  if (!RequireHttpMethod(cntl, brpc::HTTP_METHOD_POST, "POST")) {
    return;
  }
  auto transaction_id = TransactionIdFromPath(cntl);
  if (!transaction_id) {
    result<std::string> error = tl::unexpected(transaction_id.error());
    protocol_.send_query_response(cntl, error);
    return;
  }
  auto response = transaction_manager_.Execute(
      transaction_id.value(), cntl->request_attachment().to_string());
  protocol_.send_query_response(cntl, response);
}

void HttpServiceImpl::CommitTransaction(
    google::protobuf::RpcController* cntl_base, const HttpRequest*,
    HttpResponse*, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(cntl_base);
  FinishTransaction(cntl, protocol_, [this](std::string_view transaction_id) {
    return transaction_manager_.Commit(transaction_id);
  });
}

void HttpServiceImpl::RollbackTransaction(
    google::protobuf::RpcController* cntl_base, const HttpRequest*,
    HttpResponse*, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(cntl_base);
  FinishTransaction(cntl, protocol_, [this](std::string_view transaction_id) {
    return transaction_manager_.Rollback(transaction_id);
  });
}

BrpcServiceManager::BrpcServiceManager(
    neug::NeugDB& neug_db, TpExecutionSlotPool& execution_slot_pool,
    ServiceTransactionManager& transaction_manager)
    : neug_db_(neug_db),
      execution_slot_pool_(execution_slot_pool),
      transaction_manager_(transaction_manager) {
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
      "/schema => GetSchema,"
      "/transactions => BeginTransaction,"
      "/transactions/*/query => ExecuteTransactionQuery,"
      "/transactions/*/commit => CommitTransaction,"
      "/transactions/*/rollback => RollbackTransaction";

#ifdef ENABLE_HTTP_PROTOCOL
  auto http_svc = std::make_unique<HttpServiceImpl>(
      neug_db_, execution_slot_pool_, transaction_manager_);
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
            << neug_db_.config().max_thread_num
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
  if (brpc_server_->IsRunning()) {
    brpc_server_->Stop(0);
    brpc_server_->Join();
  }
  LOG(INFO) << "Brpc server stopped";
}

uint32_t BrpcServiceManager::resolve_num_threads() const {
  if (service_config_.thread_num != 0) {
    return service_config_.thread_num;
  }
  const auto max_thread_num = neug_db_.config().max_thread_num;
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
