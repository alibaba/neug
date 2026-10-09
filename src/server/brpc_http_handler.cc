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

#include "brpc_http_handler.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/http_status_code.h>
#include <glog/logging.h>

#include <chrono>
#include <cstdio>
#include <ctime>
#include <utility>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "neug/generated/proto/plan/error.pb.h"
#include "neug/main/query_request.h"
#include "neug/server/tp_operations.h"

namespace neug {

namespace {

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

void SendHttpResponse(brpc::Controller* cntl,
                      const neug::result<std::string>& response) {
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
}

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

std::string SerializeBeginResponse(const ServiceTransactionInfo& transaction,
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

result<std::string> SerializeQueryResult(result<QueryResult>&& query_result) {
  if (!query_result) {
    RETURN_ERROR(query_result.error());
  }
  try {
    return query_result.value().Serialize();
  } catch (const std::exception& e) {
    RETURN_ERROR(Status::RuntimeError(e.what()));
  }
}

template <typename Execute>
result<std::string> ParseAndExecuteQuery(const std::string& request,
                                         Execute&& execute) {
  auto parsed = RequestParser::ParseFromString(request);
  if (!parsed) {
    RETURN_ERROR(parsed.error());
  }
  return SerializeQueryResult(execute(parsed.value()));
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
void FinishTransaction(brpc::Controller* cntl, Operation&& operation) {
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
  SendHttpResponse(cntl, response);
}

}  // namespace

void BrpcHttpHandler::PostCypherQuery(
    google::protobuf::RpcController* cntl_base, const HttpRequest*,
    HttpResponse*, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(cntl_base);
  const auto query_request = cntl->request_attachment().to_string();
  if (query_request.empty()) {
    result<std::string> error = tl::unexpected(
        Status(StatusCode::ERR_INVALID_ARGUMENT, "Query request is empty"));
    SendHttpResponse(cntl, error);
    return;
  }
  auto response =
      ParseAndExecuteQuery(query_request, [this](const auto& request) {
        return tp_operations_.ExecuteQuery(request);
      });
  SendHttpResponse(cntl, response);
}

void BrpcHttpHandler::GetSchema(google::protobuf::RpcController* cntl_base,
                                const google::protobuf::Empty*,
                                HttpResponse* response,
                                google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
  auto ret = tp_operations_.GetSchema();

  SendHttpResponse(cntl, ret);
}

void BrpcHttpHandler::GetServiceStatus(
    google::protobuf::RpcController* cntl_base, const google::protobuf::Empty*,
    HttpResponse* response, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
  auto ret = tp_operations_.GetServiceStatus();

  SendHttpResponse(cntl, ret);
}

void BrpcHttpHandler::BeginTransaction(
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
    SendHttpResponse(cntl, error);
    return;
  }
  auto transaction = tp_operations_.BeginTransaction(mode.value());
  if (!transaction) {
    result<std::string> error = tl::unexpected(transaction.error());
    SendHttpResponse(cntl, error);
    return;
  }
  result<std::string> response =
      SerializeBeginResponse(transaction.value(), mode.value());
  SendHttpResponse(cntl, response);
  cntl->http_response().set_status_code(brpc::HTTP_STATUS_CREATED);
  cntl->http_response().set_content_type("application/json");
  cntl->http_response().SetHeader(
      "Location", "/transactions/" + transaction->transaction_id);
}

void BrpcHttpHandler::ExecuteTransactionQuery(
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
    SendHttpResponse(cntl, error);
    return;
  }
  auto request =
      RequestParser::ParseFromString(cntl->request_attachment().to_string());
  if (!request) {
    result<std::string> error = tl::unexpected(request.error());
    SendHttpResponse(cntl, error);
    return;
  }
  auto response = SerializeQueryResult(tp_operations_.ExecuteInTransaction(
      transaction_id.value(), request.value()));
  SendHttpResponse(cntl, response);
}

void BrpcHttpHandler::CommitTransaction(
    google::protobuf::RpcController* cntl_base, const HttpRequest*,
    HttpResponse*, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(cntl_base);
  FinishTransaction(cntl, [this](std::string_view transaction_id) {
    return tp_operations_.CommitTransaction(transaction_id);
  });
}

void BrpcHttpHandler::RollbackTransaction(
    google::protobuf::RpcController* cntl_base, const HttpRequest*,
    HttpResponse*, google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  auto* cntl = static_cast<brpc::Controller*>(cntl_base);
  FinishTransaction(cntl, [this](std::string_view transaction_id) {
    return tp_operations_.RollbackTransaction(transaction_id);
  });
}

const char* BrpcHttpHandler::Routes() {
  return "/cypher => PostCypherQuery,"
         "/service_status => GetServiceStatus,"
         "/schema => GetSchema,"
         "/transactions => BeginTransaction,"
         "/transactions/*/query => ExecuteTransactionQuery,"
         "/transactions/*/commit => CommitTransaction,"
         "/transactions/*/rollback => RollbackTransaction";
}

}  // namespace neug
