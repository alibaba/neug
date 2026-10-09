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

#include <utility>

#include "neug/generated/proto/plan/error.pb.h"
#include "neug/main/query_request.h"
#include "neug/server/tp_operations.h"
#include "service_http_common.h"

namespace neug {

namespace {

void SendHttpResponse(brpc::Controller* cntl,
                      const neug::result<std::string>& response) {
  if (response) {
    cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
    const auto& results = response.value();
    cntl->response_attachment().append(results.data(), results.size());
  } else {
    const auto& status = response.error();
    LOG(ERROR) << "Query failed: " << status.ToString();
    auto http_code =
        service_http::status_code_to_http_code(status.error_code());
    cntl->SetFailed(http_code, "%s", status.ToString().c_str());
    // brpc treats SetFailed's integer as an RPC error code and maps unknown
    // values (including HTTP 501) back to 500. Override the HTTP status after
    // SetFailed, as required by brpc::Controller's contract.
    cntl->http_response().set_status_code(http_code);
  }
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
void FinishTransaction(brpc::Controller* cntl, Operation&& operation) {
  MarkTransactionResponse(cntl);
  if (!RequireHttpMethod(cntl, brpc::HTTP_METHOD_POST, "POST")) {
    return;
  }
  auto transaction_id = service_http::TransactionIdFromPath(
      cntl->http_request().unresolved_path());
  Status status = transaction_id ? service_http::RequireEmptyBody(
                                       cntl->request_attachment().to_string())
                                 : transaction_id.error();
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
  auto response = service_http::ParseAndExecuteQuery(
      query_request, [this](const auto& request) {
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
  auto mode = service_http::ParseTransactionMode(
      cntl->request_attachment().to_string());
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
      service_http::SerializeBeginResponse(transaction.value(), mode.value());
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
  auto transaction_id = service_http::TransactionIdFromPath(
      cntl->http_request().unresolved_path());
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
  auto response = service_http::SerializeQueryResult(
      tp_operations_.ExecuteInTransaction(transaction_id.value(),
                                          request.value()));
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
