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

#include <chrono>
#include <cstdio>
#include <ctime>
#include <string>
#include <string_view>
#include <utility>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "neug/main/query_request.h"
#include "neug/main/query_result.h"
#include "neug/server/tp_operations.h"
#include "neug/utils/result.h"

namespace neug {
namespace service_http {

// HTTP status mapping shared by the BRPC and cpp-httplib transports. The
// values are standard HTTP codes; brpc's HTTP_STATUS_* constants expand to
// exactly these numbers, so both transports answer with identical statuses.
inline int32_t status_code_to_http_code(StatusCode code) {
  switch (code) {
  case StatusCode::OK:
    return 200;
  case StatusCode::ERR_PERMISSION:
    return 500;
  case StatusCode::ERR_DATABASE_LOCKED:
    return 500;
  case StatusCode::ERR_NOT_SUPPORTED:
    return 501;
  case StatusCode::ERR_NOT_IMPLEMENTED:
    return 501;
  case StatusCode::ERR_QUERY_SYNTAX:
    return 400;
  case StatusCode::ERR_NOT_INITIALIZED:
    return 500;
  case StatusCode::ERR_QUERY_EXECUTION:
    return 500;
  case StatusCode::ERR_INTERNAL_ERROR:
    return 500;
  case StatusCode::ERR_NOT_FOUND:
    return 404;
  case StatusCode::ERR_NO_CHECKPOINT:
    return 404;
  case StatusCode::ERR_INVALID_ARGUMENT:
    return 400;
  case StatusCode::ERR_COMPILATION:
    return 500;
  case StatusCode::ERR_SERVICE_UNAVAILABLE:
    return 503;
  case StatusCode::ERR_TX_STATE_CONFLICT:
    return 409;
  case StatusCode::ERR_TX_TIMEOUT:
  case StatusCode::ERR_TX_NOT_FOUND:
    return 410;
  default:
    return 500;
  }
}

inline result<std::string_view> TransactionIdFromPath(
    std::string_view transaction_id) {
  if (transaction_id.empty() || transaction_id.find('/') != std::string::npos) {
    RETURN_ERROR(Status(StatusCode::ERR_INVALID_ARGUMENT,
                        "A transaction ID is required in the request path."));
  }
  return transaction_id;
}

inline std::string FormatExpiresAt(std::chrono::system_clock::time_point expires_at) {
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

inline std::string SerializeBeginResponse(const ServiceTransactionInfo& transaction,
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

inline result<TransactionMode> ParseTransactionMode(
    const std::string& body) {
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

inline Status RequireEmptyBody(const std::string& body) {
  if (!body.empty()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "This transaction operation does not accept a request body.");
  }
  return Status::OK();
}

inline result<std::string> SerializeQueryResult(result<QueryResult>&& query_result) {
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

}  // namespace service_http
}  // namespace neug
