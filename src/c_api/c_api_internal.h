/** Copyright 2026 Alibaba Group Holding Limited. Licensed under Apache-2.0. */
#pragma once

#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

#include <rapidjson/document.h>

#include "neug/c/neug.h"
#include "neug/main/connection.h"
#include "neug/main/neug_db.h"

struct neug_error {
  std::string message;
};
struct neug_database {
  std::shared_ptr<neug::NeugDB> impl;
};
struct neug_connection {
  // Keep the database alive even when its public (already closed) handle goes.
  std::shared_ptr<neug::NeugDB> database;
  std::shared_ptr<neug::Connection> impl;
};
struct neug_result {
  std::shared_ptr<const neug::QueryResponse> response;
  mutable std::mutex cache_mutex;
  mutable std::map<std::pair<const neug::Array*, uint64_t>,
                   std::unique_ptr<rapidjson::Document>>
      graph_cache;
};

namespace neug {
namespace capi {

class Error : public std::runtime_error {
 public:
  Error(neug_status status, const std::string& message)
      : std::runtime_error(message), code(status) {}
  neug_status code;
};

inline void Require(bool valid, const char* message,
                    neug_status code = NEUG_ERROR_INVALID_ARGUMENT) {
  if (!valid) {
    throw Error(code, message);
  }
}

neug_status Report(neug_status code, const char* message,
                   neug_error** out_error) noexcept;
neug_status CurrentException(neug_error** out_error) noexcept;

template <typename F>
neug_status Guard(neug_error** error, F&& function) noexcept {
  // Never overwrite an existing error allocation.
  if (error && *error) {
    return NEUG_ERROR_INVALID_ARGUMENT;
  }
  try {
    function();
    return NEUG_OK;
  } catch (...) { return CurrentException(error); }
}

template <typename T>
T& Output(T* value) {
  Require(value != nullptr, "Output pointer is NULL");
  *value = {};
  return *value;
}

template <typename T>
void NewOutput(T** value) {
  Require(value && !*value, "Object output must point to NULL");
}

inline std::string Text(neug_string_view text, bool allow_zero = true) {
  Require(text.data || !text.size, "Nonempty string has NULL data");
  Require(text.size <= std::numeric_limits<rapidjson::SizeType>::max(),
          "String is too large", NEUG_ERROR_OVERFLOW);
  std::string value(text.data ? text.data : "", text.size);
  Require(allow_zero || value.find('\0') == std::string::npos,
          "Text must not contain a zero byte");
  return value;
}
inline void ValidateJsonDepth(const rapidjson::Value& value,
                              unsigned depth = 0) {
  Require(depth <= 64, "JSON nesting exceeds 64", NEUG_ERROR_OVERFLOW);
  if (value.IsArray()) {
    for (const auto& child : value.GetArray())
      ValidateJsonDepth(child, depth + 1);
  } else if (value.IsObject()) {
    for (const auto& member : value.GetObject())
      ValidateJsonDepth(member.value, depth + 1);
  }
}
inline void ParseJson(rapidjson::Document& output, neug_string_view input) {
  auto text = Text(input, false);
  Require(!text.empty(), "JSON input is empty");
  output.Parse<rapidjson::kParseValidateEncodingFlag |
               rapidjson::kParseIterativeFlag>(text.data(), text.size());
  Require(!output.HasParseError(), "Invalid JSON input",
          NEUG_ERROR_BAD_ENCODING);
  ValidateJsonDepth(output);
}
inline neug_string_view View(const std::string& value) {
  return {value.data(), value.size()};
}
inline void CheckStatus(const Status& status) {
  if (!status.ok()) {
    throw Error(static_cast<neug_status>(status.error_code()),
                status.error_message());
  }
}
inline Connection& ConnectionImpl(const neug_connection* connection) {
  Require(connection && connection->impl, "Connection is NULL");
  Require(!connection->impl->IsClosed(), "Connection is closed",
          NEUG_ERROR_CONNECTION_CLOSED);
  return *connection->impl;
}
inline const QueryResponse& Response(const neug_result* result) {
  Require(result && result->response, "Result is NULL");
  return *result->response;
}
}  // namespace capi
}  // namespace neug
