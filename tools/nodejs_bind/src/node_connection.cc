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

#include "node_connection.h"

#include <memory>
#include <string>

#include "neug/execution/common/params_map.h"
#include "neug/main/neug_db.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/yaml_utils.h"
#include "node_query_request.h"

#include <rapidjson/document.h>

namespace neug {

namespace {

Napi::Object StatusToJs(Napi::Env env, const Status& status) {
  auto result = Napi::Object::New(env);
  result.Set("code", Napi::Number::New(env, status.error_code()));
  result.Set("message", Napi::String::New(env, status.error_message()));
  return result;
}

Status ClosedConnectionStatus() {
  return Status(StatusCode::ERR_CONNECTION_CLOSED, "Connection is closed.");
}

}  // namespace

Napi::FunctionReference NodeConnection::constructor;

Napi::Object NodeConnection::Init(Napi::Env env, Napi::Object exports) {
  Napi::Function func = DefineClass(
      env, "NodeConnection",
      {
          InstanceMethod("execute", &NodeConnection::Execute),
          InstanceMethod("beginTransaction", &NodeConnection::BeginTransaction),
          InstanceMethod("commit", &NodeConnection::Commit),
          InstanceMethod("rollback", &NodeConnection::Rollback),
          InstanceMethod("hasActiveTransaction",
                         &NodeConnection::HasActiveTransaction),
          InstanceMethod("getSchema", &NodeConnection::GetSchema),
          InstanceMethod("close", &NodeConnection::Close),
      });
  constructor = Napi::Persistent(func);
  constructor.SuppressDestruct();
  exports.Set("NodeConnection", func);
  return exports;
}

NodeConnection::NodeConnection(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<NodeConnection>(info), db_(nullptr), conn_(nullptr) {}

void NodeConnection::SetConnection(NeugDB* db,
                                   std::shared_ptr<Connection> conn) {
  db_ = db;
  conn_ = std::move(conn);
  if (!conn_) {
    THROW_RUNTIME_ERROR("Connection is null");
  }
}

Napi::Object NodeConnection::NewInstance(Napi::Env env, NeugDB& db,
                                         std::shared_ptr<Connection> conn) {
  Napi::Object obj = constructor.New({});
  NodeConnection* wrapped = Napi::ObjectWrap<NodeConnection>::Unwrap(obj);
  wrapped->SetConnection(&db, std::move(conn));
  return obj;
}

Napi::Value NodeConnection::Execute(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (info.Length() < 1 || !info[0].IsString()) {
    Napi::TypeError::New(env, "Query string required")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  std::string query_string = info[0].As<Napi::String>().Utf8Value();
  std::string access_mode = "";
  if (info.Length() >= 2 && info[1].IsString()) {
    access_mode = info[1].As<Napi::String>().Utf8Value();
  }

  // Parse parameters from JavaScript object
  rapidjson::Document params_json(rapidjson::kObjectType);
  if (info.Length() >= 3 && info[2].IsObject()) {
    Napi::Object params_obj = info[2].As<Napi::Object>();
    Napi::Array keys = params_obj.GetPropertyNames();
    for (uint32_t i = 0; i < keys.Length(); ++i) {
      std::string key = keys.Get(i).As<Napi::String>().Utf8Value();
      Napi::Value value = params_obj.Get(key);
      NodeParameterSerializer::SerializeParameter(params_json, key, value);
    }
  }

  auto query_result = conn_->Query(query_string, access_mode, params_json);
  if (!query_result) {
    return NodeQueryResult::NewInstanceFromStatus(env, query_result.error());
  }
  return NodeQueryResult::NewInstance(env, std::move(query_result.value()));
}

Napi::Value NodeConnection::BeginTransaction(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (info.Length() > 1 || (info.Length() == 1 && !info[0].IsBoolean())) {
    Napi::TypeError::New(env, "readOnly must be a boolean")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  const bool read_only =
      info.Length() == 1 && info[0].As<Napi::Boolean>().Value();
  const auto status =
      conn_ ? conn_->BeginTransaction(read_only ? TransactionMode::kReadOnly
                                                : TransactionMode::kReadWrite)
            : ClosedConnectionStatus();
  return StatusToJs(env, status);
}

Napi::Value NodeConnection::Commit(const Napi::CallbackInfo& info) {
  const auto status = conn_ ? conn_->Commit() : ClosedConnectionStatus();
  return StatusToJs(info.Env(), status);
}

Napi::Value NodeConnection::Rollback(const Napi::CallbackInfo& info) {
  const auto status = conn_ ? conn_->Rollback() : ClosedConnectionStatus();
  return StatusToJs(info.Env(), status);
}

Napi::Value NodeConnection::HasActiveTransaction(
    const Napi::CallbackInfo& info) {
  return Napi::Boolean::New(info.Env(), conn_ && conn_->HasActiveTransaction());
}

Napi::Value NodeConnection::GetSchema(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  try {
    return Napi::String::New(env, conn_->GetSchema());
  } catch (const neug::exception::Exception& e) {
    Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
  } catch (const std::exception& e) {
    Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
  }
  return env.Null();
}

Napi::Value NodeConnection::Close(const Napi::CallbackInfo& info) {
  if (conn_) {
    conn_->Close();
    conn_.reset();
  }
  return info.Env().Undefined();
}

}  // namespace neug
