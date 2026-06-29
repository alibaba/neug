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

Napi::FunctionReference NodeConnection::constructor;

Napi::Object NodeConnection::Init(Napi::Env env, Napi::Object exports) {
  Napi::Function func =
      DefineClass(env, "NodeConnection",
                  {
                      InstanceMethod("execute", &NodeConnection::Execute),
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
  rapidjson::Document params_json;
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

Napi::Value NodeConnection::GetSchema(const Napi::CallbackInfo& info) {
  return Napi::String::New(info.Env(), conn_->GetSchema());
}

Napi::Value NodeConnection::Close(const Napi::CallbackInfo& info) {
  if (conn_) {
    db_->RemoveConnection(conn_);
    conn_.reset();
  }
  return info.Env().Undefined();
}

}  // namespace neug
