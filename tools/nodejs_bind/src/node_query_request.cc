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

#include "node_query_request.h"

#include <sstream>
#include <string>

#include "neug/execution/common/types/value.h"
#include "neug/main/query_request.h"
#include "neug/utils/access_mode.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/in_archive.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace neug {

Napi::FunctionReference NodeQueryRequest::constructor;

rapidjson::Document napi_value_to_rapidjson_document(
    const Napi::Value& val, rapidjson::Document::AllocatorType& allocator) {
  rapidjson::Document doc(&allocator);

  if (val.IsNull() || val.IsUndefined()) {
    doc.SetNull();
  } else if (val.IsBoolean()) {
    doc.SetBool(val.As<Napi::Boolean>().Value());
  } else if (val.IsNumber()) {
    Napi::Number num = val.As<Napi::Number>();
    double d = num.DoubleValue();
    // Check if it's an integer
    if (d == static_cast<double>(static_cast<int64_t>(d))) {
      doc.SetInt64(static_cast<int64_t>(d));
    } else {
      doc.SetDouble(d);
    }
  } else if (val.IsString()) {
    std::string s = val.As<Napi::String>().Utf8Value();
    doc.SetString(s.c_str(), s.length(), allocator);
  } else if (val.IsArray()) {
    Napi::Array arr = val.As<Napi::Array>();
    doc.SetArray();
    for (uint32_t i = 0; i < arr.Length(); ++i) {
      rapidjson::Document element_doc =
          napi_value_to_rapidjson_document(arr.Get(i), allocator);
      doc.PushBack(element_doc, allocator);
    }
  } else if (val.IsDate()) {
    // Convert Date to ISO string
    Napi::Object date_obj = val.As<Napi::Object>();
    Napi::Function to_iso = date_obj.Get("toISOString").As<Napi::Function>();
    std::string iso_str =
        to_iso.Call(date_obj, {}).As<Napi::String>().Utf8Value();
    doc.SetString(iso_str.c_str(), iso_str.length(), allocator);
  } else if (val.IsObject()) {
    // Serialize plain object as JSON object
    Napi::Object obj = val.As<Napi::Object>();
    Napi::Array keys = obj.GetPropertyNames();
    doc.SetObject();
    for (uint32_t i = 0; i < keys.Length(); ++i) {
      std::string key = keys.Get(i).As<Napi::String>().Utf8Value();
      rapidjson::Document val_doc =
          napi_value_to_rapidjson_document(obj.Get(key), allocator);
      rapidjson::Value json_key;
      json_key.SetString(key.c_str(), key.length(), allocator);
      doc.AddMember(json_key, val_doc, allocator);
    }
  } else {
    throw std::invalid_argument(
        "Unsupported parameter type for serialization.");
  }
  return doc;
}

void NodeParameterSerializer::SerializeParameter(rapidjson::Document& doc,
                                                 const std::string& key,
                                                 const Napi::Value& parameter) {
  if (doc.IsNull()) {
    doc.SetObject();
  }
  auto& allocator = doc.GetAllocator();
  rapidjson::Value json_key;
  json_key.SetString(key.c_str(), key.length(), allocator);
  rapidjson::Document json_value =
      napi_value_to_rapidjson_document(parameter, allocator);
  doc.AddMember(json_key, json_value, allocator);
}

Napi::Object NodeQueryRequest::Init(Napi::Env env, Napi::Object exports) {
  Napi::Function func = DefineClass(
      env, "NodeQueryRequest",
      {
          StaticMethod("serializeRequest", &NodeQueryRequest::SerializeRequest),
      });
  constructor = Napi::Persistent(func);
  constructor.SuppressDestruct();
  exports.Set("NodeQueryRequest", func);
  return exports;
}

NodeQueryRequest::NodeQueryRequest(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<NodeQueryRequest>(info) {}

Napi::Value NodeQueryRequest::SerializeRequest(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();

  if (info.Length() < 1 || !info[0].IsString()) {
    Napi::TypeError::New(env, "Query string required")
        .ThrowAsJavaScriptException();
    return env.Null();
  }

  std::string query = info[0].As<Napi::String>().Utf8Value();
  std::string access_mode = "update";
  if (info.Length() >= 2 && info[1].IsString()) {
    access_mode = info[1].As<Napi::String>().Utf8Value();
  }

  rapidjson::Document req_doc(rapidjson::kObjectType);
  auto& allocator = req_doc.GetAllocator();
  req_doc.AddMember("query", rapidjson::Value(query.c_str(), allocator),
                    allocator);
  req_doc.AddMember("access_mode",
                    rapidjson::Value(access_mode.c_str(), allocator),
                    allocator);

  rapidjson::Document params_doc;
  if (info.Length() >= 3 && info[2].IsObject()) {
    Napi::Object params_obj = info[2].As<Napi::Object>();
    Napi::Array keys = params_obj.GetPropertyNames();
    for (uint32_t i = 0; i < keys.Length(); ++i) {
      std::string key = keys.Get(i).As<Napi::String>().Utf8Value();
      NodeParameterSerializer::SerializeParameter(params_doc, key,
                                                  params_obj.Get(key));
    }
  }
  req_doc.AddMember("parameters", params_doc, allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  req_doc.Accept(writer);
  return Napi::String::New(env, buffer.GetString());
}

}  // namespace neug
