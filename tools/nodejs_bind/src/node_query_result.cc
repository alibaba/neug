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

#include "node_query_result.h"

#include <cstdint>
#include <memory>
#include <vector>

#include "neug/storages/graph/schema.h"
#include "neug/utils/bolt_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/types.h"

namespace neug {

Napi::FunctionReference NodeQueryResult::constructor;

Napi::Object NodeQueryResult::Init(Napi::Env env, Napi::Object exports) {
  Napi::Function func = DefineClass(
      env, "NodeQueryResult",
      {
          InstanceMethod("hasNext", &NodeQueryResult::HasNext),
          InstanceMethod("getNext", &NodeQueryResult::GetNext),
          InstanceMethod("getAt", &NodeQueryResult::GetAt),
          InstanceMethod("length", &NodeQueryResult::Length),
          InstanceMethod("columnNames", &NodeQueryResult::ColumnNames),
          InstanceMethod("statusCode", &NodeQueryResult::StatusCode),
          InstanceMethod("statusMessage", &NodeQueryResult::StatusMessage),
          InstanceMethod("getBoltResponse", &NodeQueryResult::GetBoltResponse),
          InstanceMethod("close", &NodeQueryResult::Close),
          StaticMethod("fromString", &NodeQueryResult::FromString),
      });
  constructor = Napi::Persistent(func);
  constructor.SuppressDestruct();
  exports.Set("NodeQueryResult", func);
  return exports;
}

NodeQueryResult::NodeQueryResult(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<NodeQueryResult>(info),
      status_(Status::OK()),
      query_result_() {}

NodeQueryResult::~NodeQueryResult() {}

Napi::Object NodeQueryResult::NewInstance(Napi::Env env, QueryResult&& result) {
  Napi::Object obj = constructor.New({});
  NodeQueryResult* wrapped = Napi::ObjectWrap<NodeQueryResult>::Unwrap(obj);
  wrapped->status_ = Status::OK();
  // QueryResult has no move assignment operator, use Swap instead
  QueryResult tmp(std::move(result));
  wrapped->query_result_.Swap(tmp);
  wrapped->index_ = 0;
  return obj;
}

Napi::Object NodeQueryResult::NewInstanceFromStatus(Napi::Env env,
                                                    const Status& status) {
  Napi::Object obj = constructor.New({});
  NodeQueryResult* wrapped = Napi::ObjectWrap<NodeQueryResult>::Unwrap(obj);
  wrapped->status_ = status;
  wrapped->index_ = 0;
  return obj;
}

Napi::Object NodeQueryResult::NewInstanceFromString(Napi::Env env,
                                                    std::string&& result_str) {
  Napi::Object obj = constructor.New({});
  NodeQueryResult* wrapped = Napi::ObjectWrap<NodeQueryResult>::Unwrap(obj);
  wrapped->status_ = Status::OK();
  wrapped->query_result_.Swap(QueryResult::From(std::move(result_str)));
  wrapped->index_ = 0;
  return obj;
}

bool NodeQueryResult::hasNext() { return index_ < query_result_.length(); }

int32_t NodeQueryResult::length() const { return query_result_.length(); }

std::vector<std::string> NodeQueryResult::column_names() const {
  const auto& schema = query_result_.result_schema();
  std::vector<std::string> names(schema.name_size());
  for (int i = 0; i < schema.name_size(); ++i) {
    names[i] = schema.name(i);
  }
  return names;
}

int32_t NodeQueryResult::status_code() const { return status_.error_code(); }

const std::string& NodeQueryResult::status_message() const {
  return status_.error_message();
}

namespace {

inline bool is_valid(const std::string& map, size_t i) {
  return map.empty() || (static_cast<uint8_t>(map[i >> 3]) >> (i & 7)) & 1;
}

Napi::Value ParseJsonToJsObject(Napi::Env env, const std::string& json_str) {
  Napi::Object global = env.Global();
  Napi::Object json_obj = global.Get("JSON").As<Napi::Object>();
  Napi::Function parse_fn = json_obj.Get("parse").As<Napi::Function>();
  try {
    return parse_fn.Call(json_obj, {Napi::String::New(env, json_str)});
  } catch (const Napi::Error& e) {
    // Fall back to returning the raw string on parse failure
    return Napi::String::New(env, json_str);
  }
}

Napi::Value FetchValueFromColumn(Napi::Env env, const neug::Array& column,
                                 size_t index) {
  if (column.has_bool_array()) {
    const auto& col = column.bool_array();
    if (is_valid(col.validity(), index)) {
      return Napi::Boolean::New(env, col.values(index));
    } else {
      return env.Null();
    }
  } else if (column.has_int32_array()) {
    const auto& col = column.int32_array();
    if (is_valid(col.validity(), index)) {
      return Napi::Number::New(env, col.values(index));
    } else {
      return env.Null();
    }
  } else if (column.has_uint32_array()) {
    const auto& col = column.uint32_array();
    if (is_valid(col.validity(), index)) {
      return Napi::Number::New(env, col.values(index));
    } else {
      return env.Null();
    }
  } else if (column.has_int64_array()) {
    const auto& col = column.int64_array();
    if (is_valid(col.validity(), index)) {
      return Napi::BigInt::New(env, col.values(index));
    } else {
      return env.Null();
    }
  } else if (column.has_uint64_array()) {
    const auto& col = column.uint64_array();
    if (is_valid(col.validity(), index)) {
      return Napi::BigInt::New(env, static_cast<uint64_t>(col.values(index)));
    } else {
      return env.Null();
    }
  } else if (column.has_float_array()) {
    const auto& col = column.float_array();
    if (is_valid(col.validity(), index)) {
      return Napi::Number::New(env, col.values(index));
    } else {
      return env.Null();
    }
  } else if (column.has_double_array()) {
    const auto& col = column.double_array();
    if (is_valid(col.validity(), index)) {
      return Napi::Number::New(env, col.values(index));
    } else {
      return env.Null();
    }
  } else if (column.has_string_array()) {
    const auto& col = column.string_array();
    if (is_valid(col.validity(), index)) {
      return Napi::String::New(env, col.values(index));
    } else {
      return env.Null();
    }
  } else if (column.has_date_array()) {
    const auto& col = column.date_array();
    if (is_valid(col.validity(), index)) {
      Date day;
      day.from_timestamp(col.values(index));
      // Return ISO date string (YYYY-MM-DD with leading zeros)
      return Napi::String::New(env, day.to_string());
    } else {
      return env.Null();
    }
  } else if (column.has_timestamp_array()) {
    const auto& col = column.timestamp_array();
    if (is_valid(col.validity(), index)) {
      int64_t ms = col.values(index);
      // Return as JS Date object
      Napi::Object global = env.Global();
      Napi::Function date_ctor = global.Get("Date").As<Napi::Function>();
      return date_ctor.New({Napi::Number::New(env, static_cast<double>(ms))});
    } else {
      return env.Null();
    }
  } else if (column.has_interval_array()) {
    const auto& col = column.interval_array();
    if (is_valid(col.validity(), index)) {
      return Napi::String::New(env, col.values(index));
    } else {
      return env.Null();
    }
  } else if (column.has_list_array()) {
    const auto& col = column.list_array();
    if (is_valid(col.validity(), index)) {
      uint32_t list_size = col.offsets(index + 1) - col.offsets(index);
      size_t offset = col.offsets(index);
      Napi::Array arr = Napi::Array::New(env, list_size);
      for (uint32_t i = 0; i < list_size; ++i) {
        arr.Set(i, FetchValueFromColumn(env, col.elements(), offset + i));
      }
      return arr;
    } else {
      return env.Null();
    }
  } else if (column.has_struct_array()) {
    const auto& col = column.struct_array();
    if (is_valid(col.validity(), index)) {
      Napi::Array arr = Napi::Array::New(env, col.fields_size());
      for (int i = 0; i < col.fields_size(); ++i) {
        arr.Set(i, FetchValueFromColumn(env, col.fields(i), index));
      }
      return arr;
    } else {
      return env.Null();
    }
  } else if (column.has_vertex_array()) {
    const auto& col = column.vertex_array();
    if (is_valid(col.validity(), index)) {
      return ParseJsonToJsObject(env, col.values(index));
    } else {
      return env.Null();
    }
  } else if (column.has_edge_array()) {
    const auto& col = column.edge_array();
    if (is_valid(col.validity(), index)) {
      return ParseJsonToJsObject(env, col.values(index));
    } else {
      return env.Null();
    }
  } else if (column.has_path_array()) {
    const auto& col = column.path_array();
    if (is_valid(col.validity(), index)) {
      return ParseJsonToJsObject(env, col.values(index));
    } else {
      return env.Null();
    }
  } else {
    return env.Null();
  }
}

}  // namespace

Napi::Value NodeQueryResult::HasNext(const Napi::CallbackInfo& info) {
  return Napi::Boolean::New(info.Env(), hasNext());
}

Napi::Value NodeQueryResult::GetNext(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!hasNext()) {
    Napi::Error::New(env, "No more results").ThrowAsJavaScriptException();
    return env.Null();
  }
  const auto& response = query_result_.response();
  int num_columns = response.arrays_size();
  Napi::Array arr = Napi::Array::New(env, num_columns);
  for (int i = 0; i < num_columns; ++i) {
    arr.Set(i, FetchValueFromColumn(env, response.arrays(i), index_));
  }
  ++index_;
  return arr;
}

Napi::Value NodeQueryResult::GetAt(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (info.Length() < 1) {
    Napi::TypeError::New(env, "Index argument required")
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  int32_t idx = info[0].As<Napi::Number>().Int32Value();
  int32_t total = query_result_.length();
  if (idx < 0) {
    idx += total;
  }
  if (idx < 0 || idx >= total) {
    Napi::RangeError::New(env, "Index out of range")
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  const auto& response = query_result_.response();
  int num_columns = response.arrays_size();
  Napi::Array arr = Napi::Array::New(env, num_columns);
  for (int i = 0; i < num_columns; ++i) {
    arr.Set(i, FetchValueFromColumn(env, response.arrays(i), idx));
  }
  return arr;
}

Napi::Value NodeQueryResult::Length(const Napi::CallbackInfo& info) {
  return Napi::Number::New(info.Env(), length());
}

Napi::Value NodeQueryResult::ColumnNames(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  auto names = column_names();
  Napi::Array arr = Napi::Array::New(env, names.size());
  for (size_t i = 0; i < names.size(); ++i) {
    arr.Set(i, Napi::String::New(env, names[i]));
  }
  return arr;
}

Napi::Value NodeQueryResult::StatusCode(const Napi::CallbackInfo& info) {
  return Napi::Number::New(info.Env(), status_code());
}

Napi::Value NodeQueryResult::StatusMessage(const Napi::CallbackInfo& info) {
  return Napi::String::New(info.Env(), status_message());
}

Napi::Value NodeQueryResult::GetBoltResponse(const Napi::CallbackInfo& info) {
  auto names = column_names();
  return Napi::String::New(
      info.Env(), results_to_bolt_response(query_result_.response(), names));
}

Napi::Value NodeQueryResult::Close(const Napi::CallbackInfo& info) {
  // Release the underlying query result resources early
  QueryResult empty;
  query_result_.Swap(empty);
  return info.Env().Undefined();
}

Napi::Value NodeQueryResult::FromString(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (info.Length() < 1) {
    Napi::TypeError::New(env, "String or Buffer argument required")
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  std::string result_str;
  if (info[0].IsBuffer()) {
    auto buf = info[0].As<Napi::Buffer<char>>();
    result_str.assign(buf.Data(), buf.Length());
  } else if (info[0].IsString()) {
    result_str = info[0].As<Napi::String>().Utf8Value();
  } else {
    Napi::TypeError::New(env, "String or Buffer argument required")
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  return NewInstanceFromString(env, std::move(result_str));
}

}  // namespace neug
