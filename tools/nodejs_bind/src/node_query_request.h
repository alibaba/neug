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

#include <napi.h>

#include <string>

#include <rapidjson/document.h>

namespace neug {

/**
 * A helper struct for serializing parameters from NAPI objects to JSON strings.
 */
struct NodeParameterSerializer {
  static void SerializeParameter(rapidjson::Document& doc,
                                 const std::string& key,
                                 const Napi::Value& parameter);
};

class NodeQueryRequest : public Napi::ObjectWrap<NodeQueryRequest> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);

  NodeQueryRequest(const Napi::CallbackInfo& info);

 private:
  static Napi::FunctionReference constructor;

  static Napi::Value SerializeRequest(const Napi::CallbackInfo& info);
};

}  // namespace neug
