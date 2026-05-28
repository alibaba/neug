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

#ifndef TOOLS_NODEJS_BIND_SRC_NODE_QUERY_RESULT_H_
#define TOOLS_NODEJS_BIND_SRC_NODE_QUERY_RESULT_H_

#include <napi.h>

#include <memory>
#include <string>
#include <vector>

#include "neug/main/query_result.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/result.h"

namespace neug {

class NodeQueryResult : public Napi::ObjectWrap<NodeQueryResult> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  static Napi::Object NewInstance(Napi::Env env, QueryResult&& result);
  static Napi::Object NewInstanceFromStatus(Napi::Env env,
                                            const Status& status);
  static Napi::Object NewInstanceFromString(Napi::Env env,
                                            std::string&& result_str);

  NodeQueryResult(const Napi::CallbackInfo& info);
  ~NodeQueryResult();

  bool hasNext();
  int32_t length() const;
  std::vector<std::string> column_names() const;
  int32_t status_code() const;
  const std::string& status_message() const;

  QueryResult& query_result() { return query_result_; }

 private:
  static Napi::FunctionReference constructor;

  Napi::Value HasNext(const Napi::CallbackInfo& info);
  Napi::Value GetNext(const Napi::CallbackInfo& info);
  Napi::Value GetAt(const Napi::CallbackInfo& info);
  Napi::Value Length(const Napi::CallbackInfo& info);
  Napi::Value ColumnNames(const Napi::CallbackInfo& info);
  Napi::Value StatusCode(const Napi::CallbackInfo& info);
  Napi::Value StatusMessage(const Napi::CallbackInfo& info);
  Napi::Value GetBoltResponse(const Napi::CallbackInfo& info);
  Napi::Value Close(const Napi::CallbackInfo& info);

  size_t index_{0};
  Status status_;
  QueryResult query_result_;
};

}  // namespace neug

#endif  // TOOLS_NODEJS_BIND_SRC_NODE_QUERY_RESULT_H_
