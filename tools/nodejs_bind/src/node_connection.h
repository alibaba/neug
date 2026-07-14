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

#ifndef TOOLS_NODEJS_BIND_SRC_NODE_CONNECTION_H_
#define TOOLS_NODEJS_BIND_SRC_NODE_CONNECTION_H_

#include <napi.h>

#include <memory>
#include <string>

#include "neug/common/types/value.h"
#include "neug/main/connection.h"
#include "neug/main/neug_db.h"
#include "node_query_result.h"

namespace neug {

class NodeConnection : public Napi::ObjectWrap<NodeConnection> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  static Napi::Object NewInstance(Napi::Env env, NeugDB& db,
                                  std::shared_ptr<Connection> conn);

  NodeConnection(const Napi::CallbackInfo& info);
  ~NodeConnection() = default;

  void SetConnection(NeugDB* db, std::shared_ptr<Connection> conn);

 private:
  static Napi::FunctionReference constructor;

  Napi::Value Execute(const Napi::CallbackInfo& info);
  Napi::Value GetSchema(const Napi::CallbackInfo& info);
  Napi::Value Close(const Napi::CallbackInfo& info);

  NeugDB* db_{nullptr};
  std::shared_ptr<Connection> conn_;
};

}  // namespace neug

#endif  // TOOLS_NODEJS_BIND_SRC_NODE_CONNECTION_H_
