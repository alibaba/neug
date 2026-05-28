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

#ifndef TOOLS_NODEJS_BIND_SRC_NODE_DATABASE_H_
#define TOOLS_NODEJS_BIND_SRC_NODE_DATABASE_H_

#include <napi.h>

#include <memory>
#include <mutex>
#include <string>

#include "neug/main/neug_db.h"
#ifdef BUILD_HTTP_SERVER
#include "neug/server/neug_db_service.h"
#endif
#include "node_connection.h"

namespace neug {

class NodeDatabase : public Napi::ObjectWrap<NodeDatabase> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);

  NodeDatabase(const Napi::CallbackInfo& info);
  ~NodeDatabase();

  Napi::Value Connect(const Napi::CallbackInfo& info);
  Napi::Value Close(const Napi::CallbackInfo& info);
  Napi::Value Serve(const Napi::CallbackInfo& info);
  Napi::Value StopServing(const Napi::CallbackInfo& info);

 private:
  static Napi::FunctionReference constructor;

  MemoryLevel ParseBufferStrategy(const std::string& level);

  std::recursive_mutex mtx_;
  std::string db_dir_;
  std::unique_ptr<NeugDB> database;
#ifdef BUILD_HTTP_SERVER
  std::unique_ptr<NeugDBService> service_;
#endif
};

}  // namespace neug

#endif  // TOOLS_NODEJS_BIND_SRC_NODE_DATABASE_H_
