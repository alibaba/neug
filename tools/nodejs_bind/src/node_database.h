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
#include <string>

#include "neug/main/neug_db.h"
#include "node_connection.h"

namespace neug {

struct AddonData {
  Napi::FunctionReference connection_constructor;
  Napi::FunctionReference query_result_constructor;
};

// Contains no JS references: environments share this state, not their wrappers.
struct DatabaseState : public std::enable_shared_from_this<DatabaseState> {
  DatabaseState(const NeugDBConfig& config, const std::string& mode);
  ~DatabaseState();

  static std::shared_ptr<DatabaseState> Attach(const std::string& handle);
  std::string Share();
  void Close();

  // Keep the native object alive until all attachments/connections release it.
  const std::unique_ptr<NeugDB> database;
  const NeugDBConfig options;
  const std::string mode;

 private:
  void RevokeHandle();
  std::string handle_;
};

class NodeDatabase : public Napi::ObjectWrap<NodeDatabase> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  static Napi::Value GetCpuCount(const Napi::CallbackInfo& info);

  NodeDatabase(const Napi::CallbackInfo& info);
  ~NodeDatabase();

  Napi::Value Connect(const Napi::CallbackInfo& info);
  Napi::Value Close(const Napi::CallbackInfo& info);
  Napi::Value Share(const Napi::CallbackInfo& info);
  Napi::Value GetOptions(const Napi::CallbackInfo& info);

 private:
  MemoryLevel ParseBufferStrategy(const std::string& level);

  std::shared_ptr<DatabaseState> state_;
  bool owner_{false};
};

}  // namespace neug

#endif  // TOOLS_NODEJS_BIND_SRC_NODE_DATABASE_H_
