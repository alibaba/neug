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

#include "node_database.h"

#include <unistd.h>

#include <string>

#include "neug/config.h"
#include "neug/utils/exception/exception.h"

namespace neug {

Napi::FunctionReference NodeDatabase::constructor;

Napi::Object NodeDatabase::Init(Napi::Env env, Napi::Object exports) {
  Napi::Function func =
      DefineClass(env, "NodeDatabase",
                  {
                      InstanceMethod("connect", &NodeDatabase::Connect),
                      InstanceMethod("close", &NodeDatabase::Close),
                      StaticMethod("cpuCount", &NodeDatabase::GetCpuCount),
                  });
  constructor = Napi::Persistent(func);
  constructor.SuppressDestruct();
  exports.Set("NodeDatabase", func);
  return exports;
}

NodeDatabase::NodeDatabase(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<NodeDatabase>(info) {
  Napi::Env env = info.Env();

  if (info.Length() < 1 || !info[0].IsObject()) {
    Napi::TypeError::New(env, "Options object required")
        .ThrowAsJavaScriptException();
    return;
  }

  Napi::Object opts = info[0].As<Napi::Object>();

  // Parse options
  std::string database_path = "";
  if (opts.Has("databasePath") && opts.Get("databasePath").IsString()) {
    database_path = opts.Get("databasePath").As<Napi::String>().Utf8Value();
  }

  int32_t max_thread_num = 0;
  if (opts.Has("maxThreadNum") && opts.Get("maxThreadNum").IsNumber()) {
    max_thread_num = opts.Get("maxThreadNum").As<Napi::Number>().Int32Value();
  }

  std::string mode = "r";
  if (opts.Has("mode") && opts.Get("mode").IsString()) {
    mode = opts.Get("mode").As<Napi::String>().Utf8Value();
  }

  std::string planner = "gopt";
  if (opts.Has("planner") && opts.Get("planner").IsString()) {
    planner = opts.Get("planner").As<Napi::String>().Utf8Value();
  }

  bool checkpoint_on_close = true;
  if (opts.Has("checkpointOnClose") &&
      opts.Get("checkpointOnClose").IsBoolean()) {
    checkpoint_on_close =
        opts.Get("checkpointOnClose").As<Napi::Boolean>().Value();
  }

  std::string buffer_strategy = "M_FULL";
  if (opts.Has("bufferStrategy") && opts.Get("bufferStrategy").IsString()) {
    buffer_strategy = opts.Get("bufferStrategy").As<Napi::String>().Utf8Value();
  }

  // Parse mode
  neug::DBMode mode_;
  if (mode == "read" || mode == "r" || mode == "read-only" ||
      mode == "read_only") {
    mode_ = DBMode::READ_ONLY;
  } else if (mode == "read_write" || mode == "rw" || mode == "w" ||
             mode == "wr" || mode == "write" || mode == "readwrite" ||
             mode == "read-write") {
    mode_ = DBMode::READ_WRITE;
  } else {
    Napi::Error::New(env, "Invalid mode: " + mode).ThrowAsJavaScriptException();
    return;
  }

  db_dir_ = database_path;
  database = std::make_unique<NeugDB>();
  NeugDBConfig config(db_dir_, max_thread_num);
  config.mode = mode_;
  config.planner_kind = planner;
  config.checkpoint_on_close = checkpoint_on_close;
  try {
    config.memory_level = ParseBufferStrategy(buffer_strategy);
    database->Open(config);
  } catch (const neug::exception::Exception& e) {
    Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    database.reset();
    return;
  } catch (const std::exception& e) {
    Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    database.reset();
    return;
  }
}

NodeDatabase::~NodeDatabase() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  if (database) {
    database->Close();
    database.reset();
  }
}

Napi::Value NodeDatabase::Connect(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!database) {
    Napi::Error::New(env, "Database is not initialized.")
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  try {
    auto conn = database->Connect();
    return NodeConnection::NewInstance(env, *database, conn);
  } catch (const neug::exception::Exception& e) {
    Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    return env.Null();
  } catch (const std::exception& e) {
    Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    return env.Null();
  }
}

Napi::Value NodeDatabase::Close(const Napi::CallbackInfo& info) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  if (database) {
    database->Close();
    database.reset();
  }
  return info.Env().Undefined();
}

Napi::Value NodeDatabase::GetCpuCount(const Napi::CallbackInfo& info) {
  long n = sysconf(_SC_NPROCESSORS_ONLN);
  if (n < 1) {
    n = 1;
  }
  return Napi::Number::New(info.Env(), static_cast<int32_t>(n));
}

MemoryLevel NodeDatabase::ParseBufferStrategy(const std::string& level) {
  if (level == "InMemory" || level == "inmemory" || level == "in_memory" ||
      level == "M_FULL") {
    return MemoryLevel::kInMemory;
  } else if (level == "SyncToFile" || level == "synctofile" ||
             level == "sync_to_file" || level == "M_LAZY") {
    return MemoryLevel::kSyncToFile;
  } else if (level == "HugePagePreferred" || level == "hugepagepreferred" ||
             level == "huge_page_preferred" || level == "M_HUGE") {
    return MemoryLevel::kHugePagePreferred;
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid memory level: " + level);
  }
}

}  // namespace neug
