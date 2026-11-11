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

#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>

#include "neug/config.h"
#include "neug/utils/exception/exception.h"

namespace neug {

namespace {

struct DatabaseRegistry {
  std::mutex mutex;
  std::unordered_map<std::string, std::weak_ptr<DatabaseState>> databases;
};

DatabaseRegistry& GetDatabaseRegistry() {
  static DatabaseRegistry registry;
  return registry;
}

}  // namespace

DatabaseState::DatabaseState(const NeugDBConfig& config,
                             const std::string& mode)
    : database(std::make_unique<NeugDB>()), options(config), mode(mode) {
  database->Open(config);
}

DatabaseState::~DatabaseState() {
  try {
    Close();
  } catch (const std::exception& e) {
    LOG(ERROR) << "Failed to close shared database: " << e.what();
  }
  RevokeHandle();
}

std::shared_ptr<DatabaseState> DatabaseState::Attach(
    const std::string& handle) {
  std::shared_ptr<DatabaseState> state;
  {
    auto& registry = GetDatabaseRegistry();
    std::lock_guard<std::mutex> lock(registry.mutex);
    auto it = registry.databases.find(handle);
    if (it != registry.databases.end()) {
      state = it->second.lock();
    }
  }
  if (state && !state->database->IsClosed()) {
    return state;
  }
  THROW_INVALID_ARGUMENT_EXCEPTION("Invalid or closed database handle");
}

std::string DatabaseState::Share() {
  auto& registry = GetDatabaseRegistry();
  std::lock_guard<std::mutex> lock(registry.mutex);
  if (database->IsClosed()) {
    THROW_RUNTIME_ERROR("Database is closed.");
  }
  if (handle_.empty()) {
    std::random_device random;
    do {
      std::ostringstream token;
      token << std::hex;
      for (int i = 0; i < 4; ++i) {
        token << random() << '-';
      }
      handle_ = token.str();
    } while (registry.databases.count(handle_));
    registry.databases.emplace(handle_, shared_from_this());
  }
  return handle_;
}

void DatabaseState::RevokeHandle() {
  auto& registry = GetDatabaseRegistry();
  std::lock_guard<std::mutex> lock(registry.mutex);
  if (!handle_.empty()) {
    registry.databases.erase(handle_);
    handle_.clear();
  }
}

void DatabaseState::Close() {
  // Preserve the core's retry behavior if checkpoint preparation fails.
  try {
    database->Close();
  } catch (...) {
    if (database->IsClosed()) {
      RevokeHandle();
    }
    throw;
  }
  RevokeHandle();
}

Napi::Object NodeDatabase::Init(Napi::Env env, Napi::Object exports) {
  Napi::Function func =
      DefineClass(env, "NodeDatabase",
                  {
                      InstanceMethod("connect", &NodeDatabase::Connect),
                      InstanceMethod("close", &NodeDatabase::Close),
                      InstanceMethod("share", &NodeDatabase::Share),
                      InstanceMethod("getOptions", &NodeDatabase::GetOptions),
                      StaticMethod("cpuCount", &NodeDatabase::GetCpuCount),
                  });
  exports.Set("NodeDatabase", func);
  return exports;
}

NodeDatabase::NodeDatabase(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<NodeDatabase>(info) {
  Napi::Env env = info.Env();

  if (info.Length() == 1 && info[0].IsString()) {
    try {
      state_ = DatabaseState::Attach(info[0].As<Napi::String>().Utf8Value());
    } catch (const std::exception& e) {
      Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    }
    return;
  }

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

  std::string wal_sync_mode = "STANDARD";
  if (opts.Has("walSyncMode")) {
    if (!opts.Get("walSyncMode").IsString()) {
      Napi::TypeError::New(env, "walSyncMode must be a string")
          .ThrowAsJavaScriptException();
      return;
    }
    wal_sync_mode = opts.Get("walSyncMode").As<Napi::String>().Utf8Value();
  }

  bool auto_load_extension = false;
  if (opts.Has("autoLoadExtension")) {
    if (!opts.Get("autoLoadExtension").IsBoolean()) {
      Napi::TypeError::New(env, "autoLoadExtension must be a boolean")
          .ThrowAsJavaScriptException();
      return;
    }
    auto_load_extension =
        opts.Get("autoLoadExtension").As<Napi::Boolean>().Value();
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

  NeugDBConfig config(database_path, max_thread_num);
  config.auto_load_extension = auto_load_extension;
  config.mode = mode_;
  config.planner_kind = planner;
  config.checkpoint_on_close = checkpoint_on_close;
  try {
    config.wal_sync_mode = ParseWalSyncMode(wal_sync_mode);
    config.memory_level = ParseBufferStrategy(buffer_strategy);
    state_ = std::make_shared<DatabaseState>(config, mode);
    owner_ = true;
  } catch (const neug::exception::Exception& e) {
    Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    return;
  } catch (const std::exception& e) {
    Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    return;
  }
}

NodeDatabase::~NodeDatabase() = default;

Napi::Value NodeDatabase::Connect(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!state_) {
    Napi::Error::New(env, "Database is not initialized.")
        .ThrowAsJavaScriptException();
    return env.Null();
  }
  try {
    auto conn = state_->database->Connect();
    return NodeConnection::NewInstance(env, state_, std::move(conn));
  } catch (const neug::exception::Exception& e) {
    Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    return env.Null();
  } catch (const std::exception& e) {
    Napi::Error::New(env, e.what()).ThrowAsJavaScriptException();
    return env.Null();
  }
}

Napi::Value NodeDatabase::Close(const Napi::CallbackInfo& info) {
  try {
    if (state_ && owner_) {
      state_->Close();
    }
    state_.reset();
  } catch (const std::exception& e) {
    Napi::Error::New(info.Env(), e.what()).ThrowAsJavaScriptException();
  }
  return info.Env().Undefined();
}

Napi::Value NodeDatabase::Share(const Napi::CallbackInfo& info) {
  try {
    if (!state_) {
      THROW_RUNTIME_ERROR("Database is closed.");
    }
    return Napi::String::New(info.Env(), state_->Share());
  } catch (const std::exception& e) {
    Napi::Error::New(info.Env(), e.what()).ThrowAsJavaScriptException();
    return info.Env().Undefined();
  }
}

Napi::Value NodeDatabase::GetOptions(const Napi::CallbackInfo& info) {
  if (!state_) {
    Napi::Error::New(info.Env(), "Database is closed.")
        .ThrowAsJavaScriptException();
    return info.Env().Undefined();
  }
  auto options = Napi::Object::New(info.Env());
  options.Set("databasePath", state_->options.data_dir);
  options.Set("mode", state_->mode);
  options.Set("maxThreadNum", state_->options.max_thread_num);
  options.Set("walSyncMode", state_->options.wal_sync_mode == WalSyncMode::FULL
                                 ? "FULL"
                                 : "STANDARD");
  return options;
}

Napi::Value NodeDatabase::GetCpuCount(const Napi::CallbackInfo& info) {
  unsigned int n = std::thread::hardware_concurrency();
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
