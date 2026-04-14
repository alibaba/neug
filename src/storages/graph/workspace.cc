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

#include "neug/storages/workspace.h"
#include "neug/storages/snapshot_meta.h"
#include "neug/utils/exception/exception.h"

#include <charconv>
#include <filesystem>
#include <string>
#include <string_view>

#include <glog/logging.h>

namespace neug {

namespace {
constexpr std::string_view kCheckpointPrefix = "checkpoint-";
}

static bool parse_checkpoint_path(const std::string& path, int32_t& id) {
  if (!std::filesystem::is_directory(path)) {
    return false;
  }
  std::string name = std::filesystem::path(path).filename().string();
  if (name.size() <= kCheckpointPrefix.size() ||
      std::string_view(name).substr(0, kCheckpointPrefix.size()) !=
          kCheckpointPrefix) {
    return false;
  }
  const char* first = name.data() + kCheckpointPrefix.size();
  const char* last = name.data() + name.size();
  auto [ptr, ec] = std::from_chars(first, last, id);
  if (ec != std::errc{} || ptr != last || id < 0) {
    return false;
  }
  return true;
}

Workspace::Workspace() {}

Workspace::~Workspace() {}

void Workspace::Open(const std::string& db_dir) {
  if (db_dir.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("db_dir cannot be empty");
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (!db_dir_.empty()) {
    LOG(WARNING) << "Workspace::Open called on already-open workspace: "
                 << db_dir_ << ", reopening to: " << db_dir;
    db_dir_.clear();
    checkpoints_.clear();
  }
  db_dir_ = std::filesystem::absolute(db_dir).string();
  if (!std::filesystem::is_directory(db_dir_)) {
    std::filesystem::create_directories(db_dir_);
  }
  try {
    for (const auto& entry : std::filesystem::directory_iterator(db_dir_)) {
      if (entry.is_directory()) {
        int32_t id;
        if (parse_checkpoint_path(entry.path().string(), id)) {
          checkpoints_[id] =
              std::make_unique<Checkpoint>(entry.path().string(), id);
        }
      }
    }
  } catch (const std::filesystem::filesystem_error& e) {
    db_dir_.clear();
    checkpoints_.clear();
    THROW_IO_EXCEPTION("Workspace::Open: failed to enumerate " +
                       std::string(db_dir) + ": " + e.what());
  }
}

void Workspace::Close() {
  std::lock_guard<std::mutex> lock(mutex_);
  db_dir_.clear();
  checkpoints_.clear();
}

size_t Workspace::NumCheckpoints() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return checkpoints_.size();
}

int32_t Workspace::HeadId() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (checkpoints_.empty()) {
    return kInvalidCheckpointId;
  }
  return checkpoints_.rbegin()->first;
}

int32_t Workspace::CreateCheckpoint() {
  std::lock_guard<std::mutex> lock(mutex_);
  int32_t id = checkpoints_.empty() ? 0 : checkpoints_.rbegin()->first + 1;
  auto path = db_dir_ + "/checkpoint-" + std::to_string(id);

  std::filesystem::create_directories(path);
  SnapshotMeta::GenerateEmptyMeta(path + "/meta");
  checkpoints_[id] = std::make_unique<Checkpoint>(path, id);
  return id;
}

const Checkpoint& Workspace::GetCheckpoint(int32_t id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto& ptr = checkpoints_.at(id);
  assert(ptr != nullptr);
  return *ptr;
}

Checkpoint& Workspace::GetCheckpoint(int32_t id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto& ptr = checkpoints_.at(id);
  assert(ptr != nullptr);
  return *ptr;
}

}  // namespace neug
