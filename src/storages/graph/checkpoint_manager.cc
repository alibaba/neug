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

#include "neug/storages/checkpoint_manager.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/utils/exception/exception.h"

#include <charconv>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include <glog/logging.h>

namespace neug {

namespace {
constexpr std::string_view kCheckpointPrefix = "checkpoint-";
constexpr std::string_view kCheckpointGcDir = ".checkpoint-gc";
constexpr std::string_view kDeletingSuffix = ".deleting";
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

CheckpointManager::CheckpointManager() {}

CheckpointManager::~CheckpointManager() {}

void CheckpointManager::Open(const std::string& db_dir) {
  if (db_dir.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("db_dir cannot be empty");
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (!db_dir_.empty()) {
    LOG(WARNING) << "CheckpointManager::Open called on already-open workspace: "
                 << db_dir_ << ", reopening to: " << db_dir;
    db_dir_.clear();
    checkpoints_.clear();
  }
  db_dir_ = std::filesystem::absolute(db_dir).string();
  if (!std::filesystem::is_directory(db_dir_)) {
    std::filesystem::create_directories(db_dir_);
  }
  std::error_code gc_ec;
  auto gc_dir = std::filesystem::path(db_dir_) / kCheckpointGcDir;
  if (std::filesystem::exists(gc_dir, gc_ec)) {
    std::filesystem::remove_all(gc_dir, gc_ec);
    if (gc_ec) {
      LOG(WARNING) << "CheckpointManager::Open: failed to clean GC trash "
                   << gc_dir << ": " << gc_ec.message();
      gc_ec.clear();
    }
  }
  try {
    for (const auto& entry : std::filesystem::directory_iterator(db_dir_)) {
      if (entry.is_directory()) {
        int32_t id;
        if (parse_checkpoint_path(entry.path().string(), id)) {
          checkpoints_[id] = Checkpoint::Open(entry.path().string(), id);
        }
      }
    }
  } catch (const std::filesystem::filesystem_error& e) {
    db_dir_.clear();
    checkpoints_.clear();
    if (e.code() == std::errc::permission_denied) {
      THROW_PERMISSION_DENIED("CheckpointManager::Open: cannot access " +
                              std::string(db_dir) + ": " + e.what());
    }
    THROW_IO_EXCEPTION("CheckpointManager::Open: failed to enumerate " +
                       std::string(db_dir) + ": " + e.what());
  }

  // Discard incomplete checkpoints left by a previous crash.
  // GenerateEmptyMeta() creates a stub with no schema, no modules, no
  // scalars.  A fully committed checkpoint (via Save) always writes a
  // schema — even if the graph has no tables (e.g. after DROP TABLE).
  // We use has_schema() to distinguish: a stub checkpoint lacks a schema,
  // while a legitimate empty-graph checkpoint has one.
  while (checkpoints_.size() > 1) {
    auto it = std::prev(checkpoints_.end());
    if (!it->second->GetMeta().has_schema()) {
      LOG(WARNING) << "CheckpointManager::Open: removing incomplete checkpoint-"
                   << it->first << " at " << it->second->path();
      std::error_code ec;
      std::filesystem::remove_all(it->second->path(), ec);
      if (ec) {
        LOG(WARNING) << "CheckpointManager::Open: failed to remove directory: "
                     << ec.message();
      }
      checkpoints_.erase(it);
    } else {
      break;
    }
  }
}

void CheckpointManager::Close() {
  std::lock_guard<std::mutex> lock(mutex_);
  db_dir_.clear();
  checkpoints_.clear();
}

size_t CheckpointManager::NumCheckpoints() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return checkpoints_.size();
}

int32_t CheckpointManager::HeadId() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (checkpoints_.empty()) {
    return kInvalidCheckpointId;
  }
  return checkpoints_.rbegin()->first;
}

int32_t CheckpointManager::CreateCheckpoint() {
  std::lock_guard<std::mutex> lock(mutex_);
  int32_t id = checkpoints_.empty() ? 0 : checkpoints_.rbegin()->first + 1;
  auto path = db_dir_ + "/checkpoint-" + std::to_string(id);

  std::filesystem::create_directories(path);
  CheckpointManifest::GenerateEmptyMeta(path + "/meta");
  checkpoints_[id] = Checkpoint::Open(path, id);
  return id;
}

void CheckpointManager::RemoveCheckpoint(int32_t id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = checkpoints_.find(id);
  if (it == checkpoints_.end()) {
    return;
  }
  auto path = it->second->path();
  checkpoints_.erase(it);
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    LOG(WARNING) << "CheckpointManager::RemoveCheckpoint: failed to remove "
                 << path << ": " << ec.message();
  }
}

std::shared_ptr<Checkpoint> CheckpointManager::GetCheckpoint(int32_t id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto& ptr = checkpoints_.at(id);
  assert(ptr != nullptr);
  return ptr;
}

void CheckpointManager::PruneOldCheckpoints(
    size_t max_keep, const std::unordered_set<int32_t>& protected_ids) {
  if (max_keep == 0) {
    return;
  }

  std::vector<std::filesystem::path> trash_paths;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (checkpoints_.empty()) {
      return;
    }

    struct Candidate {
      int32_t id;
      std::filesystem::path path;
    };
    std::vector<Candidate> candidates;
    size_t kept_valid = 0;
    const int32_t head_id = checkpoints_.rbegin()->first;

    for (auto it = checkpoints_.rbegin(); it != checkpoints_.rend(); ++it) {
      auto& checkpoint = it->second;
      if (!checkpoint || !checkpoint->GetMeta().has_schema()) {
        continue;
      }
      if (kept_valid < max_keep) {
        ++kept_valid;
        continue;
      }
      if (it->first == head_id || protected_ids.contains(it->first)) {
        continue;
      }
      candidates.push_back({it->first, checkpoint->path()});
    }

    if (candidates.empty()) {
      return;
    }

    auto gc_dir = std::filesystem::path(db_dir_) / kCheckpointGcDir;
    std::error_code ec;
    std::filesystem::create_directories(gc_dir, ec);
    if (ec) {
      LOG(WARNING) << "CheckpointManager::PruneOldCheckpoints: failed to "
                      "create GC trash dir "
                   << gc_dir << ": " << ec.message();
      return;
    }

    for (const auto& candidate : candidates) {
      auto trash_path =
          gc_dir / (std::string(kCheckpointPrefix) +
                    std::to_string(candidate.id) + std::string(kDeletingSuffix));
      ec.clear();
      std::filesystem::rename(candidate.path, trash_path, ec);
      if (ec) {
        LOG(WARNING) << "CheckpointManager::PruneOldCheckpoints: failed to "
                        "rename "
                     << candidate.path << " -> " << trash_path << ": "
                     << ec.message();
        continue;
      }
      checkpoints_.erase(candidate.id);
      trash_paths.push_back(std::move(trash_path));
    }
  }

  for (const auto& path : trash_paths) {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    if (ec) {
      LOG(WARNING) << "CheckpointManager::PruneOldCheckpoints: failed to "
                      "remove GC trash "
                   << path << ": " << ec.message();
    }
  }
}

}  // namespace neug
