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
#include "neug/utils/io/file/file_utils.h"

#include <charconv>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <glog/logging.h>

namespace neug {

namespace {
constexpr std::string_view kCheckpointPrefix = "checkpoint-";
constexpr std::string_view kCheckpointPointer = "CHECKPOINT";
constexpr std::string_view kCheckpointGcDir = ".checkpoint-gc";
constexpr std::string_view kNextSuffix = ".next";

struct CheckpointDirs {
  std::vector<std::filesystem::path> published;
  std::vector<std::filesystem::path> staging;
};

std::string checkpoint_name(int32_t id) {
  return std::string(kCheckpointPrefix) + std::to_string(id);
}

std::string staging_checkpoint_name(int32_t id) {
  return checkpoint_name(id) + std::string(kNextSuffix);
}

std::string trim_ascii_whitespace(std::string value) {
  const auto first = value.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) {
    return "";
  }
  const auto last = value.find_last_not_of(" \t\r\n");
  return value.substr(first, last - first + 1);
}

void remove_checkpoint_dir_best_effort(const std::string& path,
                                       std::string_view context) {
  if (path.empty()) {
    return;
  }
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    LOG(WARNING) << context << ": failed to remove checkpoint " << path << ": "
                 << ec.message();
  }
}

bool parse_checkpoint_name(const std::string& name, int32_t& id) {
  if (name.size() <= kCheckpointPrefix.size() ||
      std::string_view(name).substr(0, kCheckpointPrefix.size()) !=
          kCheckpointPrefix) {
    return false;
  }
  const char* first = name.data() + kCheckpointPrefix.size();
  const char* last = name.data() + name.size();
  auto [ptr, ec] = std::from_chars(first, last, id);
  return ec == std::errc{} && ptr == last && id >= 0;
}

bool parse_checkpoint_path(const std::filesystem::path& path, int32_t& id) {
  if (!std::filesystem::is_directory(path)) {
    return false;
  }
  return parse_checkpoint_name(path.filename().string(), id);
}

bool parse_staging_checkpoint_path(const std::filesystem::path& path,
                                   int32_t& id) {
  if (!std::filesystem::is_directory(path)) {
    return false;
  }
  std::string name = path.filename().string();
  if (name.size() <= kNextSuffix.size() ||
      std::string_view(name).substr(name.size() - kNextSuffix.size()) !=
          kNextSuffix) {
    return false;
  }
  name.resize(name.size() - kNextSuffix.size());
  return parse_checkpoint_name(name, id);
}

CheckpointDirs discover_checkpoint_dirs(const std::string& db_dir) {
  CheckpointDirs dirs;
  for (const auto& entry : std::filesystem::directory_iterator(db_dir)) {
    if (!entry.is_directory()) {
      continue;
    }
    int32_t id;
    if (parse_staging_checkpoint_path(entry.path(), id)) {
      dirs.staging.push_back(entry.path());
    } else if (parse_checkpoint_path(entry.path(), id)) {
      dirs.published.push_back(entry.path());
    }
  }
  return dirs;
}

void cleanup_open_trash(const std::string& db_dir) {
  std::error_code ec;
  auto gc_dir = std::filesystem::path(db_dir) / std::string(kCheckpointGcDir);
  if (std::filesystem::exists(gc_dir, ec)) {
    std::filesystem::remove_all(gc_dir, ec);
    if (ec) {
      LOG(WARNING) << "CheckpointManager::Open: failed to clean GC trash "
                   << gc_dir << ": " << ec.message();
      ec.clear();
    }
  }

  auto pointer_tmp_path = std::filesystem::path(db_dir) /
                          (std::string(kCheckpointPointer) + ".tmp");
  std::filesystem::remove(pointer_tmp_path, ec);
}

std::shared_ptr<Checkpoint> open_checkpoint_checked(
    const std::filesystem::path& path, int32_t id) {
  auto checkpoint = Checkpoint::Open(path.string(), id);
  if (!checkpoint->GetMeta().has_schema()) {
    THROW_CHECKPOINT_EXCEPTION("Checkpoint " + path.string() +
                               " is incomplete");
  }
  return checkpoint;
}

void write_checkpoint_pointer(const std::string& db_dir,
                              const std::string& checkpoint_dir_name) {
  const auto pointer_path =
      std::filesystem::path(db_dir) / std::string(kCheckpointPointer);
  file_utils::AtomicFileWriter writer(pointer_path.string());
  writer.stream() << checkpoint_dir_name << "\n";
  writer.Commit();
}

std::shared_ptr<Checkpoint> load_current_from_pointer(
    const std::string& db_dir, const std::filesystem::path& pointer_path,
    int32_t& current_id) {
  std::ifstream input(pointer_path);
  if (!input.is_open()) {
    THROW_IO_EXCEPTION("CheckpointManager::Open: cannot open " +
                       pointer_path.string());
  }

  std::string checkpoint_dir_name;
  std::getline(input, checkpoint_dir_name);
  checkpoint_dir_name = trim_ascii_whitespace(checkpoint_dir_name);
  if (checkpoint_dir_name.empty() ||
      checkpoint_dir_name.find('/') != std::string::npos ||
      checkpoint_dir_name.find('\\') != std::string::npos) {
    THROW_CHECKPOINT_EXCEPTION("Invalid CHECKPOINT pointer: " +
                               checkpoint_dir_name);
  }

  int32_t id;
  if (!parse_checkpoint_name(checkpoint_dir_name, id)) {
    THROW_CHECKPOINT_EXCEPTION("Invalid CHECKPOINT target: " +
                               checkpoint_dir_name);
  }

  auto checkpoint_path = std::filesystem::path(db_dir) / checkpoint_dir_name;
  if (!std::filesystem::is_directory(checkpoint_path)) {
    THROW_CHECKPOINT_EXCEPTION("CHECKPOINT target is missing: " +
                               checkpoint_path.string());
  }

  current_id = id;
  return open_checkpoint_checked(checkpoint_path, id);
}

std::shared_ptr<Checkpoint> load_newest_checkpoint(
    const std::string& db_dir,
    const std::vector<std::filesystem::path>& checkpoint_dirs,
    int32_t& current_id) {
  std::shared_ptr<Checkpoint> newest;
  int32_t newest_id = kInvalidCheckpointId;
  for (const auto& path : checkpoint_dirs) {
    int32_t id;
    if (!parse_checkpoint_path(path, id)) {
      continue;
    }
    try {
      auto checkpoint = open_checkpoint_checked(path, id);
      if (id > newest_id) {
        newest_id = id;
        newest = std::move(checkpoint);
      }
    } catch (const std::exception& e) {
      LOG(WARNING) << "CheckpointManager::Open: removing invalid legacy "
                      "checkpoint "
                   << path << ": " << e.what();
      remove_checkpoint_dir_best_effort(path.string(),
                                        "CheckpointManager::Open");
    }
  }

  if (newest) {
    write_checkpoint_pointer(db_dir, checkpoint_name(newest_id));
    current_id = newest_id;
  }
  return newest;
}

void gc_non_current_checkpoints(
    const std::vector<std::filesystem::path>& checkpoint_dirs,
    int32_t current_id) {
  for (const auto& path : checkpoint_dirs) {
    int32_t id;
    if (parse_checkpoint_path(path, id) && id != current_id) {
      LOG(WARNING) << "CheckpointManager: cleaning up old checkpoint " << id
                   << " at " << path
                   << " (only the current checkpoint is retained)";
      remove_checkpoint_dir_best_effort(path.string(),
                                        "CheckpointManager::Open");
    }
  }
}

bool find_staging_dir(const std::string& db_dir, std::filesystem::path* path,
                      int32_t* id) {
  if (db_dir.empty() || !std::filesystem::is_directory(db_dir)) {
    return false;
  }
  for (const auto& entry : std::filesystem::directory_iterator(db_dir)) {
    if (!entry.is_directory()) {
      continue;
    }
    int32_t parsed_id;
    if (parse_staging_checkpoint_path(entry.path(), parsed_id)) {
      if (path != nullptr) {
        *path = entry.path();
      }
      if (id != nullptr) {
        *id = parsed_id;
      }
      return true;
    }
  }
  return false;
}

void restore_staging_name_best_effort(const std::filesystem::path& final_path,
                                      const std::filesystem::path& staging_path,
                                      std::string_view context) {
  std::error_code ec;
  std::filesystem::rename(final_path, staging_path, ec);
  if (!ec) {
    return;
  }

  ec.clear();
  std::filesystem::remove_all(final_path, ec);
  if (ec) {
    LOG(WARNING) << context << ": failed to clean " << final_path << ": "
                 << ec.message();
  }
}
}  // namespace

CheckpointManager::CheckpointManager() {}

CheckpointManager::~CheckpointManager() {}

void CheckpointManager::Open(const std::string& db_dir) {
  if (db_dir.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("db_dir cannot be empty");
  }

  std::lock_guard<std::mutex> lock(mutex_);
  try {
    if (!db_dir_.empty()) {
      LOG(WARNING)
          << "CheckpointManager::Open called on already-open workspace: "
          << db_dir_ << ", reopening to: " << db_dir;
    }

    db_dir_ = std::filesystem::absolute(db_dir).string();
    current_checkpoint_.reset();
    staging_checkpoint_.reset();
    current_id_ = kInvalidCheckpointId;

    if (!std::filesystem::is_directory(db_dir_)) {
      std::filesystem::create_directories(db_dir_);
    }

    cleanup_open_trash(db_dir_);
    auto pointer_path =
        std::filesystem::path(db_dir_) / std::string(kCheckpointPointer);
    auto dirs = discover_checkpoint_dirs(db_dir_);
    for (const auto& staging : dirs.staging) {
      remove_checkpoint_dir_best_effort(staging.string(),
                                        "CheckpointManager::Open");
    }

    if (std::filesystem::exists(pointer_path)) {
      current_checkpoint_ =
          load_current_from_pointer(db_dir_, pointer_path, current_id_);
    } else {
      current_checkpoint_ =
          load_newest_checkpoint(db_dir_, dirs.published, current_id_);
    }
    gc_non_current_checkpoints(dirs.published, current_id_);
  } catch (const std::filesystem::filesystem_error& e) {
    db_dir_.clear();
    current_checkpoint_.reset();
    staging_checkpoint_.reset();
    current_id_ = kInvalidCheckpointId;
    if (e.code() == std::errc::permission_denied) {
      THROW_PERMISSION_DENIED("CheckpointManager::Open: cannot access " +
                              std::string(db_dir) + ": " + e.what());
    }
    THROW_IO_EXCEPTION("CheckpointManager::Open: failed to open " +
                       std::string(db_dir) + ": " + e.what());
  }
}

void CheckpointManager::Close() {
  std::lock_guard<std::mutex> lock(mutex_);
  db_dir_.clear();
  current_checkpoint_.reset();
  staging_checkpoint_.reset();
  current_id_ = kInvalidCheckpointId;
}

bool CheckpointManager::HasCurrentCheckpoint() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return current_checkpoint_ != nullptr;
}

int32_t CheckpointManager::CurrentCheckpointId() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return current_id_;
}

int32_t CheckpointManager::CreateStagingCheckpoint() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (staging_checkpoint_ != nullptr) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::CreateStagingCheckpoint: active staging "
        "checkpoint already exists: " +
        std::to_string(staging_checkpoint_->id()));
  }

  int32_t active_staging_id = kInvalidCheckpointId;
  if (find_staging_dir(db_dir_, nullptr, &active_staging_id)) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::CreateStagingCheckpoint: active staging "
        "checkpoint directory already exists: " +
        std::to_string(active_staging_id));
  }

  int32_t id = current_id_ == kInvalidCheckpointId ? 0 : current_id_ + 1;
  auto path =
      (std::filesystem::path(db_dir_) / staging_checkpoint_name(id)).string();

  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    THROW_IO_EXCEPTION(
        "CheckpointManager::CreateStagingCheckpoint: failed to clean " + path +
        ": " + ec.message());
  }

  std::filesystem::create_directories(path);
  CheckpointManifest::GenerateEmptyMeta(path + "/meta");
  staging_checkpoint_ = Checkpoint::Open(path, id);
  return id;
}

std::shared_ptr<Checkpoint> CheckpointManager::PublishStagingCheckpoint(
    int32_t id) {
  std::string obsolete_checkpoint_path;
  auto checkpoint = PublishStagingCheckpoint(id, &obsolete_checkpoint_path);
  remove_checkpoint_dir_best_effort(
      obsolete_checkpoint_path, "CheckpointManager::PublishStagingCheckpoint");
  return checkpoint;
}

std::shared_ptr<Checkpoint> CheckpointManager::PublishStagingCheckpoint(
    int32_t id, std::string* obsolete_checkpoint_path) {
  if (obsolete_checkpoint_path != nullptr) {
    obsolete_checkpoint_path->clear();
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (staging_checkpoint_ == nullptr ||
      static_cast<int32_t>(staging_checkpoint_->id()) != id) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::PublishStagingCheckpoint: unknown staging "
        "checkpoint id " +
        std::to_string(id));
  }
  if (current_id_ != kInvalidCheckpointId && id <= current_id_) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::PublishStagingCheckpoint: refusing to publish "
        "non-increasing checkpoint id " +
        std::to_string(id) + " over current id " + std::to_string(current_id_));
  }

  auto staging_path = std::filesystem::path(staging_checkpoint_->path());
  int32_t parsed_id;
  if (staging_path.filename().string() != staging_checkpoint_name(id) ||
      !parse_staging_checkpoint_path(staging_path, parsed_id) ||
      parsed_id != id) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::PublishStagingCheckpoint: invalid staging "
        "checkpoint path: " +
        staging_path.string());
  }

  auto dirs = discover_checkpoint_dirs(db_dir_);
  int32_t disk_staging_id = kInvalidCheckpointId;
  if (dirs.staging.size() != 1 ||
      !parse_staging_checkpoint_path(dirs.staging.front(), disk_staging_id) ||
      disk_staging_id != id ||
      dirs.staging.front().filename() != staging_path.filename()) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::PublishStagingCheckpoint: staging checkpoint "
        "directory is missing or ambiguous: " +
        staging_path.string());
  }
  if (!staging_checkpoint_->GetMeta().has_schema()) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::PublishStagingCheckpoint: checkpoint has no "
        "schema: " +
        staging_path.string());
  }

  auto final_name = checkpoint_name(id);
  auto final_path = std::filesystem::path(db_dir_) / final_name;

  std::error_code ec;
  if (std::filesystem::exists(final_path, ec)) {
    std::filesystem::remove_all(final_path, ec);
    if (ec) {
      THROW_IO_EXCEPTION(
          "CheckpointManager::PublishStagingCheckpoint: failed to remove "
          "stale " +
          final_path.string() + ": " + ec.message());
    }
  }

  ec.clear();
  std::filesystem::rename(staging_path, final_path, ec);
  if (ec) {
    THROW_IO_EXCEPTION("CheckpointManager::PublishStagingCheckpoint: rename " +
                       staging_path.string() + " -> " + final_path.string() +
                       " failed: " + ec.message());
  }
  if (!file_utils::fsync_directory(db_dir_)) {
    LOG(WARNING)
        << "CheckpointManager::PublishStagingCheckpoint: failed to fsync "
        << db_dir_;
  }

  std::shared_ptr<Checkpoint> final_checkpoint;
  try {
    final_checkpoint = Checkpoint::Open(final_path.string(), id);
  } catch (...) {
    restore_staging_name_best_effort(
        final_path, staging_path,
        "CheckpointManager::PublishStagingCheckpoint");
    throw;
  }

  try {
    write_checkpoint_pointer(db_dir_, final_name);
  } catch (...) {
    restore_staging_name_best_effort(
        final_path, staging_path,
        "CheckpointManager::PublishStagingCheckpoint");
    throw;
  }

  if (current_checkpoint_ != nullptr && current_id_ != id) {
    if (obsolete_checkpoint_path != nullptr) {
      *obsolete_checkpoint_path = current_checkpoint_->path();
    }
  }
  current_checkpoint_ = std::move(final_checkpoint);
  current_id_ = id;
  staging_checkpoint_.reset();
  return current_checkpoint_;
}

void CheckpointManager::RestoreCurrentCheckpoint(
    std::shared_ptr<Checkpoint> checkpoint) {
  if (checkpoint == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "CheckpointManager::RestoreCurrentCheckpoint: checkpoint is null");
  }

  std::lock_guard<std::mutex> lock(mutex_);
  const int32_t id = static_cast<int32_t>(checkpoint->id());
  const auto checkpoint_path = std::filesystem::path(checkpoint->path());
  if (!std::filesystem::is_directory(checkpoint_path)) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::RestoreCurrentCheckpoint: checkpoint is missing: " +
        checkpoint->path());
  }

  int32_t parsed_id;
  if (!parse_checkpoint_path(checkpoint_path, parsed_id) || parsed_id != id) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::RestoreCurrentCheckpoint: invalid checkpoint "
        "path: " +
        checkpoint->path());
  }

  write_checkpoint_pointer(db_dir_, checkpoint_path.filename().string());
  current_checkpoint_ = std::move(checkpoint);
  current_id_ = id;
  staging_checkpoint_.reset();
}

void CheckpointManager::DiscardStagingCheckpoint(int32_t id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (staging_checkpoint_ == nullptr) {
    return;
  }
  if (static_cast<int32_t>(staging_checkpoint_->id()) != id) {
    if (current_checkpoint_ != nullptr &&
        static_cast<int32_t>(current_checkpoint_->id()) == id) {
      THROW_CHECKPOINT_EXCEPTION(
          "CheckpointManager::DiscardStagingCheckpoint: checkpoint is not "
          "staging: " +
          current_checkpoint_->path());
    }
    return;
  }

  auto path = staging_checkpoint_->path();
  int32_t staging_id;
  if (!parse_staging_checkpoint_path(path, staging_id) || staging_id != id) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::DiscardStagingCheckpoint: checkpoint is not "
        "staging: " +
        path);
  }

  staging_checkpoint_.reset();
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    LOG(WARNING)
        << "CheckpointManager::DiscardStagingCheckpoint: failed to remove "
        << path << ": " << ec.message();
  }
}

std::shared_ptr<Checkpoint> CheckpointManager::GetCheckpoint(int32_t id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (staging_checkpoint_ != nullptr &&
      static_cast<int32_t>(staging_checkpoint_->id()) == id) {
    return staging_checkpoint_;
  }
  if (current_checkpoint_ != nullptr &&
      static_cast<int32_t>(current_checkpoint_->id()) == id) {
    return current_checkpoint_;
  }
  THROW_CHECKPOINT_EXCEPTION(
      "CheckpointManager::GetCheckpoint: unknown "
      "checkpoint id " +
      std::to_string(id));
}

std::string CheckpointManager::db_dir() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return db_dir_;
}

}  // namespace neug
