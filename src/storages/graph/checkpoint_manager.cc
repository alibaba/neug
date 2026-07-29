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
#include "neug/storages/module/module_factory.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/file/file_utils.h"

#include <algorithm>
#include <charconv>
#include <exception>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <glog/logging.h>

namespace neug {

namespace {
constexpr std::string_view kCheckpointPrefix = "checkpoint-";
constexpr std::string_view kNextSuffix = ".next";
constexpr int32_t kInvalidCheckpointGeneration = -1;

struct ParsedCheckpointDir {
  int32_t id;
  std::filesystem::path path;
};

struct CheckpointOpenResult {
  std::shared_ptr<Checkpoint> current_checkpoint;
  int32_t current_generation = kInvalidCheckpointGeneration;
};

std::string checkpoint_name(int32_t id) {
  return std::string(kCheckpointPrefix) + std::to_string(id);
}

std::string staging_checkpoint_name(int32_t id) {
  return checkpoint_name(id) + std::string(kNextSuffix);
}

void remove_checkpoint_dir_best_effort(const std::filesystem::path& path,
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

std::vector<ParsedCheckpointDir> discover_published_checkpoints(
    const std::string& db_dir) {
  std::vector<ParsedCheckpointDir> dirs;
  for (const auto& entry : std::filesystem::directory_iterator(db_dir)) {
    int32_t id;
    if (entry.is_directory() && parse_checkpoint_path(entry.path(), id)) {
      dirs.push_back({id, entry.path()});
    }
  }
  std::sort(dirs.begin(), dirs.end(),
            [](const auto& lhs, const auto& rhs) { return lhs.id > rhs.id; });
  return dirs;
}

void cleanup_staging_checkpoints(const std::string& db_dir,
                                 std::string_view context) {
  if (db_dir.empty() || !std::filesystem::is_directory(db_dir)) {
    return;
  }
  for (const auto& entry : std::filesystem::directory_iterator(db_dir)) {
    int32_t id;
    if (entry.is_directory() &&
        parse_staging_checkpoint_path(entry.path(), id)) {
      remove_checkpoint_dir_best_effort(entry.path(), context);
    }
  }
}

std::shared_ptr<Checkpoint> open_checkpoint_checked(
    const std::filesystem::path& path, int32_t id) {
  auto checkpoint = Checkpoint::Open(path.string(), id);
  if (!checkpoint->GetMeta().has_schema()) {
    THROW_CHECKPOINT_EXCEPTION("Checkpoint " + path.string() +
                               " is incomplete");
  }
  for (const auto& [module_name, desc] : checkpoint->GetMeta().modules()) {
    if (!desc.module_type.empty() &&
        ModuleFactory::instance().Create(desc.module_type) == nullptr) {
      THROW_CHECKPOINT_EXCEPTION(
          "Checkpoint " + path.string() + " references unknown module type " +
          desc.module_type + " from module " + module_name);
    }
    for (const auto& [path_name, file_path] : desc.paths()) {
      if (file_path.empty()) {
        continue;
      }
      std::error_code ec;
      if (!std::filesystem::exists(file_path, ec) || ec) {
        THROW_CHECKPOINT_EXCEPTION(
            "Checkpoint " + path.string() + " references missing file " +
            file_path + " from module " + module_name + "/" + path_name);
      }
    }
  }
  return checkpoint;
}

void cleanup_non_current_checkpoints(
    const std::vector<ParsedCheckpointDir>& dirs, int32_t current_generation,
    std::string_view context) {
  for (const auto& dir : dirs) {
    if (dir.id == current_generation) {
      continue;
    }
    remove_checkpoint_dir_best_effort(dir.path, context);
  }
}

CheckpointOpenResult open_current_checkpoint(const std::string& db_dir,
                                             bool recover_workspace) {
  CheckpointOpenResult result;
  if (!std::filesystem::is_directory(db_dir)) {
    if (!recover_workspace) {
      return result;
    }
    std::filesystem::create_directories(db_dir);
  }

  if (recover_workspace) {
    cleanup_staging_checkpoints(db_dir, "CheckpointManager::Open");
  }

  auto dirs = discover_published_checkpoints(db_dir);
  for (const auto& dir : dirs) {
    try {
      result.current_checkpoint = open_checkpoint_checked(dir.path, dir.id);
      result.current_generation = dir.id;
      break;
    } catch (const std::exception& e) {
      LOG(WARNING) << "CheckpointManager::Open: "
                   << (recover_workspace ? "removing" : "skipping")
                   << " invalid checkpoint " << dir.path << ": " << e.what();
      if (recover_workspace) {
        remove_checkpoint_dir_best_effort(dir.path, "CheckpointManager::Open");
      }
    } catch (...) {
      LOG(WARNING) << "CheckpointManager::Open: "
                   << (recover_workspace ? "removing" : "skipping")
                   << " invalid checkpoint " << dir.path;
      if (recover_workspace) {
        remove_checkpoint_dir_best_effort(dir.path, "CheckpointManager::Open");
      }
    }
  }

  return result;
}

std::shared_ptr<Checkpoint> create_staging_checkpoint(const std::string& db_dir,
                                                      int32_t generation) {
  auto path =
      std::filesystem::path(db_dir) / staging_checkpoint_name(generation);

  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    THROW_IO_EXCEPTION(
        "CheckpointManager::CreateStagingCheckpoint: failed to clean " +
        path.string() + ": " + ec.message());
  }

  std::filesystem::create_directories(path);
  CheckpointManifest::GenerateEmptyMeta((path / "meta").string());
  return Checkpoint::Open(path.string(), generation);
}

std::shared_ptr<Checkpoint> publish_staging_checkpoint(
    const std::string& db_dir,
    const std::shared_ptr<Checkpoint>& staging_checkpoint,
    const std::shared_ptr<Checkpoint>& current_checkpoint,
    int32_t current_generation, std::string* previous_checkpoint_path) {
  const int32_t generation = static_cast<int32_t>(staging_checkpoint->id());
  if (current_generation != kInvalidCheckpointGeneration &&
      generation <= current_generation) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::CommitStagingCheckpoint: refusing to commit "
        "non-increasing checkpoint generation " +
        std::to_string(generation) + " over current generation " +
        std::to_string(current_generation));
  }

  auto staging_path = std::filesystem::path(staging_checkpoint->path());
  int32_t parsed_generation;
  if (staging_path.filename().string() != staging_checkpoint_name(generation) ||
      !parse_staging_checkpoint_path(staging_path, parsed_generation) ||
      parsed_generation != generation) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::CommitStagingCheckpoint: invalid staging "
        "checkpoint path: " +
        staging_path.string());
  }

  if (!staging_checkpoint->GetMeta().has_schema()) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::CommitStagingCheckpoint: checkpoint has no "
        "schema: " +
        staging_path.string());
  }

  const auto final_path =
      std::filesystem::path(db_dir) / checkpoint_name(generation);
  if (std::filesystem::exists(final_path)) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::CommitStagingCheckpoint: published checkpoint "
        "already exists: " +
        final_path.string());
  }

  std::error_code ec;
  std::filesystem::rename(staging_path, final_path, ec);
  if (ec) {
    THROW_IO_EXCEPTION("CheckpointManager::CommitStagingCheckpoint: rename " +
                       staging_path.string() + " -> " + final_path.string() +
                       " failed: " + ec.message());
  }
  if (!file_utils::fsync_directory(db_dir)) {
    LOG(WARNING)
        << "CheckpointManager::CommitStagingCheckpoint: failed to fsync "
        << db_dir;
  }

  std::shared_ptr<Checkpoint> final_checkpoint;
  try {
    final_checkpoint = Checkpoint::Open(final_path.string(), generation);
  } catch (...) {
    remove_checkpoint_dir_best_effort(
        final_path, "CheckpointManager::CommitStagingCheckpoint");
    throw;
  }

  if (previous_checkpoint_path != nullptr && current_checkpoint != nullptr) {
    *previous_checkpoint_path = current_checkpoint->path();
  }
  return final_checkpoint;
}

void discard_staging_checkpoint(
    const std::shared_ptr<Checkpoint>& staging_checkpoint) {
  if (staging_checkpoint == nullptr) {
    return;
  }

  const int32_t generation = static_cast<int32_t>(staging_checkpoint->id());
  const auto path = std::filesystem::path(staging_checkpoint->path());
  int32_t staging_generation;
  if (!parse_staging_checkpoint_path(path, staging_generation) ||
      staging_generation != generation) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::DiscardStagingCheckpoint: checkpoint is not "
        "staging: " +
        path.string());
  }

  remove_checkpoint_dir_best_effort(
      path, "CheckpointManager::DiscardStagingCheckpoint");
}

void discard_staging_checkpoint_best_effort(
    const std::shared_ptr<Checkpoint>& staging_checkpoint,
    std::string_view context) {
  if (staging_checkpoint == nullptr) {
    return;
  }
  try {
    discard_staging_checkpoint(staging_checkpoint);
  } catch (const std::exception& e) {
    LOG(WARNING) << context << ": failed to discard staging "
                 << staging_checkpoint->path() << ": " << e.what();
  } catch (...) {
    LOG(WARNING) << context << ": failed to discard staging "
                 << staging_checkpoint->path();
  }
}

}  // namespace

CheckpointManager::StagingCheckpoint::StagingCheckpoint(
    CheckpointManager& manager, std::shared_ptr<Checkpoint> checkpoint)
    : manager_(manager), checkpoint_(std::move(checkpoint)) {}

CheckpointManager::StagingCheckpoint::~StagingCheckpoint() { Discard(); }

CheckpointManager::StagingCheckpoint::StagingCheckpoint(
    StagingCheckpoint&& other) noexcept
    : manager_(other.manager_), checkpoint_(std::move(other.checkpoint_)) {
  other.release();
}

std::shared_ptr<Checkpoint> CheckpointManager::StagingCheckpoint::checkpoint()
    const {
  if (checkpoint_ == nullptr) {
    THROW_CHECKPOINT_EXCEPTION("Staging checkpoint handle is inactive.");
  }
  return checkpoint_;
}

std::shared_ptr<Checkpoint> CheckpointManager::StagingCheckpoint::Commit(
    std::string* previous_checkpoint_path) {
  if (checkpoint_ == nullptr) {
    THROW_CHECKPOINT_EXCEPTION("Staging checkpoint handle is inactive.");
  }
  return manager_.CommitStagingCheckpoint(*this, previous_checkpoint_path);
}

void CheckpointManager::StagingCheckpoint::Discard() noexcept {
  if (checkpoint_ != nullptr) {
    manager_.DiscardStagingCheckpoint(*this);
    return;
  }
  release();
}

void CheckpointManager::StagingCheckpoint::release() { checkpoint_.reset(); }

void CheckpointManager::Open(const std::string& db_dir,
                             bool recover_workspace) {
  if (db_dir.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("db_dir cannot be empty");
  }

  std::shared_ptr<Checkpoint> stale_staging_checkpoint;
  try {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!db_dir_.empty()) {
        LOG(WARNING)
            << "CheckpointManager::Open called on already-open workspace: "
            << db_dir_ << ", reopening to: " << db_dir;
      }

      stale_staging_checkpoint = staging_checkpoint_;
      db_dir_ = std::filesystem::absolute(db_dir).string();
      current_checkpoint_.reset();
      staging_checkpoint_.reset();
      current_generation_ = kInvalidCheckpointGeneration;

      auto result = open_current_checkpoint(db_dir_, recover_workspace);
      current_checkpoint_ = std::move(result.current_checkpoint);
      current_generation_ = result.current_generation;
    }
    discard_staging_checkpoint_best_effort(stale_staging_checkpoint,
                                           "CheckpointManager::Open");
  } catch (const std::filesystem::filesystem_error& e) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      db_dir_.clear();
      current_checkpoint_.reset();
      staging_checkpoint_.reset();
      current_generation_ = kInvalidCheckpointGeneration;
    }
    discard_staging_checkpoint_best_effort(stale_staging_checkpoint,
                                           "CheckpointManager::Open");
    if (e.code() == std::errc::permission_denied) {
      THROW_PERMISSION_DENIED("CheckpointManager::Open: cannot access " +
                              std::string(db_dir) + ": " + e.what());
    }
    THROW_IO_EXCEPTION("CheckpointManager::Open: failed to open " +
                       std::string(db_dir) + ": " + e.what());
  }
}

void CheckpointManager::Close() {
  std::shared_ptr<Checkpoint> staging_checkpoint;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    staging_checkpoint = staging_checkpoint_;
    db_dir_.clear();
    current_checkpoint_.reset();
    staging_checkpoint_.reset();
    current_generation_ = kInvalidCheckpointGeneration;
  }
  discard_staging_checkpoint_best_effort(staging_checkpoint,
                                         "CheckpointManager::Close");
}

bool CheckpointManager::HasCurrentCheckpoint() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return current_checkpoint_ != nullptr;
}

std::shared_ptr<Checkpoint> CheckpointManager::CurrentCheckpoint() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return current_checkpoint_;
}

CheckpointManager::StagingCheckpoint
CheckpointManager::CreateStagingCheckpoint() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (db_dir_.empty()) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::CreateStagingCheckpoint: manager is not open");
  }
  if (staging_checkpoint_ != nullptr) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::CreateStagingCheckpoint: active staging "
        "checkpoint already exists: " +
        std::to_string(staging_checkpoint_->id()));
  }

  const int32_t generation = current_generation_ == kInvalidCheckpointGeneration
                                 ? 0
                                 : current_generation_ + 1;
  staging_checkpoint_ = create_staging_checkpoint(db_dir_, generation);
  return StagingCheckpoint(*this, staging_checkpoint_);
}

std::shared_ptr<Checkpoint> CheckpointManager::CommitStagingCheckpoint(
    StagingCheckpoint& staging, std::string* previous_checkpoint_path) {
  if (previous_checkpoint_path != nullptr) {
    previous_checkpoint_path->clear();
  }

  std::shared_ptr<Checkpoint> checkpoint;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (&staging.manager_ != this || staging.checkpoint_ == nullptr) {
      THROW_CHECKPOINT_EXCEPTION(
          "CheckpointManager::CommitStagingCheckpoint: inactive or foreign "
          "staging checkpoint");
    }
    if (staging_checkpoint_ == nullptr ||
        staging_checkpoint_ != staging.checkpoint_) {
      THROW_CHECKPOINT_EXCEPTION(
          "CheckpointManager::CommitStagingCheckpoint: staging checkpoint is "
          "not active");
    }

    current_checkpoint_ = publish_staging_checkpoint(
        db_dir_, staging_checkpoint_, current_checkpoint_, current_generation_,
        previous_checkpoint_path);
    current_generation_ = static_cast<int32_t>(current_checkpoint_->id());
    staging_checkpoint_.reset();
    staging.release();
    checkpoint = current_checkpoint_;
  }

  return checkpoint;
}

void CheckpointManager::RestoreCurrentCheckpoint(
    std::shared_ptr<Checkpoint> checkpoint) {
  if (checkpoint == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "CheckpointManager::RestoreCurrentCheckpoint: checkpoint is null");
  }

  const auto checkpoint_path = std::filesystem::path(checkpoint->path());
  int32_t parsed_generation;
  if (!parse_checkpoint_path(checkpoint_path, parsed_generation) ||
      parsed_generation != static_cast<int32_t>(checkpoint->id())) {
    THROW_CHECKPOINT_EXCEPTION(
        "CheckpointManager::RestoreCurrentCheckpoint: invalid checkpoint "
        "path: " +
        checkpoint->path());
  }

  std::lock_guard<std::mutex> lock(mutex_);
  current_generation_ = static_cast<int32_t>(checkpoint->id());
  current_checkpoint_ = std::move(checkpoint);
  staging_checkpoint_.reset();
}

void CheckpointManager::CleanupPublishedCheckpoint(
    std::shared_ptr<Checkpoint> checkpoint) {
  if (checkpoint == nullptr) {
    return;
  }

  const int32_t generation = static_cast<int32_t>(checkpoint->id());
  auto path = std::filesystem::path(checkpoint->path());
  if (path.filename().string() != checkpoint_name(generation)) {
    LOG(WARNING)
        << "CheckpointManager::CleanupPublishedCheckpoint: refusing to "
        << "remove non-published checkpoint path " << path;
    return;
  }

  std::string db_dir;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    db_dir = db_dir_;
    if (current_checkpoint_ != nullptr && current_generation_ == generation &&
        current_checkpoint_->path() == checkpoint->path()) {
      LOG(WARNING)
          << "CheckpointManager::CleanupPublishedCheckpoint: refusing to "
          << "remove current checkpoint " << path;
      return;
    }
  }
  if (db_dir.empty() || path.parent_path() != std::filesystem::path(db_dir)) {
    LOG(WARNING)
        << "CheckpointManager::CleanupPublishedCheckpoint: refusing to "
        << "remove checkpoint outside current workspace " << path;
    return;
  }

  remove_checkpoint_dir_best_effort(
      path, "CheckpointManager::CleanupPublishedCheckpoint");
}

void CheckpointManager::CleanupRetiredCheckpoints() {
  std::string db_dir;
  int32_t current_generation;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (db_dir_.empty() ||
        current_generation_ == kInvalidCheckpointGeneration) {
      return;
    }
    db_dir = db_dir_;
    current_generation = current_generation_;
  }

  try {
    cleanup_non_current_checkpoints(
        discover_published_checkpoints(db_dir), current_generation,
        "CheckpointManager::CleanupRetiredCheckpoints");
  } catch (const std::exception& e) {
    LOG(WARNING) << "CheckpointManager::CleanupRetiredCheckpoints failed for "
                 << db_dir << ": " << e.what();
  } catch (...) {
    LOG(WARNING) << "CheckpointManager::CleanupRetiredCheckpoints failed for "
                 << db_dir;
  }
}

void CheckpointManager::DiscardStagingCheckpoint(
    StagingCheckpoint& staging) noexcept {
  std::shared_ptr<Checkpoint> staging_checkpoint;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (&staging.manager_ != this || staging.checkpoint_ == nullptr) {
      staging.release();
      return;
    }
    if (staging_checkpoint_ != staging.checkpoint_) {
      staging.release();
      return;
    }
    staging_checkpoint = staging_checkpoint_;
    staging_checkpoint_.reset();
  }

  discard_staging_checkpoint_best_effort(
      staging_checkpoint, "CheckpointManager::DiscardStagingCheckpoint");
  staging.release();
}

std::string CheckpointManager::db_dir() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return db_dir_;
}

}  // namespace neug
