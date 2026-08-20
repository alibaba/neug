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

#include <charconv>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <utility>

#include <glog/logging.h>

#include "neug/utils/exception/exception.h"
#include "neug/utils/io/file/file_utils.h"
#include "neug/utils/uuid.h"

#include "legacy_checkpoint_migrator.h"

namespace neug {

namespace {

std::filesystem::path checkpoint_dir(const std::string& db_dir) {
  return std::filesystem::path(db_dir) / "checkpoint";
}

std::filesystem::path current_path(const std::string& db_dir) {
  return checkpoint_dir(db_dir) / "CURRENT";
}

uint64_t read_current(const std::filesystem::path& path) {
  std::ifstream input(path);
  std::string value;
  std::getline(input, value);
  if (!input || value.empty()) {
    THROW_CHECKPOINT_EXCEPTION("Invalid CURRENT selector: " + path.string());
  }

  uint64_t id = 0;
  const char* first = value.data();
  const char* last = first + value.size();
  const auto [ptr, ec] = std::from_chars(first, last, id);
  if (ec != std::errc{} || ptr != last) {
    THROW_CHECKPOINT_EXCEPTION("Invalid CURRENT selector: " + path.string());
  }
  return id;
}

std::optional<uint64_t> try_read_current(
    const std::filesystem::path& path) noexcept {
  try {
    if (!std::filesystem::exists(path)) {
      return std::nullopt;
    }
    return read_current(path);
  } catch (...) { return std::nullopt; }
}

bool current_matches(const std::filesystem::path& path,
                     std::optional<uint64_t> expected) noexcept {
  std::error_code ec;
  const bool exists = std::filesystem::exists(path, ec);
  if (ec) {
    return false;
  }
  if (!expected.has_value()) {
    return !exists;
  }
  return exists && try_read_current(path) == expected;
}

bool restore_current(const std::filesystem::path& path,
                     std::optional<uint64_t> previous_id) noexcept {
  try {
    if (previous_id.has_value()) {
      file_utils::AtomicFileWriter writer(path.string());
      writer.stream() << *previous_id << '\n';
      if (writer.Commit() !=
          file_utils::AtomicFileWriter::CommitResult::kDurable) {
        return false;
      }
    } else {
      std::error_code ec;
      std::filesystem::remove(path, ec);
      if (ec || !file_utils::fsync_directory(path.parent_path().string())) {
        return false;
      }
    }
    return current_matches(path, previous_id);
  } catch (...) { return false; }
}

void ensure_directory(const std::filesystem::path& path,
                      std::string_view context) {
  std::error_code ec;
  std::filesystem::create_directories(path, ec);
  if (ec) {
    THROW_IO_EXCEPTION(std::string(context) + ": failed to create " +
                       path.string() + ": " + ec.message());
  }
}

std::optional<uint64_t> checkpoint_id_from_name(
    const std::filesystem::path& path, std::string_view suffix) {
  const auto name = path.filename().string();
  if (!name.ends_with(suffix)) {
    return std::nullopt;
  }
  const auto id_text =
      std::string_view(name).substr(0, name.size() - suffix.size());
  if (id_text.empty()) {
    return std::nullopt;
  }
  uint64_t id = 0;
  const auto [ptr, ec] =
      std::from_chars(id_text.data(), id_text.data() + id_text.size(), id);
  if (ec != std::errc{} || ptr != id_text.data() + id_text.size()) {
    return std::nullopt;
  }
  return id;
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
    THROW_CHECKPOINT_EXCEPTION("Staging checkpoint handle is inactive");
  }
  return checkpoint_;
}

std::shared_ptr<Checkpoint> CheckpointManager::StagingCheckpoint::Publish() {
  if (checkpoint_ == nullptr) {
    THROW_CHECKPOINT_EXCEPTION("Staging checkpoint handle is inactive");
  }
  return manager_.PublishStagingCheckpoint(*this);
}

void CheckpointManager::StagingCheckpoint::Discard() noexcept {
  if (checkpoint_ != nullptr) {
    manager_.DiscardStagingCheckpoint(*this);
  }
}

void CheckpointManager::StagingCheckpoint::release() { checkpoint_.reset(); }

void CheckpointManager::Open(const std::string& database_dir,
                             bool create_if_missing) {
  if (database_dir.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("database_dir cannot be empty");
  }

  const auto absolute_db_dir = std::filesystem::absolute(database_dir);
  if (!std::filesystem::exists(absolute_db_dir)) {
    if (!create_if_missing) {
      return;
    }
    ensure_directory(absolute_db_dir, "CheckpointManager::Open");
  }

  const auto root = checkpoint_dir(absolute_db_dir.string());
  const bool has_current = std::filesystem::exists(root / "CURRENT");
  // CURRENT is authoritative. Do not even inspect residual legacy entries
  // when it exists; their validity and accessibility must not affect opening
  // the published v2 checkpoint.
  if (!has_current && !create_if_missing &&
      LegacyCheckpointMigrator::HasLegacyDirectories(absolute_db_dir)) {
    THROW_NOT_SUPPORTED_EXCEPTION(
        "A legacy checkpoint-N database must be opened once in read-write "
        "mode before it can be opened read-only");
  }

  std::optional<LegacyCheckpointCandidate> legacy;
  if (create_if_missing && !has_current) {
    ensure_directory(root / "manifests", "CheckpointManager::Open");
    ensure_directory(root / "objects", "CheckpointManager::Open");
    ensure_directory(absolute_db_dir / "wal", "CheckpointManager::Open");
    legacy = LegacyCheckpointMigrator::FindLatest(absolute_db_dir);
  }
  const auto runtime =
      absolute_db_dir / "runtime" / ("open-" + UUIDGenerator::Generate());
  auto runtime_workspace = RuntimeWorkspace::Create(runtime.string());
  {
    std::lock_guard<std::mutex> lock(mutex_);
    database_dir_ = absolute_db_dir.string();
    runtime_workspace_ = runtime_workspace;
    current_checkpoint_.reset();
    staging_checkpoint_.reset();
    published_checkpoints_.clear();
  }

  if (has_current) {
    const auto current = current_path(absolute_db_dir.string());
    const uint64_t id = read_current(current);
    auto checkpoint = Checkpoint::OpenPublished(absolute_db_dir.string(), id,
                                                std::move(runtime_workspace));
    if (!checkpoint->manifest().has_schema()) {
      THROW_CHECKPOINT_EXCEPTION("CURRENT manifest has no schema: " +
                                 checkpoint->manifest_path());
    }
    if (!std::filesystem::is_directory(checkpoint->wal_dir())) {
      THROW_CHECKPOINT_EXCEPTION("CURRENT manifest WAL epoch is missing: " +
                                 checkpoint->wal_dir());
    }
    std::lock_guard<std::mutex> lock(mutex_);
    current_checkpoint_ = std::move(checkpoint);
    published_checkpoints_.push_back(current_checkpoint_);
    return;
  }

  if (!legacy.has_value()) {
    return;
  }

  auto staging = [&] {
    std::lock_guard<std::mutex> lock(mutex_);
    return CreateStagingLocked(legacy->id);
  }();
  LegacyCheckpointMigrator::Import(*legacy, *staging.checkpoint());
  auto migrated = staging.Publish();
  LOG(INFO) << "Migrated legacy checkpoint " << legacy->root << " to "
            << migrated->manifest_path();
}

void CheckpointManager::Close() {
  std::lock_guard<std::mutex> lock(mutex_);
  current_checkpoint_.reset();
  staging_checkpoint_.reset();
  runtime_workspace_.reset();
  database_dir_.clear();
  published_checkpoints_.clear();
}

std::shared_ptr<Checkpoint> CheckpointManager::Current() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return current_checkpoint_;
}

CheckpointManager::StagingCheckpoint CheckpointManager::CreateStaging() {
  std::lock_guard<std::mutex> lock(mutex_);
  const uint64_t id =
      current_checkpoint_ == nullptr ? 0 : current_checkpoint_->id() + 1;
  return CreateStagingLocked(id);
}

CheckpointManager::StagingCheckpoint CheckpointManager::CreateStagingLocked(
    uint64_t id) {
  if (database_dir_.empty()) {
    THROW_CHECKPOINT_EXCEPTION("CheckpointManager is not open");
  }
  if (staging_checkpoint_ != nullptr) {
    THROW_CHECKPOINT_EXCEPTION("A staging checkpoint is already active");
  }

  staging_checkpoint_ =
      Checkpoint::CreateStaging(database_dir_, id, runtime_workspace_);
  return StagingCheckpoint(*this, staging_checkpoint_);
}

std::shared_ptr<Checkpoint> CheckpointManager::PublishStagingCheckpoint(
    StagingCheckpoint& staging) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (&staging.manager_ != this || staging.checkpoint_ == nullptr ||
      staging.checkpoint_ != staging_checkpoint_) {
    THROW_CHECKPOINT_EXCEPTION("Inactive or foreign staging checkpoint");
  }
  if (!staging_checkpoint_->manifest().has_schema()) {
    THROW_CHECKPOINT_EXCEPTION("Cannot publish a checkpoint without schema");
  }
  const std::optional<uint64_t> previous_id =
      current_checkpoint_ == nullptr
          ? std::nullopt
          : std::optional<uint64_t>(current_checkpoint_->id());
  if (previous_id.has_value() && staging_checkpoint_->id() <= *previous_id) {
    THROW_CHECKPOINT_EXCEPTION("Checkpoint id must increase monotonically");
  }
  const auto wal_dir = std::filesystem::path(staging_checkpoint_->wal_dir());
  ensure_directory(wal_dir, "CheckpointManager::Publish");
  if (!file_utils::fsync_directory(wal_dir.string()) ||
      !file_utils::fsync_directory(wal_dir.parent_path().string())) {
    THROW_IO_EXCEPTION("CheckpointManager::Publish: failed to fsync WAL epoch");
  }
  staging_checkpoint_->persist_manifest();

  const auto selector = current_path(database_dir_);
  file_utils::AtomicFileWriter::CommitResult publish_result;
  try {
    file_utils::AtomicFileWriter writer(selector.string());
    writer.stream() << staging_checkpoint_->id() << '\n';
    publish_result = writer.Commit();
  } catch (const std::exception& e) {
    if (current_matches(selector, previous_id)) {
      throw;
    }
    if (restore_current(selector, previous_id)) {
      THROW_IO_EXCEPTION(
          "CheckpointManager::Publish: CURRENT replacement failed and was "
          "rolled back: " +
          std::string(e.what()));
    }
    THROW_IO_EXCEPTION(
        "CheckpointManager::Publish: CURRENT replacement failed and rollback "
        "could not be confirmed: " +
        std::string(e.what()));
  }
  // Commit-unknown handling: AtomicFileWriter::Commit() returns kCommitUnknown
  // only when the rename succeeded but the parent-directory fsync failed, so
  // CURRENT has already been atomically replaced with the new ID. Rolling
  // back to the previous ID is attempted first because that rename is
  // verifiable; if the rollback itself cannot be confirmed, the new CURRENT
  // is deliberately kept: it names a manifest that was fully persisted
  // above, which is strictly safer than an unverified rollback. This branch
  // is review-verified rather than test-covered: no unit-test seam can
  // inject a parent-directory fsync failure after a successful rename
  // without a fault-injection hook in AtomicFileWriter.
  if (publish_result != file_utils::AtomicFileWriter::CommitResult::kDurable) {
    if (restore_current(selector, previous_id)) {
      THROW_IO_EXCEPTION(
          "CheckpointManager::Publish: CURRENT replacement durability failed "
          "and was rolled back");
    }
    THROW_IO_EXCEPTION(
        "CheckpointManager::Publish: CURRENT replacement durability and "
        "rollback are commit-unknown");
  }

  current_checkpoint_ = staging_checkpoint_;
  published_checkpoints_.push_back(current_checkpoint_);
  staging_checkpoint_.reset();
  staging.release();
  return current_checkpoint_;
}

void CheckpointManager::DiscardStagingCheckpoint(
    StagingCheckpoint& staging) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  if (&staging.manager_ == this && staging.checkpoint_ == staging_checkpoint_) {
    staging_checkpoint_.reset();
  }
  staging.release();
}

void CheckpointManager::CollectGarbage() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (database_dir_.empty()) {
    return;
  }
  // A staging checkpoint may already own immutable objects that are not yet
  // reachable from a published manifest. Do not race GC with its dump.
  if (staging_checkpoint_ != nullptr) {
    return;
  }

  std::unordered_set<uint64_t> retained_ids;
  std::vector<std::weak_ptr<Checkpoint>> live_checkpoints;
  for (const auto& weak_checkpoint : published_checkpoints_) {
    auto checkpoint = weak_checkpoint.lock();
    if (checkpoint == nullptr) {
      continue;
    }
    retained_ids.insert(checkpoint->id());
    live_checkpoints.push_back(checkpoint);
  }
  published_checkpoints_ = std::move(live_checkpoints);

  std::unordered_set<std::string> retained_objects;
  for (const auto& weak_checkpoint : published_checkpoints_) {
    auto checkpoint = weak_checkpoint.lock();
    if (checkpoint == nullptr) {
      continue;
    }
    for (const auto& [_, desc] : checkpoint->manifest().Modules()) {
      for (const auto& [__, object_path] : desc.paths()) {
        if (!object_path.empty()) {
          retained_objects.insert(
              std::filesystem::path(object_path).filename().string());
        }
      }
    }
  }

  std::error_code ec;
  const auto root = checkpoint_dir(database_dir_);
  const auto manifests = root / "manifests";
  for (const auto& entry : std::filesystem::directory_iterator(manifests)) {
    auto id = checkpoint_id_from_name(entry.path(), ".manifest");
    if (id.has_value() && !retained_ids.contains(*id)) {
      std::filesystem::remove(entry.path(), ec);
      if (ec) {
        THROW_IO_EXCEPTION("Checkpoint GC: failed to remove manifest " +
                           entry.path().string() + ": " + ec.message());
      }
    }
  }

  const auto wal_root = std::filesystem::path(database_dir_) / "wal";
  for (const auto& entry : std::filesystem::directory_iterator(wal_root)) {
    auto id = checkpoint_id_from_name(entry.path(), "");
    if (id.has_value() && !retained_ids.contains(*id)) {
      std::filesystem::remove_all(entry.path(), ec);
      if (ec) {
        THROW_IO_EXCEPTION("Checkpoint GC: failed to remove WAL epoch " +
                           entry.path().string() + ": " + ec.message());
      }
    }
  }

  const auto objects = root / "objects";
  for (const auto& entry : std::filesystem::directory_iterator(objects)) {
    if (entry.is_regular_file() &&
        !retained_objects.contains(entry.path().filename().string())) {
      std::filesystem::remove(entry.path(), ec);
      if (ec) {
        THROW_IO_EXCEPTION("Checkpoint GC: failed to remove object " +
                           entry.path().string() + ": " + ec.message());
      }
    }
  }

  bool removed_runtime_workspace = false;
  const auto runtime_root = std::filesystem::path(database_dir_) / "runtime";
  const auto active_runtime = std::filesystem::path(runtime_workspace_->path());
  if (std::filesystem::is_directory(runtime_root)) {
    for (const auto& entry :
         std::filesystem::directory_iterator(runtime_root)) {
      if (!entry.is_directory() ||
          !entry.path().filename().string().starts_with("open-") ||
          entry.path() == active_runtime) {
        continue;
      }
      std::filesystem::remove_all(entry.path(), ec);
      if (ec) {
        THROW_IO_EXCEPTION(
            "Checkpoint GC: failed to remove runtime workspace " +
            entry.path().string() + ": " + ec.message());
      }
      removed_runtime_workspace = true;
    }
  }

  if (!file_utils::fsync_directory(manifests.string()) ||
      !file_utils::fsync_directory(wal_root.string()) ||
      !file_utils::fsync_directory(objects.string()) ||
      (removed_runtime_workspace &&
       !file_utils::fsync_directory(runtime_root.string()))) {
    THROW_IO_EXCEPTION("Checkpoint GC: failed to fsync checkpoint directories");
  }
  if (current_checkpoint_ != nullptr) {
    LegacyCheckpointMigrator::RemoveLegacyDirectories(database_dir_);
  }
}

std::string CheckpointManager::database_dir() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return database_dir_;
}

}  // namespace neug
