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

#include "neug/storages/checkpoint_file_manager.h"

#include <filesystem>
#include <utility>

#include "neug/utils/io/file/file_utils.h"

#include <glog/logging.h>

namespace neug {

struct CheckpointFileManager::RuntimeFileCleanupContext {
  explicit RuntimeFileCleanupContext(std::string runtime_dir)
      : runtime_dir(std::move(runtime_dir)) {}

  bool IsRuntimeFile(const std::string& path) const {
    if (path.empty()) {
      return false;
    }
    auto parent = std::filesystem::path(path).parent_path().string();
    return parent == runtime_dir;
  }

  void RemoveIfRuntimeFile(const std::string& path) {
    if (!IsRuntimeFile(path)) {
      return;
    }
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
      LOG(WARNING) << "RuntimeFileCleanup: failed to remove " << path << ": "
                   << ec.message();
    }
  }

  std::string runtime_dir;
};

CheckpointFileManager::RuntimeFileHandle::RuntimeFileHandle(
    std::shared_ptr<RuntimeFileCleanupContext> cleanup, std::string uuid,
    std::string path)
    : cleanup_(std::move(cleanup)),
      uuid_(std::move(uuid)),
      path_(std::move(path)) {}

CheckpointFileManager::RuntimeFileHandle::~RuntimeFileHandle() {
  if (cleanup_) {
    cleanup_->RemoveIfRuntimeFile(path_);
  }
}

CheckpointFileManager::RuntimeFileHandle::RuntimeFileHandle(
    RuntimeFileHandle&& other) noexcept
    : cleanup_(std::move(other.cleanup_)),
      uuid_(std::move(other.uuid_)),
      path_(std::move(other.path_)) {}

void CheckpointFileManager::RuntimeFileHandle::Release() {
  cleanup_.reset();
  uuid_.clear();
  path_.clear();
}

CheckpointFileManager::CheckpointFileManager(const std::string& snapshot_dir,
                                             const std::string& runtime_dir)
    : snapshot_dir_(snapshot_dir),
      runtime_dir_(runtime_dir),
      runtime_cleanup_(
          std::make_shared<RuntimeFileCleanupContext>(runtime_dir_)) {}

CheckpointFileManager::~CheckpointFileManager() = default;

std::shared_ptr<IDataContainer> CheckpointFileManager::OpenFile(
    const std::string& file_path, MemoryLevel level) {
  std::string runtime_path;
  if (level == MemoryLevel::kSyncToFile) {
    runtime_path = CreateRuntimeContainerPath();
  }
  try {
    auto container = OpenContainer(file_path, runtime_path, level);
    return WrapWithRuntimeCleanup(std::move(container));
  } catch (...) {
    runtime_cleanup_->RemoveIfRuntimeFile(runtime_path);
    throw;
  }
}

std::shared_ptr<IDataContainer> CheckpointFileManager::CreateRuntimeContainer(
    size_t size, MemoryLevel level) {
  std::string path;
  if (level == MemoryLevel::kSyncToFile) {
    path = CreateRuntimeContainerPath();
  }
  try {
    auto ret = OpenContainer("", path, level);
    if (ret == nullptr) {
      return nullptr;
    }
    ret->Resize(size);
    return WrapWithRuntimeCleanup(std::move(ret));
  } catch (...) {
    runtime_cleanup_->RemoveIfRuntimeFile(path);
    throw;
  }
}

std::shared_ptr<IDataContainer> CheckpointFileManager::WrapWithRuntimeCleanup(
    std::unique_ptr<IDataContainer> container) const {
  if (container == nullptr) {
    return nullptr;
  }
  auto cleanup = runtime_cleanup_;
  auto initial_path = container->GetPath();
  auto* raw = container.get();
  auto shared = std::shared_ptr<IDataContainer>(
      raw, [cleanup, initial_path](IDataContainer* ptr) {
        std::string path;
        if (ptr != nullptr) {
          path = ptr->GetPath();
        }
        delete ptr;
        cleanup->RemoveIfRuntimeFile(path);
        if (initial_path != path) {
          cleanup->RemoveIfRuntimeFile(initial_path);
        }
      });
  container.release();
  return shared;
}

static bool is_file_in_dir(const std::string& path, const std::string& dir) {
  if (path.empty()) {
    return false;
  }
  auto parent = std::filesystem::path(path).parent_path().string();
  return parent == dir;
}

std::string CheckpointFileManager::Commit(IDataContainer& buffer) {
  auto original_path = buffer.GetPath();
  if (!buffer.IsDirty() && is_file_in_dir(original_path, snapshot_dir_)) {
    buffer.Close();
    return original_path;
  }

  auto runtime_file = CreateRuntimeFile();
  buffer.Dump(runtime_file.path());
  runtime_cleanup_->RemoveIfRuntimeFile(original_path);
  return CommitRuntimeFile(std::move(runtime_file));
}

CheckpointFileManager::RuntimeFileHandle
CheckpointFileManager::CreateRuntimeFile() {
  std::lock_guard<std::mutex> lock(mutex_);
  auto uuid = CreateRuntimeObjectNameLocked();
  return RuntimeFileHandle(runtime_cleanup_, uuid, runtime_dir_ + "/" + uuid);
}

std::string CheckpointFileManager::CreateRuntimeContainerPath() {
  std::lock_guard<std::mutex> lock(mutex_);
  auto uuid = CreateRuntimeObjectNameLocked();
  return runtime_dir_ + "/" + uuid;
}

std::string CheckpointFileManager::CreateRuntimeObjectNameLocked() const {
  while (true) {
    std::string uuid = UUIDGenerator::Generate();
    auto runtime_path = runtime_dir_ + "/" + uuid;
    auto snapshot_path = snapshot_dir_ + "/" + uuid;
    if (!std::filesystem::exists(runtime_path) &&
        !std::filesystem::exists(snapshot_path)) {
      return uuid;
    }
  }
}

std::string CheckpointFileManager::CommitRuntimeFile(RuntimeFileHandle&& file) {
  if (!file.valid()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "CheckpointFileManager::CommitRuntimeFile: invalid runtime file");
  }
  std::lock_guard<std::mutex> lock(mutex_);
  auto dst = commitRuntimeFileLocked(file.uuid_, file.path_);
  file.Release();
  return dst;
}

std::string CheckpointFileManager::copyToSnapshotLocked(
    const std::string& abs_path) {
  auto parent = std::filesystem::path(abs_path).parent_path().string();
  if (parent == snapshot_dir_) {
    return abs_path;
  }

  if (!std::filesystem::exists(abs_path)) {
    THROW_IO_EXCEPTION("CopyToSnapshot: source file does not exist: " +
                       abs_path);
  }

  std::string new_uuid = CreateRuntimeObjectNameLocked();
  auto dst = snapshot_dir_ + "/" + new_uuid;
  file_utils::copy_file(abs_path, dst, false);
  VLOG(1) << "CopyToSnapshot: " << abs_path
          << " copied to snapshot_dir: " << dst;
  return dst;
}

std::string CheckpointFileManager::commitRuntimeFileLocked(
    const std::string& uuid, const std::string& abs_path) {
  auto parent = std::filesystem::path(abs_path).parent_path().string();
  if (parent != runtime_dir_) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "CommitRuntimeFile: path is not in checkpoint runtime_dir: " +
        abs_path);
  }
  if (!std::filesystem::exists(abs_path)) {
    THROW_IO_EXCEPTION("CommitRuntimeFile: source file does not exist: " +
                       abs_path);
  }

  auto dst = snapshot_dir_ + "/" + uuid;
  while (std::filesystem::exists(dst)) {
    dst = snapshot_dir_ + "/" + CreateRuntimeObjectNameLocked();
  }
  std::error_code ec;
  std::filesystem::rename(abs_path, dst, ec);
  if (!ec) {
    VLOG(1) << "CommitRuntimeFile: " << abs_path << " moved to " << dst;
    return dst;
  }

  VLOG(1) << "CommitRuntimeFile: rename failed (" << ec.message()
          << "), falling back to copy for " << abs_path;
  file_utils::copy_file(abs_path, dst, false);
  std::filesystem::remove(abs_path, ec);
  if (ec) {
    LOG(WARNING) << "CommitRuntimeFile: failed to remove " << abs_path
                 << " after copy fallback: " << ec.message();
  }
  return dst;
}

std::string CheckpointFileManager::LinkToSnapshot(const std::string& abs_path) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto parent = std::filesystem::path(abs_path).parent_path().string();
  if (parent == snapshot_dir_) {
    return abs_path;
  }
  if (parent == runtime_dir_) {
    // The current checkpoint runtime file can still be backed by a live
    // MAP_SHARED container. Materialize an independent snapshot instead of
    // aliasing future writes through a hardlink. Retired runtime files from
    // older checkpoints are allowed to use the hardlink path below by caller
    // contract.
    return copyToSnapshotLocked(abs_path);
  }
  std::string new_uuid = CreateRuntimeObjectNameLocked();
  auto dst = snapshot_dir_ + "/" + new_uuid;
  std::error_code ec;
  std::filesystem::create_hard_link(abs_path, dst, ec);
  if (ec) {
    VLOG(1) << "LinkToSnapshot: hardlink failed (" << ec.message()
            << "), falling back to copy for " << abs_path;
    file_utils::copy_file(abs_path, dst, /*overwrite=*/false);
  } else {
    VLOG(1) << "LinkToSnapshot: hardlinked " << abs_path << " -> " << dst;
  }
  return dst;
}

bool CheckpointFileManager::SyncSnapshotDirectory() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return file_utils::fsync_directory(snapshot_dir_);
}

// True iff the first path component of @p p is exactly "..".  A path escapes
// its root only when normalization leaves a leading parent-dir component;
// filenames that merely *contain* ".." (e.g. "..hidden", "foo..bar") are safe.
bool escapes_root(const std::filesystem::path& p) {
  static const std::filesystem::path kParent("..");
  auto it = p.begin();
  return it != p.end() && *it == kParent;
}

std::string CheckpointFileManager::MakeRelativePath(
    const std::string& abs_path, const std::string& checkpoint_root) const {
  if (abs_path.empty() || checkpoint_root.empty()) {
    return abs_path;
  }
  try {
    auto rel_path =
        std::filesystem::relative(std::filesystem::path(abs_path),
                                  std::filesystem::path(checkpoint_root))
            .lexically_normal();
    if (rel_path.empty() || escapes_root(rel_path)) {
      return abs_path;
    }
    return rel_path.string();
  } catch (...) { return abs_path; }
}

std::string CheckpointFileManager::ResolveAbsolutePath(
    const std::string& rel_path, const std::string& checkpoint_root) const {
  if (rel_path.empty() || checkpoint_root.empty()) {
    return rel_path;
  }
  std::filesystem::path rel_fs(rel_path);
  if (rel_fs.is_absolute() || escapes_root(rel_fs.lexically_normal())) {
    return rel_path;
  }
  try {
    return (std::filesystem::path(checkpoint_root) / rel_fs).string();
  } catch (...) { return rel_path; }
}

}  // namespace neug
