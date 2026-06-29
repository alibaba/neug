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

#include "neug/utils/io/file/file_utils.h"

#include <glog/logging.h>

namespace neug {

CheckpointFileManager::CheckpointFileManager(const std::string& snapshot_dir,
                                             const std::string& runtime_dir)
    : snapshot_dir_(snapshot_dir), runtime_dir_(runtime_dir) {}

CheckpointFileManager::~CheckpointFileManager() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& uuid : uncommitted_runtime_objects_) {
    auto path = runtime_dir_ + "/" + uuid;
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
      VLOG(1) << "CheckpointFileManager::dtor: failed to remove uncommitted "
                 "object "
              << path << ": " << ec.message();
    } else {
      VLOG(10) << "CheckpointFileManager::dtor: removed uncommitted object "
               << path;
    }
  }
  uncommitted_runtime_objects_.clear();
}

std::unique_ptr<IDataContainer> CheckpointFileManager::OpenFile(
    const std::string& file_path, MemoryLevel level) {
  std::string new_path;
  if (level == MemoryLevel::kSyncToFile) {
    new_path = runtime_dir_ + "/" + CreateRuntimeObject();
  }
  return OpenContainer(file_path, new_path, level);
}

std::unique_ptr<IDataContainer> CheckpointFileManager::CreateRuntimeContainer(
    size_t size, MemoryLevel level) {
  std::string path = runtime_dir_ + "/" + CreateRuntimeObject();
  auto ret = OpenContainer("", path, level);
  ret->Resize(size);
  return ret;
}

std::string CheckpointFileManager::Commit(IDataContainer& buffer) {
  if (!buffer.IsDirty() && !buffer.GetPath().empty()) {
    return LinkToSnapshot(buffer.GetPath());
  }
  buffer.Sync();
  if (buffer.GetPath().empty()) {
    std::string new_obj_id = CreateRuntimeObject();
    auto runtime_path = runtime_dir_ + "/" + new_obj_id;
    buffer.Dump(runtime_path);
    return CommitRuntimeObject(new_obj_id);
  }
  auto parent = std::filesystem::path(buffer.GetPath()).parent_path().string();
  if (parent == runtime_dir_ || parent == snapshot_dir_) {
    return CommitToSnapshot(buffer.GetPath());
  }
  std::string new_obj_id = CreateRuntimeObject();
  auto new_path = runtime_dir_ + "/" + new_obj_id;
  buffer.Dump(new_path);
  return CommitRuntimeObject(new_obj_id);
}

std::string CheckpointFileManager::CreateRuntimeObject() {
  std::lock_guard<std::mutex> lock(mutex_);
  while (true) {
    std::string uuid = UUIDGenerator::Generate();
    if (uncommitted_runtime_objects_.count(uuid) == 0) {
      uncommitted_runtime_objects_.insert(uuid);
      return uuid;
    }
  }
}

std::string CheckpointFileManager::CommitRuntimeObject(
    const std::string& uuid) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!uncommitted_runtime_objects_.contains(uuid)) {
    LOG(WARNING) << "CheckpointFileManager::CommitRuntimeObject: uuid " << uuid
                 << " not found in uncommitted_runtime_objects_";
    return runtime_dir_ + "/" + uuid;
  }
  return commitToSnapshotLocked(runtime_dir_ + "/" + uuid);
}

std::string CheckpointFileManager::CommitToSnapshot(
    const std::string& abs_path) {
  std::lock_guard<std::mutex> lock(mutex_);
  return commitToSnapshotLocked(abs_path);
}

std::string CheckpointFileManager::commitToSnapshotLocked(
    const std::string& abs_path) {
  auto parent = std::filesystem::path(abs_path).parent_path().string();
  std::string uuid = std::filesystem::path(abs_path).filename().string();

  if (parent == snapshot_dir_) {
    return abs_path;
  }

  if (parent == runtime_dir_) {
    auto dst = snapshot_dir_ + "/" + uuid;
    if (!std::filesystem::exists(abs_path)) {
      uncommitted_runtime_objects_.erase(uuid);
      VLOG(1) << "CommitToSnapshot: " << uuid
              << " has no backing file (memory-only), skipping move";
      return dst;
    }
    try {
      std::filesystem::rename(abs_path, dst);
      uncommitted_runtime_objects_.erase(uuid);
      VLOG(1) << "CommitToSnapshot: " << uuid
              << " moved from runtime to snapshot_dir: " << dst;
      return dst;
    } catch (const std::filesystem::filesystem_error& e) {
      THROW_RUNTIME_ERROR(
          "CommitToSnapshot failed to move file to snapshot_dir: " +
          std::string(e.what()));
    }
  }

  std::string new_uuid = UUIDGenerator::Generate();
  auto dst = snapshot_dir_ + "/" + new_uuid;
  file_utils::copy_file(abs_path, dst, false);
  VLOG(1) << "CommitToSnapshot: " << abs_path
          << " copied to snapshot_dir with new uuid " << new_uuid;
  return dst;
}

std::string CheckpointFileManager::LinkToSnapshot(const std::string& abs_path) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto parent = std::filesystem::path(abs_path).parent_path().string();
  if (parent == snapshot_dir_) {
    return abs_path;
  }
  std::string new_uuid = UUIDGenerator::Generate();
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
