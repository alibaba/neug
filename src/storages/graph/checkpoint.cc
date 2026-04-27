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

#include "neug/storages/checkpoint.h"

#include <filesystem>
#include <regex>
#include <string>

#include "neug/storages/container/container_utils.h"
#include "neug/storages/snapshot_meta.h"
#include "neug/utils/file_utils.h"

#include <glog/logging.h>

namespace neug {

bool is_valid_uuid(const std::string& uuid) {
  std::regex uuid_regex(
      "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]"
      "{12}$");
  return std::regex_match(uuid, uuid_regex);
}

static void CollectReferencedFiles(const ModuleDescriptor& desc,
                                   const std::string& target_dir,
                                   std::set<std::string>& referenced_files) {
  for (const auto& [key, sub_desc] : desc.sub_modules()) {
    CollectReferencedFiles(sub_desc, target_dir, referenced_files);
  }

  if (!desc.path.empty()) {
    auto parent = std::filesystem::path(desc.path).parent_path().string();
    if (parent == target_dir) {
      referenced_files.insert(
          std::filesystem::path(desc.path).filename().string());
    }
  }
}

Checkpoint::~Checkpoint() {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const auto& uuid : uncommitted_runtime_objects_) {
    auto path = runtime_dir() + "/" + uuid;
    std::error_code ec;
    std::filesystem::remove(path, ec);
    if (ec) {
      VLOG(1) << "Checkpoint::dtor: failed to remove uncommitted object "
              << path << ": " << ec.message();
    } else {
      VLOG(10) << "Checkpoint::dtor: removed uncommitted object " << path;
    }
  }
  uncommitted_runtime_objects_.clear();
}

Checkpoint::Checkpoint(std::string path, uint32_t id)
    : path_(std::move(path)), id_(id) {
  create_dirs();
  meta_ = std::make_unique<SnapshotMeta>();
  meta_->Open(meta_path());

  for (const auto& [key, desc] : meta_->modules()) {
    ModuleDescriptor absolute_desc = desc;
    absolute_desc.path = resolveAbsolutePath(desc.path);
    meta_->set_module(key, std::move(absolute_desc));
  }

  // Simplified cleanup logic:
  // 1. Collect all files referenced in meta that are in runtime_dir
  // 2. Delete unreferenced files from runtime_dir
  // 3. Leave snapshot_dir untouched

  std::set<std::string> referenced_runtime_files;

  if (meta_ != nullptr) {
    for (auto& [key, desc] : meta_->modules()) {
      CollectReferencedFiles(desc, runtime_dir(), referenced_runtime_files);
    }
  }

  try {
    for (const auto& entry :
         std::filesystem::directory_iterator(runtime_dir())) {
      if (entry.is_regular_file()) {
        std::string name = entry.path().filename().string();

        // Only delete valid UUIDs that aren't referenced
        if (is_valid_uuid(name) && referenced_runtime_files.count(name) == 0) {
          std::filesystem::remove(entry.path());
          VLOG(1) << "Checkpoint::ctor: cleaned orphan file " << name;
        }
      }
    }
  } catch (const std::filesystem::filesystem_error& e) {
    LOG(WARNING) << "Checkpoint::ctor: error during cleanup: " << e.what();
  }

  // snapshot_dir files are not touched (committed data is preserved)
}

void Checkpoint::create_dirs() const {
  assert(!IsEmpty());
#define CREATE_DIR(dir)                                                   \
  do {                                                                    \
    std::error_code ec;                                                   \
    std::filesystem::create_directories(dir(), ec);                       \
    if (ec) {                                                             \
      LOG(ERROR) << "Checkpoint::create_dirs: failed to create " << dir() \
                 << ": " << ec.message();                                 \
    }                                                                     \
  } while (0)
  CREATE_DIR(snapshot_dir);
  CREATE_DIR(runtime_dir);
  CREATE_DIR(wal_dir);
  CREATE_DIR(allocator_dir);
#undef CREATE_DIR
}

std::unique_ptr<IDataContainer> Checkpoint::CreateRuntimeContainer(
    size_t size, MemoryLevel level) {
  assert(!IsEmpty());
  std::string path = runtime_dir() + "/" + create_runtime_object();
  auto ret = OpenContainer("", path, level);
  ret->Resize(size);
  return ret;
}

// Will move file under runtime_dir to snapshot_dir, or copy from elsewhere if
// needed
void Checkpoint::UpdateMeta(SnapshotMeta&& meta) {
  assert(!IsEmpty());
  auto old_meta = std::move(meta_);
  try {
    SnapshotMeta meta_with_relative_paths = meta;
    for (const auto& [key, desc] : meta.modules()) {
      ModuleDescriptor relative_desc = desc;
      relative_desc.path = makeRelativePath(desc.path);
      meta_with_relative_paths.set_module(key, std::move(relative_desc));
    }

    meta_with_relative_paths.Dump(meta_path());
    meta_ = std::make_unique<SnapshotMeta>(std::move(meta_with_relative_paths));

    std::set<std::string> referenced_snapshot_files;
    for (const auto& [key, desc] : meta_->modules()) {
      CollectReferencedFiles(desc, snapshot_dir(), referenced_snapshot_files);
    }
    try {
      for (const auto& entry :
           std::filesystem::directory_iterator(snapshot_dir())) {
        if (!entry.is_regular_file()) {
          continue;
        }
        std::string name = entry.path().filename().string();
        if (is_valid_uuid(name) && referenced_snapshot_files.count(name) == 0) {
          std::filesystem::remove(entry.path());
          LOG(INFO) << "UpdateMeta: removed orphan from snapshot_dir: " << name;
        }
      }
    } catch (const std::filesystem::filesystem_error& e) {
      LOG(WARNING) << "UpdateMeta: error cleaning snapshot_dir: " << e.what();
    }
    return;
  } catch (const std::exception& e) {
    LOG(ERROR) << "Checkpoint::UpdateMeta: failed to update meta: " << e.what();
  }
  meta_ = std::move(old_meta);
}

// Internal implementation without locking.
std::string Checkpoint::commitToSnapshotLocked(const std::string& abs_path) {
  auto parent = std::filesystem::path(abs_path).parent_path().string();
  std::string uuid = std::filesystem::path(abs_path).filename().string();

  if (parent == snapshot_dir()) {
    return abs_path;
  }

  if (parent == runtime_dir()) {
    auto dst = snapshot_dir() + "/" + uuid;
    if (!std::filesystem::exists(abs_path)) {
      // Memory-only container (InMemory mode): no physical file to move.
      // Clean up tracking state and return the expected snapshot path
      // so the descriptor path is consistent across memory levels.
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

  // File lives elsewhere → copy to snapshot_dir for independence
  std::string new_uuid = UUIDGenerator().generate();
  auto dst = snapshot_dir() + "/" + new_uuid;
  file_utils::copy_file(abs_path, dst, false);
  VLOG(1) << "CommitToSnapshot: " << abs_path
          << " copied to snapshot_dir with new uuid " << new_uuid;
  return dst;
}

std::string Checkpoint::CommitRuntimeObject(const std::string& uuid) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!uncommitted_runtime_objects_.contains(uuid)) {
    LOG(WARNING) << "Checkpoint::CommitRuntimeObject: uuid " << uuid
                 << " not found in uncommitted_runtime_objects_";
    // File was not tracked; just return the runtime path
    return runtime_dir() + "/" + uuid;
  }
  // Move the file from runtime_dir to snapshot_dir (handles memory-only
  // gracefully)
  return commitToSnapshotLocked(runtime_dir() + "/" + uuid);
}

std::string Checkpoint::CommitToSnapshot(const std::string& abs_path) {
  std::lock_guard<std::mutex> lock(mutex_);
  return commitToSnapshotLocked(abs_path);
}

std::string Checkpoint::LinkToSnapshot(const std::string& abs_path) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto parent = std::filesystem::path(abs_path).parent_path().string();
  if (parent == snapshot_dir()) {
    // Already in this checkpoint's snapshot_dir – nothing to do.
    return abs_path;
  }
  std::string new_uuid = UUIDGenerator().generate();
  auto dst = snapshot_dir() + "/" + new_uuid;
  std::error_code ec;
  std::filesystem::create_hard_link(abs_path, dst, ec);
  if (ec) {
    // Cross-device or unsupported FS – fall back to a regular file copy.
    VLOG(1) << "linkOrReuseInSnapshot: hardlink failed (" << ec.message()
            << "), falling back to copy for " << abs_path;
    file_utils::copy_file(abs_path, dst, /*overwrite=*/false);
  } else {
    VLOG(1) << "linkOrReuseInSnapshot: hardlinked " << abs_path << " -> "
            << dst;
  }
  return dst;
}

std::string Checkpoint::create_runtime_object() {
  std::lock_guard<std::mutex> lock(mutex_);
  while (true) {
    std::string uuid = UUIDGenerator().generate();
    if (uncommitted_runtime_objects_.count(uuid) == 0) {
      uncommitted_runtime_objects_.insert(uuid);
      return uuid;
    }
  }
}

std::set<std::string> Checkpoint::loadRuntimeObjects() {
  std::set<std::string> runtime_objects;
  try {
    for (const auto& entry :
         std::filesystem::directory_iterator(runtime_dir())) {
      if (entry.is_regular_file()) {
        std::string name = entry.path().filename().string();
        if (!is_valid_uuid(name)) {
          continue;
        }
        runtime_objects.insert(name);
      }
    }
  } catch (const std::filesystem::filesystem_error& e) {
    LOG(ERROR) << "Checkpoint::loadRuntimeObjects: " << e.what();
  }
  return runtime_objects;
}

std::string Checkpoint::makeRelativePath(const std::string& abs_path) const {
  if (abs_path.empty() || path_.empty()) {
    return abs_path;
  }
  std::filesystem::path abs_fs = abs_path;
  std::filesystem::path root_fs = path_;

  try {
    auto rel_path = std::filesystem::relative(abs_fs, root_fs);
    std::string result = rel_path.string();
    if (result.find("..") == 0) {
      return abs_path;
    }
    return result;
  } catch (...) { return abs_path; }
}

std::string Checkpoint::resolveAbsolutePath(const std::string& rel_path) const {
  if (rel_path.empty() || path_.empty()) {
    return rel_path;
  }
  if (std::filesystem::path(rel_path).is_absolute() ||
      rel_path.find("..") != std::string::npos) {
    return rel_path;
  }
  try {
    std::filesystem::path abs_path = std::filesystem::path(path_) / rel_path;
    return abs_path.string();
  } catch (...) { return rel_path; }
}

}  // namespace neug
