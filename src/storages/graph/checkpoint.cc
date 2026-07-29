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

#include "neug/storages/checkpoint_manifest.h"
#include "neug/utils/exception/exception.h"

#include <glog/logging.h>

namespace neug {

bool is_valid_uuid(const std::string& uuid) {
  static const std::regex uuid_regex(
      "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]"
      "{12}$");
  return std::regex_match(uuid, uuid_regex);
}

static void CollectReferencedFiles(const ModuleDescriptor& desc,
                                   const std::string& target_dir,
                                   std::set<std::string>& referenced_files) {
  for (const auto& [_, p] : desc.paths()) {
    if (p.empty()) {
      continue;
    }
    auto parent = std::filesystem::path(p).parent_path().string();
    if (parent == target_dir) {
      referenced_files.insert(std::filesystem::path(p).filename().string());
    }
  }
}

Checkpoint::~Checkpoint() = default;

Checkpoint::Checkpoint(std::string path, uint32_t id)
    : path_(std::move(path)), id_(id) {}

std::shared_ptr<Checkpoint> Checkpoint::Open(std::string path, uint32_t id) {
  // Can't use make_shared because constructor is private.
  auto ckp = std::shared_ptr<Checkpoint>(new Checkpoint(std::move(path), id));
  ckp->initialize();
  return ckp;
}

void Checkpoint::initialize() {
  create_dirs();
  file_mgr_ =
      std::make_unique<CheckpointFileManager>(snapshot_dir(), runtime_dir());
  meta_ = std::make_unique<CheckpointManifest>();
  meta_->Load(meta_path());

  for (const auto& [key, desc] : meta_->modules()) {
    ModuleDescriptor absolute_desc = desc;
    for (auto& [_, v] : absolute_desc.mutable_paths()) {
      v = file_mgr_->ResolveAbsolutePath(v, path_);
    }
    meta_->set_module(key, std::move(absolute_desc));
  }

  // Clean orphaned runtime files not referenced by meta.
  std::set<std::string> referenced_runtime_files;
  for (auto& [key, desc] : meta_->modules()) {
    CollectReferencedFiles(desc, runtime_dir(), referenced_runtime_files);
  }

  try {
    for (const auto& entry :
         std::filesystem::directory_iterator(runtime_dir())) {
      if (entry.is_regular_file()) {
        std::string name = entry.path().filename().string();
        if (is_valid_uuid(name) && referenced_runtime_files.count(name) == 0) {
          std::filesystem::remove(entry.path());
          VLOG(1) << "Checkpoint::initialize: cleaned orphan file " << name;
        }
      }
    }
  } catch (const std::filesystem::filesystem_error& e) {
    LOG(WARNING) << "Checkpoint::initialize: error during cleanup: "
                 << e.what();
  }
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

void Checkpoint::UpdateMeta(CheckpointManifest&& meta) {
  assert(!IsEmpty());
  std::lock_guard<std::mutex> lock(mutex_);
  auto old_meta = std::move(meta_);
  try {
    // Persist a copy with paths rewritten relative to the checkpoint root, so
    // the directory remains relocatable on disk.
    CheckpointManifest meta_with_relative_paths = meta;
    for (const auto& [key, desc] : meta.modules()) {
      ModuleDescriptor relative_desc = desc;
      for (auto& [_, v] : relative_desc.mutable_paths()) {
        v = file_mgr_->MakeRelativePath(v, path_);
      }
      meta_with_relative_paths.set_module(key, std::move(relative_desc));
    }
    if (!file_mgr_->SyncSnapshotDirectory()) {
      THROW_IO_EXCEPTION(
          "Checkpoint::UpdateMeta: failed to fsync snapshot_dir " +
          snapshot_dir());
    }
    meta_with_relative_paths.Save(meta_path());

    // Keep the in-memory meta_ in absolute-path form so downstream consumers
    // (CollectReferencedFiles for orphan cleanup, Module::Open, …) can keep
    // operating on usable paths without knowing the checkpoint root.
    meta_ = std::make_unique<CheckpointManifest>(std::move(meta));

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
          VLOG(1) << "UpdateMeta: removed orphan from snapshot_dir: " << name;
        }
      }
    } catch (const std::filesystem::filesystem_error& e) {
      LOG(WARNING) << "UpdateMeta: error cleaning snapshot_dir: " << e.what();
    }
    return;
  } catch (...) {
    LOG(ERROR) << "Checkpoint::UpdateMeta: failed to persist meta to "
               << meta_path();
    meta_ = std::move(old_meta);
    throw;
  }
}

}  // namespace neug
