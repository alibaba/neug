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
#include <string>

#include "neug/utils/exception/exception.h"

namespace neug {

Checkpoint::~Checkpoint() = default;

Checkpoint::Checkpoint(
    std::string database_dir, uint64_t id,
    std::shared_ptr<const RuntimeWorkspace> runtime_workspace)
    : database_dir_(
          std::filesystem::absolute(std::move(database_dir)).string()),
      manifest_path_((std::filesystem::path(database_dir_) / "checkpoint" /
                      "manifests" / (std::to_string(id) + ".manifest"))
                         .string()),
      object_dir_(
          (std::filesystem::path(database_dir_) / "checkpoint" / "objects")
              .string()),
      runtime_workspace_(std::move(runtime_workspace)),
      wal_dir_(
          (std::filesystem::path(database_dir_) / "wal" / std::to_string(id))
              .string()),
      id_(id) {}

std::shared_ptr<Checkpoint> Checkpoint::OpenPublished(
    std::string database_dir, uint64_t id,
    std::shared_ptr<const RuntimeWorkspace> runtime_workspace) {
  auto checkpoint = std::shared_ptr<Checkpoint>(new Checkpoint(
      std::move(database_dir), id, std::move(runtime_workspace)));
  checkpoint->initialize(true);
  return checkpoint;
}

std::shared_ptr<Checkpoint> Checkpoint::CreateStaging(
    std::string database_dir, uint64_t id,
    std::shared_ptr<const RuntimeWorkspace> runtime_workspace) {
  auto checkpoint = std::shared_ptr<Checkpoint>(new Checkpoint(
      std::move(database_dir), id, std::move(runtime_workspace)));
  checkpoint->initialize(false);
  return checkpoint;
}

const std::string& Checkpoint::runtime_dir() const {
  return runtime_workspace_->path();
}

void Checkpoint::initialize(bool load_manifest) {
  if (load_manifest) {
    if (!std::filesystem::is_regular_file(manifest_path())) {
      THROW_CHECKPOINT_EXCEPTION("Checkpoint manifest is missing: " +
                                 manifest_path());
    }
    if (!std::filesystem::is_directory(object_dir_)) {
      THROW_CHECKPOINT_EXCEPTION("Checkpoint object directory is missing: " +
                                 object_dir_);
    }
    const auto create_runtime_dir = [](const std::string& path) {
      std::error_code ec;
      std::filesystem::create_directories(path, ec);
      if (ec) {
        THROW_IO_EXCEPTION("Checkpoint: failed to create " + path + ": " +
                           ec.message());
      }
    };
    create_runtime_dir(runtime_dir());
    create_runtime_dir(allocator_dir());
  } else {
    create_dirs();
  }
  file_mgr_.reset(new CheckpointFileManager(object_dir_, runtime_workspace_));
  if (!load_manifest) {
    return;
  }

  manifest_.Load(manifest_path());
  resolve_object_paths();
}

void Checkpoint::create_dirs() const {
  const auto create = [](const std::string& path) {
    std::error_code ec;
    std::filesystem::create_directories(path, ec);
    if (ec) {
      THROW_IO_EXCEPTION("Checkpoint: failed to create " + path + ": " +
                         ec.message());
    }
  };
  create(object_dir_);
  create(std::filesystem::path(manifest_path()).parent_path().string());
  create(runtime_dir());
  create(allocator_dir());
}

void Checkpoint::resolve_object_paths() {
  for (const auto& [key, desc] : manifest_.Modules()) {
    ModuleDescriptor resolved = desc;
    for (auto& [_, object_id] : resolved.mutable_paths()) {
      if (object_id.empty()) {
        continue;
      }
      const std::filesystem::path relative(object_id);
      if (relative.is_absolute() || relative.has_parent_path()) {
        THROW_CHECKPOINT_EXCEPTION(
            "Checkpoint manifest contains invalid "
            "object id: " +
            object_id);
      }
      object_id = (std::filesystem::path(object_dir_) / relative).string();
      if (!std::filesystem::exists(object_id)) {
        THROW_CHECKPOINT_EXCEPTION(
            "Checkpoint manifest references missing "
            "object: " +
            object_id);
      }
    }
    manifest_.SetModule(key, std::move(resolved));
  }
}

void Checkpoint::SetManifest(CheckpointManifest&& manifest) {
  manifest_ = std::move(manifest);
}

void Checkpoint::persist_manifest() {
  if (!file_mgr_->SyncObjectDirectory()) {
    THROW_IO_EXCEPTION(
        "Checkpoint::persist_manifest: failed to fsync objects " + object_dir_);
  }

  CheckpointManifest persisted = manifest_;
  for (const auto& [key, desc] : manifest_.Modules()) {
    ModuleDescriptor object_desc = desc;
    for (auto& [_, path] : object_desc.mutable_paths()) {
      if (path.empty()) {
        continue;
      }
      if (std::filesystem::path(path).parent_path() !=
          std::filesystem::path(object_dir_)) {
        THROW_CHECKPOINT_EXCEPTION(
            "Checkpoint manifest object is outside "
            "the object store: " +
            path);
      }
      path = std::filesystem::path(path).filename().string();
    }
    persisted.SetModule(key, std::move(object_desc));
  }
  persisted.Save(manifest_path());
}

}  // namespace neug
