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

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
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
      object_id = ResolveObjectId(object_id);
    }
    manifest_.SetModule(key, std::move(resolved));
  }
}

std::string Checkpoint::ResolveObjectId(const std::string& object_id) const {
  const std::filesystem::path relative(object_id);
  if (relative.empty() || relative.is_absolute() ||
      relative.has_parent_path()) {
    THROW_CHECKPOINT_EXCEPTION("Checkpoint contains invalid object id: " +
                               object_id);
  }
  const auto path = (std::filesystem::path(object_dir_) / relative).string();
  if (!std::filesystem::exists(path)) {
    THROW_CHECKPOINT_EXCEPTION("Checkpoint references missing object: " + path);
  }
  return path;
}

std::shared_ptr<IDataContainer> Checkpoint::OpenObject(
    const std::string& object_id, MemoryLevel level) {
  // Immutable packed objects are mapped directly even in disk/hugepage modes.
  // Their mutable successors use AllocateChunkBuffer with the requested mode.
  (void) level;
  std::lock_guard<std::mutex> lock(chunk_mutex_);
  const auto path = ResolveObjectId(object_id);
  auto& cached = immutable_objects_[object_id];
  if (auto object = cached.lock())
    return object;
  auto object = file_mgr_->OpenFile(path, MemoryLevel::kInMemory);
  cached = object;
  return object;
}

Checkpoint::ChunkBuffer Checkpoint::AllocateChunkBuffer(size_t bytes,
                                                        MemoryLevel level) {
  constexpr size_t kArenaBytes = 2 * 1024 * 1024;
  if (bytes == 0)
    THROW_INVALID_ARGUMENT_EXCEPTION("Empty chunk allocation");
  const auto index = static_cast<size_t>(level);
  if (index == 0 || index >= chunk_arenas_.size())
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid chunk memory level");
  std::lock_guard<std::mutex> lock(chunk_mutex_);
  auto& arena = chunk_arenas_[index];
  if (!arena.container || bytes > arena.container->GetDataSize() - arena.used) {
    arena.container =
        file_mgr_->CreateRuntimeContainer(std::max(bytes, kArenaBytes), level);
    arena.used = 0;
  }
  ChunkBuffer result{arena.container, arena.used};
  arena.used += (bytes + 63) & ~size_t{63};
  // All callers request at most an arena; padding must remain in bounds.
  if (arena.used > arena.container->GetDataSize())
    arena.used = arena.container->GetDataSize();
  return result;
}

std::string Checkpoint::ObjectIdForPath(const std::string& object_path) const {
  const std::filesystem::path path(object_path);
  if (path.parent_path() != std::filesystem::path(object_dir_) ||
      path.filename().empty()) {
    THROW_CHECKPOINT_EXCEPTION(
        "Checkpoint object is outside the object store: " + object_path);
  }
  return path.filename().string();
}

void Checkpoint::SetManifest(CheckpointManifest&& manifest) {
  FinalizeObjectWriter(manifest);
  manifest_ = std::move(manifest);
}

void Checkpoint::persist_manifest() {
  FinalizeObjectWriter(manifest_);
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
      path = ObjectIdForPath(path);
    }
    persisted.SetModule(key, std::move(object_desc));
  }
  persisted.Save(manifest_path());
}

ObjectWriter& Checkpoint::object_writer() {
  if (!object_writer_) {
    object_writer_ = std::make_unique<ObjectWriter>(
        [this](const void* data, size_t length) -> uint64_t {
          // Publish the packed payload through the container Commit path so the
          // object carries the standard FileHeader that OpenFile expects; a raw
          // byte write would be misparsed (MMapContainer skips the header).
          auto container =
              file_mgr_->CreateRuntimeContainer(length, MemoryLevel::kInMemory);
          std::memcpy(container->GetData(), data, length);
          auto object_path = file_mgr_->Commit(*container);
          object_table_.push_back(object_path);
          return object_table_.size() - 1;
        });
  }
  return *object_writer_;
}

void Checkpoint::SealObjects() {
  if (object_writer_) {
    object_writer_->Seal();
  }
}

void Checkpoint::RegisterObjectFinalizer(
    std::function<void(Checkpoint&, CheckpointManifest&)> finalizer) {
  object_finalizers_.push_back(std::move(finalizer));
}

void Checkpoint::FinalizeObjectWriter(CheckpointManifest& manifest) {
  SealObjects();
  auto finalizers = std::move(object_finalizers_);
  object_finalizers_.clear();
  for (auto& finalizer : finalizers) {
    finalizer(*this, manifest);
  }
}

}  // namespace neug
