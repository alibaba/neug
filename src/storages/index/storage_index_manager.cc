/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/storages/index/storage_index_manager.h"

#include <glog/logging.h>

#include "neug/compiler/common/string_utils.h"
#include "neug/storages/graph/vertex_table.h"
#include "neug/storages/index/index_utils.h"
#include "neug/storages/module/module_factory.h"
#include "neug/utils/property/vec_column.h"

namespace neug {

static constexpr const char* kIndexPrefix = "index_";

static Status PendingIndexError(const std::string& name,
                                const std::string& operation,
                                const std::string& module_type) {
  return Status(
      StatusCode::ERR_ILLEGAL_OPERATION,
      "Cannot " + operation + " pending index '" + name +
          "' before module type '" + module_type +
          "' is loaded. Load the extension that registers this module type "
          "first.");
}

neug::result<CreatedIndex> StorageIndexManager::CreateIndex(
    std::unique_ptr<IndexMeta> meta,
    std::unique_ptr<IndexIDAccessor> index_id_accessor,
    const ColumnBase* column, const VertexSet& vertex_set, bool required) {
  if (!meta) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Cannot create index with null metadata");
  }
  if (!index_id_accessor) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Cannot create index with null IndexIDAccessor");
  }
  if (!column) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Cannot create index with null property column");
  }
  const auto name = meta->name;
  if (name.empty()) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Cannot create index with an empty name");
  }
  if (indexes_.count(name) > 0 || pending_indexes_.count(name) > 0) {
    RETURN_STATUS_ERROR(StatusCode::ERR_ILLEGAL_OPERATION,
                        "Index already exists: " + name);
  }
  if (!ckp_) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INTERNAL_ERROR,
                        "Cannot create index before IndexManager is opened");
  }

  auto type_name = meta->type;
  common::StringUtils::toLower(type_name);
  auto module_type = type_name + "_index";
  auto module = ModuleFactory::instance().Create(module_type);
  if (!module && !required) {
    ModuleDescriptor desc;
    desc.module_type = module_type;
    desc.required = false;
    desc.set("index_meta", meta->ToJsonString());
    auto [it, inserted] = pending_indexes_.emplace(
        name, PendingIndex{GetKey(name), std::move(desc), std::move(*meta),
                           PendingIndex::State::kCreated});
    CHECK(inserted);
    catalog_dirty_ = true;
    LOG(INFO) << "Deferred new index: " << name << " (type=" << module_type
              << ") until its extension is loaded";
    return CreatedIndex{&it->second};
  }
  auto* raw_index = dynamic_cast<StorageIndex*>(module.get());
  if (!raw_index) {
    RETURN_STATUS_ERROR(StatusCode::ERR_SCHEMA_MISMATCH,
                        "Module is not an index type: " + module_type);
  }
  std::unique_ptr<StorageIndex> index(
      dynamic_cast<StorageIndex*>(module.release()));
  ModuleDescriptor desc;
  desc.module_type = module_type;
  RETURN_STATUS_ERROR_IF_NOT_OK(
      index->Init(std::move(meta), std::move(index_id_accessor)));
  index->Open(*ckp_, desc, memory_level_);

  RETURN_STATUS_ERROR_IF_NOT_OK(index->Rebind(IndexBindContext{column}));
  RETURN_STATUS_ERROR_IF_NOT_OK(index->BulkBuild(vertex_set));

  auto* raw_ptr = index.get();
  indexes_[name] = std::move(index);
  dirty_index_names_.insert(name);
  catalog_dirty_ = true;
  return CreatedIndex{raw_ptr};
}

Status StorageIndexManager::DropIndex(const std::string& name) {
  auto pending_it = pending_indexes_.find(name);
  if (pending_it != pending_indexes_.end()) {
    pending_indexes_.erase(pending_it);
    catalog_dirty_ = true;
    if (pending_indexes_.empty()) {
      pending_mutations_.clear();
    }
    LOG(INFO) << "Dropped pending index metadata: " << name;
    return Status::OK();
  }
  auto it = indexes_.find(name);
  if (it == indexes_.end()) {
    return Status(StatusCode::ERR_NOT_FOUND, "Index not found: " + name);
  }
  indexes_.erase(it);
  dirty_index_names_.erase(name);
  catalog_dirty_ = true;
  return Status::OK();
}

neug::result<std::vector<StorageIndex*>> StorageIndexManager::GetIndex(
    label_t label_id, const std::string& property_name) const {
  for (const auto& [name, pending] : pending_indexes_) {
    if (pending.meta.schema.label_id == label_id &&
        pending.meta.schema.property_name == property_name) {
      return tl::unexpected(
          PendingIndexError(name, "access", pending.descriptor.module_type));
    }
  }
  std::vector<StorageIndex*> target_indexes;
  for (const auto& [name, index] : indexes_) {
    if (!index)
      continue;
    const auto& meta = index->GetMeta();
    if (meta.schema.label_id != label_id) {
      continue;
    }
    if (meta.schema.property_name == property_name) {
      target_indexes.push_back(index.get());
    }
  }
  return target_indexes;
}

neug::result<std::vector<StorageIndex*>> StorageIndexManager::GetIndexForUpdate(
    label_t label_id, const std::string& property_name) {
  auto indexes = GetIndex(label_id, property_name);
  if (!indexes) {
    return indexes;
  }
  for (auto* index : indexes.value()) {
    dirty_index_names_.insert(index->GetMeta().name);
  }
  return indexes;
}

bool StorageIndexManager::HasPendingIndex(label_t label_id) const {
  for (const auto& [_, pending] : pending_indexes_) {
    if (pending.meta.schema.label_id == label_id) {
      return true;
    }
  }
  return false;
}

bool StorageIndexManager::HasPendingIndex(
    label_t label_id, const std::string& property_name) const {
  for (const auto& [_, pending] : pending_indexes_) {
    if (pending.meta.schema.label_id == label_id &&
        pending.meta.schema.property_name == property_name) {
      return true;
    }
  }
  return false;
}

result<std::vector<StorageIndexManager::PendingIndex*>>
StorageIndexManager::GetPendingIndex(label_t label_id,
                                     const std::string& property_name) {
  std::vector<PendingIndex*> indexes;
  for (auto& [_, pending] : pending_indexes_) {
    if (pending.meta.schema.label_id == label_id &&
        pending.meta.schema.property_name == property_name) {
      indexes.push_back(&pending);
    }
  }
  return indexes;
}

result<StorageIndexManager::PendingIndex*>
StorageIndexManager::GetPendingIndexByName(const std::string& name) {
  auto it = pending_indexes_.find(name);
  if (it == pending_indexes_.end()) {
    RETURN_STATUS_ERROR(StatusCode::ERR_NOT_FOUND, "Index not found: " + name);
  }
  return &it->second;
}

result<std::vector<const StorageIndexManager::PendingIndex*>>
StorageIndexManager::GetAllPendingIndexes() const {
  std::vector<const PendingIndex*> indexes;
  indexes.reserve(pending_indexes_.size());
  for (const auto& [_, pending] : pending_indexes_) {
    indexes.push_back(&pending);
  }
  return indexes;
}

void StorageIndexManager::RecordPendingInsert(
    label_t label_id, vid_t vertex_id,
    std::vector<std::pair<std::string, Value>> properties) {
  if (pending_indexes_.empty()) {
    return;
  }
  pending_mutations_.push_back(std::make_shared<const PendingIndexMutation>(
      PendingIndexMutation{PendingIndexMutation::Type::kInsert, label_id,
                           vertex_id, std::move(properties)}));
}

void StorageIndexManager::RecordPendingUpdate(label_t label_id, vid_t vertex_id,
                                              std::string property_name,
                                              Value value) {
  if (pending_indexes_.empty()) {
    return;
  }
  pending_mutations_.push_back(std::make_shared<const PendingIndexMutation>(
      PendingIndexMutation{PendingIndexMutation::Type::kUpdate,
                           label_id,
                           vertex_id,
                           {{std::move(property_name), std::move(value)}}}));
}

void StorageIndexManager::RecordPendingDelete(label_t label_id,
                                              vid_t vertex_id) {
  if (pending_indexes_.empty()) {
    return;
  }
  pending_mutations_.push_back(
      std::make_shared<const PendingIndexMutation>(PendingIndexMutation{
          PendingIndexMutation::Type::kDelete, label_id, vertex_id, {}}));
}

static result<bool> ReplayPendingMutations(
    StorageIndex& index,
    const std::vector<
        std::shared_ptr<const StorageIndexManager::PendingIndexMutation>>&
        mutations) {
  bool replayed = false;
  const auto& meta = index.GetMeta();
  for (const auto& mutation_ptr : mutations) {
    const auto& mutation = *mutation_ptr;
    if (mutation.label_id != meta.schema.label_id) {
      continue;
    }
    if (mutation.type ==
        StorageIndexManager::PendingIndexMutation::Type::kDelete) {
      RETURN_IF_NOT_OK(index.Delete(mutation.vid));
      replayed = true;
      continue;
    }
    for (const auto& [property_name, value] : mutation.properties) {
      if (property_name == meta.schema.property_name) {
        RETURN_IF_NOT_OK(index.Upsert(mutation.vid, value));
        replayed = true;
        break;
      }
    }
  }
  return replayed;
}

neug::result<StorageIndex*> StorageIndexManager::GetIndexByName(
    const std::string& name) const {
  if (pending_indexes_.count(name) > 0) {
    const auto& pending = pending_indexes_.at(name);
    return tl::unexpected(
        PendingIndexError(name, "access", pending.descriptor.module_type));
  }
  auto it = indexes_.find(name);
  if (it == indexes_.end() || !it->second) {
    RETURN_STATUS_ERROR(StatusCode::ERR_NOT_FOUND, "Index not found: " + name);
  }
  return it->second.get();
}

neug::result<std::vector<StorageIndex*>> StorageIndexManager::GetAllIndexes()
    const {
  std::vector<StorageIndex*> target_indexes;
  for (const auto& [name, index] : indexes_) {
    if (index) {
      target_indexes.push_back(index.get());
    }
  }
  return target_indexes;
}

void StorageIndexManager::Open(std::shared_ptr<Checkpoint> ckp,
                               ModuleBroker& store, MemoryLevel level) {
  Clear();
  ckp_ = std::move(ckp);
  memory_level_ = level;
  const CheckpointManifest& meta = ckp_->manifest();
  for (const auto& [key, desc] : meta.Modules()) {
    if (!IsIndexModule(key)) {
      continue;
    }

    auto index = store.TakeModule<StorageIndex>(key, false);
    if (!index) {
      auto index_meta = desc.get("index_meta");
      if (!index_meta) {
        LOG(ERROR) << "Skipping unavailable index module '" << key
                   << "' because its descriptor has no persisted index_meta";
        continue;
      }
      auto parsed_meta = IndexMeta::FromJsonString(*index_meta);
      auto name = parsed_meta.name;
      pending_indexes_.emplace(name,
                               PendingIndex{key, desc, std::move(parsed_meta),
                                            PendingIndex::State::kPersisted});
      LOG(INFO) << "Deferred index: " << name << " (type=" << desc.module_type
                << ") until its extension is loaded";
      continue;
    }

    auto name = index->GetMeta().name;
    indexes_[name] = std::move(index);
    LOG(INFO) << "Opened index: " << name << " (type=" << desc.module_type
              << ")";
  }
}

result<size_t> StorageIndexManager::ActivateIndexes(
    const IndexColumns& columns, const IndexVertexSets& vertex_sets) {
  struct Candidate {
    std::string name;
    std::unique_ptr<StorageIndex> index;
    bool dirty;
  };
  std::vector<Candidate> candidates;
  auto& factory = ModuleFactory::instance();
  for (const auto& [name, pending] : pending_indexes_) {
    auto module = factory.Create(pending.descriptor.module_type);
    if (!module) {
      continue;
    }
    if (!dynamic_cast<StorageIndex*>(module.get())) {
      return Status(
          StatusCode::ERR_SCHEMA_MISMATCH,
          "Module is not an index type: " + pending.descriptor.module_type);
    }
    std::unique_ptr<StorageIndex> index(
        static_cast<StorageIndex*>(module.release()));
    const auto& meta = pending.meta;
    auto label_it = columns.find(meta.schema.label_id);
    if (label_it == columns.end()) {
      return Status::InternalError("Invalid label for pending index: " + name);
    }
    auto property_it = label_it->second.find(meta.schema.property_name);
    if (property_it == label_it->second.end() || !property_it->second) {
      return Status::InternalError("Invalid property for pending index: " +
                                   name);
    }
    bool dirty = false;
    if (pending.state == PendingIndex::State::kCreated) {
      std::unique_ptr<IndexIDAccessor> accessor;
      if (IsHNSWIndex(meta)) {
        auto* vec = dynamic_cast<const VecColumn*>(property_it->second);
        if (!vec) {
          return Status::InternalError("Invalid VecColumn for pending index: " +
                                       name);
        }
        accessor = std::make_unique<VecColumnBackedIndexIDAccessor>(
            *vec->get_offset_accessor());
      } else {
        accessor = std::make_unique<DefaultIndexIDAccessor>();
      }
      RETURN_IF_NOT_OK(
          index->Init(std::make_unique<IndexMeta>(meta), std::move(accessor)));
      ModuleDescriptor desc;
      desc.module_type = pending.descriptor.module_type;
      index->Open(*ckp_, desc, memory_level_);
      RETURN_IF_NOT_OK(index->Rebind(IndexBindContext{property_it->second}));
      auto vertices_it = vertex_sets.find(meta.schema.label_id);
      if (vertices_it == vertex_sets.end()) {
        return Status::InternalError("Invalid vertex set for pending index: " +
                                     name);
      }
      RETURN_IF_NOT_OK(index->BulkBuild(vertices_it->second));
      // BulkBuild reads the current graph after WAL replay, so it already
      // includes mutations recorded after CREATE INDEX. Replaying them again
      // would apply the same insert/update/delete twice.
      dirty = true;
    } else {
      static_cast<Module*>(index.get())
          ->Open(*ckp_, ckp_->manifest(), pending.descriptor, memory_level_);
      RETURN_IF_NOT_OK(index->Rebind(IndexBindContext{property_it->second}));
      auto replayed = ReplayPendingMutations(*index, pending_mutations_);
      if (!replayed) {
        return replayed.error();
      }
      dirty = replayed.value();
    }
    candidates.push_back(Candidate{name, std::move(index), dirty});
  }

  for (auto& [name, index, dirty] : candidates) {
    indexes_[name] = std::move(index);
    pending_indexes_.erase(name);
    if (dirty) {
      dirty_index_names_.insert(name);
    }
    LOG(INFO) << "Activated pending index: " << name;
  }
  if (pending_indexes_.empty()) {
    pending_mutations_.clear();
  }
  return candidates.size();
}

void StorageIndexManager::Dump(ModuleBroker& store, CheckpointManifest& meta) {
  for (const auto& [_, pending] : pending_indexes_) {
    meta.ReuseModuleClosureFrom(ckp_->manifest(), pending.key);
  }
  for (auto& [name, index] : indexes_) {
    if (!index)
      continue;

    std::string key = GetKey(name);
    store.SetModule(key, std::move(index));
  }
  indexes_.clear();
}

void StorageIndexManager::StageIncrementalModules(ModuleBroker& store,
                                                  CheckpointManifest& meta) {
  const auto& previous = ckp_->manifest();
  for (const auto& [_, pending] : pending_indexes_) {
    meta.ReuseModuleClosureFrom(previous, pending.key);
  }

  for (auto& [name, index] : indexes_) {
    if (!index) {
      continue;
    }
    const auto key = GetKey(name);
    const bool must_dump =
        dirty_index_names_.count(name) > 0 || !previous.HasModule(key);
    if (must_dump) {
      dirty_index_names_.insert(name);
      store.SetModule(key, std::move(index));
    } else {
      meta.ReuseModuleClosureFrom(previous, key);
    }
  }
}

Status StorageIndexManager::ValidateCheckpointPreconditions() const {
  if (!pending_mutations_.empty()) {
    return Status(StatusCode::ERR_ILLEGAL_OPERATION,
                  "Cannot create a checkpoint while mutations for pending "
                  "extension-backed indexes have not been applied. Load the "
                  "required extension first.");
  }
  if (!pending_indexes_.empty() && !ckp_) {
    return Status(StatusCode::ERR_INTERNAL_ERROR,
                  "Cannot preserve pending indexes without a previous "
                  "checkpoint");
  }

  const CheckpointManifest* previous = ckp_ ? &ckp_->manifest() : nullptr;
  for (const auto& [_, pending] : pending_indexes_) {
    if (previous == nullptr || !previous->HasModule(pending.key)) {
      return Status(StatusCode::ERR_INTERNAL_ERROR,
                    "Cannot preserve pending index module '" + pending.key +
                        "': descriptor is missing from previous checkpoint");
    }
  }
  return Status::OK();
}

void StorageIndexManager::InstallIncrementalCheckpoint(
    std::shared_ptr<Checkpoint> ckp) {
  CheckpointManifest reopen_manifest;
  for (const auto& name : dirty_index_names_) {
    const auto it = indexes_.find(name);
    CHECK(it != indexes_.end());
    CHECK(it->second == nullptr);
    reopen_manifest.ReuseModuleClosureFrom(ckp->manifest(), GetKey(name));
  }

  ModuleBroker reopened_modules;
  reopened_modules.Open(*ckp, reopen_manifest, memory_level_);
  for (const auto& name : dirty_index_names_) {
    indexes_[name] = reopened_modules.TakeModule<StorageIndex>(GetKey(name));
  }
  ckp_ = std::move(ckp);
  dirty_index_names_.clear();
  catalog_dirty_ = false;
}

bool StorageIndexManager::IsIndexModule(const std::string& name) {
  return name.rfind(kIndexPrefix, 0) == 0;
}

std::string StorageIndexManager::GetKey(const std::string& index_name) {
  return std::string(kIndexPrefix) + index_name;
}

std::unique_ptr<StorageIndexManager> StorageIndexManager::Clone() const {
  auto forked = std::make_unique<StorageIndexManager>();
  forked->ckp_ = ckp_;
  forked->memory_level_ = memory_level_;
  for (const auto& [name, index] : indexes_) {
    if (index) {
      auto cloned = index->Clone();
      forked->indexes_[name] = std::unique_ptr<StorageIndex>(
          static_cast<StorageIndex*>(cloned.release()));
    }
  }
  forked->pending_indexes_ = pending_indexes_;
  forked->pending_mutations_ = pending_mutations_;
  forked->dirty_index_names_ = dirty_index_names_;
  forked->catalog_dirty_ = catalog_dirty_;
  return forked;
}

void StorageIndexManager::Clear() {
  indexes_.clear();
  pending_indexes_.clear();
  pending_mutations_.clear();
  dirty_index_names_.clear();
  catalog_dirty_ = false;
  ckp_.reset();
  memory_level_ = MemoryLevel::kInMemory;
}

}  // namespace neug
