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
#include "neug/storages/module/module_factory.h"

namespace neug {

static constexpr const char* kIndexPrefix = "index_";

static Status PendingIndexError(const std::string& name,
                                const std::string& operation) {
  return Status(
      StatusCode::ERR_ILLEGAL_OPERATION,
      "Cannot " + operation + " pending index '" + name +
          "' before its module is loaded. If this is an HNSW index, execute "
          "LOAD vector_search first.");
}

neug::result<StorageIndex*> StorageIndexManager::CreateIndex(
    std::unique_ptr<IndexMeta> meta,
    std::unique_ptr<IndexIDAccessor> index_id_accessor,
    const ColumnBase* column, const VertexSet& vertex_set) {
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
  return raw_ptr;
}

Status StorageIndexManager::DropIndex(const std::string& name) {
  if (pending_indexes_.count(name) > 0) {
    return PendingIndexError(name, "drop");
  }
  auto it = indexes_.find(name);
  if (it == indexes_.end()) {
    return Status(StatusCode::ERR_NOT_FOUND, "Index not found: " + name);
  }
  indexes_.erase(it);
  return Status::OK();
}

neug::result<std::vector<StorageIndex*>> StorageIndexManager::GetIndex(
    label_t label_id, const std::string& property_name) const {
  for (const auto& [name, pending] : pending_indexes_) {
    if (pending.meta.schema.label_id == label_id &&
        pending.meta.schema.property_name == property_name) {
      return tl::unexpected(PendingIndexError(name, "access"));
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

neug::result<StorageIndex*> StorageIndexManager::GetIndexByName(
    const std::string& name) const {
  if (pending_indexes_.count(name) > 0) {
    return tl::unexpected(PendingIndexError(name, "access"));
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
  const CheckpointManifest& meta = ckp_->GetMeta();
  for (const auto& [key, desc] : meta.modules()) {
    if (!IsIndexModule(key)) {
      continue;
    }

    auto index = store.TakeModule<StorageIndex>(key, false);
    if (!index) {
      auto index_meta = desc.get("index_meta");
      if (!index_meta) {
        THROW_RUNTIME_ERROR("Index module '" + key +
                            "' has no persisted index_meta");
      }
      auto parsed_meta = IndexMeta::FromJsonString(*index_meta);
      auto name = parsed_meta.name;
      pending_indexes_.emplace(name,
                               PendingIndex{key, desc, std::move(parsed_meta)});
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

neug::result<std::vector<StorageIndex*>>
StorageIndexManager::ActivateIndexes() {
  std::vector<StorageIndex*> activated;
  auto& factory = ModuleFactory::instance();
  for (auto it = pending_indexes_.begin(); it != pending_indexes_.end();) {
    auto module = factory.Create(it->second.descriptor.module_type);
    if (!module) {
      ++it;
      continue;
    }
    if (!dynamic_cast<StorageIndex*>(module.get())) {
      RETURN_STATUS_ERROR(
          StatusCode::ERR_SCHEMA_MISMATCH,
          "Module is not an index type: " + it->second.descriptor.module_type);
    }
    module->Open(*ckp_, ckp_->GetMeta(), it->second.descriptor, memory_level_);
    std::unique_ptr<StorageIndex> index(
        static_cast<StorageIndex*>(module.release()));
    const auto name = it->first;
    auto* ptr = index.get();
    indexes_[name] = std::move(index);
    activated.push_back(ptr);
    it = pending_indexes_.erase(it);
    LOG(INFO) << "Activated pending index: " << name;
  }
  return activated;
}

void StorageIndexManager::Dump(ModuleBroker& store) {
  if (!pending_indexes_.empty()) {
    THROW_RUNTIME_ERROR(
        "Cannot create a checkpoint while extension-backed indexes are "
        "pending. Load the required extension first.");
  }
  for (auto& [name, index] : indexes_) {
    if (!index)
      continue;

    std::string key = GetKey(name);
    store.SetModule(key, std::move(index));
  }
  indexes_.clear();
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
  return forked;
}

void StorageIndexManager::Clear() {
  indexes_.clear();
  pending_indexes_.clear();
  ckp_.reset();
  memory_level_ = MemoryLevel::kInMemory;
}

}  // namespace neug
