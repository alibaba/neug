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

#include "neug/storages/index/index_manager.h"

#include <algorithm>
#include <cctype>

#include <glog/logging.h>

#include "neug/compiler/common/string_utils.h"
#include "neug/storages/module/module_factory.h"

namespace neug {

static constexpr const char* kIndexPrefix = "index_";

neug::result<Index*> IndexManager::CreateIndex(
    const std::string& name, std::unique_ptr<IndexMeta> meta) {
  if (!meta) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Cannot create index with null metadata");
  }
  if (indexes_.count(name) > 0) {
    RETURN_STATUS_ERROR(StatusCode::ERR_SCHEMA_MISMATCH,
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
  auto* raw_index = dynamic_cast<Index*>(module.get());
  if (!raw_index) {
    RETURN_STATUS_ERROR(StatusCode::ERR_SCHEMA_MISMATCH,
                        "Module is not an index type: " + module_type);
  }
  std::unique_ptr<Index> index(dynamic_cast<Index*>(module.release()));
  ModuleDescriptor desc;
  desc.module_type = module_type;
  index->SetMeta(std::move(meta));
  index->Open(*ckp_, desc, memory_level_);

  auto* raw_ptr = index.get();
  indexes_[name] = std::move(index);
  return raw_ptr;
}

Status IndexManager::DropIndex(const std::string& name) {
  auto it = indexes_.find(name);
  if (it == indexes_.end()) {
    return Status::RuntimeError("Index not found: " + name);
  }
  indexes_.erase(it);
  return Status::OK();
}

Status IndexManager::GetIndex(label_t label_id,
                              const std::vector<std::string>& property_names,
                              std::vector<Index*>& target_indexes) const {
  for (const auto& [name, index] : indexes_) {
    if (!index)
      continue;
    const auto& meta = index->GetMeta();
    if (meta.schema.label_id != label_id) {
      continue;
    }
    bool match = true;
    for (const auto& prop : property_names) {
      bool found = false;
      for (const auto& idx_prop : meta.schema.property_names) {
        if (idx_prop == prop) {
          found = true;
          break;
        }
      }
      if (!found) {
        match = false;
        break;
      }
    }
    if (match) {
      target_indexes.push_back(index.get());
    }
  }
  return Status::OK();
}

Index* IndexManager::GetIndexByName(const std::string& name) const {
  auto it = indexes_.find(name);
  if (it == indexes_.end() || !it->second) {
    return nullptr;
  }
  return it->second.get();
}

Status IndexManager::GetAllIndexes(std::vector<Index*>& target_indexes) const {
  for (const auto& [name, index] : indexes_) {
    if (index) {
      target_indexes.push_back(index.get());
    }
  }
  return Status::OK();
}

void IndexManager::Open(std::shared_ptr<Checkpoint> ckp, ModuleBroker& store,
                        MemoryLevel level) {
  Clear();
  ckp_ = std::move(ckp);
  memory_level_ = level;
  const CheckpointManifest& meta = ckp_->GetMeta();
  for (const auto& [key, desc] : meta.modules()) {
    if (!IsIndexModule(key)) {
      continue;
    }

    auto index = store.TakeModule<Index>(key, false);
    if (!index) {
      LOG(WARNING) << "Index module not found in broker: " << key;
      continue;
    }

    auto name = index->GetMeta().name;
    indexes_[name] = std::move(index);
    LOG(INFO) << "Opened index: " << name << " (type=" << desc.module_type
              << ")";
  }
}

void IndexManager::Dump(std::shared_ptr<Checkpoint> ckp, ModuleBroker& store,
                        CheckpointManifest&) {
  for (auto& [name, index] : indexes_) {
    if (!index)
      continue;

    std::string key = GetKey(name);
    store.SetModule(key, std::move(index));
  }
}

bool IndexManager::IsIndexModule(const std::string& name) {
  return name.starts_with(kIndexPrefix);
}

std::string IndexManager::GetKey(const std::string& index_name) {
  return std::string(kIndexPrefix) + index_name;
}

std::unique_ptr<IndexManager> IndexManager::Fork() const {
  auto forked = std::make_unique<IndexManager>();
  forked->ckp_ = ckp_;
  forked->memory_level_ = memory_level_;
  for (const auto& [name, index] : indexes_) {
    if (index) {
      auto cloned = index->Clone();
      forked->indexes_[name] =
          std::unique_ptr<Index>(static_cast<Index*>(cloned.release()));
    }
  }
  return forked;
}

void IndexManager::Clear() { indexes_.clear(); }

}  // namespace neug
