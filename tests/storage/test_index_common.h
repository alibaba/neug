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

#pragma once

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "neug/common/types/value.h"
#include "neug/storages/checkpoint.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/container/i_container.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/index/storage_index.h"
#include "neug/storages/module/module.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

inline constexpr const char* kExampleIndexType = "example_index";
inline constexpr const char* kIndexBufferPath = "example_index_buffer";

struct PersonRow {
  int64_t id;
  std::string name;
  int32_t age;
};

inline const std::vector<PersonRow> kPersons = {
    {1, "Alice", 30}, {2, "Bob", 25}, {3, "Charlie", 30},
    {4, "Diana", 40}, {5, "Eve", 25},
};

struct ExampleIndexQueryParams : public IndexQueryParams {
  explicit ExampleIndexQueryParams(int32_t target) : target_value(target) {}
  int32_t target_value;
};

class ExampleIndex : public StorageIndex {
 public:
  Status Rebind(const IndexBindContext& context) override {
    if (!context.column) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "ExampleIndex requires a property column binding");
    }
    bound_column_ = context.column;
    return Status::OK();
  }

  bool IsBound() const { return bound_column_ != nullptr; }
  const ColumnBase* BoundColumn() const { return bound_column_; }

  void Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
            MemoryLevel level) override {
    StorageIndex::Open(ckp, descriptor, level);
    if (!index_id_accessor_) {
      index_id_accessor_ = std::make_unique<DefaultIndexIDAccessor>();
    }
    index_id_accessor_->Open(ckp, descriptor, level);
    index_buffer_ = std::shared_ptr<IDataContainer>(ckp.OpenFile(
        descriptor.get_path(kIndexBufferPath).value_or(""), level));
  }

  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override {
    StorageIndex::Dump(ckp, meta, key);
    auto descriptor = meta.module(key).value_or(ModuleDescriptor{});
    descriptor.module_type = ModuleTypeName();
    if (index_id_accessor_) {
      CheckpointManifest accessor_meta;
      index_id_accessor_->Dump(ckp, accessor_meta, "index_id_accessor");
      auto accessor_desc = accessor_meta.module("index_id_accessor");
      if (auto next_index_id = accessor_desc->get("next_index_id")) {
        descriptor.set("next_index_id", *next_index_id);
      }
      if (auto index_id_to_vid = accessor_desc->get_path("index_id_to_vid")) {
        descriptor.set_path("index_id_to_vid", *index_id_to_vid);
      }
    }
    if (index_buffer_) {
      descriptor.set_path(kIndexBufferPath, ckp.Commit(*index_buffer_));
    }
    meta.set_module(key, std::move(descriptor));
  }

  void Detach(Checkpoint& ckp, MemoryLevel level) override {
    if (index_id_accessor_) {
      index_id_accessor_->Detach(ckp, level);
    }
  }

  std::unique_ptr<Module> Clone() const override {
    auto cloned = std::make_unique<ExampleIndex>();
    if (meta_) {
      cloned->meta_ = std::make_unique<IndexMeta>(*meta_);
    }
    if (index_id_accessor_) {
      auto cloned_accessor = index_id_accessor_->Clone();
      cloned->index_id_accessor_ = std::unique_ptr<IndexIDAccessor>(
          static_cast<IndexIDAccessor*>(cloned_accessor.release()));
    }
    cloned->index_buffer_ = index_buffer_;
    cloned->bound_column_ = bound_column_;
    return cloned;
  }

 protected:
  result<std::vector<index_id_t>> SearchImpl(
      const IndexQueryParams& params) override {
    const auto* example_params =
        dynamic_cast<const ExampleIndexQueryParams*>(&params);
    if (!example_params) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "ExampleIndex requires ExampleIndexQueryParams");
    }
    if (!index_buffer_ || !index_id_accessor_ || !bound_column_) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INTERNAL_ERROR,
                          "ExampleIndex is not open");
    }
    if (!meta_) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INTERNAL_ERROR,
                          "Index metadata is not initialized");
    }

    std::vector<index_id_t> results;
    const auto* values = static_cast<const int32_t*>(index_buffer_->GetData());
    auto* accessor =
        dynamic_cast<DefaultIndexIDAccessor*>(index_id_accessor_.get());
    if (!accessor) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INTERNAL_ERROR,
                          "ExampleIndex requires DefaultIndexIDAccessor");
    }
    for (index_id_t index_id = 0; index_id < accessor->size(); ++index_id) {
      if (values[index_id] == example_params->target_value) {
        results.push_back(index_id);
      }
    }
    return results;
  }

  Status AppendImpl(index_id_t index_id, const Value& value) override {
    if (value.IsNull() || value.type().id() != DataTypeId::kInt32) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "ExampleIndex requires one non-null INT32 value");
    }
    if (!index_buffer_) {
      return Status::InternalError("ExampleIndex is not open");
    }
    if (index_id >= capacity()) {
      Resize(std::max(capacity() * 2, kInitialCapacity));
    }
    auto* index_data = static_cast<int32_t*>(index_buffer_->GetData());
    index_data[index_id] = value.GetValue<int32_t>();
    return Status::OK();
  }

 private:
  static constexpr size_t kInitialCapacity = 1024;

  size_t capacity() const {
    return index_buffer_ ? index_buffer_->GetDataSize() / sizeof(int32_t) : 0;
  }

  void Resize(size_t new_capacity) {
    if (new_capacity > capacity()) {
      index_buffer_->Resize(new_capacity * sizeof(int32_t));
    }
  }

  std::shared_ptr<IDataContainer> index_buffer_;
  const ColumnBase* bound_column_ = nullptr;
};

}  // namespace neug
