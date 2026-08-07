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
#include <limits>
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
#include "neug/utils/property/vec_column.h"
#include "neug/utils/result.h"

namespace neug {

inline constexpr const char* kExampleIndexType = "example_index";
inline constexpr const char* kVecIndexType = "hnsw_index";
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
  index_id_t GetIndexID(vid_t vid) const {
    return index_id_accessor_ ? index_id_accessor_->GetIndexIDByVID(vid)
                              : INVALID_INDEX_ID;
  }
  index_id_t GetNextIndexID() const {
    const auto* accessor =
        dynamic_cast<const DefaultIndexIDAccessor*>(index_id_accessor_.get());
    return accessor ? accessor->GetNextIndexID() : 0;
  }

  Status BulkBuild(const VertexSet& vertices) override {
    if (!bound_column_) {
      return Status::InternalError("ExampleIndex is not bound");
    }
    if (!index_buffer_ || !index_id_accessor_) {
      return Status::InternalError("ExampleIndex is not open");
    }
    for (vid_t vid : vertices) {
      RETURN_IF_NOT_OK(Upsert(vid, bound_column_->get_any(vid)));
    }
    return Status::OK();
  }

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
      if (auto next_index_id =
              accessor_desc->get(ModuleDescriptor::kNextIndexId)) {
        descriptor.set(ModuleDescriptor::kNextIndexId, *next_index_id);
      }
      if (auto vid_to_index_id =
              accessor_desc->get_path(ModuleDescriptor::kVidToIndexIdPath)) {
        descriptor.set_path(ModuleDescriptor::kVidToIndexIdPath,
                            *vid_to_index_id);
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
  result<std::vector<SearchCandidate>> SearchImpl(
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

    std::vector<SearchCandidate> results;
    const auto* values = static_cast<const int32_t*>(index_buffer_->GetData());
    const auto* accessor =
        dynamic_cast<const DefaultIndexIDAccessor*>(index_id_accessor_.get());
    if (!accessor) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INTERNAL_ERROR,
                          "ExampleIndex requires DefaultIndexIDAccessor");
    }
    for (index_id_t index_id = 0; index_id < accessor->GetNextIndexID();
         ++index_id) {
      if (values[index_id] == example_params->target_value) {
        results.push_back({index_id});
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
    if (index_id >= size()) {
      Resize(index_id < 4096 ? 4096 : index_id + index_id / 4);
    }
    auto* index_data = static_cast<int32_t*>(index_buffer_->GetData());
    index_data[index_id] = value.GetValue<int32_t>();
    return Status::OK();
  }

 private:
  size_t size() const {
    return index_buffer_ ? index_buffer_->GetDataSize() / sizeof(int32_t) : 0;
  }

  void Resize(size_t new_capacity) {
    if (new_capacity > size()) {
      index_buffer_->Resize(new_capacity * sizeof(int32_t));
    }
  }

  std::shared_ptr<IDataContainer> index_buffer_;
  const ColumnBase* bound_column_ = nullptr;
};

struct VecIndexQueryParams : public IndexQueryParams {
  explicit VecIndexQueryParams(std::vector<float> query)
      : query(std::move(query)) {}

  std::vector<float> query;
};

class VecSource {
 public:
  VecSource(const VecColumn* column, size_t vector_count)
      : column_(column),
        dimension_(column ? column->array_size() : 0),
        vector_count_(vector_count) {}

  const void* get_vector(index_id_t node_id) const {
    if (!column_ || node_id >= vector_count_) {
      return nullptr;
    }
    const auto* buffer = static_cast<const float*>(column_->get_buffer_ptr());
    return buffer ? buffer + static_cast<size_t>(node_id) * dimension_
                  : nullptr;
  }

  size_t dimension() const { return dimension_; }
  size_t vector_count() const { return vector_count_; }
  void set_vector_count(size_t vector_count) { vector_count_ = vector_count; }

 private:
  const VecColumn* column_ = nullptr;
  size_t dimension_ = 0;
  size_t vector_count_ = 0;
};

class VecIndex final : public StorageIndex {
 public:
  Status Rebind(const IndexBindContext& context) override {
    auto* column = dynamic_cast<const VecColumn*>(context.column);
    if (!column || ArrayType::GetChildType(column->array_type()).id() !=
                       DataTypeId::kFloat) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "VecIndex requires a FLOAT VecColumn");
    }
    auto* mutable_column = const_cast<VecColumn*>(column);
    index_id_accessor_ = std::make_unique<VecColumnBackedIndexIDAccessor>(
        *mutable_column->get_offset_accessor());
    source_ = std::make_unique<VecSource>(column,
                                          index_id_accessor_->GetNextIndexID());
    return Status::OK();
  }

  Status BulkBuild(const VertexSet&) override { return Status::OK(); }

  void Detach(Checkpoint&, MemoryLevel) override {}

  std::unique_ptr<Module> Clone() const override {
    auto cloned = std::make_unique<VecIndex>();
    if (meta_) {
      cloned->meta_ = std::make_unique<IndexMeta>(*meta_);
    }
    if (index_id_accessor_) {
      auto accessor = index_id_accessor_->Clone();
      cloned->index_id_accessor_.reset(
          static_cast<IndexIDAccessor*>(accessor.release()));
    }
    return cloned;
  }

 protected:
  result<std::vector<SearchCandidate>> SearchImpl(
      const IndexQueryParams& params) override {
    const auto* vec_params = dynamic_cast<const VecIndexQueryParams*>(&params);
    if (!vec_params) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "VecIndex requires VecIndexQueryParams");
    }
    if (!source_) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INTERNAL_ERROR,
                          "VecIndex is not bound to a VecColumn");
    }
    if (vec_params->query.size() != source_->dimension()) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "VecIndex query dimension does not match the column");
    }

    index_id_t best_id = INVALID_INDEX_ID;
    double best_distance = std::numeric_limits<double>::max();
    for (index_id_t node_id = 0; node_id < source_->vector_count(); ++node_id) {
      if (index_id_accessor_->GetVIDByIndexID(node_id) == INVALID_VID) {
        continue;
      }
      const auto* vector =
          static_cast<const float*>(source_->get_vector(node_id));
      double distance = 0.0;
      for (size_t i = 0; i < source_->dimension(); ++i) {
        const double delta =
            static_cast<double>(vector[i]) - vec_params->query[i];
        distance += delta * delta;
      }
      if (distance < best_distance) {
        best_id = node_id;
        best_distance = distance;
      }
    }
    if (best_id == INVALID_INDEX_ID) {
      return std::vector<SearchCandidate>{};
    }
    return std::vector<SearchCandidate>{{best_id, best_distance}};
  }

  Status AppendImpl(index_id_t, const Value&) override {
    // VecIndex stores no index data. VecSource observes values directly from
    // the bound VecColumn buffer.
    if (source_) {
      source_->set_vector_count(index_id_accessor_->GetNextIndexID());
    }
    return Status::OK();
  }

 private:
  std::unique_ptr<VecSource> source_;
};

}  // namespace neug
