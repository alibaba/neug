/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/index/storage_index.h"
#include "neug/utils/property/vec_column.h"

namespace neug {

inline constexpr const char* kVecIndexType = "hnsw_index";

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
  static constexpr const char* type_name() { return kVecIndexType; }

  Status Rebind(const IndexBindContext& context) override {
    if (context.columns.size() != 1) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "VecIndex requires exactly one property column");
    }
    auto* column = dynamic_cast<const VecColumn*>(context.columns[0]);
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

  Status Upsert(vid_t vid, const IndexValue& new_value) override {
    if (new_value.column_id != 0) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "VecIndex column id is out of range");
    }
    if (new_value.value.IsNull()) {
      return Delete(vid);
    }
    auto index_id = index_id_accessor_->UpsertVID(vid);
    return AppendImpl(index_id, IndexValues{new_value.value});
  }

  void Dump(Checkpoint& ckp, CheckpointManifest& manifest,
            const std::string& key) override {
    StorageIndex::Dump(ckp, manifest, key);
    manifest.FindMutableModule(key)->required = false;
  }

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

  Status AppendImpl(index_id_t, const IndexValues& values) override {
    if (values.size() != 1) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "VecIndex requires exactly one value");
    }
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
