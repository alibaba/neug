/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

#include <limits>
#include <memory>
#include <string>

#include "neug/common/extra_type_info.h"
#include "neug/common/types.h"
#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/index/index_id_accessor.h"
#include "neug/utils/property/array_column.h"
#include "neug/utils/property/column.h"

namespace neug {

inline constexpr index_id_t INVALID_OFFSET = INVALID_INDEX_ID;

enum class VectorRepresentation : uint8_t {
  kRaw = 0,
  kL2Normalized = 1,
};

class VecColumn;

class VectorNormalizer {
 public:
  static bool Sample(const VecColumn& column);
  static Status Ensure(VecColumn& column);
  static Status ValueNormalize(const Value& value, Value& normalized);

 private:
  static constexpr double kNormalizedTolerance = 1e-4;
  static constexpr double kMinimumNorm = 1e-12;
  static constexpr size_t kNormalizationSampleSize = 4096;

  static bool IsNormalized(const float* vector, size_t dimension);
  static Status Normalize(float* vector, size_t dimension);
};

// A column specialized for vector data. VecColumn:
// 1. stores fixed-dimensional vectors;
// 2. allocates a new buffer version when growing to avoid copy-on-write; and
// 3. manages offsets shared with HNSW indexes built on this column, allowing
//    the indexes to reuse the same offset as their primary key.
class VecColumn : public ColumnBase {
 public:
  VecColumn();

  /**
   * Constructs a vector column from an existing data buffer.
   *
   * @param buffer The underlying vector-data container, typically shared
   * directly with an ArrayColumn.
   * @param offset_accessor The accessor that maps vertex IDs to offsets in
   * buffer.
   * @param array_type The fixed-size array type stored by the column.
   * @param size The allocated buffer capacity measured in vectors.
   * @param default_value The value used to initialize newly allocated vector
   * slots.
   * @param ckp The checkpoint used to allocate a new buffer when the column
   * grows.
   * @param level The memory level used when allocating a new buffer.
   */
  VecColumn(std::shared_ptr<IDataContainer> buffer,
            std::unique_ptr<IndexIDAccessor> offset_accessor,
            const DataType& array_type, size_t size, const Value& default_value,
            Checkpoint& ckp, MemoryLevel level);

  void Open(Checkpoint& ckp, const ModuleDescriptor& desc,
            MemoryLevel level) override;
  void Open(Checkpoint& ckp, const CheckpointManifest& manifest,
            const ModuleDescriptor& desc, MemoryLevel level) override;
  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override;

  size_t size() const override;
  void resize(size_t size) override;
  void resize(size_t size, const Value& default_value) override;
  DataTypeId type() const override;
  void set_any(size_t vid, const Value& value, bool insert_safe) override;
  Value get_any(size_t vid) const override;

  // Exposes the offset accessor for use by HNSW indexes.
  IndexIDAccessor* get_offset_accessor() const;
  // Exposes the vector-data buffer for use by HNSW indexes.
  const void* get_buffer_ptr() const;
  uint64_t array_size() const;
  DataType array_type() const;
  VectorRepresentation representation() const { return representation_; }
  bool is_l2_normalized() const {
    return representation_ == VectorRepresentation::kL2Normalized;
  }

  // Deterministically samples active vectors. This is an
  // optimization hint only and does not prove that every vector is normalized.
  bool SampleIsL2Normalized() const { return VectorNormalizer::Sample(*this); }
  // Marks the column as L2-normalized when sampling succeeds. Otherwise,
  // validates and normalizes all active vectors in one pass using
  // a private replacement buffer.
  Status EnsureL2Normalized() { return VectorNormalizer::Ensure(*this); }

  std::unique_ptr<Module> Clone() const override;
  void Detach(Checkpoint& ckp, MemoryLevel level) override;

  std::string ModuleTypeName() const override;
  static std::string type_name();

 private:
  friend class VectorNormalizer;

  void openInternal(Checkpoint& ckp, const CheckpointManifest* manifest,
                    const ModuleDescriptor& desc, MemoryLevel level);
  void validatePodType() const;
  void validateState() const;
  void validateValue(const Value& value) const;

  std::unique_ptr<IndexIDAccessor> offset_accessor_;
  std::shared_ptr<IDataContainer> buffer_;
  Checkpoint* ckp_;
  MemoryLevel level_;
  DataType array_type_;
  size_t size_;
  Value default_value_;
  VectorRepresentation representation_;
};

class VecRefColumn : public RefColumnBase {
 public:
  explicit VecRefColumn(const VecColumn& column) : column_(column) {}
  Value get_any(size_t index) const override { return column_.get_any(index); }
  DataTypeId type() const override { return DataTypeId::kArray; }
  ColType col_type() const override { return ColType::kInternal; }

 private:
  const VecColumn& column_;
};

}  // namespace neug
