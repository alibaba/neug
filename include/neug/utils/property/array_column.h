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

#include <memory>
#include <string>

#include "neug/common/extra_type_info.h"
#include "neug/common/types.h"
#include "neug/execution/common/types/value.h"
#include "neug/utils/property/column.h"

namespace neug {

/**
 * @brief Fixed-length array column backed by a child column.
 *
 * For row i, element j is stored at child_column[i * array_size + j].
 * The ArrayColumn itself has no data buffer; it stores only metadata
 * (array_type, array_size, row count) in its module descriptor.
 * The child column handles actual storage.
 */
class ArrayColumn : public ColumnBase {
 public:
  ArrayColumn() : array_size_(0), size_(0) {}
  explicit ArrayColumn(const DataType& array_type);
  ~ArrayColumn() override = default;

  void Open(Checkpoint& ckp, const ModuleDescriptor& desc,
            MemoryLevel level) override;
  void Open(Checkpoint& ckp, const CheckpointManifest& manifest,
            const ModuleDescriptor& desc, MemoryLevel level) override;

  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override;

  size_t size() const override { return size_; }

  void resize(size_t size) override;
  void resize(size_t size, const execution::Value& default_value) override;

  DataTypeId type() const override { return DataTypeId::kArray; }

  void set_any(size_t index, const execution::Value& value,
               bool insert_safe) override;

  execution::Value get_any(size_t index) const override;

  void ingest(uint32_t index, OutArchive& arc) override;

  std::unique_ptr<Module> Clone() const override;

  void Detach(Checkpoint& ckp, MemoryLevel level) override;

  std::string ModuleTypeName() const override { return type_name(); }

  static std::string type_name() { return "column<array>"; }

 private:
  void openInternal(Checkpoint& ckp, const CheckpointManifest* manifest,
                    const ModuleDescriptor& desc, MemoryLevel level);
  ModuleDescriptor dumpSelfDescriptor() const;

  DataType array_type_;
  uint64_t array_size_;
  size_t size_;
  std::unique_ptr<ColumnBase> child_column_;
};

/**
 * @brief Lightweight reference wrapper for ArrayColumn used in scanning.
 */
class ArrayRefColumn : public RefColumnBase {
 public:
  explicit ArrayRefColumn(const ArrayColumn& column) : column_(column) {}
  ~ArrayRefColumn() override = default;

  execution::Value get_any(size_t index) const override {
    return column_.get_any(index);
  }

  DataTypeId type() const override { return DataTypeId::kArray; }

  ColType col_type() const override { return ColType::kInternal; }

 private:
  const ArrayColumn& column_;
};

}  // namespace neug
