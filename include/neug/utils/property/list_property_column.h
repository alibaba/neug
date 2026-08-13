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

#include "neug/common/types.h"
#include "neug/common/types/value.h"
#include "neug/utils/property/column.h"

namespace neug {

struct list_meta_item {
  uint64_t offset : 48;
  uint64_t length : 16;
};
static_assert(sizeof(list_meta_item) == sizeof(uint64_t),
              "list_meta_item must be 8 bytes to alias uint64_t");

class ListPropertyColumn : public ColumnBase {
 public:
  ListPropertyColumn()
      : elements_tail_(0), items_(std::make_unique<ULongColumn>()) {}
  explicit ListPropertyColumn(const DataType& list_type);
  ~ListPropertyColumn() override = default;

  void Open(Checkpoint& ckp, const ModuleDescriptor& desc,
            MemoryLevel level) override;
  void Open(Checkpoint& ckp, const CheckpointManifest& manifest,
            const ModuleDescriptor& desc, MemoryLevel level) override;

  // Dump consumes the child buffers. Dead top-level element ranges are moved
  // forward in physical-offset order, then the child column is dumped
  // recursively. A direct VARCHAR child moves only string-item metadata, so
  // compaction does not append a second copy of the live string payload.
  // Other child types currently move through get_any/set_any: fixed-width
  // children can still allocate a compact buffer while shrinking, and ARRAY or
  // nested LIST children with variable-width leaves can append payload during
  // relocation. If measurements show that these cases matter, first add
  // targeted representation-level row relocation for variable-width nested
  // children or prefix/range dumping for fixed-width children; introduce a
  // common nested-column compaction interface only when concrete consumers
  // justify it.
  void Dump(Checkpoint& ckp, CheckpointManifest& meta,
            const std::string& key) override;

  // Row count is derived from the items column: one list_meta_item per
  // row and ULongColumn sizes exactly (no spare capacity), so no separate
  // size_ member is needed.  (elements_tail_ IS separate: elements_ keeps
  // spare capacity after doubling resizes.)
  size_t size() const override { return items_->size(); }
  void resize(size_t size) override;
  void resize(size_t size, const Value& default_value) override;

  DataTypeId type() const override { return DataTypeId::kList; }
  // When insert_safe is false (the insert-transaction path), setting a
  // non-empty list on a row whose current list length differs will throw a
  // StorageException if the elements column has no spare capacity.  After
  // loading from a checkpoint, elements_tail_ == elements_->size(), so there
  // is zero spare space and any non-empty list insertion will fail.
  // See storages/README.md §5.4 for details.
  void set_any(size_t index, const Value& value, bool insert_safe) override;
  Value get_any(size_t index) const override;

  std::unique_ptr<Module> Clone() const override;
  void Detach(Checkpoint& ckp, MemoryLevel level) override;

  std::string ModuleTypeName() const override { return type_name(); }
  static std::string type_name() { return "column<list>"; }

  const DataType& list_type() const { return list_type_; }
  const DataType& child_type() const { return child_type_; }

 private:
  void openInternal(Checkpoint& ckp, const CheckpointManifest* manifest,
                    const ModuleDescriptor& desc, MemoryLevel level);
  ModuleDescriptor dumpSelfDescriptor() const;

  DataType list_type_;
  DataType child_type_;
  size_t elements_tail_;
  std::unique_ptr<ULongColumn> items_;
  std::unique_ptr<ColumnBase> elements_;

  inline list_meta_item get_item(size_t idx) const {
    return reinterpret_cast<const list_meta_item*>(items_->data())[idx];
  }

  inline void set_item(size_t idx, const list_meta_item& item) {
    reinterpret_cast<list_meta_item*>(items_->mutable_data())[idx] = item;
  }
};

class ListPropertyRefColumn : public RefColumnBase {
 public:
  explicit ListPropertyRefColumn(const ListPropertyColumn& column)
      : column_(column) {}

  Value get_any(size_t index) const override { return column_.get_any(index); }
  DataTypeId type() const override { return DataTypeId::kList; }
  ColType col_type() const override { return ColType::kInternal; }

 private:
  const ListPropertyColumn& column_;
};

}  // namespace neug
