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

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "neug/utils/property/column.h"

namespace neug {

struct list_entry {
  uint64_t offset;
  uint64_t length;
};

class ListColumn : public ColumnBase {
 public:
  static constexpr uint16_t DEFAULT_AVG_LEN = 64;

  ListColumn() : size_(0), child_frontier_(0), list_type_(DataType{}) {}
  explicit ListColumn(const DataType& list_type)
      : size_(0), child_frontier_(0), list_type_(list_type) {
    if (list_type_.id() == DataTypeId::kList) {
      auto child_type = ListType::GetChildType(list_type_);
      child_column_ = CreateColumn(child_type);
    }
  }
  ListColumn(ListColumn&& rhs)
      : items_buffer_(std::move(rhs.items_buffer_)),
        child_column_(std::move(rhs.child_column_)),
        size_(rhs.size_),
        child_frontier_(rhs.child_frontier_.load()),
        list_type_(rhs.list_type_) {
    rhs.size_ = 0;
    rhs.child_frontier_.store(0);
  }

  ~ListColumn() = default;

  void Open(Checkpoint& ckp, const ModuleDescriptor& desc,
            MemoryLevel level) override {
    auto offsets_path = desc.get_path("offsets");
    if (offsets_path.has_value() && !offsets_path.value().empty()) {
      auto offsets_buf = ckp.OpenFile(offsets_path.value(), level);
      size_t num_offsets = offsets_buf->GetDataSize() / sizeof(uint64_t);
      if (num_offsets == 0) {
        size_ = 0;
        child_frontier_.store(0);
      } else {
        size_ = num_offsets - 1;
        auto* offsets =
            reinterpret_cast<const uint64_t*>(offsets_buf->GetData());
        child_frontier_.store(offsets[size_]);

        items_buffer_ = ckp.OpenFile("", level);
        items_buffer_->Resize(size_ * sizeof(list_entry));
        auto* entries = reinterpret_cast<list_entry*>(items_buffer_->GetData());
        for (size_t i = 0; i < size_; ++i) {
          entries[i] = {offsets[i], offsets[i + 1] - offsets[i]};
        }
      }
    } else {
      items_buffer_ = ckp.OpenFile("", level);
      size_ = 0;
      child_frontier_.store(0);
    }

    if (child_column_ &&
        !desc.get_path(ModuleDescriptor::kDataPath).has_value()) {
      child_column_->Open(ckp, ModuleDescriptor{}, level);
    }
  }

  void Close() {
    items_buffer_.reset();
    child_column_.reset();
  }

  ModuleDescriptor Dump(Checkpoint& ckp) override {
    ModuleDescriptor desc;
    desc.module_type = ModuleTypeName();
    if (!items_buffer_) {
      THROW_RUNTIME_ERROR("List column items buffer not initialized for dump");
    }
    compact(ckp);

    // Write cumulative offsets to disk
    auto offsets_buf = ckp.OpenFile("", MemoryLevel::kInMemory);
    offsets_buf->Resize((size_ + 1) * sizeof(uint64_t));
    auto* offsets = reinterpret_cast<uint64_t*>(offsets_buf->GetData());
    offsets[0] = 0;
    for (size_t i = 0; i < size_; ++i) {
      auto entry = get_entry(i);
      offsets[i + 1] = offsets[i] + entry.length;
    }

    desc.set_path("offsets", ckp.Commit(*offsets_buf));
    return desc;
  }

  void DumpTo(Checkpoint& ckp, CheckpointManifest& meta,
              const std::string& key) override;

  void RestoreChildren(ModuleBroker& store, const std::string& key) override;

  size_t size() const override { return size_; }

  void resize(size_t size) override {
    items_buffer_->Resize(size * sizeof(list_entry));
    if (child_column_) {
      uint64_t needed = child_frontier_.load() +
                        (size > size_ ? (size - size_) : 0) * DEFAULT_AVG_LEN;
      if (needed > child_column_->size()) {
        child_column_->resize(needed);
      }
    }
    size_ = size;
  }

  void resize(size_t size, const execution::Value& default_value) override {
    size_t old_size = size_;
    items_buffer_->Resize(size * sizeof(list_entry));
    size_ = size;

    if (old_size >= size) {
      return;
    }

    if (default_value.IsNull() ||
        default_value.type().id() != DataTypeId::kList) {
      for (size_t i = old_size; i < size; ++i) {
        set_entry(i, {0, 0});
      }
      return;
    }

    const auto& children = execution::ListValue::GetChildren(default_value);
    uint64_t L = children.size();

    if (L == 0) {
      for (size_t i = old_size; i < size; ++i) {
        set_entry(i, {0, 0});
      }
      return;
    }

    uint64_t shared_start = child_frontier_.fetch_add(L);
    ensure_child_capacity(shared_start + L);
    for (uint64_t j = 0; j < L; ++j) {
      child_column_->set_any(shared_start + j, children[j], true);
    }
    for (size_t i = old_size; i < size; ++i) {
      set_entry(i, {shared_start, L});
    }
  }

  DataTypeId type() const override { return DataTypeId::kList; }
  DataType list_data_type() const { return list_type_; }

  void set_any(size_t idx, const execution::Value& value,
               bool insert_safe) override {
    if (idx >= size_) {
      THROW_RUNTIME_ERROR("Index out of range");
    }
    if (value.IsNull()) {
      set_entry(idx, {0, 0});
      return;
    }
    const auto& children = execution::ListValue::GetChildren(value);
    uint64_t L = children.size();
    if (L == 0) {
      set_entry(idx, {0, 0});
      return;
    }
    uint64_t start = child_frontier_.fetch_add(L);
    if (start + L > child_column_->size()) {
      if (insert_safe) {
        size_t new_cap = std::max(static_cast<size_t>(start + L),
                                  child_column_->size() * 3 / 2);
        child_column_->resize(new_cap);
      } else {
        THROW_STORAGE_EXCEPTION(
            "Not enough child capacity and insert_safe is false");
      }
    }
    for (uint64_t j = 0; j < L; ++j) {
      child_column_->set_any(start + j, children[j], true);
    }
    set_entry(idx, {start, L});
  }

  execution::Value get_any(size_t idx) const override {
    assert(idx < size_);
    auto entry = get_entry(idx);
    if (entry.length == 0) {
      return execution::Value::LIST(ListType::GetChildType(list_type_), {});
    }
    std::vector<execution::Value> children;
    children.reserve(entry.length);
    for (uint64_t j = 0; j < entry.length; ++j) {
      children.push_back(child_column_->get_any(entry.offset + j));
    }
    return execution::Value::LIST(ListType::GetChildType(list_type_),
                                  std::move(children));
  }

  void ingest(uint32_t index, OutArchive& arc) override {
    execution::Value v;
    arc >> v;
    set_any(index, v, true);
  }

  const DataType& list_type() const { return list_type_; }
  void SetListType(const DataType& t) {
    list_type_ = t;
    if (!child_column_ && list_type_.id() == DataTypeId::kList) {
      child_column_ = CreateColumn(ListType::GetChildType(list_type_));
    }
  }
  void SetChildColumn(std::unique_ptr<ColumnBase> col) {
    child_column_ = std::move(col);
  }

  ColumnBase* child_column() const { return child_column_.get(); }
  uint64_t child_frontier() const { return child_frontier_.load(); }

  std::string ModuleTypeName() const override { return type_name(); }

  static std::string type_name() { return "column<list>"; }

 private:
  void compact(Checkpoint& ckp) {
    if (!child_column_ || size_ == 0) {
      return;
    }
    uint64_t frontier = child_frontier_.load();
    uint64_t total_len = 0;
    for (size_t i = 0; i < size_; ++i) {
      total_len += get_entry(i).length;
    }
    if (total_len == frontier && is_sequential()) {
      return;
    }

    auto child_type = ListType::GetChildType(list_type_);
    auto fresh = CreateColumn(child_type);
    fresh->Open(ckp, ModuleDescriptor{}, MemoryLevel::kInMemory);
    fresh->resize(total_len);

    uint64_t new_offset = 0;
    for (size_t i = 0; i < size_; ++i) {
      auto entry = get_entry(i);
      if (entry.length == 0) {
        set_entry(i, {new_offset, 0});
        continue;
      }
      for (uint64_t j = 0; j < entry.length; ++j) {
        fresh->set_any(new_offset + j, child_column_->get_any(entry.offset + j),
                       true);
      }
      set_entry(i, {new_offset, entry.length});
      new_offset += entry.length;
    }

    child_column_ = std::move(fresh);
    child_frontier_.store(new_offset);
  }

  bool is_sequential() const {
    uint64_t expected_offset = 0;
    for (size_t i = 0; i < size_; ++i) {
      auto entry = get_entry(i);
      if (entry.offset != expected_offset) {
        return false;
      }
      expected_offset += entry.length;
    }
    return true;
  }

  void ensure_child_capacity(uint64_t needed) {
    if (!child_column_) {
      THROW_RUNTIME_ERROR("ListColumn child_column_ is null");
    }
    if (needed > child_column_->size()) {
      child_column_->resize(
          std::max(static_cast<size_t>(needed), child_column_->size() * 3 / 2));
    }
  }

  inline list_entry get_entry(size_t idx) const {
    assert(idx < size_);
    return reinterpret_cast<const list_entry*>(items_buffer_->GetData())[idx];
  }

  inline void set_entry(size_t idx, const list_entry& item) {
    assert(idx < size_);
    reinterpret_cast<list_entry*>(items_buffer_->GetData())[idx] = item;
  }

  std::unique_ptr<IDataContainer> items_buffer_;
  std::unique_ptr<ColumnBase> child_column_;
  size_t size_;
  std::atomic<uint64_t> child_frontier_;
  DataType list_type_;
};

class ListRefColumn : public RefColumnBase {
 public:
  explicit ListRefColumn(const ListColumn& column) : column_(column) {}
  ~ListRefColumn() override = default;

  execution::Value get_any(size_t index) const override {
    return column_.get_any(index);
  }

  DataTypeId type() const override { return DataTypeId::kList; }

  DataType list_data_type() const { return column_.list_type(); }

  ColType col_type() const override { return ColType::kInternal; }

 private:
  const ListColumn& column_;
};

}  // namespace neug
