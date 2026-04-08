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

#include <glog/logging.h>
#include <stddef.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <ostream>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "neug/config.h"
#include "neug/storages/container/container_utils.h"
#include "neug/storages/container/i_container.h"
#include "neug/storages/file_names.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/likely.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {
class Table;

std::string_view truncate_utf8(std::string_view str, size_t length);

class ColumnBase {
 public:
  virtual ~ColumnBase() {}

  virtual void open(const std::string& name, const std::string& snapshot_dir,
                    const std::string& work_dir) = 0;

  virtual void open_in_memory(const std::string& name) = 0;

  virtual void open_with_hugepages(const std::string& name) = 0;

  virtual void close() = 0;

  virtual void dump(const std::string& filename) = 0;

  virtual size_t size() const = 0;

  virtual void resize(size_t size) = 0;
  virtual void resize(size_t size, const Property& default_value) = 0;

  virtual DataTypeId type() const = 0;

  // insert_safe is true when the column needs to be resized to accommodate the
  // new value, which can happen when the value is not fixed length. If the
  // value is fixed length, we should already have enough space allocated, so
  // insert_safe can be false.
  virtual void set_any(size_t index, const Property& value,
                       bool insert_safe = false) = 0;

  virtual Property get_prop(size_t index) const = 0;

  virtual void set_prop(size_t index, const Property& prop) {
    LOG(FATAL) << "Not implemented";
  }

  virtual void ingest(uint32_t index, OutArchive& arc) = 0;
};

template <typename T>
class TypedColumn : public ColumnBase {
 public:
  explicit TypedColumn() : size_(0) {}
  ~TypedColumn() { close(); }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    buffer_ = OpenContainer(snapshot_dir + "/" + name, work_dir + "/" + name,
                            MemoryLevel::kSyncToFile);
    size_ = buffer_->GetDataSize() / sizeof(T);
  }

  void open_in_memory(const std::string& name) override {
    buffer_ = OpenContainer(name, "", MemoryLevel::kInMemory);
    size_ = buffer_->GetDataSize() / sizeof(T);
  }

  void open_with_hugepages(const std::string& name) override {
    buffer_ = OpenContainer(name, "", MemoryLevel::kHugePagePrefered);
    size_ = buffer_->GetDataSize() / sizeof(T);
  }

  void close() override { buffer_.reset(); }

  void dump(const std::string& filename) override { buffer_->Dump(filename); }

  size_t size() const override { return size_; }

  void resize(size_t size) override {
    size_ = size;
    buffer_->Resize(size_ * sizeof(T));
  }

  // Assume it is safe to insert the default value even if it is reserving,
  // since user could always override
  void resize(size_t size, const Property& default_value) override {
    if (default_value.type() != type()) {
      THROW_RUNTIME_ERROR("Default value type does not match column type");
    }
    size_t old_size = size_;
    size_ = size;
    buffer_->Resize(size_ * sizeof(T));
    auto default_typed_value = PropUtils<T>::to_typed(default_value);
    for (size_t i = old_size; i < size_; ++i) {
      set_value(i, default_typed_value);
    }
  }

  DataTypeId type() const override { return PropUtils<T>::prop_type(); }

  void set_value(size_t index, const T& val) {
    if (index < size_) {
      reinterpret_cast<T*>(buffer_->GetData())[index] = val;
    } else {
      THROW_RUNTIME_ERROR("Index out of range");
    }
  }

  void set_any(size_t index, const Property& value, bool insert_safe) override {
    // allow resize is ignored for fixed-length types
    set_value(index, PropUtils<T>::to_typed(value));
  }

  inline T get_view(size_t index) const {
    assert(index < size_);
    return reinterpret_cast<const T*>(buffer_->GetData())[index];
  }

  Property get_prop(size_t index) const override {
    return PropUtils<T>::to_prop(get_view(index));
  }

  void set_prop(size_t index, const Property& prop) override {
    set_value(index, PropUtils<T>::to_typed(prop));
  }

  void ingest(uint32_t index, OutArchive& arc) override {
    T val;
    arc >> val;
    set_value(index, val);
  }

  const IDataContainer& buffer() const { return *buffer_; }
  size_t buffer_size() const { return size_; }

 private:
  std::unique_ptr<IDataContainer> buffer_;
  size_t size_;
};

using BoolColumn = TypedColumn<bool>;
using UInt8Column = TypedColumn<uint8_t>;
using UInt16Column = TypedColumn<uint16_t>;
using IntColumn = TypedColumn<int32_t>;
using UIntColumn = TypedColumn<uint32_t>;
using LongColumn = TypedColumn<int64_t>;
using ULongColumn = TypedColumn<uint64_t>;
using DateColumn = TypedColumn<Date>;
using DoubleColumn = TypedColumn<double>;
using FloatColumn = TypedColumn<float>;
using DateTimeColumn = TypedColumn<DateTime>;
using IntervalColumn = TypedColumn<Interval>;

template <>
class TypedColumn<EmptyType> : public ColumnBase {
 public:
  explicit TypedColumn() {}
  ~TypedColumn() {}

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {}
  void open_in_memory(const std::string& name) override {}
  void open_with_hugepages(const std::string& name) override {}
  void dump(const std::string& filename) override {}
  void close() override {}
  size_t size() const override { return 0; }
  void resize(size_t size) override {}
  void resize(size_t size, const Property& default_value) override {}

  DataTypeId type() const override { return DataTypeId::kEmpty; }

  void set_any(size_t index, const Property& value, bool insert_safe) override {
  }

  void set_value(size_t index, const EmptyType& value) {}

  Property get_prop(size_t index) const override { return Property::empty(); }

  void set_prop(size_t index, const Property& prop) override {}

  EmptyType get_view(size_t index) const { return EmptyType(); }

  void ingest(uint32_t index, OutArchive& arc) override {}
};

struct string_item {
  uint64_t offset : 48;
  uint32_t length : 16;
};

template <>
class TypedColumn<std::string_view> : public ColumnBase {
 public:
  TypedColumn(uint16_t width)
      : size_(0), pos_(0), width_(width), type_(DataTypeId::kVarchar) {}
  explicit TypedColumn()
      : size_(0),
        pos_(0),
        width_(STRING_DEFAULT_MAX_LENGTH),
        type_(DataTypeId::kVarchar) {}
  TypedColumn(TypedColumn<std::string_view>&& rhs) {
    items_buffer_ = std::move(rhs.items_buffer_);
    data_buffer_ = std::move(rhs.data_buffer_);
    size_ = rhs.size_;
    pos_ = rhs.pos_.load();
    width_ = rhs.width_;
    type_ = rhs.type_;
  }

  ~TypedColumn() { close(); }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    items_buffer_ = OpenContainer(snapshot_dir + "/" + name + ".items",
                                  work_dir + "/" + name + ".items",
                                  MemoryLevel::kSyncToFile);
    data_buffer_ = OpenContainer(snapshot_dir + "/" + name + ".data",
                                 work_dir + "/" + name + ".data",
                                 MemoryLevel::kSyncToFile);
    size_ = items_buffer_->GetDataSize() / sizeof(string_item);
    init_pos(snapshot_dir + "/" + name + ".pos");
  }

  void open_in_memory(const std::string& prefix) override {
    items_buffer_ =
        OpenContainer(prefix + ".items", "", MemoryLevel::kInMemory);
    data_buffer_ = OpenContainer(prefix + ".data", "", MemoryLevel::kInMemory);
    size_ = items_buffer_->GetDataSize() / sizeof(string_item);
    init_pos(prefix + ".pos");
  }

  void open_with_hugepages(const std::string& prefix) override {
    items_buffer_ =
        OpenContainer(prefix + ".items", "", MemoryLevel::kHugePagePrefered);
    data_buffer_ =
        OpenContainer(prefix + ".data", "", MemoryLevel::kHugePagePrefered);
    size_ = items_buffer_->GetDataSize() / sizeof(string_item);
    init_pos(prefix + ".pos");
  }

  void close() override {
    if (items_buffer_) {
      items_buffer_->Close();
    }
    if (data_buffer_) {
      data_buffer_->Close();
    }
  }

  void dump(const std::string& filename) override {
    if (!items_buffer_ || !data_buffer_) {
      THROW_RUNTIME_ERROR("Buffers not initialized for dumping");
    }
    compact_in_place();
    size_t pos_val = pos_.load();
    write_file(filename + ".pos", &pos_val, sizeof(pos_val), 1);
    items_buffer_->Dump(filename + ".items");
    data_buffer_->Dump(filename + ".data");
    items_buffer_->Close();
    data_buffer_->Close();
  }

  size_t size() const override { return size_; }

  void resize(size_t size) override {
    if (items_buffer_->GetDataSize() == 0) {
      items_buffer_->Resize(size * sizeof(string_item));
      data_buffer_->Resize(
          std::max(size * static_cast<size_t>(width_), pos_.load()));
    } else {
      size_t avg_size = string_avg_size() > 0 ? string_avg_size() : width_;
      items_buffer_->Resize(size * sizeof(string_item));
      data_buffer_->Resize(std::max(size * avg_size, pos_.load()));
    }
    size_ = size;
  }

  void resize(size_t size, const Property& default_value) override {
    if (default_value.type() != type()) {
      THROW_RUNTIME_ERROR("Default value type does not match column type");
    }
    size_t old_size = size_;
    size_ = size;
    auto default_str = PropUtils<std::string_view>::to_typed(default_value);
    default_str = truncate_utf8(default_str, width_);

    size_t new_items = (size > old_size) ? (size - old_size) : 0;
    items_buffer_->Resize(size * sizeof(string_item));
    size_t needed = pos_.load() + new_items * static_cast<size_t>(width_);
    data_buffer_->Resize(std::max(needed, size * string_avg_size()));

    if (default_str.size() == 0) {
      return;
    }

    if (old_size < size_) {
      set_value(old_size, default_str);
      const auto& string_item = get_string_item(old_size);
      for (size_t i = old_size + 1; i < size_; ++i) {
        set_string_item(i, string_item);
      }
    }
  }

  DataTypeId type() const override { return type_; }

  void set_value(size_t idx, const std::string_view& val) {
    auto copied_val = val;
    if (copied_val.size() >= width_) {
      VLOG(1) << "String length" << copied_val.size()
              << " exceeds the maximum length: " << width_ << ", cut off.";
      copied_val = truncate_utf8(copied_val, width_);
    }
    if (idx < size_ &&
        pos_.load() + copied_val.size() <= data_buffer_->GetDataSize()) {
      // NOTE: Even if idx has been set before, we always append the new value
      // to the end of buffer_. The previous value is not reclaimed, and should
      // be handled by garbage collection or compaction.
      size_t offset = pos_.fetch_add(copied_val.size());
      set_string_item(idx, {offset, static_cast<uint32_t>(copied_val.size())});
      assert(offset + copied_val.size() <= data_buffer_->GetDataSize());
      auto raw_data = reinterpret_cast<char*>(data_buffer_->GetData());
      memcpy(raw_data + offset, copied_val.data(), copied_val.size());
    } else {
      THROW_RUNTIME_ERROR("Index out of range or not enough space in buffer");
    }
  }

  // When insert_safe is set to true, concurrency control should be guaranteed
  // by caller.
  void set_any(size_t idx, const Property& value, bool insert_safe) override {
    if (idx >= size_) {
      THROW_RUNTIME_ERROR("Index out of range");
    }
    auto dst_value = value.as_string_view();
    if (pos_.load() + dst_value.size() > data_buffer_->GetDataSize()) {
      size_t new_avg_width = (pos_.load() + idx) / (idx + 1);
      size_t new_len =
          std::max(size_ * new_avg_width, pos_.load() + dst_value.size());
      data_buffer_->Resize(new_len);
    }
    set_value(idx, dst_value);
  }

  inline std::string_view get_view(size_t idx) const {
    const auto& item = get_string_item(idx);
    assert(item.offset + item.length <= data_buffer_->GetDataSize());
    auto raw_data = reinterpret_cast<const char*>(data_buffer_->GetData());
    return std::string_view(raw_data + item.offset, item.length);
  }

  Property get_prop(size_t index) const override {
    return PropUtils<std::string_view>::to_prop(get_view(index));
  }

  void set_prop(size_t index, const Property& prop) override {
    set_value(index, PropUtils<std::string_view>::to_typed(prop));
  }

  void ingest(uint32_t index, OutArchive& arc) override {
    std::string_view val;
    arc >> val;
    set_value(index, val);
  }

  size_t available_space() const {
    if (!data_buffer_) {
      return 0;
    }
    assert(pos_.load() <= data_buffer_->GetDataSize());
    return data_buffer_->GetDataSize() - pos_.load();
  }

 private:
  inline void init_pos(const std::string& file_path) {
    if (std::filesystem::exists(file_path)) {
      size_t pos_val = 0;
      read_file(file_path, &pos_val, sizeof(pos_val), 1);
      pos_.store(pos_val);
    } else {
      pos_.store(0);
    }
  }

  inline string_item get_string_item(size_t idx) const {
    assert(idx < size_);
    auto raw_items =
        reinterpret_cast<const string_item*>(items_buffer_->GetData());
    return raw_items[idx];
  }

  inline void set_string_item(size_t idx, const string_item& item) {
    assert(idx < size_);
    auto raw_items = reinterpret_cast<string_item*>(items_buffer_->GetData());
    raw_items[idx] = item;
  }

  size_t string_avg_size() const {
    if (size_ == 0) {
      return 0;
    }
    size_t total_length = 0;
    size_t non_zero_count = 0;
    for (size_t i = 0; i < size_; ++i) {
      if (get_string_item(i).length > 0) {
        total_length += get_string_item(i).length;
        non_zero_count++;
      }
    }
    return non_zero_count > 0 ? total_length / non_zero_count : 0;
  }

  // Compact data_buffer_ in-place, reclaiming stale bytes left by
  // append-only updates (each set_value appends without freeing the old copy).
  void compact_in_place() {
    if (size_ == 0)
      return;
    size_t unique_total = 0;
    {
      std::unordered_set<uint64_t> seen_offsets;
      for (size_t i = 0; i < size_; ++i) {
        const auto item = get_string_item(i);
        if (item.length != 0 && seen_offsets.insert(item.offset).second) {
          unique_total += item.length;
        }
      }
    }
    if (unique_total == pos_.load()) {
      VLOG(10) << "StringColumn compact_in_place: no stale bytes to reclaim";
      return;
    }
    struct Entry {
      size_t index;
      uint64_t offset;
      uint32_t length;
    };

    std::vector<Entry> entries;
    entries.reserve(size_);
    for (size_t i = 0; i < size_; ++i) {
      const auto item = get_string_item(i);
      entries.push_back({i, item.offset, static_cast<uint32_t>(item.length)});
    }

    std::sort(
        entries.begin(), entries.end(),
        [](const Entry& a, const Entry& b) { return a.offset < b.offset; });

    auto* raw = reinterpret_cast<char*>(data_buffer_->GetData());
    size_t write_ptr = 0;
    // Entries are sorted by offset, so duplicate offsets are always contiguous.
    uint64_t cur_old_offset = UINT64_MAX;
    uint64_t cur_new_offset = 0;

    for (const auto& e : entries) {
      if (e.length == 0) {
        set_string_item(e.index, {0, 0});
        continue;
      }
      if (e.offset != cur_old_offset) {
        if (write_ptr != e.offset) {
          memmove(raw + write_ptr, raw + e.offset, e.length);
        }
        cur_old_offset = e.offset;
        cur_new_offset = write_ptr;
        write_ptr += e.length;
      }
      set_string_item(e.index, {cur_new_offset, e.length});
    }

    size_t old_pos = pos_.load();
    // After compaction, also reserve some extra space based on average string
    // length.
    size_t avg_size = string_avg_size() > 0 ? string_avg_size() : width_;
    data_buffer_->Resize(std::max(size_ * avg_size, write_ptr));
    pos_.store(write_ptr);
    VLOG(1) << "StringColumn compact_in_place: " << old_pos << " -> "
            << write_ptr << " bytes";
  }

  std::unique_ptr<IDataContainer> items_buffer_;
  std::unique_ptr<IDataContainer> data_buffer_;
  size_t size_;
  std::atomic<size_t> pos_;
  uint16_t width_;
  DataTypeId type_;
};

using StringColumn = TypedColumn<std::string_view>;

std::shared_ptr<ColumnBase> CreateColumn(DataType type);

/// Create RefColumn for ease of usage for hqps
class RefColumnBase {
 public:
  enum class ColType {
    kInternal,
    kExternal,
  };
  virtual ~RefColumnBase() {}
  virtual Property get(size_t index) const = 0;
  virtual DataTypeId type() const = 0;
  virtual ColType col_type() const = 0;
};

// RefColumn is a wrapper of TypedColumn
template <typename T>
class TypedRefColumn : public RefColumnBase {
 public:
  using value_type = T;

  explicit TypedRefColumn(const TypedColumn<T>& column)
      : basic_buffer(reinterpret_cast<const T*>(column.buffer().GetData())),
        basic_size(column.buffer_size()) {}
  ~TypedRefColumn() {}

  inline T get_view(size_t index) const {
    assert(index < basic_size);
    return basic_buffer[index];
  }

  Property get(size_t index) const override {
    return PropUtils<T>::to_prop(get_view(index));
  }

  DataTypeId type() const override { return PropUtils<T>::prop_type(); }

  ColType col_type() const override { return ColType::kInternal; }

 private:
  const T* basic_buffer;
  size_t basic_size;
};

template <>
class TypedRefColumn<std::string_view> : public RefColumnBase {
 public:
  using value_type = std::string_view;

  explicit TypedRefColumn(const TypedColumn<std::string_view>& column)
      : column_(column), basic_size(column.size()) {}
  ~TypedRefColumn() {}

  inline std::string_view get_view(size_t index) const {
    assert(index < basic_size);
    return column_.get_view(index);
  }

  Property get(size_t index) const override {
    return PropUtils<std::string_view>::to_prop(get_view(index));
  }

  DataTypeId type() const override {
    return PropUtils<std::string_view>::prop_type();
  }

  ColType col_type() const override { return ColType::kInternal; }

 private:
  const TypedColumn<std::string_view>& column_;
  size_t basic_size;
};

// Create a reference column from a ColumnBase that contains a const reference
// to the actual column storage, offering a column-based store interface for
// vertex properties.
std::shared_ptr<RefColumnBase> CreateRefColumn(const ColumnBase& column);

}  // namespace neug
