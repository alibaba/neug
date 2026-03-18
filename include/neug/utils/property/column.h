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
#include <vector>

#include "neug/storages/file_names.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/likely.h"
#include "neug/utils/mmap_array.h"
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

  virtual void open_with_hugepages(const std::string& name, bool force) = 0;

  virtual void close() = 0;

  virtual void dump(const std::string& filename) = 0;

  virtual size_t size() const = 0;

  virtual void copy_to_tmp(const std::string& cur_path,
                           const std::string& tmp_path) = 0;
  virtual void resize(size_t size) = 0;

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

  virtual StorageStrategy storage_strategy() const = 0;

  virtual void ensure_writable(const std::string& work_dir) = 0;
};

template <typename T>
class TypedColumn : public ColumnBase {
 public:
  explicit TypedColumn(const T& default_value, StorageStrategy strategy)
      : default_value_(default_value), size_(0), strategy_(strategy) {}
  ~TypedColumn() { close(); }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    std::string basic_path = snapshot_dir + "/" + name;
    if (std::filesystem::exists(basic_path)) {
      buffer_.open(basic_path, false, false);
      size_ = buffer_.size();
    } else {
      if (work_dir == "") {
        size_ = 0;
      } else {
        buffer_.open(work_dir + "/" + name, true);
        size_ = buffer_.size();
      }
    }
  }

  void open_in_memory(const std::string& name) override {
    if (!name.empty() && std::filesystem::exists(name)) {
      buffer_.open(name, false);
      size_ = buffer_.size();
    } else {
      buffer_.reset();
      size_ = 0;
    }
  }

  void open_with_hugepages(const std::string& name, bool force) override {
    if (strategy_ == StorageStrategy::kMem || force) {
      if (!name.empty() && std::filesystem::exists(name)) {
        buffer_.open_with_hugepages(name);
        size_ = buffer_.size();
      } else {
        buffer_.reset();
        buffer_.set_hugepage_prefered(true);
        size_ = 0;
      }
    } else if (strategy_ == StorageStrategy::kDisk) {
      LOG(INFO) << "Open " << name << " with normal mmap pages";
      open_in_memory(name);
    }
  }

  void close() override { buffer_.reset(); }

  void copy_to_tmp(const std::string& cur_path,
                   const std::string& tmp_path) override {
    mmap_array<T> tmp;
    if (!std::filesystem::exists(cur_path)) {
      return;
    }
    copy_file(cur_path, tmp_path);
    tmp.open(tmp_path, true);
    buffer_.reset();
    buffer_.swap(tmp);
    tmp.reset();
  }

  void dump(const std::string& filename) override { buffer_.dump(filename); }

  size_t size() const override { return size_; }

  void resize(size_t size) override {
    size_t old_size = size_;
    size_ = size;
    buffer_.resize(size_);
    for (size_t i = old_size; i < size_; ++i) {
      buffer_.set(i, default_value_);
    }
  }

  DataTypeId type() const override { return PropUtils<T>::prop_type(); }

  void set_value(size_t index, const T& val) {
    if (index < size_) {
      buffer_.set(index, val);
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
    return buffer_.get(index);
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

  StorageStrategy storage_strategy() const override { return strategy_; }

  const mmap_array<T>& buffer() const { return buffer_; }
  size_t buffer_size() const { return size_; }

  void ensure_writable(const std::string& work_dir) override {
    buffer_.ensure_writable(work_dir);
  }

 private:
  T default_value_;
  mmap_array<T> buffer_;
  size_t size_;
  StorageStrategy strategy_;
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
  explicit TypedColumn(StorageStrategy strategy) : strategy_(strategy) {}
  ~TypedColumn() {}

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {}
  void open_in_memory(const std::string& name) override {}
  void open_with_hugepages(const std::string& name, bool force) override {}
  void dump(const std::string& filename) override {}
  void copy_to_tmp(const std::string& cur_path,
                   const std::string& tmp_path) override {}
  void close() override {}
  size_t size() const override { return 0; }
  void resize(size_t size) override {}

  DataTypeId type() const override { return DataTypeId::kEmpty; }

  void set_any(size_t index, const Property& value, bool insert_safe) override {
  }

  void set_value(size_t index, const EmptyType& value) {}

  Property get_prop(size_t index) const override { return Property::empty(); }

  void set_prop(size_t index, const Property& prop) override {}

  EmptyType get_view(size_t index) const { return EmptyType(); }

  void ingest(uint32_t index, OutArchive& arc) override {}

  StorageStrategy storage_strategy() const override { return strategy_; }

  void ensure_writable(const std::string& work_dir) override {}

 private:
  StorageStrategy strategy_;
};

// No default value for StringColumn
template <>
class TypedColumn<std::string_view> : public ColumnBase {
 public:
  TypedColumn(StorageStrategy strategy, uint16_t width,
              std::string_view default_value = "")
      : size_(0),
        pos_(0),
        strategy_(strategy),
        width_(width),
        default_value_(default_value),
        type_(DataTypeId::kVarchar) {}
  explicit TypedColumn(StorageStrategy strategy)
      : size_(0),
        pos_(0),
        strategy_(strategy),
        width_(STRING_DEFAULT_MAX_LENGTH),
        default_value_(""),
        type_(DataTypeId::kVarchar) {}
  TypedColumn(TypedColumn<std::string_view>&& rhs) {
    buffer_.swap(rhs.buffer_);
    size_ = rhs.size_;
    pos_ = rhs.pos_.load();
    strategy_ = rhs.strategy_;
    width_ = rhs.width_;
    default_value_ = rhs.default_value_;
    type_ = rhs.type_;
  }

  ~TypedColumn() { close(); }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    std::string basic_path = snapshot_dir + "/" + name;
    if (std::filesystem::exists(basic_path + ".items")) {
      buffer_.open(basic_path, false, false);
      size_ = buffer_.size();
      init_pos(basic_path + ".pos");
    } else {
      if (work_dir == "") {
        size_ = 0;
        pos_.store(0);
      } else {
        buffer_.open(work_dir + "/" + name, true);
        size_ = buffer_.size();
        init_pos(work_dir + "/" + name + ".pos");
      }
    }
  }

  void open_in_memory(const std::string& prefix) override {
    buffer_.open(prefix, false);
    size_ = buffer_.size();
    init_pos(prefix + ".pos");
  }

  void open_with_hugepages(const std::string& prefix, bool force) override {
    if (strategy_ == StorageStrategy::kMem || force) {
      buffer_.open_with_hugepages(prefix);
      size_ = buffer_.size();
      init_pos(prefix + ".pos");

    } else if (strategy_ == StorageStrategy::kDisk) {
      LOG(INFO) << "Open " << prefix << " with normal mmap pages";
      open_in_memory(prefix);
    }
  }

  void close() override { buffer_.reset(); }

  void copy_to_tmp(const std::string& cur_path,
                   const std::string& tmp_path) override {
    mmap_array<std::string_view> tmp;
    if (!std::filesystem::exists(cur_path + ".data")) {
      return;
    }
    copy_file(cur_path + ".data", tmp_path + ".data");
    copy_file(cur_path + ".items", tmp_path + ".items");
    copy_file(cur_path + ".pos", tmp_path + ".pos");

    buffer_.reset();
    tmp.open(tmp_path, true);
    buffer_.swap(tmp);
    tmp.reset();
    init_pos(tmp_path + ".pos");
  }

  void dump(const std::string& filename) override {
    size_t pos_val = pos_.load();
    write_file(filename + ".pos", &pos_val, sizeof(pos_val), 1);
    buffer_.dump(filename);
  }

  size_t size() const override { return size_; }

  void resize(size_t size) override {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    size_ = size;
    if (buffer_.size() != 0) {
      size_t avg_width =
          buffer_.avg_size();  // calculate average width of existing strings
      buffer_.resize(
          size_, std::max(size_ * (avg_width > 0 ? avg_width
                                                 : STRING_DEFAULT_MAX_LENGTH),
                          pos_.load()));
    } else {
      buffer_.resize(size_, std::max(size_ * width_, pos_.load()));
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
    if (idx < size_ && pos_.load() + copied_val.size() <= buffer_.data_size()) {
      // NOTE: Even if idx has been set before, we always append the new value
      // to the end of buffer_. The previous value is not reclaimed, and should
      // be handled by garbage collection or compaction.
      size_t offset = pos_.fetch_add(copied_val.size());
      buffer_.set(idx, offset, copied_val);
    } else {
      THROW_RUNTIME_ERROR("Index out of range or not enough space in buffer");
    }
  }

  void set_any(size_t idx, const Property& value, bool insert_safe) override {
    if (insert_safe) {
      set_value_safe(idx, PropUtils<std::string_view>::to_typed(value));
    } else {
      set_value(idx, value.as_string_view());
    }
  }

  void set_value_safe(size_t idx, const std::string_view& value);

  inline std::string_view get_view(size_t idx) const {
    return buffer_.get(idx);
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

  const mmap_array<std::string_view>& buffer() const { return buffer_; }

  StorageStrategy storage_strategy() const override { return strategy_; }

  size_t buffer_size() const { return size_; }

  void ensure_writable(const std::string& work_dir) override {
    buffer_.ensure_writable(work_dir);
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
  mmap_array<std::string_view> buffer_;
  size_t size_;
  std::atomic<size_t> pos_;
  StorageStrategy strategy_;
  std::shared_mutex rw_mutex_;
  uint16_t width_;
  std::string_view default_value_;
  DataTypeId type_;
};

using StringColumn = TypedColumn<std::string_view>;

std::shared_ptr<ColumnBase> CreateColumn(
    DataType type, Property default_value,
    StorageStrategy strategy = StorageStrategy::kMem);

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

// Different from TypedColumn, RefColumn is a wrapper of mmap_array
template <typename T>
class TypedRefColumn : public RefColumnBase {
 public:
  using value_type = T;

  explicit TypedRefColumn(const TypedColumn<T>& column)
      : basic_buffer(column.buffer()), basic_size(column.buffer_size()) {}
  ~TypedRefColumn() {}

  inline T get_view(size_t index) const {
    assert(index < basic_size);
    return basic_buffer.get(index);
  }

  Property get(size_t index) const override {
    return PropUtils<T>::to_prop(get_view(index));
  }

  DataTypeId type() const override { return PropUtils<T>::prop_type(); }

  ColType col_type() const override { return ColType::kInternal; }

 private:
  const mmap_array<T>& basic_buffer;
  size_t basic_size;
};

// Create a reference column from a ColumnBase that contains a const reference
// to the actual column storage, offering a column-based store interface for
// vertex properties.
std::shared_ptr<RefColumnBase> CreateRefColumn(const ColumnBase& column);

}  // namespace neug
