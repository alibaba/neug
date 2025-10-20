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

#ifndef INCLUDE_NEUG_UTILS_PROPERTY_COLUMN_H_
#define INCLUDE_NEUG_UTILS_PROPERTY_COLUMN_H_

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

#include "libgrape-lite/grape/serialization/out_archive.h"
#include "libgrape-lite/grape/types.h"
#include "neug/storages/file_names.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/mmap_array.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"

namespace gs {
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

  virtual PropertyType type() const = 0;

  virtual void set_any(size_t index, const Any& value) = 0;

  virtual void set_any_with_resize(size_t index, const Any& value) = 0;

  virtual Any get(size_t index) const = 0;

  virtual Prop get_prop(size_t index) const {
    THROW_NOT_IMPLEMENTED_EXCEPTION("get_prop not implemented: index = " +
                                    std::to_string(index));
    return Prop::empty();
  }

  virtual void set_prop(size_t index, const Prop& prop) {
    LOG(FATAL) << "Not implemented";
  }

  virtual void ingest(uint32_t index, grape::OutArchive& arc) = 0;

  virtual StorageStrategy storage_strategy() const = 0;

  virtual void ensure_writable(const std::string& work_dir) = 0;
};

template <typename T>
class TypedColumn : public ColumnBase {
 public:
  explicit TypedColumn(StorageStrategy strategy)
      : size_(0), strategy_(strategy) {}
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
    size_ = size;
    buffer_.resize(size_);
  }

  PropertyType type() const override { return AnyConverter<T>::type(); }

  void set_value(size_t index, const T& val) {
    if (index < size_) {
      buffer_.set(index, val);
    } else {
      THROW_RUNTIME_ERROR("Index out of range");
    }
  }

  void set_value_with_check(size_t index, const T& val) {
    if (index < size_) {
      set_value(index, val);
    } else {
      // TODO(zhanglei): Revisit the size increase logic
      size_t new_size = std::max(index, 64UL) + index / 4;
      resize(new_size);
      set_value(index, val);
    }
  }

  void set_any(size_t index, const Any& value) override {
    set_value(index, AnyConverter<T>::from_any(value));
  }

  void set_any_with_resize(size_t index, const Any& value) override {
    set_value_with_check(index, AnyConverter<T>::from_any(value));
  }

  inline T get_view(size_t index) const {
    CHECK(index < size_) << "Index out of range: " << index << " >= " << size_;
    return buffer_.get(index);
  }

  Any get(size_t index) const override {
    return AnyConverter<T>::to_any(get_view(index));
  }

  Prop get_prop(size_t index) const override {
    return PropUtils<T>::to_prop(get_view(index));
  }

  void set_prop(size_t index, const Prop& prop) override {
    set_value(index, PropUtils<T>::to_typed(prop));
  }

  void ingest(uint32_t index, grape::OutArchive& arc) override {
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
  mmap_array<T> buffer_;
  size_t size_;
  StorageStrategy strategy_;
};

#if 0
template <>
class TypedColumn<RecordView> : public ColumnBase {
 public:
  explicit TypedColumn(const std::vector<PropertyType>& types) : types_(types) {
    if (types.size() == 0) {
      LOG(FATAL) << "RecordView column must have sub types.";
    }
  }

  ~TypedColumn() { close(); }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    LOG(FATAL) << "RecordView column does not support open.";
  }

  void open_in_memory(const std::string& name) override;

  void open_with_hugepages(const std::string& name, bool force) override {
    LOG(FATAL) << "RecordView column does not support open with hugepages.";
  }

  void dump(const std::string& filename) override {
    LOG(FATAL) << "RecordView column does not support dump.";
  }

  void copy_to_tmp(const std::string& cur_path,
                   const std::string& tmp_path) override {
    LOG(FATAL) << "RecordView column does not support copy_to_tmp.";
  }
  void close() override;

  size_t size() const override;
  void resize(size_t size) override;

  PropertyType type() const override { return PropertyType::kRecordView; }

  void set_any(size_t index, const Any& value) override;

  void set_any_with_resize(size_t index, const Any& value) override;

  void set_value(size_t index, const RecordView& val);

  void set_value_with_check(size_t index, const RecordView& val);

  RecordView get_view(size_t index) const;

  Any get(size_t index) const override;

  void ingest(uint32_t index, grape::OutArchive& arc) override {
    LOG(FATAL) << "RecordView column does not support ingest.";
  }

  StorageStrategy storage_strategy() const override {
    LOG(ERROR) << "RecordView column does not have storage strategy.";
    return StorageStrategy::kMem;
  }

  std::vector<PropertyType> sub_types() const { return types_; }

  void ensure_writable(const std::string& work_dir) override {}

 private:
  std::vector<PropertyType> types_;
  std::shared_ptr<Table> table_;
};
#endif

using BoolColumn = TypedColumn<bool>;
using UInt8Column = TypedColumn<uint8_t>;
using UInt16Column = TypedColumn<uint16_t>;
using IntColumn = TypedColumn<int32_t>;
using UIntColumn = TypedColumn<uint32_t>;
using LongColumn = TypedColumn<int64_t>;
using ULongColumn = TypedColumn<uint64_t>;
using DateColumn = TypedColumn<Date>;
// using DayColumn = TypedColumn<Day>;
using DoubleColumn = TypedColumn<double>;
using FloatColumn = TypedColumn<float>;
// using RecordViewColumn = TypedColumn<RecordView>;
using DateTimeColumn = TypedColumn<DateTime>;
using IntervalColumn = TypedColumn<Interval>;
using TimeStampColumn = TypedColumn<TimeStamp>;

template <>
class TypedColumn<grape::EmptyType> : public ColumnBase {
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

  PropertyType type() const override { return PropertyType::kEmpty; }

  void set_any(size_t index, const Any& value) override {}

  void set_any_with_resize(size_t index, const Any& value) override {}

  void set_value(size_t index, const grape::EmptyType& value) {}

  Any get(size_t index) const override { return Any(); }

  Prop get_prop(size_t index) const override { return Prop::empty(); }

  void set_prop(size_t index, const Prop& prop) override {}

  grape::EmptyType get_view(size_t index) const { return grape::EmptyType(); }

  void ingest(uint32_t index, grape::OutArchive& arc) override {}

  StorageStrategy storage_strategy() const override { return strategy_; }

  void ensure_writable(const std::string& work_dir) override {}

 private:
  StorageStrategy strategy_;
};
template <>
class TypedColumn<std::string_view> : public ColumnBase {
 public:
  TypedColumn(StorageStrategy strategy, uint16_t width)
      : size_(0),
        pos_(0),
        strategy_(strategy),
        width_(width),
        type_(PropertyType::Varchar(width_)) {}
  explicit TypedColumn(StorageStrategy strategy)
      : size_(0),
        pos_(0),
        strategy_(strategy),
        width_(PropertyType::GetStringDefaultMaxLength()),
        type_(PropertyType::kStringView) {}
  TypedColumn(TypedColumn<std::string_view>&& rhs) {
    buffer_.swap(rhs.buffer_);
    size_ = rhs.size_;
    pos_ = rhs.pos_.load();
    strategy_ = rhs.strategy_;
    width_ = rhs.width_;
    type_ = rhs.type_;
  }

  ~TypedColumn() { close(); }

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {
    std::string basic_path = snapshot_dir + "/" + name;
    if (std::filesystem::exists(basic_path + ".items")) {
      buffer_.open(basic_path, false, false);
      size_ = buffer_.size();
      pos_ = buffer_.data_size();
    } else {
      if (work_dir == "") {
        size_ = 0;
        pos_.store(0);
      } else {
        buffer_.open(work_dir + "/" + name, true);
        size_ = buffer_.size();
        pos_ = buffer_.data_size();
      }
    }
  }

  void open_in_memory(const std::string& prefix) override {
    buffer_.open(prefix, false);
    size_ = buffer_.size();
    pos_ = buffer_.data_size();
  }

  void open_with_hugepages(const std::string& prefix, bool force) override {
    if (strategy_ == StorageStrategy::kMem || force) {
      buffer_.open_with_hugepages(prefix);
      size_ = buffer_.size();
      pos_ = buffer_.data_size();

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

    buffer_.reset();
    tmp.open(tmp_path, true);
    buffer_.swap(tmp);
    tmp.reset();
    pos_.store(buffer_.data_size());
  }

  void dump(const std::string& filename) override {
    buffer_.resize(size_, pos_.load());
    buffer_.dump(filename);
  }

  size_t size() const override { return size_; }

  void resize(size_t size) override {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    size_ = size;
    if (buffer_.size() != 0) {
      size_t avg_width =
          (buffer_.data_size() + buffer_.size() - 1) / buffer_.size();
      //  extra_size_ * basic_avg_width may be smaller than pos_.load()
      buffer_.resize(size_, std::max(size_ * avg_width, pos_.load()));
    } else {
      buffer_.resize(size_, std::max(size_ * width_, pos_.load()));
    }
  }

  PropertyType type() const override { return type_; }

  void set_value(size_t idx, const std::string_view& val) {
    auto copied_val = val;
    if (copied_val.size() >= width_) {
      VLOG(1) << "String length" << copied_val.size()
              << " exceeds the maximum length: " << width_ << ", cut off.";
      copied_val = truncate_utf8(copied_val, width_);
    }
    if (idx < size_) {
      size_t offset = pos_.fetch_add(copied_val.size());
      buffer_.set(idx, offset, copied_val);
    } else {
      LOG(FATAL) << "Index out of range: " << idx << ", size_: " << size_;
    }
  }

  void set_any(size_t idx, const Any& value) override {
    set_value(idx, value.AsStringView());
  }

  void set_any_with_resize(size_t idx, const Any& value) override {
    return set_value_with_resize(idx, value.AsStringView());
  }

  void set_value_with_resize(size_t idx, const std::string_view& value) {
    if (idx >= size_) {
      size_t new_size = std::max(idx, 64UL) + idx / 4;
      resize(new_size);
    }
    set_value_with_check(idx, value);
  }

  // make sure there is enough space for the value
  void set_value_with_check(size_t idx, const std::string_view& value) {
    if (idx < size_) {
      size_t offset = pos_.fetch_add(value.size());
      if (pos_.load() > buffer_.data_size()) {
        buffer_.resize(buffer_.size(), pos_.load());
      }
      buffer_.set(idx, offset, value);
    } else {
      LOG(FATAL) << "Index out of range";
    }
  }

  void set_value_safe(size_t idx, const std::string_view& value);

  inline std::string_view get_view(size_t idx) const {
    return buffer_.get(idx);
  }

  Any get(size_t idx) const override {
    return AnyConverter<std::string_view>::to_any(get_view(idx));
  }

  Prop get_prop(size_t index) const override {
    return PropUtils<std::string_view>::to_prop(get_view(index));
  }

  void set_prop(size_t index, const Prop& prop) override {
    set_value(index, PropUtils<std::string_view>::to_typed(prop));
  }

  void ingest(uint32_t index, grape::OutArchive& arc) override {
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
  mmap_array<std::string_view> buffer_;
  size_t size_;
  std::atomic<size_t> pos_;
  StorageStrategy strategy_;
  std::shared_mutex rw_mutex_;
  uint16_t width_;
  PropertyType type_;
};

using StringColumn = TypedColumn<std::string_view>;

std::shared_ptr<ColumnBase> CreateColumn(
    PropertyType type, StorageStrategy strategy = StorageStrategy::kMem,
    const std::vector<PropertyType>& sub_types = {});

/// Create RefColumn for ease of usage for hqps
class RefColumnBase {
 public:
  virtual ~RefColumnBase() {}
  virtual Any get(size_t index) const = 0;
};

// Different from TypedColumn, RefColumn is a wrapper of mmap_array
template <typename T>
class TypedRefColumn : public RefColumnBase {
 public:
  using value_type = T;

  TypedRefColumn(const mmap_array<T>& buffer, StorageStrategy strategy)
      : basic_buffer(buffer), basic_size(0), strategy_(strategy) {}
  explicit TypedRefColumn(const TypedColumn<T>& column)
      : basic_buffer(column.buffer()),
        basic_size(column.buffer_size()),
        strategy_(column.storage_strategy()) {}
  ~TypedRefColumn() {}

  inline T get_view(size_t index) const {
    CHECK(index < basic_size)
        << "Index out of range: " << index << " >= " << basic_size;
    return basic_buffer.get(index);
  }

  size_t size() const { return basic_size; }

  Any get(size_t index) const override {
    return AnyConverter<T>::to_any(get_view(index));
  }

 private:
  const mmap_array<T>& basic_buffer;
  size_t basic_size;

  StorageStrategy strategy_;
};

template <>
class TypedRefColumn<LabelKey> : public RefColumnBase {
 public:
  explicit TypedRefColumn(LabelKey label_key) : label_key_(label_key) {}

  ~TypedRefColumn() {}

  inline LabelKey get_view(size_t index) const { return label_key_; }

  Any get(size_t index) const override {
    LOG(ERROR) << "LabelKeyColumn does not support get() to Any";
    return Any();
  }

 private:
  LabelKey label_key_;
};

template <>
class TypedRefColumn<GlobalId> : public RefColumnBase {
 public:
  using label_t = typename LabelKey::label_data_type;
  explicit TypedRefColumn(label_t label_key) : label_key_(label_key) {}

  ~TypedRefColumn() {}

  inline GlobalId get_view(size_t index) const {
    return GlobalId(label_key_, index);
  }

  Any get(size_t index) const override {
    LOG(ERROR) << "GlobalId Column does not support get() to Any";
    return Any();
  }

 private:
  label_t label_key_;
};

// Create a reference column from a ColumnBase that contains a const reference
// to the actual column storage, offering a column-based store interface for
// vertex properties.
std::shared_ptr<RefColumnBase> CreateRefColumn(
    std::shared_ptr<ColumnBase> column);

}  // namespace gs

#endif  // INCLUDE_NEUG_UTILS_PROPERTY_COLUMN_H_
