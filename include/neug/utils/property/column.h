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
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "neug/config.h"
#include "neug/storages/container/container_utils.h"
#include "neug/storages/container/file_header.h"
#include "neug/storages/container/i_container.h"
#include "neug/storages/file_names.h"
#include "neug/storages/module/module.h"
#include "neug/storages/module/type_name.h"
#include "neug/storages/workspace.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/likely.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/out_archive.h"

#include <glog/logging.h>

namespace neug {
class Table;

std::string_view truncate_utf8(std::string_view str, size_t length);

class ColumnBase : public Module {
 public:
  virtual ~ColumnBase() {}

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
  ~TypedColumn() { Close(); }

  void Open(Checkpoint& ckp, const ModuleDescriptor& desc,
            MemoryLevel level) override {
    assert(desc.module_type.empty() || desc.module_type == ModuleTypeName());
    size_ = desc.size;
    buffer_ = ckp.OpenFile(desc.path, level);
    size_ = buffer_->GetDataSize() / sizeof(T);
  }

  void Close() override { buffer_.reset(); }

  ModuleDescriptor Dump(Checkpoint& ckp) override {
    // TODO(zhanglei): When DataContainer related code is merged, we need to use
    // IsDirty() to check whether we need to dump the column or not. If the
    // column is not dirty, we can skip dumping and just return the existing
    // path and size.
    ModuleDescriptor desc = ckp.Commit(*buffer_);
    desc.size = size_;
    desc.type = StorageTypeName<T>::value;
    desc.module_type = ModuleTypeName();
    return desc;
  }

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

  std::unique_ptr<Module> Fork(Checkpoint& ckp, MemoryLevel level) override {
    // TODO(zhanglei): Current implementation is not correct, fix it
    auto new_col = std::make_unique<TypedColumn<T>>();
    new_col->buffer_ = buffer_->Fork(ckp, level);
    new_col->size_ = size_;
    return new_col;
  }

  std::string ModuleTypeName() const override {
    return std::string("column_") + StorageTypeName<T>::value;
  }

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

  void Open(Checkpoint& ckp, const ModuleDescriptor& desc,
            MemoryLevel level) override {}

  ModuleDescriptor Dump(Checkpoint& ckp) override { return ModuleDescriptor(); }
  void Close() override {}
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

  std::unique_ptr<Module> Fork(Checkpoint& ckp, MemoryLevel level) override {
    return std::make_unique<TypedColumn<EmptyType>>();
  }

  std::string ModuleTypeName() const override { return "column_empty"; }
};

// Compact representation of a string slot stored in the items buffer.
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

  ~TypedColumn() { Close(); }

  void Open(Checkpoint& ckp, const ModuleDescriptor& desc,
            MemoryLevel level) override {
    size_ = desc.size;
    items_buffer_ = ckp.OpenFile(desc.get_sub_module("items").path, level);
    data_buffer_ = ckp.OpenFile(desc.get_sub_module("data").path, level);
    CHECK(size_ == items_buffer_->GetDataSize() / sizeof(string_item));
    // Restore the append-position cursor.  New format stores it as an extra
    // field; fall back to reading the legacy .pos side-car file.
    if (auto pos_str = desc.get("pos")) {
      pos_.store(std::stoull(*pos_str));
    } else {
      pos_.store(0);
    }
  }

  void Close() override {
    if (items_buffer_) {
      items_buffer_->Close();
    }
    if (data_buffer_) {
      data_buffer_->Close();
    }
  }

  ModuleDescriptor Dump(Checkpoint& ckp) override {
    ModuleDescriptor desc;
    desc.size = size_;
    desc.type = StorageTypeName<std::string_view>::value;
    desc.module_type = ModuleTypeName();

    if (!items_buffer_ || !data_buffer_) {
      THROW_RUNTIME_ERROR("Buffers not initialized for dumping");
    }
    resize(size_);  // Resize the string column with avg size to shrink or
                    // expand data buffer

    if (size_ > 0) {
      auto plan = prepare_compaction_plan();
      if (plan.reused_size > 0) {
        // ── Stream-compaction path ────────────────────────────────────────
        // The data buffer contains stale copies from update operations.
        // We stream the compacted bytes to a fresh UUID file under runtime/
        // and build the data sub-module descriptor manually, because there
        // is no IDataContainer holding the compacted bytes at this point.
        std::string data_uuid = ckp.create_runtime_object();
        std::string data_path = ckp.runtime_dir() + "/" + data_uuid;
        size_t pos_val = stream_compact_and_dump(plan, data_path);
        desc.set("pos", std::to_string(pos_val));

        // items_buffer_ was updated in-place by stream_compact_and_dump;
        // commit it through the checkpoint so the path is recorded.
        ModuleDescriptor items_desc = ckp.Commit(*items_buffer_);
        desc.set_sub_module("items", std::move(items_desc));

        // Build data sub-module descriptor manually for the compacted file.
        ModuleDescriptor data_desc;
        data_desc.path = data_path;
        data_desc.size = pos_val;
        desc.set_sub_module("data", std::move(data_desc));
        items_buffer_->Close();
        data_buffer_->Close();
        return desc;
      }
    }

    // ── No-compaction path ──────────────────────────────────────────────
    // Both buffers are clean; delegate persistence to Checkpoint::Commit()
    // which handles MAP_SHARED (sync-in-place) and all other containers
    // (copy to a fresh UUID file) uniformly.
    desc.set("pos", std::to_string(pos_.load()));
    if (items_buffer_) {
      desc.set_sub_module("items", ckp.Commit(*items_buffer_));
    }
    if (data_buffer_) {
      desc.set_sub_module("data", ckp.Commit(*data_buffer_));
    }
    items_buffer_->Close();
    data_buffer_->Close();
    return desc;
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

  std::unique_ptr<Module> Fork(Checkpoint& ckp, MemoryLevel level) override {
    // TODO(zhanglei): this implementation is not correct, fix it
    auto new_col = std::make_unique<TypedColumn<std::string_view>>(width_);
    new_col->items_buffer_ = items_buffer_->Fork(ckp, level);
    new_col->data_buffer_ = data_buffer_->Fork(ckp, level);
    new_col->size_ = size_;
    new_col->pos_ = pos_.load();
    return new_col;
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

  std::string ModuleTypeName() const override { return "column_string"; }

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

  // Descriptor of a single live string entry used during compaction.
  struct CompactionPlan {
    struct Entry {
      size_t index;     // position in items array
      uint64_t offset;  // current byte offset in the data buffer
      uint32_t length;  // byte length of the string
    };
    std::vector<Entry> entries;
    size_t total_size = 0;   // sum of all entry lengths (includes duplicates)
    size_t reused_size = 0;  // bytes shared by entries that point to the same
                             // offset (i.e. slots updated with the same value)
  };

  // Scan items and build a compaction plan that records which offsets are
  // shared across multiple item slots (those are stale copies from updates).
  CompactionPlan prepare_compaction_plan() const {
    CompactionPlan plan;
    plan.entries.reserve(size_);
    std::unordered_set<uint64_t> seen_offsets;
    for (size_t i = 0; i < size_; ++i) {
      const auto item = get_string_item(i);
      plan.total_size += item.length;
      plan.entries.push_back(
          {i, item.offset, static_cast<uint32_t>(item.length)});
      if (item.length > 0) {
        if (seen_offsets.count(item.offset)) {
          // This offset is already referenced by an earlier slot: the current
          // slot was set via resize(default) and shares the same backing
          // bytes.
          plan.reused_size += item.length;
        } else {
          seen_offsets.insert(item.offset);
        }
      }
    }
    return plan;
  }

  // Remove stale string data produced by update operations (which append a
  // new copy without reclaiming the old one) by streaming compacted bytes
  // directly to data_filename.
  //   * Iterates plan.entries in order; unique offsets are fwrite'd once.
  //   * MD5 is accumulated in a single forward pass and written into the
  //     FileHeader at position 0 via fseek after all data is written.
  //   * item offsets in items_buffer_ are updated to match the new layout.
  //   * pos_ is updated to effective_size for future appends.
  // Returns the effective (compacted) data size.
  size_t stream_compact_and_dump(const CompactionPlan& plan,
                                 const std::string& data_filename) {
    auto parent_dir = std::filesystem::path(data_filename).parent_path();
    if (!parent_dir.empty()) {
      std::filesystem::create_directories(parent_dir);
    }
    FILE* fout = fopen(data_filename.c_str(), "wb");
    if (!fout) {
      THROW_IO_EXCEPTION("Failed to open output for stream compaction: " +
                         data_filename);
    }

    // Write a placeholder header; will be overwritten after MD5 is finalised.
    FileHeader header{};
    if (fwrite(&header, sizeof(header), 1, fout) != 1) {
      fclose(fout);
      THROW_IO_EXCEPTION("Failed to write placeholder header: " +
                         data_filename);
    }

    const auto* raw_data =
        reinterpret_cast<const char*>(data_buffer_->GetData());
    size_t write_offset = 0;
    std::unordered_map<uint64_t, uint64_t> old_offset_to_new;
    MD5_CTX md5_ctx;
    MD5_Init(&md5_ctx);

    for (const auto& entry : plan.entries) {
      if (entry.length > 0) {
        auto it = old_offset_to_new.find(entry.offset);
        if (it != old_offset_to_new.end()) {
          // Duplicate offset: already written; just remap the item.
          set_string_item(entry.index,
                          {it->second, static_cast<uint32_t>(entry.length)});
          continue;
        }
        old_offset_to_new.emplace(entry.offset, write_offset);
        const char* src = raw_data + entry.offset;
        if (fwrite(src, 1, entry.length, fout) != entry.length) {
          fclose(fout);
          THROW_IO_EXCEPTION("Failed to fwrite compacted data to: " +
                             data_filename);
        }
        MD5_Update(&md5_ctx, src, entry.length);
      }
      set_string_item(entry.index,
                      {write_offset, static_cast<uint32_t>(entry.length)});
      write_offset += entry.length;
    }

    // Seek back and stamp the real MD5 into the file header.
    MD5_Final(header.data_md5, &md5_ctx);
    if (fseek(fout, 0, SEEK_SET) != 0) {
      fclose(fout);
      THROW_IO_EXCEPTION("Failed to seek to header in: " + data_filename);
    }
    if (fwrite(&header, sizeof(header), 1, fout) != 1) {
      fclose(fout);
      THROW_IO_EXCEPTION("Failed to write final header to: " + data_filename);
    }

    if (fflush(fout) != 0) {
      fclose(fout);
      THROW_IO_EXCEPTION("Failed to fflush: " + data_filename);
    }
    if (fclose(fout) != 0) {
      THROW_IO_EXCEPTION("Failed to fclose: " + data_filename);
    }

    size_t effective_size = plan.total_size - plan.reused_size;
    pos_.store(effective_size);
    VLOG(1) << "StringColumn stream compaction: " << plan.total_size << " -> "
            << effective_size << " bytes saved to " << data_filename;
    return effective_size;
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
