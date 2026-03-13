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

#include "neug/utils/property/column.h"

#include <limits>

#include "neug/utils/id_indexer.h"
#include "neug/utils/mmap_array.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

std::string_view truncate_utf8(std::string_view str, size_t length) {
  if (str.size() <= length) {
    return str;
  }
  size_t byte_count = 0;

  for (const char* p = str.data(); *p && byte_count < length;) {
    unsigned char ch = *p;
    size_t char_length = 0;
    if ((ch & 0x80) == 0) {
      char_length = 1;
    } else if ((ch & 0xE0) == 0xC0) {
      char_length = 2;
    } else if ((ch & 0xF0) == 0xE0) {
      char_length = 3;
    } else if ((ch & 0xF8) == 0xF0) {
      char_length = 4;
    }
    if (byte_count + char_length > length) {
      break;
    }
    p += char_length;
    byte_count += char_length;
  }
  return str.substr(0, byte_count);
}

template <typename T>
class TypedEmptyColumn : public ColumnBase {
 public:
  TypedEmptyColumn() {}
  ~TypedEmptyColumn() {}

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {}
  void open_in_memory(const std::string& name) override {}
  void open_with_hugepages(const std::string& name) override {}
  void dump(const std::string& filename) override {}
  void copy_to_tmp(const std::string& cur_path,
                   const std::string& tmp_path) override {}
  void close() override {}
  size_t size() const override { return 0; }
  void resize(size_t size) override {}
  void resize(size_t size, const Property& default_value) override {}

  DataTypeId type() const override { return PropUtils<T>::prop_type(); }

  void set_value(size_t index, const T& val) {}

  void set_any(size_t index, const Property& value, bool insert_safe) override {
  }

  T get_view(size_t index) const { return T{}; }

  Property get_prop(size_t index) const override { return Property(); }

  void ingest(uint32_t index, OutArchive& arc) override {
    T val;
    arc >> val;
  }

  MemoryLevel storage_strategy() const override { return MemoryLevel::kUnSet; }

  void ensure_writable(const std::string& work_dir) override {}
};

template <>
class TypedEmptyColumn<std::string_view> : public ColumnBase {
 public:
  TypedEmptyColumn() {}
  ~TypedEmptyColumn() {}

  void open(const std::string& name, const std::string& snapshot_dir,
            const std::string& work_dir) override {}
  void open_in_memory(const std::string& name) override {}
  void open_with_hugepages(const std::string& name) override {}
  void dump(const std::string& filename) override {}
  void copy_to_tmp(const std::string& cur_path,
                   const std::string& tmp_path) override {}
  void close() override {}
  size_t size() const override { return 0; }
  void resize(size_t size) override {}
  void resize(size_t size, const Property& default_value) override {}

  DataTypeId type() const override { return DataTypeId::kVarchar; }

  void set_value(size_t index, const std::string_view& val) {}

  void set_any(size_t index, const Property& value, bool insert_safe) override {
  }

  std::string_view get_view(size_t index) const { return std::string_view{}; }

  Property get_prop(size_t index) const override { return Property(); }

  void ingest(uint32_t index, OutArchive& arc) override {
    std::string_view val;
    arc >> val;
  }

  MemoryLevel storage_strategy() const override { return MemoryLevel::kUnSet; }

  void ensure_writable(const std::string& work_dir) override {}
};

std::shared_ptr<ColumnBase> CreateColumn(DataType type, MemoryLevel strategy) {
  auto type_id = type.id();
  auto extra_type_info = type.RawExtraTypeInfo();
  if (strategy == MemoryLevel::kUnSet) {
    switch (type_id) {
#define TYPE_DISPATCHER(enum_val, type) \
  case DataTypeId::enum_val:            \
    return std::make_shared<TypedEmptyColumn<type>>();
      FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
    case DataTypeId::kVarchar:
      return std::make_shared<TypedEmptyColumn<std::string_view>>();
    default:
      THROW_NOT_SUPPORTED_EXCEPTION("Unsupported type for empty column: " +
                                    type.ToString());
    }
  } else {
    switch (type_id) {
#define TYPE_DISPATCHER(enum_val, type) \
  case DataTypeId::enum_val:            \
    return std::make_shared<TypedColumn<type>>(strategy);
      FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
    case DataTypeId::kVarchar: {
      uint16_t max_length = STRING_DEFAULT_MAX_LENGTH;
      if (extra_type_info) {
        auto str_info = dynamic_cast<const StringTypeInfo*>(extra_type_info);
        if (str_info) {
          max_length = str_info->max_length;
        }
      }
      return std::make_shared<StringColumn>(strategy, max_length);
    }
    case DataTypeId::kEmpty: {
      return std::make_shared<TypedColumn<EmptyType>>(strategy);
    }
    default: {
      THROW_NOT_SUPPORTED_EXCEPTION("Unsupported type for column: " +
                                    type.ToString());
    }
    }
  }
}

void TypedColumn<std::string_view>::set_value_safe(
    size_t idx, const std::string_view& value) {
  std::shared_lock<std::shared_mutex> lock(rw_mutex_);
  if (idx < size_) {
    std::string_view v = value;
    if (v.size() >= width_) {
      v = truncate_utf8(v, width_);
    }
    size_t offset = pos_.fetch_add(v.size());
    if (pos_.load() > container_.data_size()) {
      lock.unlock();
      std::unique_lock<std::shared_mutex> w_lock(rw_mutex_);
      if (pos_.load() > container_.data_size()) {
        size_t new_avg_width = (pos_.load() + idx) / (idx + 1);
        size_t new_len = std::max(size_ * new_avg_width, pos_.load());
        container_.resize(container_.size(), new_len);
      }
      w_lock.unlock();
      lock.lock();
    }
    container_.set(idx, offset, v);
  } else {
    THROW_INDEX_EXCEPTION(
        "Index out of range in set_value_safe: " + std::to_string(idx) +
        " for size: " + std::to_string(size_));
  }
}

std::shared_ptr<RefColumnBase> CreateRefColumn(const ColumnBase& column) {
  auto type = column.type();
  switch (type) {
#define TYPE_DISPATCHER(enum_val, type)            \
  case DataTypeId::enum_val:                       \
    return std::make_shared<TypedRefColumn<type>>( \
        dynamic_cast<const TypedColumn<type>&>(column));
    FOR_EACH_DATA_TYPE_NO_STRING(TYPE_DISPATCHER)
#undef TYPE_DISPATCHER
  case DataTypeId::kVarchar: {
    return std::make_shared<TypedRefColumn<std::string_view>>(
        dynamic_cast<const StringColumn&>(column));
  }
  default: {
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported type for reference column: " +
                                  std::to_string(type));
  }
  }
}

void StringContainer::open(const std::string& name,
                           const std::string& snapshot_dir,
                           const std::string& work_dir) {
  LOG(INFO) << "open string container " << name << " from snapshot dir "
            << snapshot_dir << " and work dir " << work_dir;
  items = std::make_unique<FileSharedMMap>();
  data = std::make_unique<FileSharedMMap>();
  auto items_path = snapshot_dir + "/" + name + ".items";
  auto data_path = snapshot_dir + "/" + name + ".data";
  auto tmp_items_path = work_dir + "/" + name + ".items";
  auto tmp_data_path = work_dir + "/" + name + ".data";
  if (std::filesystem::exists(tmp_items_path)) {
    std::filesystem::remove(tmp_items_path);
  }
  if (std::filesystem::exists(tmp_data_path)) {
    std::filesystem::remove(tmp_data_path);
  }
  if (std::filesystem::exists(items_path)) {
    file_utils::copy_file(items_path, tmp_items_path, true);
  } else {
    file_utils::create_file(tmp_items_path, sizeof(FileHeader));
  }
  if (std::filesystem::exists(data_path)) {
    file_utils::copy_file(data_path, tmp_data_path, true);
  } else {
    file_utils::create_file(tmp_data_path, sizeof(FileHeader));
  }
  LOG(INFO) << "open string container with items file " << items_path
            << " and data file " << data_path;
  items->Open(tmp_items_path);
  LOG(INFO) << "items file opened, size: " << items->GetDataSize();
  data->Open(tmp_data_path);
}

void StringContainer::open(const std::string& prefix) {
  items = std::make_unique<FilePrivateMMap>();
  data = std::make_unique<FilePrivateMMap>();
  items->Open(prefix + ".items");
  data->Open(prefix + ".data");
}

void StringContainer::open_with_hugepages(const std::string& prefix) {
  items = std::make_unique<AnonHugeMMap>();
  data = std::make_unique<AnonHugeMMap>();
  items->Open(prefix + ".items");
  data->Open(prefix + ".data");
}

size_t StringContainer::size() const {
  LOG(INFO) << "StringContainer::size() called, items size: "
            << (items ? items->GetDataSize() : 0)
            << ", data size: " << (data ? data->GetDataSize() : 0);
  if (items) {
    return items->GetDataSize() / sizeof(string_item);
  }
  return 0;
}

size_t StringContainer::data_size() const {
  return data ? data->GetDataSize() : 0;
}

size_t StringContainer::avg_size() const {
  if (items->GetDataSize() == 0) {
    return 0;
  }
  size_t total_length = 0;
  size_t non_zero_count = 0;
  auto items_size = items->GetDataSize() / sizeof(string_item);
  auto items_data = reinterpret_cast<string_item*>(items->GetData());
  for (size_t i = 0; i < items_size; ++i) {
    if (items_data[i].length > 0) {
      ++non_zero_count;
      total_length += items_data[i].length;
    }
  }
  return non_zero_count > 0
             ? (total_length + non_zero_count - 1) / non_zero_count
             : 0;
}

void StringContainer::resize(size_t size, size_t data_size) {
  items->Resize(size * sizeof(string_item));
  data->Resize(data_size);
}

void StringContainer::close() {
  if (items) {
    items->Close();
    items.reset();
  }
  if (data) {
    data->Close();
    data.reset();
  }
}

void StringContainer::swap(StringContainer& rhs) {
  items.swap(rhs.items);
  data.swap(rhs.data);
}

void StringContainer::dump(const std::string& filename) {
  // TODO(zhanglei): Support compact and dump;
  if (items) {
    items->Dump(filename + ".items");
  }
  if (data) {
    data->Dump(filename + ".data");
  }
}

}  // namespace neug
