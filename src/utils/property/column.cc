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

namespace gs {

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
  void open_with_hugepages(const std::string& name, bool force) override {}
  void dump(const std::string& filename) override {}
  void copy_to_tmp(const std::string& cur_path,
                   const std::string& tmp_path) override {}
  void close() override {}
  size_t size() const override { return 0; }
  void resize(size_t size) override {}

  DataTypeId type() const override { return PropUtils<T>::prop_type(); }

  void set_value(size_t index, const T& val) {}

  void set_any(size_t index, const Property& value) override {}

  T get_view(size_t index) const { T{}; }

  Property get_prop(size_t index) const override { return Property(); }

  void ingest(uint32_t index, OutArchive& arc) override {
    T val;
    arc >> val;
  }

  StorageStrategy storage_strategy() const override {
    return StorageStrategy::kNone;
  }

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
  void open_with_hugepages(const std::string& name, bool force) override {}
  void dump(const std::string& filename) override {}
  void copy_to_tmp(const std::string& cur_path,
                   const std::string& tmp_path) override {}
  void close() override {}
  size_t size() const override { return 0; }
  void resize(size_t size) override {}

  DataTypeId type() const override { return DataTypeId::kStringView; }

  void set_value(size_t index, const std::string_view& val) {}

  void set_any(size_t index, const Property& value) override {}

  std::string_view get_view(size_t index) const { return std::string_view{}; }

  Property get_prop(size_t index) const override { return Property(); }

  void ingest(uint32_t index, OutArchive& arc) override {
    std::string_view val;
    arc >> val;
  }

  StorageStrategy storage_strategy() const override {
    return StorageStrategy::kNone;
  }

  void ensure_writable(const std::string& work_dir) override {}
};

using IntEmptyColumn = TypedEmptyColumn<int32_t>;
using UIntEmptyColumn = TypedEmptyColumn<uint32_t>;
using LongEmptyColumn = TypedEmptyColumn<int64_t>;
using ULongEmptyColumn = TypedEmptyColumn<uint64_t>;
using DateEmptyColumn = TypedEmptyColumn<Date>;
using DateTimeEmptyColumn = TypedEmptyColumn<DateTime>;
using IntervalEmptyColumn = TypedEmptyColumn<Interval>;
// using DayEmptyColumn = TypedEmptyColumn<Day>;
using BoolEmptyColumn = TypedEmptyColumn<bool>;
using FloatEmptyColumn = TypedEmptyColumn<float>;
using DoubleEmptyColumn = TypedEmptyColumn<double>;
using StringEmptyColumn = TypedEmptyColumn<std::string_view>;

std::shared_ptr<ColumnBase> CreateColumn(
    DataTypeId type, Property default_value, StorageStrategy strategy,
    std::shared_ptr<ExtraTypeInfo> extra_type_info,
    const std::vector<DataTypeId>& sub_types) {
  if (strategy == StorageStrategy::kNone) {
    if (type == DataTypeId::kBool) {
      return std::make_shared<BoolEmptyColumn>();
    } else if (type == DataTypeId::kInt32) {
      return std::make_shared<IntEmptyColumn>();
    } else if (type == DataTypeId::kInt64) {
      return std::make_shared<LongEmptyColumn>();
    } else if (type == DataTypeId::kUInt32) {
      return std::make_shared<UIntEmptyColumn>();
    } else if (type == DataTypeId::kUInt64) {
      return std::make_shared<ULongEmptyColumn>();
    } else if (type == DataTypeId::kDouble) {
      return std::make_shared<DoubleEmptyColumn>();
    } else if (type == DataTypeId::kFloat) {
      return std::make_shared<FloatEmptyColumn>();
    } else if (type == DataTypeId::kStringView) {
      return std::make_shared<StringEmptyColumn>();
    } else if (type == DataTypeId::kDate) {
      return std::make_shared<DateEmptyColumn>();
    } else if (type == DataTypeId::kDateTime) {
      return std::make_shared<DateTimeEmptyColumn>();
    } else if (type == DataTypeId::kInterval) {
      return std::make_shared<IntervalEmptyColumn>();
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION("Unsupported type for empty column: " +
                                    std::to_string(type));
    }
  } else {
    if (type == DataTypeId::kEmpty) {
      return std::make_shared<TypedColumn<EmptyType>>(strategy);
    } else if (type == DataTypeId::kBool) {
      return std::make_shared<BoolColumn>(default_value.as_bool(), strategy);
    } else if (type == DataTypeId::kInt32) {
      return std::make_shared<IntColumn>(default_value.as_int32(), strategy);
    } else if (type == DataTypeId::kInt64) {
      return std::make_shared<LongColumn>(default_value.as_int64(), strategy);
    } else if (type == DataTypeId::kUInt32) {
      return std::make_shared<UIntColumn>(default_value.as_uint32(), strategy);
    } else if (type == DataTypeId::kUInt64) {
      return std::make_shared<ULongColumn>(default_value.as_uint64(), strategy);
    } else if (type == DataTypeId::kDouble) {
      return std::make_shared<DoubleColumn>(default_value.as_double(),
                                            strategy);
    } else if (type == DataTypeId::kFloat) {
      return std::make_shared<FloatColumn>(default_value.as_float(), strategy);
    } else if (type == DataTypeId::kDate) {
      return std::make_shared<DateColumn>(default_value.as_date(), strategy);
    } else if (type == DataTypeId::kStringView) {
      uint16_t max_length = STRING_DEFAULT_MAX_LENGTH;
      if (extra_type_info) {
        auto str_info =
            std::dynamic_pointer_cast<StringTypeInfo>(extra_type_info);
        if (str_info) {
          max_length = str_info->max_length;
        }
      }
      return std::make_shared<StringColumn>(strategy, max_length,
                                            default_value.as_string_view());
    } else if (type == DataTypeId::kDateTime) {
      return std::make_shared<DateTimeColumn>(default_value.as_datetime(),
                                              strategy);
    } else if (type == DataTypeId::kInterval) {
      return std::make_shared<IntervalColumn>(default_value.as_interval(),
                                              strategy);
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION("Unsupported type for column: " +
                                    std::to_string(type));
    }
  }
}

void TypedColumn<std::string_view>::set_value_safe(
    size_t idx, const std::string_view& value) {
  std::shared_lock<std::shared_mutex> lock(rw_mutex_);
  if (idx < size_) {
    size_t offset = pos_.fetch_add(value.size());
    if (pos_.load() > buffer_.data_size()) {
      lock.unlock();
      std::unique_lock<std::shared_mutex> w_lock(rw_mutex_);
      if (pos_.load() > buffer_.data_size()) {
        size_t new_avg_width = (pos_.load() + idx) / (idx + 1);
        size_t new_len = std::max(size_ * new_avg_width, pos_.load());
        buffer_.resize(buffer_.size(), new_len);
      }
      w_lock.unlock();
      lock.lock();
    }
    buffer_.set(idx, offset, value);
  } else {
    THROW_INDEX_EXCEPTION(
        "Index out of range in set_value_safe: " + std::to_string(idx) +
        " for size: " + std::to_string(size_));
  }
}

std::shared_ptr<RefColumnBase> CreateRefColumn(const ColumnBase& column) {
  auto type = column.type();
  if (type == DataTypeId::kBool) {
    return std::make_shared<TypedRefColumn<bool>>(
        dynamic_cast<const TypedColumn<bool>&>(column));
  } else if (type == DataTypeId::kInt32) {
    return std::make_shared<TypedRefColumn<int32_t>>(
        dynamic_cast<const TypedColumn<int32_t>&>(column));
  } else if (type == DataTypeId::kInt64) {
    return std::make_shared<TypedRefColumn<int64_t>>(
        dynamic_cast<const TypedColumn<int64_t>&>(column));
  } else if (type == DataTypeId::kUInt32) {
    return std::make_shared<TypedRefColumn<uint32_t>>(
        dynamic_cast<const TypedColumn<uint32_t>&>(column));
  } else if (type == DataTypeId::kUInt64) {
    return std::make_shared<TypedRefColumn<uint64_t>>(
        dynamic_cast<const TypedColumn<uint64_t>&>(column));
  } else if (type == DataTypeId::kStringView) {
    return std::make_shared<TypedRefColumn<std::string_view>>(
        dynamic_cast<const TypedColumn<std::string_view>&>(column));
  } else if (type == DataTypeId::kFloat) {
    return std::make_shared<TypedRefColumn<float>>(
        dynamic_cast<const TypedColumn<float>&>(column));
  } else if (type == DataTypeId::kDouble) {
    return std::make_shared<TypedRefColumn<double>>(
        dynamic_cast<const TypedColumn<double>&>(column));
  } else if (type == DataTypeId::kDate) {
    return std::make_shared<TypedRefColumn<Date>>(
        dynamic_cast<const TypedColumn<Date>&>(column));
  } else if (type == DataTypeId::kDateTime) {
    return std::make_shared<TypedRefColumn<DateTime>>(
        dynamic_cast<const TypedColumn<DateTime>&>(column));
  } else if (type == DataTypeId::kInterval) {
    return std::make_shared<TypedRefColumn<Interval>>(
        dynamic_cast<const TypedColumn<Interval>&>(column));
  } else {
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported type for reference column: " +
                                  std::to_string(type));
  }
}

}  // namespace gs
