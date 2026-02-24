/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>
// header for size_t
#include <cstddef>

#include "flat_hash_map/flat_hash_map.hpp"
#include "glog/logging.h"
#include "neug/utils/bitset.h"
#include "neug/utils/likely.h"
#include "neug/utils/mmap_array.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/in_archive.h"
#include "neug/utils/serialization/out_archive.h"
#include "neug/utils/string_view_vector.h"

namespace neug {

namespace id_indexer_impl {

static constexpr int8_t min_lookups = 4;
static constexpr double max_load_factor = 0.5f;

inline int8_t log2(size_t value) {
  static constexpr int8_t table[64] = {
      63, 0,  58, 1,  59, 47, 53, 2,  60, 39, 48, 27, 54, 33, 42, 3,
      61, 51, 37, 40, 49, 18, 28, 20, 55, 30, 34, 11, 43, 14, 22, 4,
      62, 57, 46, 52, 38, 26, 32, 41, 50, 36, 17, 19, 29, 10, 13, 21,
      56, 45, 25, 31, 35, 16, 9,  12, 44, 24, 15, 8,  23, 7,  6,  5};
  value |= value >> 1;
  value |= value >> 2;
  value |= value >> 4;
  value |= value >> 8;
  value |= value >> 16;
  value |= value >> 32;
  return table[((value - (value >> 1)) * 0x07EDD5E59A4E28C2) >> 58];
}

template <typename T>
struct KeyBuffer {
  using type = std::vector<T>;

  static void serialize(std::ostream& os, const type& buffer) {
    size_t size = buffer.size();
    os.write(reinterpret_cast<const char*>(&size), sizeof(size_t));
    if (size > 0) {
      os.write(reinterpret_cast<const char*>(buffer.data()), size * sizeof(T));
    }
  }

  static void deserialize(std::istream& is, type& buffer) {
    size_t size;
    is.read(reinterpret_cast<char*>(&size), sizeof(size_t));
    if (size > 0) {
      buffer.resize(size);
      is.read(reinterpret_cast<char*>(buffer.data()), size * sizeof(T));
    }
  }
};

template <>
struct KeyBuffer<std::string> {
  using type = std::vector<std::string>;
  static void serialize(std::ostream& os, const type& buffer) {
    InArchive arc;
    arc << buffer;
    size_t size = arc.GetSize();
    os.write(reinterpret_cast<const char*>(&size), sizeof(size_t));
    os.write(reinterpret_cast<const char*>(arc.GetBuffer()), size);
  }

  static void deserialize(std::istream& is, type& buffer) {
    OutArchive arc;
    size_t size;
    is.read(reinterpret_cast<char*>(&size), sizeof(size_t));
    arc.Allocate(size);
    is.read(reinterpret_cast<char*>(arc.GetBuffer()), size);
    arc >> buffer;
  }
};

template <>
struct KeyBuffer<std::string_view> {
  using type = StringViewVector;

  static void serialize(std::ostream& os, const type& buffer) {
    size_t content_buffer_size = buffer.content_buffer().size();
    os.write(reinterpret_cast<const char*>(&content_buffer_size),
             sizeof(size_t));
    if (content_buffer_size > 0) {
      os.write(buffer.content_buffer().data(),
               content_buffer_size * sizeof(char));
    }
    size_t offset_buffer_size = buffer.offset_buffer().size();
    os.write(reinterpret_cast<const char*>(&offset_buffer_size),
             sizeof(size_t));
    if (offset_buffer_size > 0) {
      os.write(reinterpret_cast<const char*>(buffer.offset_buffer().data()),
               offset_buffer_size * sizeof(size_t));
    }
  }

  static void deserialize(std::istream& is, type& buffer) {
    size_t content_buffer_size;
    is.read(reinterpret_cast<char*>(&content_buffer_size), sizeof(size_t));
    if (content_buffer_size > 0) {
      buffer.content_buffer().resize(content_buffer_size);
      is.read(buffer.content_buffer().data(),
              content_buffer_size * sizeof(char));
    }
    size_t offset_buffer_size;
    is.read(reinterpret_cast<char*>(&offset_buffer_size), sizeof(size_t));
    if (offset_buffer_size > 0) {
      buffer.offset_buffer().resize(offset_buffer_size);
      is.read(reinterpret_cast<char*>(buffer.offset_buffer().data()),
              offset_buffer_size * sizeof(size_t));
    }
  }
};

}  // namespace id_indexer_impl

template <typename T>
struct GHash {
  size_t operator()(const T& val) const { return std::hash<T>()(val); }
};

template <>
struct GHash<int64_t> {
  size_t operator()(const int64_t& val) const {
    uint64_t x = static_cast<uint64_t>(val);
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    x = x ^ (x >> 31);
    return x;
  }
};

template <>
struct GHash<Property> {
  size_t operator()(const Property& val) const {
    switch (val.type()) {
#define TYPE_DISPATCHER(enum_val, type)                   \
  case DataTypeId::enum_val: {                            \
    return GHash<type>()(PropUtils<type>::to_typed(val)); \
  }
      TYPE_DISPATCHER(kInt64, int64_t)
      TYPE_DISPATCHER(kInt32, int32_t)
      TYPE_DISPATCHER(kUInt64, uint64_t)
      TYPE_DISPATCHER(kUInt32, uint32_t)
      TYPE_DISPATCHER(kVarchar, std::string_view)
#undef TYPE_DISPATCHER
    default: {
      THROW_NOT_IMPLEMENTED_EXCEPTION(
          "Hash function not implemented for type: " +
          std::to_string(val.type()));
    }
    }
  }
};

template <typename KEY_T, typename INDEX_T>
class IdIndexer;

template <typename INDEX_T>
class LFIndexer;

template <typename KEY_T, typename INDEX_T>
void build_lf_indexer(const IdIndexer<KEY_T, INDEX_T>& input,
                      const std::string& filename, LFIndexer<INDEX_T>& lf,
                      const std::string& snapshot_dir,
                      const std::string& work_dir, DataTypeId type);

template <typename INDEX_T>
class LFIndexer {
  static constexpr INDEX_T sentinel = std::numeric_limits<INDEX_T>::max();

 public:
  LFIndexer()
      : indices_(),
        indices_size_(0),
        num_elements_(0),
        num_slots_minus_one_(0),
        keys_(nullptr),
        hasher_() {}
  LFIndexer(LFIndexer&& rhs)
      : indices_(std::move(rhs.indices_)),
        indices_size_(rhs.indices_size_),
        num_elements_(rhs.num_elements_.load()),
        num_slots_minus_one_(rhs.num_slots_minus_one_),
        hasher_(rhs.hasher_) {
    if (keys_ != rhs.keys_) {
      keys_ = rhs.keys_;
    }
    hash_policy_.set_mod_function_by_index(
        rhs.hash_policy_.get_mod_function_index());
  }

  ~LFIndexer() {}

  static std::string prefix() { return "indexer"; }

  void swap(LFIndexer& other) {
    indices_.swap(other.indices_);
    std::swap(indices_size_, other.indices_size_);
    size_t temp_num = num_elements_.load();
    num_elements_.store(other.num_elements_.load());
    other.num_elements_.store(temp_num);
    std::swap(num_slots_minus_one_, other.num_slots_minus_one_);
    std::swap(keys_, other.keys_);
    hash_policy_.swap(other.hash_policy_);
    std::swap(hasher_, other.hasher_);
  }

  void init(const DataTypeId& type,
            std::shared_ptr<ExtraTypeInfo> extra_type_info = nullptr) {
    keys_ = nullptr;
    auto default_value = get_default_value(type);
    switch (type) {
#define TYPE_DISPATCHER(enum_val, T)                                   \
  case DataTypeId::enum_val: {                                         \
    keys_ = std::make_shared<TypedColumn<T>>(                          \
        PropUtils<T>::to_typed(default_value), StorageStrategy::kMem); \
    break;                                                             \
  }
      TYPE_DISPATCHER(kInt64, int64_t)
      TYPE_DISPATCHER(kInt32, int32_t)
      TYPE_DISPATCHER(kUInt64, uint64_t)
      TYPE_DISPATCHER(kUInt32, uint32_t)
#undef TYPE_DISPATCHER
    case DataTypeId::kVarchar: {
      uint16_t max_length = STRING_DEFAULT_MAX_LENGTH;
      if (extra_type_info) {
        auto str_type_info =
            std::dynamic_pointer_cast<StringTypeInfo>(extra_type_info);
        if (str_type_info) {
          max_length = str_type_info->max_length;
        }
      }
      keys_ = std::make_shared<StringColumn>(StorageStrategy::kMem, max_length);
      break;
    }
    default: {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "Only (u)int64/32 and string_view types for pk are supported, but "
          "got: " +
          std::to_string(type));
    }
    }
  }

  void build_empty_LFIndexer(const std::string& filename,
                             const std::string& snapshot_dir,
                             const std::string& work_dir) {
    keys_->open(filename + ".keys", "", work_dir);
    indices_.open(work_dir + "/" + filename + ".indices", true);

    num_elements_.store(0);
    indices_size_ = 0;
    dump_meta(work_dir + "/" + filename + ".meta");
    indices_.reset();
    keys_->close();
  }

  void reserve(size_t size) { rehash(std::max(size, num_elements_.load())); }

  void rehash(size_t size) {
    size = std::max(size, 4ul);
    keys_->resize(size);
    size =
        static_cast<size_t>(std::ceil(size / id_indexer_impl::max_load_factor));
    if (size == indices_size_) {
      return;
    }

    size_t num_elements = num_elements_.load();
    Bitset oid_set;
    oid_set.resize(num_elements);
    for (INDEX_T idx = 0; idx < num_elements; ++idx) {
      if (contains(keys_->get_prop(idx))) {
        oid_set.set(idx);
      }
    }
    auto new_prime_index = hash_policy_.next_size_over(size);
    hash_policy_.commit(new_prime_index);
    indices_.resize(size);
    indices_size_ = size;
    for (size_t k = 0; k != size; ++k) {
      indices_[k] = LFIndexer<INDEX_T>::sentinel;
    }
    num_slots_minus_one_ = size - 1;
    for (INDEX_T idx = 0; idx < num_elements; ++idx) {
      const auto& oid = keys_->get_prop(idx);
      if (oid_set.get(idx)) {
        size_t index =
            hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
        while (true) {
          if (indices_[index] == LFIndexer<INDEX_T>::sentinel) {
            indices_[index] = idx;
            break;
          }
          index = (index + 1) % (num_slots_minus_one_ + 1);
        }
      }
    }
  }

  size_t capacity() const { return keys_->size(); }

  size_t size() const { return num_elements_.load(); }
  DataTypeId get_type() const { return keys_->type(); }

  // only for update transaction
  INDEX_T insert_safe(const Property& oid) {
    INDEX_T ind = static_cast<INDEX_T>(num_elements_.load());
    if (ind >= capacity()) {
      reserve(capacity() + (capacity() >> 2));
    }
    return insert(oid);
  }

  INDEX_T insert(const Property& oid) {
    assert(oid.type() == get_type());
    INDEX_T ind = static_cast<INDEX_T>(num_elements_.fetch_add(1));
    if (!NEUG_LIKELY(ind >= 0 && ind < capacity())) {
      THROW_INTERNAL_EXCEPTION(
          "Reserved size is not enough: " + std::to_string(capacity()) +
          " vs " + std::to_string(ind));
    }
    keys_->set_any(ind, oid);
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    while (true) {
      if (__sync_bool_compare_and_swap(&indices_.data()[index],
                                       LFIndexer<INDEX_T>::sentinel, ind)) {
        break;
      }
      index = (index + 1) % (num_slots_minus_one_ + 1);
    }
    return ind;
  }

  INDEX_T get_index(const Property& oid) const {
    assert(oid.type() == get_type());
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    while (true) {
      INDEX_T ind = indices_.get(index);
      if (ind == LFIndexer<INDEX_T>::sentinel) {
        VLOG(10) << "cannot find " << oid.to_string() << " in lf_indexer";
        return ind;
      } else if (keys_->get_prop(ind) == oid) {
        return ind;
      } else {
        index = (index + 1) % (num_slots_minus_one_ + 1);
      }
    }
  }

  bool get_index(const Property& oid, INDEX_T& ret) const {
    if (indices_.size() <= 0) {
      return false;
    }
    if (oid.type() != get_type()) {
      return false;
    }
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    while (true) {
      INDEX_T ind = indices_.get(index);
      if (ind == LFIndexer<INDEX_T>::sentinel) {
        return false;
      } else if (keys_->get_prop(ind) == oid) {
        ret = ind;
        return true;
      } else {
        index = (index + 1) % (num_slots_minus_one_ + 1);
      }
    }
    return false;
  }

  bool contains(const Property& oid) const {
    assert(oid.type() == get_type());
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    while (true) {
      INDEX_T ind = indices_.get(index);
      if (ind == LFIndexer<INDEX_T>::sentinel) {
        return false;
      } else if (keys_->get_prop(ind) == oid) {
        return true;
      } else {
        index = (index + 1) % (num_slots_minus_one_ + 1);
      }
    }
  }

  Property get_key(const INDEX_T& index) const {
    return keys_->get_prop(index);
  }

  void copy_to_tmp(const std::string& cur_path, const std::string& tmp_path) {
    copy_file(cur_path + ".meta", tmp_path + ".meta");
    load_meta(tmp_path + ".meta");
    copy_file(cur_path + ".keys", tmp_path + ".keys");
    copy_file(cur_path + ".indices", tmp_path + ".indices");
  }

  void open(const std::string& name, const std::string& data_dir) {
    if (!std::filesystem::exists(data_dir + "/" + name + ".meta")) {
      build_empty_LFIndexer(name, "", data_dir);
    }

    load_meta(data_dir + "/" + name + ".meta");
    keys_->open(name + ".keys", "", data_dir);
    indices_.open(data_dir + "/" + name + ".indices", false);
    size_t num_elements = num_elements_.load();
    keys_->resize(num_elements + (num_elements >> 2));

    indices_size_ = indices_.size();
  }

  void open(const std::string& name, const std::string& checkpoint_dir,
            const std::string& work_dir) {
    if (std::filesystem::exists(checkpoint_dir + "/" + name + ".meta")) {
      copy_to_tmp(checkpoint_dir + "/" + name, tmp_dir(work_dir) + "/" + name);
    } else {
      build_empty_LFIndexer(name, "", tmp_dir(work_dir));
    }

    load_meta(tmp_dir(work_dir) + "/" + name + ".meta");
    keys_->open(name + ".keys", "", tmp_dir(work_dir));
    keys_->ensure_writable(work_dir);
    LOG(INFO) << "Open indices file in "
              << tmp_dir(work_dir) + "/" + name + ".indices";
    indices_.open(tmp_dir(work_dir) + "/" + name + ".indices", true);
    size_t num_elements = num_elements_.load();

    keys_->resize(num_elements + (num_elements >> 2));

    indices_size_ = indices_.size();
  }

  void open_in_memory(const std::string& name) {
    if (std::filesystem::exists(name + ".meta")) {
      load_meta(name + ".meta");
    } else {
      num_elements_.store(0);
    }
    keys_->open_in_memory(name + ".keys");
    indices_.open(name + ".indices", false);
    indices_size_ = indices_.size();
    size_t num_elements = num_elements_.load();
    keys_->resize(num_elements + (num_elements >> 2));
  }

  void open_with_hugepages(const std::string& name, bool hugepage_table) {
    if (std::filesystem::exists(name + ".meta")) {
      load_meta(name + ".meta");
    } else {
      num_elements_.store(0);
    }
    keys_->open_with_hugepages(name + ".keys", true);
    if (hugepage_table) {
      indices_.open_with_hugepages(name + ".indices");
    } else {
      indices_.open(name + ".indices", false);
    }
    indices_size_ = indices_.size();
    size_t num_elements = num_elements_.load();
    keys_->resize(num_elements + (num_elements >> 2));
  }

  void dump(const std::string& name, const std::string& snapshot_dir) {
    keys_->resize(num_elements_.load());
    keys_->dump(snapshot_dir + "/" + name + ".keys");
    indices_.dump(snapshot_dir + "/" + name + ".indices");
    dump_meta(snapshot_dir + "/" + name + ".meta");
    close();
  }

  void close() {
    keys_->close();
    indices_.reset();
  }

  void drop() {
    close();
    // TODO(zhanglei): delete files in work_dir
  }

  void dump_meta(const std::string& filename) const {
    InArchive arc;
    arc << get_type() << num_elements_.load() << num_slots_minus_one_
        << hash_policy_.get_mod_function_index();
    FILE* fout = fopen(filename.c_str(), "wb");
    fwrite(arc.GetBuffer(), sizeof(char), arc.GetSize(), fout);
    fflush(fout);
    fclose(fout);
  }

  void load_meta(const std::string& filename) {
    OutArchive arc;
    FILE* fin = fopen(filename.c_str(), "r");
    size_t meta_file_size = std::filesystem::file_size(filename);
    std::vector<char> buf(meta_file_size);
    CHECK_EQ(fread(buf.data(), sizeof(char), meta_file_size, fin),
             meta_file_size);
    arc.SetSlice(buf.data(), meta_file_size);
    size_t mod_function_index;
    DataTypeId type;
    arc >> type;
    size_t num_elements;
    arc >> num_elements;

    num_elements_.store(num_elements);
    arc >> num_slots_minus_one_ >> mod_function_index;
    init(type);
    hash_policy_.set_mod_function_by_index(mod_function_index);
    fclose(fin);
  }

  // get keys
  const ColumnBase& get_keys() const { return *keys_; }

  void ensure_writable(const std::string& work_dir) {
    indices_.ensure_writable(work_dir);
    keys_->ensure_writable(work_dir);
  }

 private:
  mmap_array<INDEX_T>
      indices_;  // size() == indices_size_ == num_slots_minus_one_ +
                 // log(num_slots_minus_one_)
  size_t indices_size_;
  std::atomic<size_t> num_elements_;
  size_t num_slots_minus_one_;
  std::shared_ptr<ColumnBase> keys_;

  ska::ska::prime_number_hash_policy hash_policy_;
  GHash<Property> hasher_;

  // _KEY_T is defined in sys/_types/_key_t.h on macos
  template <typename __KEY_T, typename _INDEX_T>
  friend void build_lf_indexer(const IdIndexer<__KEY_T, _INDEX_T>& input,
                               const std::string& filename,
                               LFIndexer<_INDEX_T>& output,
                               const std::string& snapshot_dir,
                               const std::string& work_dir, DataTypeId type);
};

template <typename INDEX_T>
class IdIndexerBase {
 public:
  IdIndexerBase() = default;
  virtual ~IdIndexerBase() = default;
  virtual DataTypeId get_type() const = 0;
  virtual void _add(const Property& oid) = 0;
  virtual bool add(const Property& oid, INDEX_T& lid) = 0;
  virtual bool get_key(const INDEX_T& lid, Property& oid) const = 0;
  virtual bool get_index(const Property& oid, INDEX_T& lid) const = 0;
  virtual size_t size() const = 0;
};

template <typename KEY_T, typename INDEX_T>
class IdIndexer : public IdIndexerBase<INDEX_T> {
 public:
  using key_buffer_t = typename id_indexer_impl::KeyBuffer<KEY_T>::type;
  using ind_buffer_t = std::vector<INDEX_T>;
  using dist_buffer_t = std::vector<int8_t>;

  IdIndexer() : hasher_() { reset_to_empty_state(); }
  ~IdIndexer() {}

  DataTypeId get_type() const override { return PropUtils<KEY_T>::prop_type(); }

  void _add(const Property& oid) override {
    assert(get_type() == oid.type());
    KEY_T oid_ = PropUtils<KEY_T>::to_typed(oid);
    _add(oid_);
  }

  bool add(const Property& oid, INDEX_T& lid) override {
    assert(get_type() == oid.type());
    KEY_T oid_ = PropUtils<KEY_T>::to_typed(oid);
    return add(oid_, lid);
  }

  bool get_key(const INDEX_T& lid, Property& oid) const override {
    KEY_T oid_;
    bool flag = get_key(lid, oid_);
    if (flag) {
      oid = Property::From(oid_);
    }
    return flag;
  }

  bool get_index(const Property& oid, INDEX_T& lid) const override {
    assert(get_type() == oid.type());
    KEY_T oid_ = PropUtils<KEY_T>::to_typed(oid);
    return get_index(oid_, lid);
  }
  void Clear() {
    keys_.clear();
    indices_.clear();
    distances_.clear();
    num_elements_ = 0;
    num_slots_minus_one_ = 0;
    hash_policy_.reset();
  }

  size_t entry_num() const { return distances_.size(); }

  bool add(const KEY_T& oid, INDEX_T& lid) {
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);

    int8_t distance_from_desired = 0;
    for (; distances_[index] >= distance_from_desired;
         ++index, ++distance_from_desired) {
      INDEX_T cur_lid = indices_[index];
      if (keys_[cur_lid] == oid) {
        lid = cur_lid;
        return false;
      }
    }

    lid = static_cast<INDEX_T>(keys_.size());
    keys_.push_back(oid);
    assert(keys_.size() == num_elements_ + 1);
    emplace_new_value(distance_from_desired, index, lid);
    assert(keys_.size() == num_elements_);
    return true;
  }

  bool remove(const KEY_T& oid) {
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);

    int8_t distance_from_desired = 0;
    for (; distances_[index] >= distance_from_desired;
         ++index, ++distance_from_desired) {
      INDEX_T cur_lid = indices_[index];
      if (keys_[cur_lid] == oid) {
        keys_[cur_lid] = keys_.back();
        keys_.pop_back();
        indices_[index] = indices_.back();
        indices_.pop_back();
        distances_[index] = -1;
        --num_elements_;
        return true;
      }
    }
    return false;
  }

  bool add(KEY_T&& oid, INDEX_T& lid) {
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);

    int8_t distance_from_desired = 0;
    for (; distances_[index] >= distance_from_desired;
         ++index, ++distance_from_desired) {
      INDEX_T cur_lid = indices_[index];
      if (keys_[cur_lid] == oid) {
        lid = cur_lid;
        return false;
      }
    }

    lid = static_cast<INDEX_T>(keys_.size());
    keys_.push_back(std::move(oid));
    assert(keys_.size() == num_elements_ + 1);
    emplace_new_value(distance_from_desired, index, lid);
    assert(keys_.size() == num_elements_);
    return true;
  }

  bool _add(const KEY_T& oid, size_t hash_value, INDEX_T& lid) {
    size_t index =
        hash_policy_.index_for_hash(hash_value, num_slots_minus_one_);

    int8_t distance_from_desired = 0;
    for (; distances_[index] >= distance_from_desired;
         ++index, ++distance_from_desired) {
      INDEX_T cur_lid = indices_[index];
      if (keys_[cur_lid] == oid) {
        lid = cur_lid;
        return false;
      }
    }

    lid = static_cast<INDEX_T>(keys_.size());
    keys_.push_back(oid);
    assert(keys_.size() == num_elements_ + 1);
    emplace_new_value(distance_from_desired, index, lid);
    assert(keys_.size() == num_elements_);
    return true;
  }

  bool _add(KEY_T&& oid, size_t hash_value, INDEX_T& lid) {
    size_t index =
        hash_policy_.index_for_hash(hash_value, num_slots_minus_one_);

    int8_t distance_from_desired = 0;
    for (; distances_[index] >= distance_from_desired;
         ++index, ++distance_from_desired) {
      INDEX_T cur_lid = indices_[index];
      if (keys_[cur_lid] == oid) {
        lid = cur_lid;
        return false;
      }
    }

    lid = static_cast<INDEX_T>(keys_.size());
    keys_.push_back(std::move(oid));
    assert(keys_.size() == num_elements_ + 1);
    emplace_new_value(distance_from_desired, index, lid);
    assert(keys_.size() == num_elements_);
    return true;
  }

  void _add(const KEY_T& oid) {
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);

    int8_t distance_from_desired = 0;
    for (; distances_[index] >= distance_from_desired;
         ++index, ++distance_from_desired) {
      if (keys_[indices_[index]] == oid) {
        return;
      }
    }

    INDEX_T lid = static_cast<INDEX_T>(keys_.size());
    keys_.push_back(oid);
    assert(keys_.size() == num_elements_ + 1);
    emplace_new_value(distance_from_desired, index, lid);
    assert(keys_.size() == num_elements_);
  }

  void _add(KEY_T&& oid) {
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);

    int8_t distance_from_desired = 0;
    for (; distances_[index] >= distance_from_desired;
         ++index, ++distance_from_desired) {
      if (keys_[indices_[index]] == oid) {
        return;
      }
    }

    INDEX_T lid = static_cast<INDEX_T>(keys_.size());
    keys_.push_back(std::move(oid));
    assert(keys_.size() == num_elements_ + 1);
    emplace_new_value(distance_from_desired, index, lid);
    assert(keys_.size() == num_elements_);
  }

  size_t bucket_count() const {
    return num_slots_minus_one_ ? num_slots_minus_one_ + 1 : 0;
  }

  bool empty() const { return (num_elements_ == 0); }

  size_t size() const override { return num_elements_; }

  bool get_key(INDEX_T lid, KEY_T& oid) const {
    if (static_cast<size_t>(lid) >= num_elements_) {
      return false;
    }
    oid = keys_[lid];
    return true;
  }

  const KEY_T& get_key(INDEX_T lid) const {
    if (static_cast<size_t>(lid) >= num_elements_) {
      THROW_INDEX_EXCEPTION("Index out of range in IdIndexer::get_key " +
                            std::to_string(lid) + " with size " +
                            std::to_string(num_elements_));
    }
    return keys_[lid];
  }

  bool get_index(const KEY_T& oid, INDEX_T& lid) const {
    size_t index =
        hash_policy_.index_for_hash(hasher_(oid), num_slots_minus_one_);
    for (int8_t distance = 0; distances_[index] >= distance;
         ++distance, ++index) {
      INDEX_T ret = indices_[index];
      if (keys_[ret] == oid) {
        lid = ret;
        return true;
      }
    }
    return false;
  }

  bool _get_index(const KEY_T& oid, size_t hash, INDEX_T& lid) const {
    size_t index = hash_policy_.index_for_hash(hash, num_slots_minus_one_);
    for (int8_t distance = 0; distances_[index] >= distance;
         ++distance, ++index) {
      INDEX_T ret = indices_[index];
      if (keys_[ret] == oid) {
        lid = ret;
        return true;
      }
    }
    return false;
  }

  void swap(IdIndexer<KEY_T, INDEX_T>& rhs) {
    keys_.swap(rhs.keys_);
    indices_.swap(rhs.indices_);
    distances_.swap(rhs.distances_);

    hash_policy_.swap(rhs.hash_policy_);
    std::swap(max_lookups_, rhs.max_lookups_);
    std::swap(num_elements_, rhs.num_elements_);
    std::swap(num_slots_minus_one_, rhs.num_slots_minus_one_);

    std::swap(hasher_, rhs.hasher_);
  }

  const key_buffer_t& keys() const { return keys_; }

  key_buffer_t& keys() { return keys_; }

  void Serialize(std::ostream& os) const {
    id_indexer_impl::KeyBuffer<KEY_T>::serialize(os, keys_);
    InArchive arc;
    arc << hash_policy_.get_mod_function_index() << max_lookups_
        << num_elements_ << num_slots_minus_one_ << indices_.size()
        << distances_.size();
    size_t arc_size = arc.GetSize();
    os.write(reinterpret_cast<char*>(&arc_size), sizeof(size_t));
    os.write(arc.GetBuffer(), arc_size);

    arc.Clear();

    if (indices_.size() > 0) {
      os.write(reinterpret_cast<const char*>(indices_.data()),
               indices_.size() * sizeof(INDEX_T));
    }
    if (distances_.size() > 0) {
      os.write(reinterpret_cast<const char*>(distances_.data()),
               distances_.size() * sizeof(int8_t));
    }
  }

  void Deserialize(std::istream& is) {
    id_indexer_impl::KeyBuffer<KEY_T>::deserialize(is, keys_);
    OutArchive arc;
    size_t arc_size;
    is.read(reinterpret_cast<char*>(&arc_size), sizeof(size_t));
    arc.Allocate(arc_size);
    is.read(arc.GetBuffer(), arc_size);
    size_t mod_function_index;
    size_t indices_size, distances_size;
    arc >> mod_function_index >> max_lookups_ >> num_elements_ >>
        num_slots_minus_one_ >> indices_size >> distances_size;
    arc.Clear();

    hash_policy_.set_mod_function_by_index(mod_function_index);
    indices_.resize(indices_size);
    distances_.resize(distances_size);
    if (indices_size > 0) {
      is.read(reinterpret_cast<char*>(indices_.data()),
              indices_.size() * sizeof(INDEX_T));
    }
    if (distances_size > 0) {
      is.read(reinterpret_cast<char*>(distances_.data()),
              distances_.size() * sizeof(int8_t));
    }
  }

  void _rehash(size_t num) { rehash(num); }

 private:
  void emplace(INDEX_T lid) {
    KEY_T key = keys_[lid];
    size_t index =
        hash_policy_.index_for_hash(hasher_(key), num_slots_minus_one_);
    int8_t distance_from_desired = 0;
    for (; distances_[index] >= distance_from_desired;
         ++index, ++distance_from_desired) {
      if (indices_[index] == lid) {
        return;
      }
    }

    emplace_new_value(distance_from_desired, index, lid);
  }

  void emplace_new_value(int8_t distance_from_desired, size_t index,
                         INDEX_T lid) {
    if (num_slots_minus_one_ == 0 || distance_from_desired == max_lookups_ ||
        num_elements_ + 1 >
            (num_slots_minus_one_ + 1) * id_indexer_impl::max_load_factor) {
      grow();
      return;
    } else if (distances_[index] < 0) {
      indices_[index] = lid;
      distances_[index] = distance_from_desired;
      ++num_elements_;
      return;
    }
    INDEX_T to_insert = lid;
    std::swap(distance_from_desired, distances_[index]);
    std::swap(to_insert, indices_[index]);
    for (++distance_from_desired, ++index;; ++index) {
      if (distances_[index] < 0) {
        indices_[index] = to_insert;
        distances_[index] = distance_from_desired;
        ++num_elements_;
        return;
      } else if (distances_[index] < distance_from_desired) {
        std::swap(distance_from_desired, distances_[index]);
        std::swap(to_insert, indices_[index]);
        ++distance_from_desired;
      } else {
        ++distance_from_desired;
        if (distance_from_desired == max_lookups_) {
          grow();
          return;
        }
      }
    }
  }

  void grow() { rehash(std::max(size_t(4), 2 * bucket_count())); }

  void rehash(size_t num_buckets) {
    num_buckets = std::max(
        num_buckets, static_cast<size_t>(std::ceil(
                         num_elements_ / id_indexer_impl::max_load_factor)));

    if (num_buckets == 0) {
      reset_to_empty_state();
      return;
    }

    auto new_prime_index = hash_policy_.next_size_over(num_buckets);
    if (num_buckets == bucket_count()) {
      return;
    }

    int8_t new_max_lookups = compute_max_lookups(num_buckets);

    dist_buffer_t new_distances(num_buckets + new_max_lookups);
    ind_buffer_t new_indices(num_buckets + new_max_lookups,
                             std::numeric_limits<INDEX_T>::max());

    size_t special_end_index = num_buckets + new_max_lookups - 1;
    for (size_t i = 0; i != special_end_index; ++i) {
      new_distances[i] = -1;
    }
    new_distances[special_end_index] = 0;

    new_indices.swap(indices_);
    new_distances.swap(distances_);

    std::swap(num_slots_minus_one_, num_buckets);
    --num_slots_minus_one_;
    hash_policy_.commit(new_prime_index);

    max_lookups_ = new_max_lookups;

    num_elements_ = 0;
    INDEX_T elem_num = static_cast<INDEX_T>(keys_.size());
    for (INDEX_T lid = 0; lid < elem_num; ++lid) {
      emplace(lid);
    }
  }

  void reset_to_empty_state() {
    keys_.clear();

    indices_.clear();
    distances_.clear();
    indices_.resize(id_indexer_impl::min_lookups,
                    std::numeric_limits<INDEX_T>::max());
    distances_.resize(id_indexer_impl::min_lookups, -1);
    distances_[id_indexer_impl::min_lookups - 1] = 0;

    num_slots_minus_one_ = 0;
    hash_policy_.reset();
    max_lookups_ = id_indexer_impl::min_lookups - 1;
    num_elements_ = 0;
  }

  static int8_t compute_max_lookups(size_t num_buckets) {
    int8_t desired = id_indexer_impl::log2(num_buckets);
    return std::max(id_indexer_impl::min_lookups, desired);
  }

  key_buffer_t keys_;
  ind_buffer_t indices_;
  dist_buffer_t distances_;

  ska::ska::prime_number_hash_policy hash_policy_;
  int8_t max_lookups_ = id_indexer_impl::min_lookups - 1;
  size_t num_elements_ = 0;
  size_t num_slots_minus_one_ = 0;

  GHash<KEY_T> hasher_;

  template <typename __KEY_T, typename _INDEX_T>
  friend void build_lf_indexer(const IdIndexer<__KEY_T, _INDEX_T>& input,
                               const std::string& filename,
                               LFIndexer<_INDEX_T>& output,
                               const std::string& snapshot_dir,
                               const std::string& work_dir, DataTypeId type);
};

template <typename KEY_T, typename INDEX_T>
struct _move_data {
  using key_buffer_t = typename id_indexer_impl::KeyBuffer<KEY_T>::type;
  void operator()(const key_buffer_t& input, ColumnBase& col, size_t size) {
    auto& keys = dynamic_cast<TypedColumn<KEY_T>&>(col);
    for (size_t idx = 0; idx < size; ++idx) {
      keys.set_value(idx, input[idx]);
    }
  }
};

template <typename INDEX_T>
struct _move_data<std::string_view, INDEX_T> {
  using key_buffer_t =
      typename id_indexer_impl::KeyBuffer<std::string_view>::type;
  void operator()(const key_buffer_t& input, ColumnBase& col, size_t size) {
    auto& keys = dynamic_cast<StringColumn&>(col);
    for (size_t idx = 0; idx < size; ++idx) {
      keys.set_value(idx, input[idx]);
    }
  }
};

template <typename KEY_T, typename INDEX_T>
void build_lf_indexer(const IdIndexer<KEY_T, INDEX_T>& input,
                      const std::string& filename, LFIndexer<INDEX_T>& lf,
                      const std::string& snapshot_dir,
                      const std::string& work_dir, DataTypeId type) {
  size_t size = input.keys_.size();
  lf.init(type);
  lf.keys_->open(filename + ".keys", "", work_dir);
  lf.keys_->resize(size);
  _move_data<KEY_T, INDEX_T>()(input.keys_, *lf.keys_, size);
  lf.num_elements_.store(size);

  lf.indices_.open(snapshot_dir + "/" + filename + ".indices", true);
  lf.indices_.resize(input.num_slots_minus_one_ + 1);

  lf.indices_size_ = input.indices_.size();

  lf.hash_policy_.set_mod_function_by_index(
      input.hash_policy_.get_mod_function_index());
  lf.num_slots_minus_one_ = input.num_slots_minus_one_;
  memcpy(lf.indices_.data(), input.indices_.data(),
         lf.indices_.size() * sizeof(INDEX_T));

  std::vector<INDEX_T> residuals;
  for (size_t idx = lf.indices_.size(); idx < lf.indices_size_; ++idx) {
    if (input.indices_[idx] != LFIndexer<INDEX_T>::sentinel) {
      residuals.push_back(input.indices_[idx]);
    }
  }
  for (const auto& lid : residuals) {
    auto oid = input.keys_[lid];
    size_t index = input.hash_policy_.index_for_hash(
        input.hasher_(oid), input.num_slots_minus_one_);
    while (true) {
      if (lf.indices_[index] == lid) {
        break;
      } else if (lf.indices_[index] == LFIndexer<INDEX_T>::sentinel) {
        lf.indices_[index] = lid;
        break;
      }
      index = (index + 1) % (input.num_slots_minus_one_ + 1);
    }
  }
  lf.dump_meta(snapshot_dir + "/" + filename + ".meta");

  lf.keys_->dump(snapshot_dir + "/" + filename + ".keys");
  std::filesystem::remove(work_dir + "/" + filename + ".meta");
  lf.keys_->close();
  lf.keys_->open(filename + ".keys", snapshot_dir, "");
}

}  // namespace neug
