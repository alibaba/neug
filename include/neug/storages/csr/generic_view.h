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

#include "neug/storages/csr/nbr.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/property.h"
#include "neug/utils/property/types.h"

namespace gs {

struct NbrIterConfig {
  int stride : 16;
  int ts_offset : 8;
  int data_offset : 8;
};

struct NbrIterator {
  NbrIterator() = default;
  ~NbrIterator() = default;

  void init(const void* ptr, const void* end, NbrIterConfig cfg,
            timestamp_t timestamp) {
    cur = ptr;
    this->end = end;
    this->cfg = cfg;
    this->timestamp = timestamp;
    int stride = cfg.stride;
    while (cur != end && get_timestamp() > timestamp) {
      cur = static_cast<const char*>(cur) + stride;
    }
  }

  inline vid_t operator*() const { return get_vertex(); }

  __attribute__((always_inline)) NbrIterator& operator++() {
    cur = static_cast<const char*>(cur) + cfg.stride;
    while (cur != end && get_timestamp() > timestamp) {
      cur = static_cast<const char*>(cur) + cfg.stride;
    }
    return *this;
  }

  __attribute__((always_inline)) NbrIterator& operator+=(size_t n) {
    for (size_t i = 0; i < n; ++i) {
      ++(*this);
    }
    return *this;
  }

  __attribute__((always_inline)) bool operator==(const NbrIterator& rhs) const {
    return (cur == rhs.cur);
  }

  __attribute__((always_inline)) bool operator!=(const NbrIterator& rhs) const {
    return (cur != rhs.cur);
  }

  inline timestamp_t get_timestamp() const {
    return *(reinterpret_cast<const timestamp_t*>(
        static_cast<const char*>(cur) + cfg.ts_offset));
  }

  inline vid_t get_vertex() const {
    return *reinterpret_cast<const vid_t*>(cur);
  }

  inline const void* get_data_ptr() const {
    return static_cast<const char*>(cur) + cfg.data_offset;
  }

  inline const void* get_nbr_ptr() const { return cur; }

  inline const timestamp_t* get_timestamp_ptr() const {
    return cfg.ts_offset == 0
               ? nullptr
               : reinterpret_cast<const timestamp_t*>(
                     static_cast<const char*>(cur) + cfg.ts_offset);
  }

  const void* cur;
  const void* end;
  NbrIterConfig cfg;
  timestamp_t timestamp;
};

static_assert(std::is_pod<NbrIterator>::value, "NbrIterator should be POD");
struct NbrList {
  NbrList() = default;
  ~NbrList() = default;

  __attribute__((always_inline)) NbrIterator begin() const {
    NbrIterator it;
    it.init(start_ptr, end_ptr, cfg, timestamp);
    return it;
  }
  __attribute__((always_inline)) NbrIterator end() const {
    NbrIterator it;
    it.init(end_ptr, end_ptr, cfg, timestamp);
    return it;
  }

  bool empty() const { return start_ptr == end_ptr; }

  const void* start_ptr;
  const void* end_ptr;
  NbrIterConfig cfg;
  timestamp_t timestamp;
};

static_assert(std::is_pod<NbrList>::value, "NbrList should be POD");

struct EdgeDataAccessor {
  EdgeDataAccessor()
      : data_type_(PropertyType::kEmpty), data_column_(nullptr) {}
  EdgeDataAccessor(PropertyType data_type, ColumnBase* data_column)
      : data_type_(data_type), data_column_(data_column) {}
  EdgeDataAccessor(const EdgeDataAccessor& other)
      : data_type_(other.data_type_), data_column_(other.data_column_) {}

  bool is_bundled() const { return data_column_ == nullptr; }

  inline Property get_data(const NbrIterator& it) const {
    return data_column_ == nullptr
               ? get_generic_bundled_data_from_ptr(it.get_data_ptr())
               : get_generic_column_data(
                     *reinterpret_cast<const size_t*>(it.get_data_ptr()));
  }

  template <typename T>
  inline T get_typed_data(const NbrIterator& it) const {
    if constexpr (std::is_same<T, EmptyType>::value) {
      return EmptyType();
    } else {
      return data_column_ == nullptr
                 ? get_bundled_data_from_ptr<T>(it.get_data_ptr())
                 : get_column_data<T>(
                       *reinterpret_cast<const size_t*>(it.get_data_ptr()));
    }
  }

  template <typename T>
  inline T get_typed_data_from_ptr(const void* data_ptr) const {
    if constexpr (std::is_same<T, EmptyType>::value) {
      return EmptyType();
    } else {
      auto ret =
          data_column_ == nullptr
              ? *reinterpret_cast<const T*>(data_ptr)
              : get_column_data<T>(*reinterpret_cast<const size_t*>(data_ptr));
      return ret;
    }
  }

  inline Property get_data_from_ptr(const void* data_ptr) const {
    return data_column_ == nullptr
               ? get_generic_bundled_data_from_ptr(data_ptr)
               : get_generic_column_data(
                     *reinterpret_cast<const size_t*>(data_ptr));
  }

  inline void set_data(const NbrIterator& it, const Property& prop,
                       timestamp_t ts) {
    if (it.cfg.ts_offset != 0) {
      *const_cast<timestamp_t*>(it.get_timestamp_ptr()) = ts;
    }
    if (data_column_ != nullptr) {
      size_t idx = get_bundled_data_from_ptr<size_t>(it.get_data_ptr());
      data_column_->set_prop(idx, prop);
    } else {
      if (data_type_ == PropertyType::kEmpty) {
        return;
      } else if (data_type_ == PropertyType::kInt32) {
        *reinterpret_cast<int32_t*>(const_cast<void*>(it.get_data_ptr())) =
            prop.as_int32();
      } else if (data_type_ == PropertyType::kUInt32) {
        *reinterpret_cast<uint32_t*>(const_cast<void*>(it.get_data_ptr())) =
            prop.as_uint32();
      } else if (data_type_ == PropertyType::kInt64) {
        *reinterpret_cast<int64_t*>(const_cast<void*>(it.get_data_ptr())) =
            prop.as_int64();
      } else if (data_type_ == PropertyType::kUInt64) {
        *reinterpret_cast<uint64_t*>(const_cast<void*>(it.get_data_ptr())) =
            prop.as_uint64();
      } else if (data_type_.type_enum == impl::PropertyTypeImpl::kStringView) {
        *reinterpret_cast<std::string_view*>(
            const_cast<void*>(it.get_data_ptr())) = prop.as_string();
      } else if (data_type_ == PropertyType::kFloat) {
        *reinterpret_cast<float*>(const_cast<void*>(it.get_data_ptr())) =
            prop.as_float();
      } else if (data_type_ == PropertyType::kDouble) {
        *reinterpret_cast<double*>(const_cast<void*>(it.get_data_ptr())) =
            prop.as_double();
      } else if (data_type_ == PropertyType::kDate) {
        *reinterpret_cast<Date*>(const_cast<void*>(it.get_data_ptr())) =
            prop.as_date();
      } else if (data_type_ == PropertyType::kDateTime) {
        *reinterpret_cast<DateTime*>(const_cast<void*>(it.get_data_ptr())) =
            prop.as_datetime();
      } else if (data_type_ == PropertyType::kInterval) {
        *reinterpret_cast<Interval*>(const_cast<void*>(it.get_data_ptr())) =
            prop.as_interval();
      } else {
        LOG(FATAL) << "type - " << data_type_.ToString()
                   << " - not implemented";
      }
    }
  }

 private:
  template <typename T>
  inline T get_bundled_data_from_ptr(const void* data_ptr) const {
    return *reinterpret_cast<const T*>(data_ptr);
  }

  template <typename T>
  inline T get_column_data(size_t idx) const {
    return reinterpret_cast<const TypedColumn<T>*>(data_column_)->get_view(idx);
  }

  inline Property get_generic_bundled_data_from_ptr(
      const void* data_ptr) const {
    if (data_type_ == PropertyType::kEmpty) {
      return Property::empty();
    } else if (data_type_ == PropertyType::kInt32) {
      return Property::from_int32(get_bundled_data_from_ptr<int32_t>(data_ptr));
    } else if (data_type_ == PropertyType::kUInt32) {
      return Property::from_uint32(
          get_bundled_data_from_ptr<uint32_t>(data_ptr));
    } else if (data_type_ == PropertyType::kInt64) {
      return Property::from_int64(get_bundled_data_from_ptr<int64_t>(data_ptr));
    } else if (data_type_ == PropertyType::kUInt64) {
      return Property::from_uint64(
          get_bundled_data_from_ptr<uint64_t>(data_ptr));
    } else if (data_type_.type_enum == impl::PropertyTypeImpl::kStringView) {
      return Property::from_string_view(
          get_bundled_data_from_ptr<std::string_view>(data_ptr));
    } else if (data_type_ == PropertyType::kFloat) {
      return Property::from_float(get_bundled_data_from_ptr<float>(data_ptr));
    } else if (data_type_ == PropertyType::kDouble) {
      return Property::from_double(get_bundled_data_from_ptr<double>(data_ptr));
    } else if (data_type_ == PropertyType::kDate) {
      return Property::from_date(get_bundled_data_from_ptr<Date>(data_ptr));
    } else if (data_type_ == PropertyType::kDateTime) {
      return Property::from_datetime(
          get_bundled_data_from_ptr<DateTime>(data_ptr));
    } else if (data_type_ == PropertyType::kInterval) {
      return Property::from_interval(
          get_bundled_data_from_ptr<Interval>(data_ptr));
    } else {
      LOG(FATAL) << "type - " << data_type_.ToString() << " - not implemented";
      return Property::empty();
    }
  }

  inline Property get_generic_column_data(size_t idx) const {
    return data_column_->get_prop(idx);
  }

  PropertyType data_type_;
  ColumnBase* data_column_;
};

enum class CsrViewType {
  kSingleMutable,
  kMultipleMutable,
  kSingleImmutable,
  kMultipleImmutable,
};

template <typename T, CsrViewType TYPE>
struct TypedView {
  TypedView() = default;
  ~TypedView() = default;
};

template <typename T>
struct TypedView<T, CsrViewType::kMultipleMutable> {
  TypedView(const MutableNbr<T>** adjlists, const int* degrees,
            timestamp_t timestamp, timestamp_t unsorted_since)
      : adjlists(adjlists),
        degrees(degrees),
        timestamp(timestamp),
        unsorted_since(unsorted_since) {}
  ~TypedView() = default;

  template <typename FUNC_T>
  void foreach_nbr_gt(vid_t v, const T& threshold, const FUNC_T& func) const {
    const MutableNbr<T>* ptr = adjlists[v] + degrees[v] - 1;
    const MutableNbr<T>* end = adjlists[v] - 1;
    while (ptr != end) {
      if (ptr->timestamp > timestamp) {
        --ptr;
        continue;
      }
      if (ptr->timestamp < unsorted_since) {
        break;
      }
      if (threshold < ptr->data) {
        func(ptr->neighbor, ptr->data);
      }
      --ptr;
    }
    while (ptr != end) {
      if (threshold < ptr->data) {
        func(ptr->neighbor, ptr->data);
      } else {
        break;
      }
      --ptr;
    }
  }

  template <typename FUNC_T>
  void foreach_nbr_lt(vid_t v, const T& threshold, const FUNC_T& func) const {
    const MutableNbr<T>* ptr = adjlists[v] + degrees[v] - 1;
    const MutableNbr<T>* end = adjlists[v] - 1;
    while (ptr != end) {
      if (ptr->timestamp > timestamp) {
        --ptr;
        continue;
      }
      if (ptr->timestamp < unsorted_since) {
        break;
      }
      if (threshold < ptr->data) {
        func(ptr->neighbor, ptr->data);
      }
      --ptr;
    }
    if (ptr == end) {
      return;
    }
    ptr = std::upper_bound(
              adjlists[v], ptr + 1, threshold,
              [](const T& a, const MutableNbr<T>& b) { return a < b.data; }) -
          1;
    while (ptr != end) {
      func(ptr->neighbor, ptr->data);
      --ptr;
    }
  }

  const MutableNbr<T>** adjlists;
  const int* degrees;
  timestamp_t timestamp;
  timestamp_t unsorted_since;
};

struct GenericView {
  GenericView()
      : adjlists_(nullptr),
        degrees_(nullptr),
        cfg_({0, 0, 0}),
        timestamp_(0),
        unsorted_since_(0) {}

  GenericView(const char* adjlists, const int* degrees, NbrIterConfig cfg,
              timestamp_t timestamp, timestamp_t unsorted_since)
      : adjlists_(adjlists),
        degrees_(degrees),
        cfg_(cfg),
        timestamp_(timestamp),
        unsorted_since_(unsorted_since) {}

  GenericView(const char* adjlists, NbrIterConfig cfg, timestamp_t timestamp,
              timestamp_t unsorted_since)
      : adjlists_(adjlists),
        degrees_(nullptr),
        cfg_(cfg),
        timestamp_(timestamp),
        unsorted_since_(unsorted_since) {}

  CsrViewType type() const {
    if (degrees_ == nullptr) {
      if (cfg_.ts_offset != 0) {
        return CsrViewType::kSingleMutable;
      } else {
        return CsrViewType::kSingleImmutable;
      }
    } else {
      if (cfg_.ts_offset != 0) {
        return CsrViewType::kMultipleMutable;
      } else {
        return CsrViewType::kMultipleImmutable;
      }
    }
  }

  __attribute__((always_inline)) NbrList get_edges(vid_t v) const {
    NbrList ret;
    if (degrees_ == nullptr) {
      // single
      const char* start_ptr = adjlists_ + v * cfg_.stride;
      ret.start_ptr = start_ptr;
      ret.end_ptr = start_ptr + cfg_.stride;
    } else {
      // multiple
      const char* start_ptr = reinterpret_cast<const char*>(
          reinterpret_cast<const int64_t*>(adjlists_)[v]);
      ret.start_ptr = start_ptr;
      ret.end_ptr = start_ptr + degrees_[v] * cfg_.stride;
    }
    ret.cfg = cfg_;
    ret.timestamp = timestamp_;

    return ret;
  }

  template <typename T, CsrViewType TYPE>
  TypedView<T, TYPE> get_typed_view() const {
    if constexpr (TYPE == CsrViewType::kMultipleMutable) {
      assert(cfg_.ts_offset != 0);
      assert(cfg_.stride == sizeof(MutableNbr<T>));
      int64_t val = reinterpret_cast<int64_t>(adjlists_);
      const MutableNbr<T>** lists =
          reinterpret_cast<const MutableNbr<T>**>(val);
      return TypedView<T, TYPE>(lists, degrees_, timestamp_, unsorted_since_);
    } else {
      LOG(FATAL) << "not implemented";
      return TypedView<T, TYPE>();
    }
  }

 private:
  const char* adjlists_;
  const int* degrees_;
  NbrIterConfig cfg_;
  timestamp_t timestamp_;
  timestamp_t unsorted_since_;
};

}  // namespace gs
