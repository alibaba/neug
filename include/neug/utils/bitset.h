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

#include <cassert>
#include <cstdlib>
#include <istream>
#include <memory>
#include <ostream>
#include <utility>

#include <cstring>

#include <glog/logging.h>

#ifdef _WIN32
#include <intrin.h>
#include <malloc.h>
#endif

#define WORD_SIZE(n) (((n) + 63ul) >> 6)
#define NEUG_BYTE_SIZE(n) (WORD_SIZE(n) * sizeof(uint64_t))

#define WORD_INDEX(i) ((i) >> 6)
#define BIT_OFFSET(i) ((i) &0x3f)

#define ROUND_UP(i) (((i) + 63ul) & (~63ul))
#define ROUND_DOWN(i) ((i) & (~63ul))

// Round up to the nearest multiple of alignment
#define ROUND_UP_TO_ALIGNMENT(i, alignment) \
  (((i) + (alignment) -1) & (~((alignment) -1)))

namespace neug {
namespace detail {

inline void* AlignedAlloc(size_t alignment, size_t size) {
#ifdef _WIN32
  return _aligned_malloc(size, alignment);
#else
  return aligned_alloc(alignment, size);
#endif
}

inline void AlignedFree(void* ptr) {
#ifdef _WIN32
  _aligned_free(ptr);
#else
  free(ptr);
#endif
}

inline uint64_t AtomicFetchOr(uint64_t* ptr, uint64_t value) {
#ifdef _WIN32
  return static_cast<uint64_t>(_InterlockedOr64(
      reinterpret_cast<volatile __int64*>(ptr), static_cast<__int64>(value)));
#else
  return __sync_fetch_and_or(ptr, value);
#endif
}

inline uint64_t AtomicFetchAnd(uint64_t* ptr, uint64_t value) {
#ifdef _WIN32
  return static_cast<uint64_t>(_InterlockedAnd64(
      reinterpret_cast<volatile __int64*>(ptr), static_cast<__int64>(value)));
#else
  return __sync_fetch_and_and(ptr, value);
#endif
}

inline int PopCountLL(uint64_t value) {
#ifdef _WIN32
  return static_cast<int>(__popcnt64(value));
#else
  return __builtin_popcountll(value);
#endif
}

}  // namespace detail

class Bitset {
  static constexpr size_t kAlignment = 64;  // 64-byte alignment

 public:
  Bitset()
      : data_(nullptr),
        size_(0),
        size_in_words_(0),
        capacity_(0),
        capacity_in_words_(0) {}

  Bitset(const Bitset& other)
      : size_(other.size_),
        size_in_words_(other.size_in_words_),
        capacity_(other.capacity_),
        capacity_in_words_(other.capacity_in_words_) {
    if (capacity_ == 0) {
      data_ = nullptr;
    } else {
      assert(kAlignment <= capacity_in_words_ * sizeof(uint64_t));
      auto alloc_size = ROUND_UP_TO_ALIGNMENT(
          capacity_in_words_ * sizeof(uint64_t), kAlignment);
      data_ =
          static_cast<uint64_t*>(detail::AlignedAlloc(kAlignment, alloc_size));
      memcpy(data_, other.data_, capacity_in_words_ * sizeof(uint64_t));
    }
  }

  Bitset(Bitset&& other)
      : data_(other.data_),
        size_(other.size_),
        size_in_words_(other.size_in_words_),
        capacity_(other.capacity_),
        capacity_in_words_(other.capacity_in_words_) {
    other.data_ = nullptr;
    other.size_ = 0;
    other.size_in_words_ = 0;
    other.capacity_ = 0;
    other.capacity_in_words_ = 0;
  }

  ~Bitset() {
    if (data_ != nullptr) {
      detail::AlignedFree(data_);
    }
  }

  Bitset& operator=(const Bitset& other) {
    if (this == &other) {
      return *this;
    }

    if (data_ != nullptr) {
      detail::AlignedFree(data_);
    }

    size_ = other.size_;
    size_in_words_ = other.size_in_words_;
    capacity_ = other.capacity_;
    capacity_in_words_ = other.capacity_in_words_;

    if (capacity_ == 0) {
      data_ = nullptr;
    } else {
      assert(kAlignment <= capacity_in_words_ * sizeof(uint64_t));
      auto alloc_size = ROUND_UP_TO_ALIGNMENT(
          capacity_in_words_ * sizeof(uint64_t), kAlignment);
      data_ =
          static_cast<uint64_t*>(detail::AlignedAlloc(kAlignment, alloc_size));
      memcpy(data_, other.data_, capacity_in_words_ * sizeof(uint64_t));
    }
    return *this;
  }

  Bitset& operator=(Bitset&& other) {
    if (this == &other) {
      return *this;
    }

    if (data_ != nullptr) {
      detail::AlignedFree(data_);
    }

    data_ = other.data_;
    size_ = other.size_;
    size_in_words_ = other.size_in_words_;
    capacity_ = other.capacity_;
    capacity_in_words_ = other.capacity_in_words_;

    other.data_ = nullptr;
    other.size_ = 0;
    other.size_in_words_ = 0;
    other.capacity_ = 0;
    other.size_in_words_ = 0;
    return *this;
  }

  void reserve(size_t cap) {
    cap = std::max(cap, kAlignment * 8);  // Here 8 represents 8 bits in a byte
    size_t new_cap_in_words = WORD_SIZE(cap);
    new_cap_in_words =
        std::max(new_cap_in_words, kAlignment / sizeof(uint64_t));
    if (new_cap_in_words <= capacity_in_words_) {
      capacity_ = cap;
      return;
    }
    assert(kAlignment <= new_cap_in_words * sizeof(uint64_t));
    size_t alloc_size =
        ROUND_UP_TO_ALIGNMENT(new_cap_in_words * sizeof(uint64_t), kAlignment);
    uint64_t* new_data =
        static_cast<uint64_t*>(detail::AlignedAlloc(kAlignment, alloc_size));
    if (data_ != nullptr) {
      memcpy(new_data, data_, size_in_words_ * sizeof(uint64_t));
      detail::AlignedFree(data_);
    }
    data_ = new_data;
    capacity_ = cap;
    capacity_in_words_ = new_cap_in_words;
  }

  void clear() {
    size_ = 0;
    size_in_words_ = 0;
  }

  void reset_all() { memset(data_, 0, size_in_words_ * sizeof(uint64_t)); }

  void resize(size_t new_size) {
    if (new_size <= size_) {
      size_ = new_size;
      size_in_words_ = WORD_SIZE(size_);
      return;
    }
    reserve(new_size);

    size_t new_size_in_words = WORD_SIZE(new_size);
    memset(&data_[size_in_words_], 0,
           (new_size_in_words - size_in_words_) * sizeof(uint64_t));

    if (size_in_words_ && BIT_OFFSET(size_) != 0) {
      uint64_t mask = ((1ull << BIT_OFFSET(size_)) - 1);
      data_[size_in_words_ - 1] &= mask;
    }

    size_ = new_size;
    size_in_words_ = new_size_in_words;
  }

  void set(size_t i) { data_[WORD_INDEX(i)] |= (1ull << BIT_OFFSET(i)); }

  void reset(size_t i) { data_[WORD_INDEX(i)] &= (~(1ull << BIT_OFFSET(i))); }

  void atomic_set(size_t i) {
    uint64_t mask = 1ull << BIT_OFFSET(i);
    detail::AtomicFetchOr(data_ + WORD_INDEX(i), mask);
  }

  void atomic_reset(size_t i) {
    uint64_t mask = 1ull << BIT_OFFSET(i);
    detail::AtomicFetchAnd(data_ + WORD_INDEX(i), ~mask);
  }

  bool atomic_set_with_ret(size_t i) {
    uint64_t mask = 1ull << BIT_OFFSET(i);
    uint64_t ret = detail::AtomicFetchOr(data_ + WORD_INDEX(i), mask);
    return (ret & mask);
  }

  bool atomic_reset_with_ret(size_t i) {
    uint64_t mask = 1ull << BIT_OFFSET(i);
    uint64_t ret = detail::AtomicFetchAnd(data_ + WORD_INDEX(i), ~mask);
    return (ret & mask);
  }

  bool get(size_t i) const {
    return data_[WORD_INDEX(i)] & (1ull << BIT_OFFSET(i));
  }

  size_t count() const {
    size_t ret = 0;
    for (size_t i = 0; i < size_in_words_; ++i) {
      ret += detail::PopCountLL(data_[i]);
    }
    return ret;
  }

  inline size_t size() const { return size_; }

  void Serialize(std::ostream& os) const;

  void Deserialize(std::istream& is);

 private:
  uint64_t* data_;
  size_t size_;
  size_t size_in_words_;

  size_t capacity_;
  size_t capacity_in_words_;
};

}  // namespace neug

#undef WORD_SIZE
#undef NEUG_BYTE_SIZE
#undef WORD_INDEX
#undef BIT_OFFSET
#undef ROUND_UP
#undef ROUND_DOWN
