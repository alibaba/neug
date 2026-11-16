/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include <algorithm>
#include <array>
#include <bit>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stack>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>

#define D_ASSERT(condition) assert(condition)
namespace neug::art {
using idx_t = uint64_t;
// Keep DuckDB's eight-byte nested leaf key; the public API accepts NeuG
// index_id_t.
using row_t = int64_t;
constexpr row_t MAX_ROW_ID_LOCAL = row_t(1) << 56;
using data_t = uint8_t;
using data_ptr_t = data_t*;
using const_data_ptr_t = const data_t*;
using validity_t = uint64_t;
using std::array;
using std::set;
using std::stack;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
template <class T>
using unsafe_unique_ptr = std::unique_ptr<T>;
template <class T>
using unsafe_vector = std::vector<T>;
template <class T>
using reference = std::reference_wrapper<T>;
template <class T>
class optional_ptr {
  T* ptr = nullptr;

 public:
  optional_ptr() = default;
  optional_ptr(std::nullptr_t) {}
  optional_ptr(T* p) : ptr(p) {}
  optional_ptr(T& p) : ptr(&p) {}
  explicit operator bool() const { return ptr != nullptr; }
  T& operator*() const {
    assert(ptr);
    return *ptr;
  }
  T* operator->() const {
    assert(ptr);
    return ptr;
  }
  T* get() const { return ptr; }
  operator T*() const { return ptr; }
};
template <class T>
using unsafe_optional_ptr = optional_ptr<T>;
template <class T, class U>
constexpr T UnsafeNumericCast(U x) {
  return static_cast<T>(x);
}
template <class T>
struct ARTNumericLimits {
  static constexpr T Maximum() { return std::numeric_limits<T>::max(); }
};
template <class T>
constexpr T MinValue(T a, T b) {
  return std::min(a, b);
}
template <class T>
const_data_ptr_t const_data_ptr_cast(T* p) {
  return reinterpret_cast<const_data_ptr_t>(p);
}
using std::pair;
template <class T>
data_ptr_t data_ptr_cast(T* pointer) {
  return reinterpret_cast<data_ptr_t>(pointer);
}
template <class T>
T ARTLoad(const_data_ptr_t pointer) {
  T value;
  std::memcpy(&value, pointer, sizeof(T));
  return value;
}
template <class T>
void ARTStore(T value, data_ptr_t pointer) {
  std::memcpy(pointer, &value, sizeof(T));
}

struct StringUtil {
  static const char* Arg(const string& s);
  template <class T>
  static T Arg(T x) {
    return x;
  }
  static string Format(const char* fmt);
  template <class... A>
  static string Format(const char* fmt, A... a) {
    auto n = std::snprintf(nullptr, 0, fmt, Arg(a)...);
    if (n < 0)
      throw std::runtime_error("ArtTree formatting error");
    std::vector<char> out(n + 1);
    std::snprintf(out.data(), out.size(), fmt, Arg(a)...);
    return string(out.data(), n);
  }
};
class InternalException : public std::runtime_error {
 public:
  explicit InternalException(const std::string& message);
  template <class... A>
  explicit InternalException(const char* fmt, A... a)
      : std::runtime_error(StringUtil::Format(fmt, a...)) {}
};
using FatalException = InternalException;
using SerializationException = InternalException;
class string_t {
  std::string_view value;

 public:
  string_t(const char* p, size_t n);
  explicit string_t(const string& s);
  const char* GetData() const;
  idx_t GetSize() const;
};
class ValidityMask {
  validity_t* data;

 public:
  static constexpr idx_t BITS_PER_VALUE = 64;
  ValidityMask(validity_t* p, idx_t);
  static void GetEntryIndex(idx_t i, idx_t& entry, idx_t& bit);
  static bool RowIsValid(validity_t mask, idx_t bit);
  bool RowIsValid(idx_t i) const;
  validity_t* GetData();
  void SetInvalid(idx_t i);
  void SetAllValid(idx_t count);
  void SetValid(idx_t i);
  void SetAllInvalid(idx_t count);
};
}  // namespace neug::art
