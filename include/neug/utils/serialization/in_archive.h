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

#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>

#include "neug/utils/property/types.h"

namespace neug {

class InArchive {
 public:
  InArchive() = default;
  InArchive(InArchive&& rhs) { buffer_.swap(rhs.buffer_); }

  ~InArchive() = default;

  InArchive& operator=(InArchive&& rhs) {
    buffer_.clear();
    buffer_.swap(rhs.buffer_);
    return *this;
  }

  inline void Reset() { buffer_.clear(); }

  inline char* GetBuffer() { return buffer_.data(); }

  inline const char* GetBuffer() const { return buffer_.data(); }

  inline size_t GetSize() const { return buffer_.size(); }

  inline void AddBytes(const void* data, size_t size) {
    size_t old_size = buffer_.size();
    buffer_.resize(old_size + size);
    memcpy(buffer_.data() + old_size, data, size);
  }

  inline void Resize(size_t size) { buffer_.resize(size); }

  inline void Clear() { buffer_.clear(); }

  bool Empty() const { return buffer_.empty(); }

  void Reserve(size_t cap) { buffer_.reserve(cap); }

 private:
  std::vector<char> buffer_;
};

template <typename T,
          typename std::enable_if<std::is_pod<T>::value, T>::type* = nullptr>
inline InArchive& operator<<(InArchive& in_archive, T u) {
  in_archive.AddBytes(&u, sizeof(T));
  return in_archive;
}

inline InArchive& operator<<(InArchive& in_archive, EmptyType) {
  return in_archive;
}

inline InArchive& operator<<(InArchive& in_archive, const std::string& s) {
  in_archive << s.size();
  in_archive.AddBytes(s.data(), s.size());
  return in_archive;
}

inline InArchive& operator<<(InArchive& in_archive, std::string_view s) {
  in_archive << s.size();
  in_archive.AddBytes(s.data(), s.size());
  return in_archive;
}

template <typename T,
          typename std::enable_if<std::is_pod<T>::value, T>::type* = nullptr>
inline InArchive& operator<<(InArchive& in_archive, const std::vector<T>& vec) {
  size_t size = vec.size();
  in_archive << size;
  if (size > 0) {
    if constexpr (std::is_same_v<T, bool>) {
      // Special handling for vector<bool>
      for (size_t i = 0; i < size; ++i) {
        bool val = vec[i];
        in_archive.AddBytes(&val, sizeof(bool));
      }
      return in_archive;
    } else {
      in_archive.AddBytes(vec.data(), size * sizeof(T));
    }
  }
  return in_archive;
}

template <typename T,
          typename std::enable_if<!std::is_pod<T>::value, T>::type* = nullptr>
inline InArchive& operator<<(InArchive& in_archive, const std::vector<T>& vec) {
  size_t size = vec.size();
  in_archive << size;
  for (const auto& item : vec) {
    in_archive << item;
  }
  return in_archive;
}

template <typename... Args>
inline InArchive& operator<<(InArchive& in_archive,
                             const std::tuple<Args...>& tup) {
  std::apply(
      [&in_archive](const Args&... args) { (in_archive << ... << args); }, tup);
  return in_archive;
}

}  // namespace neug
