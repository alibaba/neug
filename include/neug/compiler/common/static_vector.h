/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include <cstring>
#include <memory>
#include <utility>

#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/copy_constructors.h"

namespace neug {
namespace common {

template <typename T>
class MaybeUninit {
 public:
  T& assumeInit() { return *ptr(); }
  const T& assumeInit() const { return *ptr(); }
  T* ptr() { return reinterpret_cast<T*>(&data); }
  const T* ptr() const { return reinterpret_cast<const T*>(&data); }

 private:
  alignas(T) std::byte data[sizeof(T)];
};

template <typename T, size_t N>
class StaticVector {
  StaticVector(const StaticVector& other) : len(other.len) {
    std::uninitialized_copy(other.begin(), other.end(), begin());
  }

 public:
  StaticVector() : len(0){};
  StaticVector(StaticVector&& other) : len(other.len) {
    std::uninitialized_move(other.begin(), other.end(), begin());
    other.len = 0;
  }
  DELETE_COPY_ASSN(StaticVector);
  EXPLICIT_COPY_METHOD(StaticVector);
  StaticVector& operator=(StaticVector&& other) {
    if (&other != this) {
      clear();
      len = other.len;
      std::uninitialized_move(other.begin(), other.end(), begin());
      other.len = 0;
    }
    return *this;
  }
  ~StaticVector() { clear(); }

  T& operator[](size_t i) {
    NEUG_ASSERT(i < len);
    return items[i].assumeInit();
  }
  const T& operator[](size_t i) const {
    NEUG_ASSERT(i < len);
    return items[i].assumeInit();
  }
  void push_back(T elem) {
    NEUG_ASSERT(len < N);
    new (items[len].ptr()) T(std::move(elem));
    len++;
  }
  T pop_back() {
    NEUG_ASSERT(len > 0);
    len--;
    return std::move(items[len].assumeInit());
  }
  T* begin() { return items[0].ptr(); }
  const T* begin() const { return items[0].ptr(); }
  T* end() { return items[len].ptr(); }
  const T* end() const { return items[len].ptr(); }

  void clear() {
    std::destroy(begin(), end());
    len = 0;
  }

  bool empty() const { return len == 0; }
  bool full() const { return len == N; }
  size_t size() const { return len; }

 private:
  MaybeUninit<T> items[N];
  size_t len;
};

}  // namespace common
}  // namespace neug
