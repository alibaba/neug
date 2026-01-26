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

#include "neug/compiler/common/utils.h"

struct ArrowSchema;

namespace neug {
namespace common {

struct ArrowBuffer {
  ArrowBuffer() : dataptr(nullptr), count(0), capacity(0) {}
  ~ArrowBuffer() {
    if (!dataptr) {
      return;
    }
    free(dataptr);
    dataptr = nullptr;
    count = 0;
    capacity = 0;
  }
  // disable copy constructors
  ArrowBuffer(const ArrowBuffer& other) = delete;
  ArrowBuffer& operator=(const ArrowBuffer&) = delete;
  //! enable move constructors
  ArrowBuffer(ArrowBuffer&& other) noexcept {
    std::swap(dataptr, other.dataptr);
    std::swap(count, other.count);
    std::swap(capacity, other.capacity);
  }
  ArrowBuffer& operator=(ArrowBuffer&& other) noexcept {
    std::swap(dataptr, other.dataptr);
    std::swap(count, other.count);
    std::swap(capacity, other.capacity);
    return *this;
  }

  void reserve(uint64_t bytes) {  // NOLINT
    auto new_capacity = nextPowerOfTwo(bytes);
    if (new_capacity <= capacity) {
      return;
    }
    reserveInternal(new_capacity);
  }

  void resize(uint64_t bytes) {  // NOLINT
    reserve(bytes);
    count = bytes;
  }

  void resize(uint64_t bytes, uint8_t value) {  // NOLINT
    reserve(bytes);
    for (uint64_t i = count; i < bytes; i++) {
      dataptr[i] = value;
    }
    count = bytes;
  }

  uint64_t size() {  // NOLINT
    return count;
  }

  uint8_t* data() {  // NOLINT
    return dataptr;
  }

 private:
  void reserveInternal(uint64_t bytes) {
    if (dataptr) {
      dataptr = (uint8_t*) realloc(dataptr, bytes);
    } else {
      dataptr = (uint8_t*) malloc(bytes);
    }
    capacity = bytes;
  }

 private:
  uint8_t* dataptr = nullptr;
  uint64_t count = 0;
  uint64_t capacity = 0;
};

}  // namespace common
}  // namespace neug
