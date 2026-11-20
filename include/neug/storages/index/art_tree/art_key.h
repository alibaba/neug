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

/**
 * This file is based on the DuckDB project
 * (https://github.com/duckdb/duckdb) Licensed under the MIT License.
 */

#pragma once

#include "neug/common/types/value.h"
#include "neug/storages/index/art_tree/arena_allocator.h"
#include "neug/storages/index/art_tree/common.h"
#include "neug/storages/index/art_tree/radix.h"
#include "neug/utils/api.h"

namespace neug::art {

class NEUG_API ARTKey {
 public:
  ARTKey();
  ARTKey(data_ptr_t data, idx_t len);
  ARTKey(ARTArenaAllocator& allocator, idx_t len);

  idx_t len;
  data_ptr_t data;

 public:
  // Adapted from DuckDB's single-Value entry point. NULL produces an empty key.
  // The caller supplies the index schema; keys are ordered within one type.
  static ARTKey CreateKey(ARTArenaAllocator& allocator,
                          const neug::Value& value);

  template <class T>
  static inline ARTKey CreateARTKey(ARTArenaAllocator& allocator,
                                    const neug::Value& value) {
    if (value.IsNull())
      return ARTKey();
    return CreateARTKey<T>(allocator, value.GetValue<T>());
  }

  template <class T>
  static inline void CreateARTKey(ARTArenaAllocator& allocator, ARTKey& key,
                                  const neug::Value& value) {
    key = CreateARTKey<T>(allocator, value);
  }

  template <class T>
  static inline ARTKey CreateARTKey(ARTArenaAllocator& allocator, T value) {
    auto data = ARTKey::CreateData<T>(allocator, value);
    return ARTKey(data, sizeof(value));
  }

  template <class T>
  static inline void CreateARTKey(ARTArenaAllocator& allocator, ARTKey& key,
                                  T value) {
    key.data = ARTKey::CreateData<T>(allocator, value);
    key.len = sizeof(value);
  }

 public:
  data_t& operator[](idx_t i) { return data[i]; }
  const data_t& operator[](idx_t i) const { return data[i]; }
  inline bool ByteMatches(const ARTKey& other, idx_t depth) const {
    return data[depth] == other[depth];
  }
  bool operator>(const ARTKey& key) const;
  bool operator>=(const ARTKey& key) const;
  bool operator==(const ARTKey& key) const;
  inline bool Empty() const { return len == 0; }
  row_t GetRowId() const;
  idx_t GetMismatchPos(const ARTKey& other, const idx_t start) const;

 private:
  template <class T>
  static inline data_ptr_t CreateData(ARTArenaAllocator& allocator, T value) {
    auto data = allocator.Allocate(sizeof(value));
    Radix::EncodeData<T>(data, value);
    return data;
  }
};

template <>
ARTKey ARTKey::CreateARTKey(ARTArenaAllocator& allocator, string_t value);
template <>
void ARTKey::CreateARTKey(ARTArenaAllocator& allocator, ARTKey& key,
                          string_t value);

}  // namespace neug::art
