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

#include <cstdint>
#include <cstring>

namespace neug {
namespace common {

class NullBuffer {
 public:
  constexpr static const uint64_t NUM_NULL_MASKS_PER_BYTE = 8;

  static inline bool isNull(const uint8_t* nullBytes, uint64_t valueIdx) {
    return nullBytes[valueIdx / NUM_NULL_MASKS_PER_BYTE] &
           (1 << (valueIdx % NUM_NULL_MASKS_PER_BYTE));
  }

  static inline void setNull(uint8_t* nullBytes, uint64_t valueIdx) {
    nullBytes[valueIdx / NUM_NULL_MASKS_PER_BYTE] |=
        (1 << (valueIdx % NUM_NULL_MASKS_PER_BYTE));
  }

  static inline void setNoNull(uint8_t* nullBytes, uint64_t valueIdx) {
    nullBytes[valueIdx / NUM_NULL_MASKS_PER_BYTE] &=
        ~(1 << (valueIdx % NUM_NULL_MASKS_PER_BYTE));
  }

  static inline uint64_t getNumBytesForNullValues(uint64_t numValues) {
    return (numValues + NUM_NULL_MASKS_PER_BYTE - 1) / NUM_NULL_MASKS_PER_BYTE;
  }

  static inline void initNullBytes(uint8_t* nullBytes, uint64_t numValues) {
    memset(nullBytes, 0 /* value */, getNumBytesForNullValues(numValues));
  }
};

}  // namespace common
}  // namespace neug
