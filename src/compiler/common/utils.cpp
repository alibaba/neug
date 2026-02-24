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

#include "neug/compiler/common/utils.h"

namespace neug {
namespace common {

uint64_t nextPowerOfTwo(uint64_t v) {
  v--;
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  v |= v >> 32;
  v++;
  return v;
}

uint64_t prevPowerOfTwo(uint64_t v) { return nextPowerOfTwo((v / 2) + 1); }

bool isLittleEndian() {
  // Little endian arch stores the least significant value in the lower bytes.
  int testNumber = 1;
  return *(uint8_t*) &testNumber == 1;
}

template <>
bool integerFitsIn<int64_t>(int64_t) {
  return true;
}

template <>
bool integerFitsIn<int32_t>(int64_t val) {
  return val >= INT32_MIN && val <= INT32_MAX;
}

template <>
bool integerFitsIn<int16_t>(int64_t val) {
  return val >= INT16_MIN && val <= INT16_MAX;
}

template <>
bool integerFitsIn<int8_t>(int64_t val) {
  return val >= INT8_MIN && val <= INT8_MAX;
}

template <>
bool integerFitsIn<uint64_t>(int64_t val) {
  return val >= 0;
}

template <>
bool integerFitsIn<uint32_t>(int64_t val) {
  return val >= 0 && val <= UINT32_MAX;
}

template <>
bool integerFitsIn<uint16_t>(int64_t val) {
  return val >= 0 && val <= UINT16_MAX;
}

template <>
bool integerFitsIn<uint8_t>(int64_t val) {
  return val >= 0 && val <= UINT8_MAX;
}

}  // namespace common
}  // namespace neug
