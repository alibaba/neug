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

#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <limits>

#include "neug/utils/md5.h"

namespace neug {

inline constexpr size_t kMD5DigestSize = 16;
using MD5Digest = std::array<unsigned char, kMD5DigestSize>;

// Adapt storage's size_t lengths to the original MD5Update interface without
// truncation. Call MD5Init first. A zero-size update is a no-op, even for
// nullptr.
inline void UpdateMD5(MD5& context, const void* data, size_t size) {
  const auto* bytes = static_cast<const unsigned char*>(data);
  while (size > 0) {
    const auto chunk = static_cast<unsigned int>(
        std::min<size_t>(size, std::numeric_limits<unsigned int>::max()));
    context.MD5Update(bytes, chunk);
    bytes += chunk;
    size -= chunk;
  }
}

inline MD5Digest ComputeMD5(const void* data, size_t size) {
  MD5 context;
  context.MD5Init();
  UpdateMD5(context, data, size);
  MD5Digest digest;
  context.MD5Final(digest.data());
  return digest;
}

}  // namespace neug
