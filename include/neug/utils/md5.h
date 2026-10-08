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

/*
** This code taken from the SQLite test library (can be found at
** https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki).
** Originally found on the internet. The original header comment follows this
*comment.
** The code has been refactored, but the algorithm stays the same.
*/
/*
 * This code implements the MD5 message-digest algorithm.
 * The algorithm is due to Ron Rivest.  This code was
 * written by Colin Plumb in 1993, no copyright is claimed.
 * This code is in the public domain; do with it what you wish.
 *
 * Equivalent code is available from RSA Data Security, Inc.
 * This code has been tested against that, and is equivalent,
 * except that you don't need to include two pages of legalese
 * with every copy.
 *
 * To compute the message digest of a chunk of bytes, declare an
 * MD5Context structure, pass it to MD5Init, call MD5Update as
 * needed on buffers full of bytes, and then call MD5Final, which
 * will fill a supplied 16-byte array with the digest.
 */

#include <array>
#include <cstddef>
#include <cstdint>

#include "neug/utils/api.h"

namespace neug {

// MD5 for compatibility with existing storage checksums.
// Each instance owns its state and performs no heap allocations.
class NEUG_API MD5 {
 public:
  static constexpr size_t kDigestSize = 16;
  using Digest = std::array<unsigned char, kDigestSize>;

  MD5() { Reset(); }

  // A zero-size update is a no-op, including when data is nullptr.
  // Otherwise data must point to at least size readable bytes.
  void Update(const void* data, size_t size);

  // Return the raw 16-byte digest without changing this context. Further
  // updates continue the original input; repeated calls are identical.
  Digest Finalize() const;

  void Reset();
  static Digest Compute(const void* data, size_t size);

 private:
  void ProcessBlock(const unsigned char* data);
  static void MD5Transform(uint32_t buf[4], const uint32_t in[16]);

  uint32_t state_[4];
  uint64_t byte_count_;
  unsigned char buffer_[64]{};
};

}  // namespace neug
