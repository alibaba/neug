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

#include "neug/storages/index/art_tree/common.h"

namespace neug::art {

class IndexPointer {
 public:
  //! Bit-shifting
  static constexpr idx_t SHIFT_OFFSET = 32;
  static constexpr idx_t SHIFT_METADATA = 56;
  //! AND operations
  static constexpr idx_t AND_OFFSET = 0x0000000000FFFFFF;
  static constexpr idx_t AND_BUFFER_ID = 0x00000000FFFFFFFF;
  static constexpr idx_t AND_METADATA = 0xFF00000000000000;

 public:
  //! Constructs an empty IndexPointer
  IndexPointer();
  //! Constructs an in-memory IndexPointer with a buffer ID and an offset
  IndexPointer(const uint32_t buffer_id, const uint32_t offset);

 public:
  //! Get data (all 64 bits)
  idx_t Get() const;
  //! Set data (all 64 bits)
  void Set(const idx_t data_p);

  //! Returns false, if the metadata is empty
  bool HasMetadata() const;
  //! Get metadata (Bits 56-63)
  uint8_t GetMetadata() const;
  //! Set metadata (Bits 56-63)
  void SetMetadata(const uint8_t metadata);

  //! Get the offset (Bits 32-55)
  idx_t GetOffset() const;
  //! Get the buffer ID (Bits 0-31)
  idx_t GetBufferId() const;

  //! Resets the IndexPointer
  void Clear();

  //! Comparison operator
  bool operator==(const IndexPointer& ptr) const;

 private:
  //! Data holds all the information contained in an IndexPointer (64-bit value)
  //! Bit layout:
  //! MSB LSB 63         56 55                      32 31 0
  //! +------------+--------------------------+------------------------------------+
  //! |  Metadata  |           Offset         |           Buffer ID | |   8 bits
  //! |          24 bits         |            32 bits                 |
  //! +------------+--------------------------+------------------------------------+
  //! NOTE: we do not use bit fields because when using bit fields Windows
  //! compiles the IndexPointer class into 16 bytes instead of the intended 8
  //! bytes, doubling the space requirements
  //! https://learn.microsoft.com/en-us/cpp/cpp/cpp-bit-fields?view=msvc-170
  idx_t data;
};

static_assert(sizeof(IndexPointer) == sizeof(idx_t),
              "Invalid size for IndexPointer.");

}  // namespace neug::art
