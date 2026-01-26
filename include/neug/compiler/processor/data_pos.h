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

#include <utility>

#include "neug/compiler/common/types/types.h"

namespace neug {
namespace processor {

using data_chunk_pos_t = common::idx_t;
constexpr data_chunk_pos_t INVALID_DATA_CHUNK_POS = common::INVALID_IDX;
using value_vector_pos_t = common::idx_t;
constexpr value_vector_pos_t INVALID_VALUE_VECTOR_POS = common::INVALID_IDX;

struct DataPos {
  data_chunk_pos_t dataChunkPos;
  value_vector_pos_t valueVectorPos;

  DataPos()
      : dataChunkPos{INVALID_DATA_CHUNK_POS},
        valueVectorPos{INVALID_VALUE_VECTOR_POS} {}
  explicit DataPos(data_chunk_pos_t dataChunkPos,
                   value_vector_pos_t valueVectorPos)
      : dataChunkPos{dataChunkPos}, valueVectorPos{valueVectorPos} {}
  explicit DataPos(std::pair<data_chunk_pos_t, value_vector_pos_t> pos)
      : dataChunkPos{pos.first}, valueVectorPos{pos.second} {}

  static DataPos getInvalidPos() { return DataPos(); }
  bool isValid() const {
    return dataChunkPos != INVALID_DATA_CHUNK_POS &&
           valueVectorPos != INVALID_VALUE_VECTOR_POS;
  }

  inline bool operator==(const DataPos& rhs) const {
    return (dataChunkPos == rhs.dataChunkPos) &&
           (valueVectorPos == rhs.valueVectorPos);
  }
};

}  // namespace processor
}  // namespace neug
