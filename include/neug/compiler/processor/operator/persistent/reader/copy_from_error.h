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

#include <string>
#include <variant>
#include <vector>

#include "neug/compiler/common/constants.h"
#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/utils/api.h"

namespace neug {
namespace common {
class ValueVector;
}
namespace storage {
class ColumnChunkData;
}

namespace processor {

template <typename T>
concept DataSource = std::same_as<storage::ColumnChunkData, T> ||
                     std::same_as<common::ValueVector, T>;

struct NEUG_API WarningSourceData {
  // we should stick to integral types here as each value essentially adds a
  // column to the output when reading from a file
  using DataType = std::variant<uint64_t, uint32_t>;

  static constexpr size_t BLOCK_IDX_IDX = 0;
  static constexpr size_t OFFSET_IN_BLOCK_IDX = 1;
  static constexpr size_t NUM_BLOCK_VALUES = 2;

  WarningSourceData() : WarningSourceData(0) {}
  explicit WarningSourceData(uint64_t numSourceSpecificValues);

  template <std::integral... Types>
  void dumpTo(uint64_t& blockIdx, uint32_t& offsetInBlock,
              Types&... vars) const;

  template <std::integral... Types>
  static WarningSourceData constructFrom(uint64_t blockIdx,
                                         uint32_t offsetInBlock,
                                         Types... newValues);

  uint64_t getBlockIdx() const;
  uint32_t getOffsetInBlock() const;

  template <DataSource T>
  static WarningSourceData constructFromData(const std::vector<T*>& chunks,
                                             common::idx_t pos);

  std::array<DataType, common::CopyConstants::MAX_NUM_WARNING_DATA_COLUMNS>
      values;
  uint64_t numValues;
};

struct LineContext {
  uint64_t startByteOffset;
  uint64_t endByteOffset;

  bool isCompleteLine;

  void setNewLine(uint64_t start);
  void setEndOfLine(uint64_t end);
};

// If parsing in parallel during parsing we may not be able to determine line
// numbers Thus we have additional fields that can be used to determine line
// numbers + reconstruct lines After parsing this will be used to populate a
// PopulatedCopyFromError instance
struct NEUG_API CopyFromFileError {
  CopyFromFileError(std::string message, WarningSourceData warningData,
                    bool completedLine = true, bool mustThrow = false);

  std::string message;
  bool completedLine;
  WarningSourceData warningData;

  bool mustThrow;

  bool operator<(const CopyFromFileError& o) const;
};

struct PopulatedCopyFromError {
  std::string message;
  std::string filePath;
  std::string skippedLineOrRecord;
  uint64_t lineNumber;
};

template <std::integral... Types>
void WarningSourceData::dumpTo(uint64_t& blockIdx, uint32_t& offsetInBlock,
                               Types&... vars) const {
  static_assert(sizeof...(Types) + NUM_BLOCK_VALUES <=
                std::tuple_size_v<decltype(values)>);
  NEUG_ASSERT(sizeof...(Types) + NUM_BLOCK_VALUES == numValues);
  common::TypeUtils::paramPackForEach(
      [this](auto idx, auto& value) {
        value = std::get<std::decay_t<decltype(value)>>(values[idx]);
      },
      blockIdx, offsetInBlock, vars...);
}

template <std::integral... Types>
WarningSourceData WarningSourceData::constructFrom(uint64_t blockIdx,
                                                   uint32_t offsetInBlock,
                                                   Types... newValues) {
  static_assert(
      sizeof...(Types) + NUM_BLOCK_VALUES <=
          std::tuple_size_v<decltype(values)>,
      "For performance reasons the number of warning metadata columns has a "
      "statically-defined limit, modify "
      "'common::CopyConstants::WARNING_DATA_MAX_NUM_COLUMNS' if you wish to "
      "increase it.");

  WarningSourceData ret{sizeof...(Types) + NUM_BLOCK_VALUES};
  common::TypeUtils::paramPackForEach(
      [&ret](auto idx, auto value) { ret.values[idx] = value; }, blockIdx,
      offsetInBlock, newValues...);
  return ret;
}

}  // namespace processor
}  // namespace neug
