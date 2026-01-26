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

#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace function {

struct BaseListSortOperation {
 public:
  static inline bool isAscOrder(const std::string& sortOrder) {
    std::string upperSortOrder = common::StringUtils::getUpper(sortOrder);
    if (upperSortOrder == "ASC") {
      return true;
    } else if (upperSortOrder == "DESC") {
      return false;
    } else {
      THROW_RUNTIME_ERROR("Invalid sortOrder");
    }
  }

  static inline bool isNullFirst(const std::string& nullOrder) {
    std::string upperNullOrder = common::StringUtils::getUpper(nullOrder);
    if (upperNullOrder == "NULLS FIRST") {
      return true;
    } else if (upperNullOrder == "NULLS LAST") {
      return false;
    } else {
      THROW_RUNTIME_ERROR("Invalid nullOrder");
    }
  }

  template <typename T>
  static void sortValues(common::list_entry_t& input,
                         common::list_entry_t& result,
                         common::ValueVector& inputVector,
                         common::ValueVector& resultVector, bool ascOrder,
                         bool nullFirst) {
    // TODO(Ziyi) - Replace this sort implementation with radix_sort
    // implementation:
    //  https://github.com/kuzudb/gs/issues/1536.
    auto inputDataVector = common::ListVector::getDataVector(&inputVector);
    auto inputPos = input.offset;

    // Calculate null count.
    auto nullCount = 0;
    for (auto i = 0u; i < input.size; i++) {
      if (inputDataVector->isNull(input.offset + i)) {
        nullCount += 1;
      }
    }

    result = common::ListVector::addList(&resultVector, input.size);
    auto resultDataVector = common::ListVector::getDataVector(&resultVector);
    auto resultPos = result.offset;

    // Add nulls first.
    if (nullFirst) {
      setVectorRangeToNull(*resultDataVector, result.offset, 0, nullCount);
      resultPos += nullCount;
    }

    // Add actual data.
    for (auto i = 0u; i < input.size; i++) {
      if (inputDataVector->isNull(inputPos)) {
        inputPos++;
        continue;
      }
      resultDataVector->copyFromVectorData(resultPos++, inputDataVector,
                                           inputPos++);
    }

    // Add nulls in the end.
    if (!nullFirst) {
      setVectorRangeToNull(*resultDataVector, result.offset,
                           input.size - nullCount, input.size);
    }

    // Determine the starting and ending position of the data to be sorted.
    auto sortStart = nullCount;
    auto sortEnd = input.size;
    if (!nullFirst) {
      sortStart = 0;
      sortEnd = input.size - nullCount;
    }

    // Sort the data based on order.
    auto sortingValues = reinterpret_cast<T*>(
        common::ListVector::getListValues(&resultVector, result));
    if (ascOrder) {
      std::sort(sortingValues + sortStart, sortingValues + sortEnd,
                std::less{});
    } else {
      std::sort(sortingValues + sortStart, sortingValues + sortEnd,
                std::greater{});
    }
  }

  static void setVectorRangeToNull(common::ValueVector& vector, uint64_t offset,
                                   uint64_t startPos, uint64_t endPos) {
    for (auto i = startPos; i < endPos; i++) {
      vector.setNull(offset + i, true);
    }
  }
};

}  // namespace function
}  // namespace neug
