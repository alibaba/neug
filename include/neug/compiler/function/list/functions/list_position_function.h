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

#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/comparison/comparison_functions.h"

namespace neug {
namespace function {

struct ListPosition {
  // Note: this function takes in a 1-based element (The index of the first
  // element in the list is 1).
  template <typename T>
  static void operation(common::list_entry_t& list, T& element, int64_t& result,
                        common::ValueVector& listVector,
                        common::ValueVector& elementVector,
                        common::ValueVector& /*resultVector*/) {
    if (common::ListType::getChildType(listVector.dataType) !=
        elementVector.dataType) {
      result = 0;
      return;
    }
    auto listElements = reinterpret_cast<T*>(
        common::ListVector::getListValues(&listVector, list));
    uint8_t comparisonResult = 0;
    for (auto i = 0u; i < list.size; i++) {
      Equals::operation(listElements[i], element, comparisonResult,
                        common::ListVector::getDataVector(&listVector),
                        &elementVector);
      if (comparisonResult) {
        result = i + 1;
        return;
      }
    }
    result = 0;
  }
};

}  // namespace function
}  // namespace neug
