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

#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/common/types/neug_string.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/string/functions/array_extract_function.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace function {

struct ListExtract {
 public:
  // Note: this function takes in a 1-based position (The index of the first
  // value in the list is 1).
  template <typename T>
  static inline void operation(common::list_entry_t& listEntry, int64_t pos,
                               T& result, common::ValueVector& listVector,
                               common::ValueVector& /*posVector*/,
                               common::ValueVector& resultVector,
                               uint64_t resPos) {
    if (pos == 0) {
      THROW_RUNTIME_ERROR("List extract takes 1-based position.");
    }
    if ((pos > 0 && pos > listEntry.size) ||
        (pos < 0 && pos < -(int64_t) listEntry.size)) {
      THROW_RUNTIME_ERROR(common::stringFormat(
          "list_extract(list, index): index={} is out of range.",
          common::TypeUtils::toString(pos)));
    }
    if (pos > 0) {
      pos--;
    } else {
      pos = listEntry.size + pos;
    }
    auto listDataVector = common::ListVector::getDataVector(&listVector);
    resultVector.setNull(resPos,
                         listDataVector->isNull(listEntry.offset + pos));
    if (!resultVector.isNull(resPos)) {
      auto listValues = common::ListVector::getListValuesWithOffset(
          &listVector, listEntry, pos);
      resultVector.copyFromVectorData(reinterpret_cast<uint8_t*>(&result),
                                      listDataVector, listValues);
    }
  }

  static inline void operation(common::neug_string_t& str, int64_t& idx,
                               common::neug_string_t& result) {
    if (str.len < idx) {
      result.set("", 0);
    } else {
      ArrayExtract::operation(str, idx, result);
    }
  }
};

}  // namespace function
}  // namespace neug
