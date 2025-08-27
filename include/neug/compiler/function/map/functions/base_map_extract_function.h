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

namespace gs {
namespace function {

struct BaseMapExtract {
  static void operation(common::list_entry_t& resultEntry,
                        common::ValueVector& resultVector, uint8_t* srcValues,
                        common::ValueVector* srcVector,
                        uint64_t numValuesToCopy) {
    resultEntry = common::ListVector::addList(&resultVector, numValuesToCopy);
    auto dstValues =
        common::ListVector::getListValues(&resultVector, resultEntry);
    auto dstDataVector = common::ListVector::getDataVector(&resultVector);
    for (auto i = 0u; i < numValuesToCopy; i++) {
      dstDataVector->copyFromVectorData(dstValues, srcVector, srcValues);
      dstValues += dstDataVector->getNumBytesPerValue();
      srcValues += srcVector->getNumBytesPerValue();
    }
  }
};

}  // namespace function
}  // namespace gs
