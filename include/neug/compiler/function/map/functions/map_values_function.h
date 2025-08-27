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
#include "neug/compiler/function/map/functions/base_map_extract_function.h"

namespace gs {
namespace function {

struct MapValues : public BaseMapExtract {
  static void operation(common::list_entry_t& listEntry,
                        common::list_entry_t& resultEntry,
                        common::ValueVector& listVector,
                        common::ValueVector& resultVector) {
    auto mapValueVector = common::MapVector::getValueVector(&listVector);
    auto mapValueValues =
        common::MapVector::getMapValues(&listVector, listEntry);
    BaseMapExtract::operation(resultEntry, resultVector, mapValueValues,
                              mapValueVector, listEntry.size);
  }
};

}  // namespace function
}  // namespace gs
