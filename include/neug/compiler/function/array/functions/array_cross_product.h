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

template <typename T>
struct ArrayCrossProduct {
  static inline void operation(common::list_entry_t& left,
                               common::list_entry_t& right,
                               common::list_entry_t& result,
                               common::ValueVector& leftVector,
                               common::ValueVector& rightVector,
                               common::ValueVector& resultVector) {
    auto leftElements =
        (T*) common::ListVector::getListValues(&leftVector, left);
    auto rightElements =
        (T*) common::ListVector::getListValues(&rightVector, right);
    result = common::ListVector::addList(&resultVector, left.size);
    auto resultElements =
        (T*) common::ListVector::getListValues(&resultVector, result);
    resultElements[0] =
        leftElements[1] * rightElements[2] - leftElements[2] * rightElements[1];
    resultElements[1] =
        leftElements[2] * rightElements[0] - leftElements[0] * rightElements[2];
    resultElements[2] =
        leftElements[0] * rightElements[1] - leftElements[1] * rightElements[0];
  }
};

}  // namespace function
}  // namespace gs
