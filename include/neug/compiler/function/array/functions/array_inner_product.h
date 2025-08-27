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
// #include <simsimd.h>

namespace gs {
namespace function {

struct ArrayInnerProduct {
  template <std::floating_point T>
  static inline void operation(common::list_entry_t& left,
                               common::list_entry_t& right, T& result,
                               common::ValueVector& leftVector,
                               common::ValueVector& rightVector,
                               common::ValueVector& /*resultVector*/) {
    auto leftElements =
        (T*) common::ListVector::getListValues(&leftVector, left);
    auto rightElements =
        (T*) common::ListVector::getListValues(&rightVector, right);
    KU_ASSERT(left.size == right.size);
    // simsimd_distance_t tmpResult = 0.0;
    // static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>);
    // if constexpr (std::is_same_v<T, float>) {
    //     simsimd_dot_f32(leftElements, rightElements, left.size, &tmpResult);
    // } else {
    //     simsimd_dot_f64(leftElements, rightElements, left.size, &tmpResult);
    // }
    // result = tmpResult;
  }
};

}  // namespace function
}  // namespace gs
