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

#include "base_list_sort_function.h"
#include "neug/compiler/common/vector/value_vector.h"

namespace neug {
namespace function {

template <typename T>
struct ListReverseSort : BaseListSortOperation {
  static inline void operation(common::list_entry_t& input,
                               common::list_entry_t& result,
                               common::ValueVector& inputVector,
                               common::ValueVector& resultVector) {
    sortValues<T>(input, result, inputVector, resultVector,
                  false /* ascOrder */, true /* nullFirst */);
  }

  static inline void operation(common::list_entry_t& input,
                               common::neug_string_t& nullOrder,
                               common::list_entry_t& result,
                               common::ValueVector& inputVector,
                               common::ValueVector& /*valueVector*/,
                               common::ValueVector& resultVector) {
    sortValues<T>(input, result, inputVector, resultVector,
                  false /* ascOrder */,
                  isNullFirst(nullOrder.getAsString()) /* nullFirst */);
  }

  static inline void operation(common::list_entry_t& /*input*/,
                               common::neug_string_t& /*sortOrder*/,
                               common::neug_string_t& /*nullOrder*/,
                               common::list_entry_t& /*result*/,
                               common::ValueVector& /*inputVector*/,
                               common::ValueVector& /*resultVector*/) {
    THROW_RUNTIME_ERROR("Invalid number of arguments");
  }
};

}  // namespace function
}  // namespace neug
