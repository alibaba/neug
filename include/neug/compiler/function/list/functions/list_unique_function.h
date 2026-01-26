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
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/common/vector/value_vector.h"

namespace neug {
namespace function {

struct ValueHashFunction {
  uint64_t operator()(const common::Value& value) const {
    return (uint64_t) value.computeHash();
  }
};

struct ValueEquality {
  bool operator()(const common::Value& a, const common::Value& b) const {
    return a == b;
  }
};

using ValueSet =
    std::unordered_set<common::Value, ValueHashFunction, ValueEquality>;

using duplicate_value_handler = std::function<void(const std::string&)>;
using unique_value_handler =
    std::function<void(common::ValueVector& dataVector, uint64_t pos)>;
using null_value_handler = std::function<void()>;

struct ListUnique {
  static uint64_t appendListElementsToValueSet(
      common::list_entry_t& input, common::ValueVector& inputVector,
      duplicate_value_handler duplicateValHandler = nullptr,
      unique_value_handler uniqueValueHandler = nullptr,
      null_value_handler nullValueHandler = nullptr);

  static void operation(common::list_entry_t& input, int64_t& result,
                        common::ValueVector& inputVector,
                        common::ValueVector& resultVector);
};

}  // namespace function
}  // namespace neug
