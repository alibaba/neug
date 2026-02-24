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
#include "neug/compiler/function/function.h"

namespace neug {
namespace function {

struct ListCreationFunction {
  static constexpr const char* name = "LIST_CREATION";

  static function_set getFunctionSet();
  static void execFunc(
      const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
      const std::vector<common::SelectionVector*>& parameterSelVectors,
      common::ValueVector& result, common::SelectionVector* resultSelVector,
      void* /*dataPtr*/ = nullptr);
};

struct ListExtractFunction {
  static constexpr const char* name = "LIST_EXTRACT";

  static function_set getFunctionSet();
};

struct ListElementFunction {
  using alias = ListExtractFunction;

  static constexpr const char* name = "LIST_ELEMENT";
};

struct ListContainsFunction {
  static constexpr const char* name = "LIST_CONTAINS";

  static function_set getFunctionSet();
};

struct ListHasFunction {
  using alias = ListContainsFunction;

  static constexpr const char* name = "LIST_HAS";
};

}  // namespace function
}  // namespace neug
