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

#include "neug/compiler/function/function.h"
#include "neug/compiler/function/list/vector_list_functions.h"

namespace gs {
namespace function {

struct ArrayValueFunction {
  static constexpr const char* name = "ARRAY_VALUE";

  static function_set getFunctionSet();
};

struct ArrayCrossProductFunction {
  static constexpr const char* name = "ARRAY_CROSS_PRODUCT";

  static function_set getFunctionSet();
};

struct ArrayCosineSimilarityFunction {
  static constexpr const char* name = "ARRAY_COSINE_SIMILARITY";

  static function_set getFunctionSet();
};

struct ArrayDistanceFunction {
  static constexpr const char* name = "ARRAY_DISTANCE";

  static function_set getFunctionSet();
};

struct ArraySquaredDistanceFunction {
  static constexpr const char* name = "ARRAY_SQUARED_DISTANCE";

  static function_set getFunctionSet();
};

struct ArrayInnerProductFunction {
  static constexpr const char* name = "ARRAY_INNER_PRODUCT";

  static function_set getFunctionSet();
};

struct ArrayDotProductFunction {
  static constexpr const char* name = "ARRAY_DOT_PRODUCT";

  static function_set getFunctionSet();
};

struct ArrayConcatFunction : public ListConcatFunction {
  static constexpr const char* name = "ARRAY_CONCAT";
};

struct ArrayCatFunction {
  using alias = ArrayConcatFunction;

  static constexpr const char* name = "ARRAY_CAT";
};

struct ArrayAppendFunction : public ListAppendFunction {
  static constexpr const char* name = "ARRAY_APPEND";
};

struct ArrayPushBackFunction {
  using alias = ArrayAppendFunction;

  static constexpr const char* name = "ARRAY_PUSH_BACK";
};

struct ArrayPrependFunction : public ListPrependFunction {
  static constexpr const char* name = "ARRAY_PREPEND";
};

struct ArrayPushFrontFunction {
  using alias = ArrayPrependFunction;

  static constexpr const char* name = "ARRAY_PUSH_FRONT";
};

struct ArrayPositionFunction : public ListPositionFunction {
  static constexpr const char* name = "ARRAY_POSITION";
};

struct ArrayIndexOfFunction {
  using alias = ArrayPositionFunction;

  static constexpr const char* name = "ARRAY_INDEXOF";
};

struct ArrayContainsFunction : public ListContainsFunction {
  static constexpr const char* name = "ARRAY_CONTAINS";
};

struct ArrayHasFunction {
  using alias = ArrayContainsFunction;

  static constexpr const char* name = "ARRAY_HAS";
};

struct ArraySliceFunction : public ListSliceFunction {
  static constexpr const char* name = "ARRAY_SLICE";
};

}  // namespace function
}  // namespace gs
