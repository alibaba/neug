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

#include "neug/compiler/function/scalar_function.h"

namespace neug {
namespace function {

/**
 *  In the system we define explicit cast and implicit cast.
 *  Explicit casts are performed from user function calls, e.g. date(),
 * string(). Implicit casts are added internally.
 */

struct CastChildFunctionExecutor;

template <typename T>
concept CastExecutor = std::is_same_v<T, UnaryFunctionExecutor> ||
                       std::is_same_v<T, CastChildFunctionExecutor>;

struct CastFunction {
  // This function is only used by expression binder when implicit cast is
  // needed. The expression binder should consider reusing the existing
  // matchFunction() API.
  static bool hasImplicitCast(const common::LogicalType& srcType,
                              const common::LogicalType& dstType);

  template <CastExecutor EXECUTOR = UnaryFunctionExecutor>
  static std::unique_ptr<ScalarFunction> bindCastFunction(
      const std::string& functionName, const common::LogicalType& sourceType,
      const common::LogicalType& targetType);
};

struct CastToDateFunction {
  static constexpr const char* name = "TO_DATE";

  static function_set getFunctionSet();
};

struct DateFunction {
  using alias = CastToDateFunction;

  static constexpr const char* name = "DATE";
};

struct CastToTimestampFunction {
  static constexpr const char* name = "TIMESTAMP";

  static function_set getFunctionSet();
};

struct CastToIntervalFunction {
  static constexpr const char* name = "TO_INTERVAL";

  static function_set getFunctionSet();
};

struct IntervalFunctionAlias {
  using alias = CastToIntervalFunction;

  static constexpr const char* name = "INTERVAL";
};

struct DurationFunction {
  using alias = CastToIntervalFunction;

  static constexpr const char* name = "DURATION";
};

struct CastAnyFunction {
  static constexpr const char* name = "CAST";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug
