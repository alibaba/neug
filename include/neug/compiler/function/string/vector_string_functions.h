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
#include "neug/execution/common/types/value.h"

namespace neug {
namespace function {

struct VectorStringFunction {
  template <class OPERATION>
  static inline function_set getUnaryStrFunction(std::string funcName) {
    function_set functionSet;
    functionSet.emplace_back(std::make_unique<ScalarFunction>(
        funcName,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::STRING},
        common::LogicalTypeID::STRING,
        ScalarFunction::UnaryStringExecFunction<
            common::neug_string_t, common::neug_string_t, OPERATION>));
    return functionSet;
  }
};

struct ContainsFunction : public VectorStringFunction {
  static constexpr const char* name = "CONTAINS";

  static function_set getFunctionSet();
};

struct EndsWithFunction : public VectorStringFunction {
  static constexpr const char* name = "ENDS_WITH";

  static function_set getFunctionSet();
};

struct SuffixFunction {
  using alias = EndsWithFunction;

  static constexpr const char* name = "SUFFIX";
};

struct LowerFunction : public VectorStringFunction {
  static constexpr const char* name = "LOWER";

  static function_set getFunctionSet();

  static execution::Value Exec(const std::vector<execution::Value>& args);
};

struct ToLowerFunction : public VectorStringFunction {
  using alias = LowerFunction;

  static constexpr const char* name = "TOLOWER";
};

struct LcaseFunction {
  using alias = LowerFunction;

  static constexpr const char* name = "LCASE";
};

struct ReverseFunction : public VectorStringFunction {
  static constexpr const char* name = "REVERSE";

  static function_set getFunctionSet();

  static neug::execution::Value Exec(
      const std::vector<neug::execution::Value>& args);
};

struct StartsWithFunction : public VectorStringFunction {
  static constexpr const char* name = "STARTS_WITH";

  static function_set getFunctionSet();
};

struct UpperFunction : public VectorStringFunction {
  static constexpr const char* name = "UPPER";

  static function_set getFunctionSet();

  static neug::execution::Value Exec(
      const std::vector<neug::execution::Value>& args);
};

struct ToUpperFunction : public VectorStringFunction {
  using alias = UpperFunction;

  static constexpr const char* name = "TOUPPER";
};

struct UCaseFunction {
  using alias = UpperFunction;

  static constexpr const char* name = "UCASE";
};

}  // namespace function
}  // namespace neug
