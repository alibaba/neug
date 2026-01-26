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

namespace neug {
namespace function {

struct AddFunction {
  static constexpr const char* name = "+";

  static function_set getFunctionSet();
};

struct SubtractFunction {
  static constexpr const char* name = "-";

  static function_set getFunctionSet();
};

struct MultiplyFunction {
  static constexpr const char* name = "*";

  static function_set getFunctionSet();
};

struct DivideFunction {
  static constexpr const char* name = "/";

  static function_set getFunctionSet();
};

struct ModuloFunction {
  static constexpr const char* name = "%";

  static function_set getFunctionSet();
};

struct PowerFunction {
  static constexpr const char* name = "^";

  static function_set getFunctionSet();
};

struct PowFunction {
  using alias = PowerFunction;

  static constexpr const char* name = "POW";
};

struct AbsFunction {
  static constexpr const char* name = "ABS";

  static function_set getFunctionSet();
};

struct BitwiseAndFunction {
  static constexpr const char* name = "BITWISE_AND";

  static function_set getFunctionSet();
};

struct BitwiseOrFunction {
  static constexpr const char* name = "BITWISE_OR";

  static function_set getFunctionSet();
};

struct BitShiftLeftFunction {
  static constexpr const char* name = "BITSHIFT_LEFT";

  static function_set getFunctionSet();
};

struct BitShiftRightFunction {
  static constexpr const char* name = "BITSHIFT_RIGHT";

  static function_set getFunctionSet();
};

struct FactorialFunction {
  static constexpr const char* name = "FACTORIAL";

  static function_set getFunctionSet();
};

struct NegateFunction {
  static constexpr const char* name = "NEGATE";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug
