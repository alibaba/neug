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

namespace gs {
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

struct AcosFunction {
  static constexpr const char* name = "ACOS";

  static function_set getFunctionSet();
};

struct AsinFunction {
  static constexpr const char* name = "ASIN";

  static function_set getFunctionSet();
};

struct AtanFunction {
  static constexpr const char* name = "ATAN";

  static function_set getFunctionSet();
};

struct Atan2Function {
  static constexpr const char* name = "ATAN2";

  static function_set getFunctionSet();
};

struct BitwiseXorFunction {
  static constexpr const char* name = "BITWISE_XOR";

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

struct CbrtFunction {
  static constexpr const char* name = "CBRT";

  static function_set getFunctionSet();
};

struct CeilFunction {
  static constexpr const char* name = "CEIL";

  static function_set getFunctionSet();
};

struct CeilingFunction {
  using alias = CeilFunction;

  static constexpr const char* name = "CEILING";
};

struct CosFunction {
  static constexpr const char* name = "COS";

  static function_set getFunctionSet();
};

struct CotFunction {
  static constexpr const char* name = "COT";

  static function_set getFunctionSet();
};

struct DegreesFunction {
  static constexpr const char* name = "DEGREES";

  static function_set getFunctionSet();
};

struct EvenFunction {
  static constexpr const char* name = "EVEN";

  static function_set getFunctionSet();
};

struct FactorialFunction {
  static constexpr const char* name = "FACTORIAL";

  static function_set getFunctionSet();
};

struct FloorFunction {
  static constexpr const char* name = "FLOOR";

  static function_set getFunctionSet();
};

struct GammaFunction {
  static constexpr const char* name = "GAMMA";

  static function_set getFunctionSet();
};

struct LgammaFunction {
  static constexpr const char* name = "LGAMMA";

  static function_set getFunctionSet();
};

struct LnFunction {
  static constexpr const char* name = "LN";

  static function_set getFunctionSet();
};

struct LogFunction {
  static constexpr const char* name = "LOG";

  static constexpr const char* alias = "LOG10";

  static function_set getFunctionSet();
};

struct Log10Function {
  using alias = LogFunction;

  static constexpr const char* name = "LOG10";
};

struct Log2Function {
  static constexpr const char* name = "LOG2";

  static function_set getFunctionSet();
};

struct NegateFunction {
  static constexpr const char* name = "NEGATE";

  static function_set getFunctionSet();
};

struct PiFunction {
  static constexpr const char* name = "PI";

  static function_set getFunctionSet();
};

struct RadiansFunction {
  static constexpr const char* name = "RADIANS";

  static function_set getFunctionSet();
};

struct RoundFunction {
  static constexpr const char* name = "ROUND";

  static function_set getFunctionSet();
};

struct SinFunction {
  static constexpr const char* name = "SIN";

  static function_set getFunctionSet();
};

struct SignFunction {
  static constexpr const char* name = "SIGN";

  static function_set getFunctionSet();
};

struct SqrtFunction {
  static constexpr const char* name = "SQRT";

  static function_set getFunctionSet();
};

struct TanFunction {
  static constexpr const char* name = "TAN";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace gs
