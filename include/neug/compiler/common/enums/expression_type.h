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

#include <cstdint>
#include <string>

#include "neug/utils/api.h"

namespace neug {
namespace common {

enum class ExpressionType : uint8_t {
  // Boolean Connection Expressions
  OR = 0,
  XOR = 1,
  AND = 2,
  NOT = 3,

  // Comparison Expressions
  EQUALS = 10,
  NOT_EQUALS = 11,
  GREATER_THAN = 12,
  GREATER_THAN_EQUALS = 13,
  LESS_THAN = 14,
  LESS_THAN_EQUALS = 15,

  // Null Operator Expressions
  IS_NULL = 50,
  IS_NOT_NULL = 51,

  PROPERTY = 60,

  LITERAL = 70,

  STAR = 80,

  VARIABLE = 90,
  PATH = 91,
  PATTERN = 92,  // Node & Rel pattern

  PARAMETER = 100,

  // At parsing stage, both aggregate and scalar functions have type FUNCTION.
  // After binding, only scalar function have type FUNCTION.
  FUNCTION = 110,

  AGGREGATE_FUNCTION = 130,

  SUBQUERY = 190,

  CASE_ELSE = 200,

  GRAPH = 210,

  LAMBDA = 220,

  // NOTE: this enum has type uint8_t so don't assign over 255.
  INVALID = 255,
};

struct ExpressionTypeUtil {
  static bool isUnary(ExpressionType type);
  static bool isBinary(ExpressionType type);
  static bool isBoolean(ExpressionType type);
  static bool isComparison(ExpressionType type);
  static bool isNullOperator(ExpressionType type);

  static ExpressionType reverseComparisonDirection(ExpressionType type);

  static NEUG_API std::string toString(ExpressionType type);
  static std::string toParsableString(ExpressionType type);
};

}  // namespace common
}  // namespace neug
