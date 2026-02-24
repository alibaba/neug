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

#include "neug/compiler/common/enums/expression_type.h"

#include "neug/compiler/common/assert.h"
#include "neug/compiler/function/comparison/vector_comparison_functions.h"

using namespace neug::function;

namespace neug {
namespace common {

bool ExpressionTypeUtil::isUnary(ExpressionType type) {
  return ExpressionType::NOT == type || ExpressionType::IS_NULL == type ||
         ExpressionType::IS_NOT_NULL == type;
}

bool ExpressionTypeUtil::isBinary(ExpressionType type) {
  return isComparison(type) || ExpressionType::OR == type ||
         ExpressionType::XOR == type || ExpressionType::AND == type;
}

bool ExpressionTypeUtil::isBoolean(ExpressionType type) {
  return ExpressionType::OR == type || ExpressionType::XOR == type ||
         ExpressionType::AND == type || ExpressionType::NOT == type;
}

bool ExpressionTypeUtil::isComparison(ExpressionType type) {
  return ExpressionType::EQUALS == type || ExpressionType::NOT_EQUALS == type ||
         ExpressionType::GREATER_THAN == type ||
         ExpressionType::GREATER_THAN_EQUALS == type ||
         ExpressionType::LESS_THAN == type ||
         ExpressionType::LESS_THAN_EQUALS == type;
}

bool ExpressionTypeUtil::isNullOperator(ExpressionType type) {
  return ExpressionType::IS_NULL == type || ExpressionType::IS_NOT_NULL == type;
}

ExpressionType ExpressionTypeUtil::reverseComparisonDirection(
    ExpressionType type) {
  NEUG_ASSERT(isComparison(type));
  switch (type) {
  case ExpressionType::GREATER_THAN:
    return ExpressionType::LESS_THAN;
  case ExpressionType::GREATER_THAN_EQUALS:
    return ExpressionType::LESS_THAN_EQUALS;
  case ExpressionType::LESS_THAN:
    return ExpressionType::GREATER_THAN;
  case ExpressionType::LESS_THAN_EQUALS:
    return ExpressionType::GREATER_THAN_EQUALS;
  default:
    return type;
  }
}

// LCOV_EXCL_START
std::string ExpressionTypeUtil::toString(ExpressionType type) {
  switch (type) {
  case ExpressionType::OR:
    return "OR";
  case ExpressionType::XOR:
    return "XOR";
  case ExpressionType::AND:
    return "AND";
  case ExpressionType::NOT:
    return "NOT";
  case ExpressionType::EQUALS:
    return EqualsFunction::name;
  case ExpressionType::NOT_EQUALS:
    return NotEqualsFunction::name;
  case ExpressionType::GREATER_THAN:
    return GreaterThanFunction::name;
  case ExpressionType::GREATER_THAN_EQUALS:
    return GreaterThanEqualsFunction::name;
  case ExpressionType::LESS_THAN:
    return LessThanFunction::name;
  case ExpressionType::LESS_THAN_EQUALS:
    return LessThanEqualsFunction::name;
  case ExpressionType::IS_NULL:
    return "IS_NULL";
  case ExpressionType::IS_NOT_NULL:
    return "IS_NOT_NULL";
  case ExpressionType::PROPERTY:
    return "PROPERTY";
  case ExpressionType::LITERAL:
    return "LITERAL";
  case ExpressionType::STAR:
    return "STAR";
  case ExpressionType::VARIABLE:
    return "VARIABLE";
  case ExpressionType::PATH:
    return "PATH";
  case ExpressionType::PATTERN:
    return "PATTERN";
  case ExpressionType::PARAMETER:
    return "PARAMETER";
  case ExpressionType::FUNCTION:
    return "SCALAR_FUNCTION";
  case ExpressionType::AGGREGATE_FUNCTION:
    return "AGGREGATE_FUNCTION";
  case ExpressionType::SUBQUERY:
    return "SUBQUERY";
  case ExpressionType::CASE_ELSE:
    return "CASE_ELSE";
  case ExpressionType::GRAPH:
    return "GRAPH";
  case ExpressionType::LAMBDA:
    return "LAMBDA";
  default:
    NEUG_UNREACHABLE;
  }
}

std::string ExpressionTypeUtil::toParsableString(ExpressionType type) {
  switch (type) {
  case ExpressionType::EQUALS:
    return "=";
  case ExpressionType::NOT_EQUALS:
    return "<>";
  case ExpressionType::GREATER_THAN:
    return ">";
  case ExpressionType::GREATER_THAN_EQUALS:
    return ">=";
  case ExpressionType::LESS_THAN:
    return "<";
  case ExpressionType::LESS_THAN_EQUALS:
    return "<=";
  case ExpressionType::IS_NULL:
    return "IS NULL";
  case ExpressionType::IS_NOT_NULL:
    return "IS NOT NULL";
  default:
    THROW_RUNTIME_ERROR(stringFormat(
        "ExpressionTypeUtil::toParsableString not implemented for {}",
        toString(type)));
  }
}
// LCOV_EXCL_STOP

}  // namespace common
}  // namespace neug
