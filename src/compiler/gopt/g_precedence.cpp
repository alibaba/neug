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

#include "neug/compiler/gopt/g_precedence.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/function/arithmetic/vector_arithmetic_functions.h"
#include "neug/compiler/gopt/g_scalar_type.h"

namespace neug {
namespace gopt {

// c++ operator precedence
int GPrecedence::getPrecedence(const binder::Expression& expr) {
  switch (expr.expressionType) {
  case common::ExpressionType::OR:
    return 1;  // Logical OR (||) - lowest precedence
  case common::ExpressionType::AND:
    return 2;  // Logical AND (&&)
  case common::ExpressionType::XOR:
    return 4;  // Bitwise XOR (^) - approximate precedence
  case common::ExpressionType::EQUALS:
  case common::ExpressionType::NOT_EQUALS:
  case common::ExpressionType::IS_NULL:
  case common::ExpressionType::IS_NOT_NULL:
    return 6;  // Equality operators (==, !=)
  case common::ExpressionType::GREATER_THAN:
  case common::ExpressionType::GREATER_THAN_EQUALS:
  case common::ExpressionType::LESS_THAN:
  case common::ExpressionType::LESS_THAN_EQUALS:
    return 7;  // Relational operators (<, <=, >, >=)
  case common::ExpressionType::FUNCTION: {
    // Handle precedence based on function type
    GScalarType scalarType{expr};
    switch (scalarType.getType()) {
    case ScalarType::ADD:
    case ScalarType::SUBTRACT:
      return 8;  // Addition and subtraction (+, -)
    case ScalarType::MULTIPLY:
    case ScalarType::DIVIDE:
    case ScalarType::MODULO:
      return 9;  // Multiplication, division, modulo (*, /, %)
    case ScalarType::POWER:
      return 10;  // Power (exponentiation) - higher precedence
    default:
      return 11;  // Other function calls - highest precedence
    }
  }
  case common::ExpressionType::NOT:
    return 10;  // Unary NOT (!) - high precedence
  // Uncomment and assign precedence for unary minus/plus if needed
  // case common::ExpressionType::UNARY_MINUS:
  // case common::ExpressionType::UNARY_PLUS:
  //     return 10;  // Unary minus/plus (-, +) - similar precedence as NOT
  case common::ExpressionType::PROPERTY:
  case common::ExpressionType::LITERAL:
  case common::ExpressionType::PARAMETER:
  case common::ExpressionType::VARIABLE:
  case common::ExpressionType::AGGREGATE_FUNCTION:
  case common::ExpressionType::CASE_ELSE:
  case common::ExpressionType::PATTERN:
    return 11;  // Atomic expressions or function calls - highest precedence
  default:
    return 0;  // Unknown or unsupported expression types
  }
}

bool GPrecedence::isLeftAssociative(const binder::Expression& expr) {
  switch (expr.expressionType) {
  // Binary logical and comparison operators – left-associative
  case common::ExpressionType::OR:
  case common::ExpressionType::XOR:
  case common::ExpressionType::AND:
  case common::ExpressionType::EQUALS:
  case common::ExpressionType::NOT_EQUALS:
  case common::ExpressionType::GREATER_THAN:
  case common::ExpressionType::GREATER_THAN_EQUALS:
  case common::ExpressionType::LESS_THAN:
  case common::ExpressionType::LESS_THAN_EQUALS:
    return true;

  case common::ExpressionType::FUNCTION: {
    // Function associativity depends on the specific operator
    GScalarType scalarType{expr};
    switch (scalarType.getType()) {
    case ScalarType::ADD:
    case ScalarType::SUBTRACT:
    case ScalarType::MULTIPLY:
    case ScalarType::DIVIDE:
    case ScalarType::MODULO:
      return true;  // Left-associative for +, -, *, /, %
    case ScalarType::POWER:
      return false;  // Right-associative for power (exponentiation)
    case ScalarType::LIST_CONTAINS:
      return false;
    default:
      return true;  // Default to left-associative for other functions
    }
  }

  // Unary operators – right-associative
  case common::ExpressionType::NOT:
    return false;

    // case common::ExpressionType::UNARY_MINUS:
    // case common::ExpressionType::UNARY_PLUS:
    //     return false;

  default:
    // By default, treat atomic expressions or unknown types as left-associative
    return true;
  }
}

bool GPrecedence::needBrace(const binder::Expression& parent,
                            const binder::Expression& child) {
  return getPrecedence(child) <= getPrecedence(parent);
}

}  // namespace gopt
}  // namespace neug
