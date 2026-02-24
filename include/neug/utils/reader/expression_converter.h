/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <arrow/compute/expression.h>

#include "neug/generated/proto/plan/expr.pb.h"

namespace neug {
namespace reader {

/**
 * @brief Template base class for converting expressions to target format
 *
 * This template class provides a generic interface for converting internal
 * common::Expression to target expression types (e.g., Arrow compute
 * expressions). Derived classes implement the conversion logic for specific
 * target formats.
 */
template <class Expr>
class ExpressionConverter {
 public:
  virtual Expr convert(const ::common::Expression& expr) = 0;
};

/**
 * @brief Helper class to get operator precedence for Arrow expression
 * conversion IMPORTANT: Lower numbers = higher precedence (e.g., 3 > 4 means *
 * has higher precedence than +) Priority order (from highest to lowest): NOT: 2
 *   MUL/DIV/MOD: 3
 *   ADD/SUB: 4
 *   Comparison (GT, LT, etc.): 6
 *   EQ/NE: 7
 *   AND: 11
 *   OR: 12
 *   LEFT_BRACE: 17 (special value to prevent premature operator application)
 */
class ArrowOperatorPrecedence {
 public:
  static int getPrecedence(const ::common::ExprOpr& opr);
};

/**
 * @brief Converts common::Expression to Arrow compute expressions
 *
 * This class implements expression conversion using a shunting-yard-like
 * algorithm to handle infix expressions with operator precedence. The input
 * expression is in natural order (left-to-right), where operators and operands
 * are stored sequentially. For example, (a+b) > 10 is stored as:
 * [LEFT_BRACE, a, +, b, RIGHT_BRACE, >, 10]
 *
 * Supported expression types:
 * - Constants: boolean, numeric, and string literals
 * - Variables: field references (tag only, property access throws error)
 * - Logical operations: AND, OR, NOT, comparison operators
 * - Arithmetic operations: ADD, SUB, MUL, DIV, MOD
 *
 * The conversion handles operator precedence correctly and supports parentheses
 * for explicit grouping.
 */
class ArrowExpressionConverter
    : ExpressionConverter<arrow::compute::Expression> {
 public:
  arrow::compute::Expression convert(const ::common::Expression& expr) override;

 private:
  /**
   * @brief Convert constant value to Arrow literal expression
   * @param const_val The constant value to convert
   * @return Arrow literal expression
   */
  arrow::compute::Expression convertConst(const ::common::Value& const_val);

  /**
   * @brief Convert variable to Arrow field reference expression
   * Only considers tag, throws error if variable has property
   * @param var The variable to convert
   * @return Arrow field reference expression
   */
  arrow::compute::Expression convertVar(const ::common::Variable& var);

  /**
   * @brief Convert logical operation to Arrow compute expression
   * @param logical_op The logical operation type
   * @param left Left operand expression
   * @param right Right operand expression (may be empty for unary operators)
   * @return Arrow compute expression
   */
  arrow::compute::Expression convertLogical(
      ::common::Logical logical_op, const arrow::compute::Expression& left,
      const arrow::compute::Expression& right);

  /**
   * @brief Convert arithmetic operation to Arrow compute expression
   * @param arith_op The arithmetic operation type
   * @param left Left operand expression
   * @param right Right operand expression
   * @return Arrow compute expression
   */
  arrow::compute::Expression convertArith(
      ::common::Arithmetic arith_op, const arrow::compute::Expression& left,
      const arrow::compute::Expression& right);
};

}  // namespace reader
}  // namespace neug
