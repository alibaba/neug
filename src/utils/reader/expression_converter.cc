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

#include "neug/utils/reader/expression_converter.h"

#include <arrow/compute/api_scalar.h>
#include <glog/logging.h>

#include <stack>
#include <string>

#include "neug/generated/proto/plan/expr.pb.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace reader {

// ArrowOperatorPrecedence implementation
int ArrowOperatorPrecedence::getPrecedence(const ::common::ExprOpr& opr) {
  switch (opr.item_case()) {
  case ::common::ExprOpr::kLogical: {
    switch (opr.logical()) {
    case ::common::Logical::AND:
      return 11;
    case ::common::Logical::OR:
      return 12;
    case ::common::Logical::NOT:
      return 2;
    case ::common::Logical::EQ:
    case ::common::Logical::NE:
      return 7;
    case ::common::Logical::GE:
    case ::common::Logical::GT:
    case ::common::Logical::LT:
    case ::common::Logical::LE:
      return 6;
    default:
      return 16;
    }
  }
  case ::common::ExprOpr::kArith: {
    switch (opr.arith()) {
    case ::common::Arithmetic::ADD:
    case ::common::Arithmetic::SUB:
      return 4;
    case ::common::Arithmetic::MUL:
    case ::common::Arithmetic::DIV:
    case ::common::Arithmetic::MOD:
      return 3;
    default:
      return 16;
    }
  }
  case ::common::ExprOpr::kBrace:
    return 17;  // Special value: prevents operators inside parentheses from
                // being applied prematurely. Lower numbers = higher precedence
  default:
    return 16;
  }
}

arrow::compute::Expression ArrowExpressionConverter::convert(
    const ::common::Expression& expr) {
  // Use shunting yard algorithm to directly evaluate infix expression
  // The algorithm processes operators based on precedence and applies them
  // immediately to build Arrow expressions, rather than converting to RPN first
  std::stack<arrow::compute::Expression> value_stack;
  std::stack<::common::ExprOpr> op_stack;

  // Helper function to apply operator
  auto apply_operator = [&value_stack,
                         this](const ::common::ExprOpr& opr) -> bool {
    switch (opr.item_case()) {
    case ::common::ExprOpr::kLogical: {
      if (opr.logical() == ::common::Logical::NOT) {
        if (value_stack.size() < 1) {
          THROW_CONVERSION_EXCEPTION("Not enough operands for NOT operation");
        }
        auto operand = value_stack.top();
        value_stack.pop();
        value_stack.push(convertLogical(opr.logical(), operand, {}));
      } else {
        if (value_stack.size() < 2) {
          THROW_CONVERSION_EXCEPTION(
              "Not enough operands for binary logical operation");
        }
        auto right = value_stack.top();
        value_stack.pop();
        auto left = value_stack.top();
        value_stack.pop();
        value_stack.push(convertLogical(opr.logical(), left, right));
      }
      break;
    }
    case ::common::ExprOpr::kArith: {
      if (value_stack.size() < 2) {
        THROW_CONVERSION_EXCEPTION(
            "Not enough operands for arithmetic operation");
      }
      auto right = value_stack.top();
      value_stack.pop();
      auto left = value_stack.top();
      value_stack.pop();
      value_stack.push(convertArith(opr.arith(), left, right));
      break;
    }
    default:
      THROW_CONVERSION_EXCEPTION("Unsupported operator type in apply_operator");
    }
    return true;
  };

  // Process each operator in natural order
  for (int i = 0; i < expr.operators_size(); ++i) {
    const auto& opr = expr.operators(i);

    switch (opr.item_case()) {
    case ::common::ExprOpr::kConst: {
      value_stack.push(convertConst(opr.const_()));
      break;
    }
    case ::common::ExprOpr::kVar: {
      value_stack.push(convertVar(opr.var()));
      break;
    }
    case ::common::ExprOpr::kBrace: {
      auto brace = opr.brace();
      if (brace == ::common::ExprOpr::Brace::ExprOpr_Brace_LEFT_BRACE) {
        // Push left brace to operator stack
        op_stack.push(opr);
      } else if (brace == ::common::ExprOpr::Brace::ExprOpr_Brace_RIGHT_BRACE) {
        // Pop operators until left brace is found and apply them immediately
        // This is where operators inside parentheses are evaluated (converted)
        // Example: (a+b) -> when RIGHT_BRACE is encountered:
        //   - value_stack = [a, b], op_stack = [LEFT_BRACE, +]
        //   - Apply +: value_stack = [add(a, b)]
        //   - Pop LEFT_BRACE: op_stack = []
        // This ensures that expressions inside parentheses are fully evaluated
        // before being used as operands for outer operators
        while (!op_stack.empty() &&
               op_stack.top().item_case() != ::common::ExprOpr::kBrace) {
          apply_operator(
              op_stack.top());  // Apply operator: convert to Arrow expression
          op_stack.pop();
        }
        if (op_stack.empty()) {
          THROW_CONVERSION_EXCEPTION(
              "Mismatched parentheses: right brace without matching left "
              "brace");
        }
        // Pop the left brace
        op_stack.pop();
      }
      break;
    }
    case ::common::ExprOpr::kLogical:
    case ::common::ExprOpr::kArith: {
      // Handle operators with precedence
      // Pop and apply operators with higher or equal precedence from stack
      // This is where operator precedence is handled (conversion happens here)
      // Note: The current operator is NOT applied immediately here, it's pushed
      // to op_stack. It will be applied later when:
      //   1. A higher precedence operator is encountered, OR
      //   2. A right parenthesis is encountered, OR
      //   3. All input tokens are processed (final step)
      // This ensures that when an operator is applied, both operands are
      // already in value_stack (left operand was processed earlier, right
      // operand will be processed in the next iteration or already processed)
      int current_prec = ArrowOperatorPrecedence::getPrecedence(opr);
      while (!op_stack.empty() &&
             op_stack.top().item_case() != ::common::ExprOpr::kBrace &&
             ArrowOperatorPrecedence::getPrecedence(op_stack.top()) <=
                 current_prec) {
        apply_operator(
            op_stack.top());  // Apply operator: convert to Arrow expression
        op_stack.pop();
      }
      op_stack.push(opr);  // Push current operator to stack (not applied yet)
      break;
    }
    case ::common::ExprOpr::kScalarFunc: {
      auto& scalar_func = opr.scalar_func();
      auto funcName = scalar_func.unique_name();
      if (funcName.starts_with("CAST") && scalar_func.parameters_size() > 0) {
        value_stack.push(convert(scalar_func.parameters(0)));
        break;
      }
    }
    default:
      THROW_CONVERSION_EXCEPTION(
          "Unsupported ExprOpr item case: " +
          std::to_string(static_cast<int>(opr.item_case())) +
          ". Only const, variable (tag only), logical, arithmetic are "
          "supported.");
    }
  }

  // Apply remaining operators after processing all input tokens
  // This is the final conversion step: evaluate remaining operators in order
  while (!op_stack.empty()) {
    if (op_stack.top().item_case() == ::common::ExprOpr::kBrace) {
      THROW_CONVERSION_EXCEPTION(
          "Mismatched parentheses: left brace without matching right brace");
    }
    apply_operator(
        op_stack.top());  // Final conversion: apply remaining operators
    op_stack.pop();
  }

  // Final result should be on value stack
  if (value_stack.empty()) {
    THROW_CONVERSION_EXCEPTION("Empty expression: no values found");
  }
  if (value_stack.size() > 1) {
    THROW_CONVERSION_EXCEPTION(
        "Invalid expression: multiple values remaining on stack");
  }

  return value_stack.top();
}

// ArrowExpressionConverter private methods implementation
arrow::compute::Expression ArrowExpressionConverter::convertConst(
    const ::common::Value& const_val) {
  switch (const_val.item_case()) {
  case ::common::Value::kBoolean:
    return arrow::compute::literal(const_val.boolean());
  case ::common::Value::kI32:
    return arrow::compute::literal(const_val.i32());
  case ::common::Value::kI64:
    return arrow::compute::literal(const_val.i64());
  case ::common::Value::kU32:
    return arrow::compute::literal(const_val.u32());
  case ::common::Value::kU64:
    return arrow::compute::literal(const_val.u64());
  case ::common::Value::kF32:
    return arrow::compute::literal(const_val.f32());
  case ::common::Value::kF64:
    return arrow::compute::literal(const_val.f64());
  case ::common::Value::kStr:
    return arrow::compute::literal(const_val.str());
  default:
    THROW_CONVERSION_EXCEPTION(
        "Unsupported constant value type: " +
        std::to_string(static_cast<int>(const_val.item_case())));
  }
}

arrow::compute::Expression ArrowExpressionConverter::convertVar(
    const ::common::Variable& var) {
  // Only consider tag, throw error if has property
  if (var.has_property()) {
    THROW_CONVERSION_EXCEPTION(
        "Variable with property is not supported, only tag is allowed");
  }
  if (!var.has_tag() || !var.tag().has_name()) {
    THROW_CONVERSION_EXCEPTION("Variable must have a tag with name");
  }
  std::string column_name = var.tag().name();
  return arrow::compute::field_ref(column_name);
}

arrow::compute::Expression ArrowExpressionConverter::convertLogical(
    ::common::Logical logical_op, const arrow::compute::Expression& left,
    const arrow::compute::Expression& right) {
  switch (logical_op) {
  case ::common::Logical::NOT:
    return arrow::compute::call("invert", {left});
  case ::common::Logical::AND:
    return arrow::compute::call("and_kleene", {left, right});
  case ::common::Logical::OR:
    return arrow::compute::call("or_kleene", {left, right});
  case ::common::Logical::EQ:
    return arrow::compute::equal(left, right);
  case ::common::Logical::NE:
    return arrow::compute::not_equal(left, right);
  case ::common::Logical::LT:
    return arrow::compute::less(left, right);
  case ::common::Logical::LE:
    return arrow::compute::less_equal(left, right);
  case ::common::Logical::GT:
    return arrow::compute::greater(left, right);
  case ::common::Logical::GE:
    return arrow::compute::greater_equal(left, right);
  default:
    THROW_CONVERSION_EXCEPTION("Unsupported logical operation: " +
                               std::to_string(static_cast<int>(logical_op)));
  }
}

arrow::compute::Expression ArrowExpressionConverter::convertArith(
    ::common::Arithmetic arith_op, const arrow::compute::Expression& left,
    const arrow::compute::Expression& right) {
  switch (arith_op) {
  case ::common::Arithmetic::ADD:
    return arrow::compute::call("add", {left, right});
  case ::common::Arithmetic::SUB:
    return arrow::compute::call("subtract", {left, right});
  case ::common::Arithmetic::MUL:
    return arrow::compute::call("multiply", {left, right});
  case ::common::Arithmetic::DIV:
    return arrow::compute::call("divide", {left, right});
  case ::common::Arithmetic::MOD:
    return arrow::compute::call("mod", {left, right});
  default:
    THROW_CONVERSION_EXCEPTION("Unsupported arithmetic operation: " +
                               std::to_string(static_cast<int>(arith_op)));
  }
}

}  // namespace reader
}  // namespace neug
