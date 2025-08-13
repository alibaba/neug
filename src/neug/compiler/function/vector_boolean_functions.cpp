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

#include "neug/compiler/function/boolean/vector_boolean_functions.h"

#include "neug/compiler/function/boolean/boolean_functions.h"
#include "neug/utils/exception/exception.h"

using namespace gs::common;

namespace gs {
namespace function {

void VectorBooleanFunction::bindExecFunction(
    ExpressionType expressionType, const binder::expression_vector& children,
    scalar_func_exec_t& func) {
  if (ExpressionTypeUtil::isBinary(expressionType)) {
    bindBinaryExecFunction(expressionType, children, func);
  } else {
    KU_ASSERT(ExpressionTypeUtil::isUnary(expressionType));
    bindUnaryExecFunction(expressionType, children, func);
  }
}

void VectorBooleanFunction::bindSelectFunction(
    ExpressionType expressionType, const binder::expression_vector& children,
    scalar_func_select_t& func) {
  if (ExpressionTypeUtil::isBinary(expressionType)) {
    bindBinarySelectFunction(expressionType, children, func);
  } else {
    KU_ASSERT(ExpressionTypeUtil::isUnary(expressionType));
    bindUnarySelectFunction(expressionType, children, func);
  }
}

void VectorBooleanFunction::bindBinaryExecFunction(
    ExpressionType expressionType, const binder::expression_vector& children,
    scalar_func_exec_t& func) {
  KU_ASSERT(children.size() == 2);
  const auto& leftType = children[0]->dataType;
  const auto& rightType = children[1]->dataType;
  (void) leftType;
  (void) rightType;
  KU_ASSERT(leftType.getLogicalTypeID() == LogicalTypeID::BOOL &&
            rightType.getLogicalTypeID() == LogicalTypeID::BOOL);
  switch (expressionType) {
  case ExpressionType::AND: {
    func = &BinaryBooleanExecFunction<And>;
    return;
  }
  case ExpressionType::OR: {
    func = &BinaryBooleanExecFunction<Or>;
    return;
  }
  case ExpressionType::XOR: {
    func = &BinaryBooleanExecFunction<Xor>;
    return;
  }
  default:
    THROW_RUNTIME_ERROR("Invalid expression type " +
                        ExpressionTypeUtil::toString(expressionType) +
                        " for VectorBooleanFunctions::bindBinaryExecFunction.");
  }
}

void VectorBooleanFunction::bindBinarySelectFunction(
    ExpressionType expressionType, const binder::expression_vector& children,
    scalar_func_select_t& func) {
  KU_ASSERT(children.size() == 2);
  const auto& leftType = children[0]->dataType;
  const auto& rightType = children[1]->dataType;
  (void) leftType;
  (void) rightType;
  KU_ASSERT(leftType.getLogicalTypeID() == LogicalTypeID::BOOL &&
            rightType.getLogicalTypeID() == LogicalTypeID::BOOL);
  switch (expressionType) {
  case ExpressionType::AND: {
    func = &BinaryBooleanSelectFunction<And>;
    return;
  }
  case ExpressionType::OR: {
    func = &BinaryBooleanSelectFunction<Or>;
    return;
  }
  case ExpressionType::XOR: {
    func = &BinaryBooleanSelectFunction<Xor>;
    return;
  }
  default:
    THROW_RUNTIME_ERROR(
        "Invalid expression type " +
        ExpressionTypeUtil::toString(expressionType) +
        " for VectorBooleanFunctions::bindBinarySelectFunction.");
  }
}

void VectorBooleanFunction::bindUnaryExecFunction(
    ExpressionType expressionType, const binder::expression_vector& children,
    scalar_func_exec_t& func) {
  KU_ASSERT(children.size() == 1 &&
            children[0]->dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
  (void) children;
  switch (expressionType) {
  case ExpressionType::NOT: {
    func = &UnaryBooleanExecFunction<Not>;
    return;
  }
  default:
    THROW_RUNTIME_ERROR("Invalid expression type " +
                        ExpressionTypeUtil::toString(expressionType) +
                        " for VectorBooleanFunctions::bindUnaryExecFunction.");
  }
}

void VectorBooleanFunction::bindUnarySelectFunction(
    ExpressionType expressionType, const binder::expression_vector& children,
    scalar_func_select_t& func) {
  KU_ASSERT(children.size() == 1 &&
            children[0]->dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
  (void) children;
  switch (expressionType) {
  case ExpressionType::NOT: {
    func = &UnaryBooleanSelectFunction<Not>;
    return;
  }
  default:
    THROW_RUNTIME_ERROR("Invalid expression type " +
                        ExpressionTypeUtil::toString(expressionType) +
                        " for VectorBooleanFunctions::bindUnaryExecFunction.");
  }
}

}  // namespace function
}  // namespace gs
