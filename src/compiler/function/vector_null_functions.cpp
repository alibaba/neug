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

#include "neug/compiler/function/null/vector_null_functions.h"

#include "neug/compiler/function/null/null_functions.h"
#include "neug/utils/exception/exception.h"

using namespace gs::common;

namespace gs {
namespace function {

void VectorNullFunction::bindExecFunction(
    ExpressionType expressionType,
    const binder::expression_vector& /*children*/, scalar_func_exec_t& func) {
  switch (expressionType) {
  case ExpressionType::IS_NULL: {
    func = UnaryNullExecFunction<IsNull>;
    return;
  }
  case ExpressionType::IS_NOT_NULL: {
    func = UnaryNullExecFunction<IsNotNull>;
    return;
  }
  default:
    THROW_RUNTIME_ERROR("Invalid expression type " +
                        ExpressionTypeUtil::toString(expressionType) +
                        "for VectorNullOperations::bindUnaryExecFunction.");
  }
}

void VectorNullFunction::bindSelectFunction(
    ExpressionType expressionType, const binder::expression_vector& children,
    scalar_func_select_t& func) {
  NEUG_ASSERT(children.size() == 1);
  (void) children;
  switch (expressionType) {
  case ExpressionType::IS_NULL: {
    func = UnaryNullSelectFunction<IsNull>;
    return;
  }
  case ExpressionType::IS_NOT_NULL: {
    func = UnaryNullSelectFunction<IsNotNull>;
    return;
  }
  default:
    THROW_RUNTIME_ERROR("Invalid expression type " +
                        ExpressionTypeUtil::toString(expressionType) +
                        "for VectorNullOperations::bindUnarySelectFunction.");
  }
}

}  // namespace function
}  // namespace gs
