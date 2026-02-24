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

#include "neug/compiler/binder/expression/scalar_function_expression.h"

#include "neug/compiler/binder/expression/expression_util.h"

using namespace neug::common;

namespace neug {
namespace binder {

std::string ScalarFunctionExpression::toStringInternal() const {
  if (function->name.starts_with("CAST")) {
    return stringFormat("CAST({}, {})", ExpressionUtil::toString(children),
                        bindData->resultType.toString());
  }
  return stringFormat("{}({})", function->name,
                      ExpressionUtil::toString(children));
}

std::string ScalarFunctionExpression::getUniqueName(
    const std::string& functionName, const expression_vector& children) {
  return stringFormat("{}({})", functionName,
                      ExpressionUtil::getUniqueName(children));
}

}  // namespace binder
}  // namespace neug
