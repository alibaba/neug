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

#include "neug/compiler/binder/expression/expression.h"

#include "neug/utils/exception/exception.h"

using namespace neug::common;

namespace neug {
namespace binder {

void Expression::cast(const LogicalType&) {
  // LCOV_EXCL_START
  THROW_BINDER_EXCEPTION(stringFormat(
      "Data type of expression {} should not be modified.", toString()));
  // LCOV_EXCL_STOP
}

expression_vector Expression::splitOnAND() {
  expression_vector result;
  if (ExpressionType::AND == expressionType) {
    for (auto& child : children) {
      for (auto& exp : child->splitOnAND()) {
        result.push_back(exp);
      }
    }
  } else {
    result.push_back(shared_from_this());
  }
  return result;
}

}  // namespace binder
}  // namespace neug
