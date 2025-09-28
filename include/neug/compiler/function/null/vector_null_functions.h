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

#include "neug/compiler/function/scalar_function.h"
#include "null_function_executor.h"

namespace gs {
namespace function {

class VectorNullFunction {
 public:
  static void bindExecFunction(common::ExpressionType expressionType,
                               const binder::expression_vector& children,
                               scalar_func_exec_t& func);

  static void bindSelectFunction(common::ExpressionType expressionType,
                                 const binder::expression_vector& children,
                                 scalar_func_select_t& func);

 private:
  template <typename FUNC>
  static void UnaryNullExecFunction(
      const std::vector<std::shared_ptr<common::ValueVector>>& params,
      const std::vector<common::SelectionVector*>& paramSelVectors,
      common::ValueVector& result, common::SelectionVector*,
      void* /*dataPtr*/ = nullptr) {
    NEUG_ASSERT(params.size() == 1);
    NullOperationExecutor::execute<FUNC>(*params[0], *paramSelVectors[0],
                                         result);
  }

  template <typename FUNC>
  static bool UnaryNullSelectFunction(
      const std::vector<std::shared_ptr<common::ValueVector>>& params,
      common::SelectionVector& selVector, void* dataPtr) {
    NEUG_ASSERT(params.size() == 1);
    return NullOperationExecutor::select<FUNC>(*params[0], selVector, dataPtr);
  }
};

}  // namespace function
}  // namespace gs
