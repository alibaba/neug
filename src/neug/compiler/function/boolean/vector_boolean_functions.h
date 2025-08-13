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

#include "boolean_function_executor.h"
#include "neug/compiler/function/scalar_function.h"

namespace gs {
namespace function {

class VectorBooleanFunction {
 public:
  static void bindExecFunction(common::ExpressionType expressionType,
                               const binder::expression_vector& children,
                               scalar_func_exec_t& func);

  static void bindSelectFunction(common::ExpressionType expressionType,
                                 const binder::expression_vector& children,
                                 scalar_func_select_t& func);

 private:
  template <typename FUNC>
  static void BinaryBooleanExecFunction(
      const std::vector<std::shared_ptr<common::ValueVector>>& params,
      const std::vector<common::SelectionVector*>& paramSelVectors,
      common::ValueVector& result, common::SelectionVector* resultSelVector,
      void* /*dataPtr*/ = nullptr) {
    KU_ASSERT(params.size() == 2);
    BinaryBooleanFunctionExecutor::execute<FUNC>(*params[0], paramSelVectors[0],
                                                 *params[1], paramSelVectors[1],
                                                 result, resultSelVector);
  }

  template <typename FUNC>
  static bool BinaryBooleanSelectFunction(
      const std::vector<std::shared_ptr<common::ValueVector>>& params,
      common::SelectionVector& selVector, void* /*dataPtr*/) {
    KU_ASSERT(params.size() == 2);
    return BinaryBooleanFunctionExecutor::select<FUNC>(*params[0], *params[1],
                                                       selVector);
  }

  template <typename FUNC>
  static void UnaryBooleanExecFunction(
      const std::vector<std::shared_ptr<common::ValueVector>>& params,
      const std::vector<common::SelectionVector*>& paramSelVectors,
      common::ValueVector& result, common::SelectionVector* resultSelVector,
      void* /*dataPtr*/ = nullptr) {
    KU_ASSERT(params.size() == 1);
    UnaryBooleanOperationExecutor::execute<FUNC>(*params[0], paramSelVectors[0],
                                                 result, resultSelVector);
  }

  template <typename FUNC>
  static bool UnaryBooleanSelectFunction(
      const std::vector<std::shared_ptr<common::ValueVector>>& params,
      common::SelectionVector& selVector, void* /*dataPtr*/) {
    KU_ASSERT(params.size() == 1);
    return UnaryBooleanOperationExecutor::select<FUNC>(*params[0], selVector);
  }

  static void bindBinaryExecFunction(common::ExpressionType expressionType,
                                     const binder::expression_vector& children,
                                     scalar_func_exec_t& func);

  static void bindBinarySelectFunction(
      common::ExpressionType expressionType,
      const binder::expression_vector& children, scalar_func_select_t& func);

  static void bindUnaryExecFunction(common::ExpressionType expressionType,
                                    const binder::expression_vector& children,
                                    scalar_func_exec_t& func);

  static void bindUnarySelectFunction(common::ExpressionType expressionType,
                                      const binder::expression_vector& children,
                                      scalar_func_select_t& func);
};

}  // namespace function
}  // namespace gs
