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

#include "neug/compiler/binder/function_evaluator.h"

#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/function/sequence/sequence_functions.h"

using namespace neug::common;
using namespace neug::processor;
using namespace neug::storage;
using namespace neug::main;
using namespace neug::binder;
using namespace neug::function;

namespace neug {
namespace evaluator {

FunctionExpressionEvaluator::FunctionExpressionEvaluator(
    std::shared_ptr<Expression> expression,
    std::vector<std::unique_ptr<ExpressionEvaluator>> children)
    : ExpressionEvaluator{type_, std::move(expression), std::move(children)} {
  auto& functionExpr = this->expression->constCast<ScalarFunctionExpression>();
  function = functionExpr.getFunction().copy();
  bindData = functionExpr.getBindData()->copy();
}

void FunctionExpressionEvaluator::evaluate() {
  auto ctx = localState.clientContext;
  for (auto& child : children) {
    child->evaluate();
  }
  if (function->execFunc != nullptr) {
    bindData->clientContext = ctx;
    runExecFunc(bindData.get());
  }
}

void FunctionExpressionEvaluator::evaluate(common::sel_t count) {
  NEUG_ASSERT(
      expression->constCast<ScalarFunctionExpression>().getFunction().name ==
      NextValFunction::name);
  for (auto& child : children) {
    child->evaluate(count);
  }
  bindData->count = count;
  bindData->clientContext = localState.clientContext;
  runExecFunc(bindData.get());
}

bool FunctionExpressionEvaluator::selectInternal(SelectionVector& selVector) {
  for (auto& child : children) {
    child->evaluate();
  }
  // Temporary code path for function whose return type is BOOL but select
  // interface is not implemented (e.g. list_contains). We should remove this if
  // statement eventually.
  if (function->selectFunc == nullptr) {
    NEUG_ASSERT(resultVector->dataType.getLogicalTypeID() ==
                LogicalTypeID::BOOL);
    runExecFunc();
    return updateSelectedPos(selVector);
  }
  return function->selectFunc(parameters, selVector, bindData.get());
}

void FunctionExpressionEvaluator::runExecFunc(void* dataPtr) {
  function->execFunc(parameters,
                     common::SelectionVector::fromValueVectors(parameters),
                     *resultVector, resultVector->getSelVectorPtr(), dataPtr);
}

void FunctionExpressionEvaluator::resolveResultVector(
    const ResultSet& /*resultSet*/, MemoryManager* memoryManager) {
  resultVector =
      std::make_shared<ValueVector>(expression->dataType.copy(), memoryManager);
  std::vector<ExpressionEvaluator*> inputEvaluators;
  inputEvaluators.reserve(children.size());
  for (auto& child : children) {
    parameters.push_back(child->resultVector);
    inputEvaluators.push_back(child.get());
  }
  resolveResultStateFromChildren(inputEvaluators);
  if (function->compileFunc != nullptr) {
    function->compileFunc(bindData.get(), parameters, resultVector);
  }
}

}  // namespace evaluator
}  // namespace neug