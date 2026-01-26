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

#include "neug/compiler/function/function.h"

namespace neug {
namespace binder {
class ExpressionBinder;
}
namespace function {

struct RewriteFunctionBindInput {
  main::ClientContext* context;
  binder::ExpressionBinder* expressionBinder;
  binder::expression_vector arguments;

  RewriteFunctionBindInput(main::ClientContext* context,
                           binder::ExpressionBinder* expressionBinder,
                           binder::expression_vector arguments)
      : context{context},
        expressionBinder{expressionBinder},
        arguments{std::move(arguments)} {}
};

// Rewrite function to a different expression, e.g. id(n) -> n._id.
using rewrite_func_rewrite_t =
    std::function<std::shared_ptr<binder::Expression>(
        const RewriteFunctionBindInput&)>;

// We write for the following functions
// ID(n) -> n._id
struct RewriteFunction final : Function {
  rewrite_func_rewrite_t rewriteFunc;

  RewriteFunction(std::string name,
                  std::vector<common::LogicalTypeID> parameterTypeIDs,
                  rewrite_func_rewrite_t rewriteFunc)
      : Function{std::move(name), std::move(parameterTypeIDs)},
        rewriteFunc{std::move(rewriteFunc)} {}
  EXPLICIT_COPY_DEFAULT_MOVE(RewriteFunction)

 private:
  RewriteFunction(const RewriteFunction& other)
      : Function{other}, rewriteFunc{other.rewriteFunc} {}
};

}  // namespace function
}  // namespace neug
