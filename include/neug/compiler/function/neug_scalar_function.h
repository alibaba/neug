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

#include "neug/compiler/function/scalar_function.h"
#include "neug/utils/function_type.h"

namespace neug {
namespace function {
struct NeugScalarFunction : public ScalarFunction {
  execution::neug_func_exec_t neugExecFunc = nullptr;

  NeugScalarFunction(std::string name,
                     std::vector<common::DataTypeId> parameterTypeIDs,
                     common::DataTypeId returnTypeID,
                     execution::neug_func_exec_t neugExecFunc)
      : ScalarFunction{std::move(name), std::move(parameterTypeIDs),
                       returnTypeID},
        neugExecFunc{std::move(neugExecFunc)} {}

  std::unique_ptr<ScalarFunction> copy() const override {
    return std::make_unique<NeugScalarFunction>(*this);
  }
};

// Keep NeugScalarFunction's layout and callback unchanged for extensions that
// only consume argument values. Context-aware functions opt in to this subtype.
struct NeugContextScalarFunction : public NeugScalarFunction {
  execution::neug_context_func_exec_t neugContextExecFunc;

  NeugContextScalarFunction(std::string name,
                            std::vector<common::DataTypeId> parameterTypeIDs,
                            common::DataTypeId returnTypeID,
                            execution::neug_context_func_exec_t contextExecFunc)
      : NeugScalarFunction(std::move(name), std::move(parameterTypeIDs),
                           returnTypeID, nullptr),
        neugContextExecFunc(contextExecFunc) {}

  std::unique_ptr<ScalarFunction> copy() const override {
    return std::make_unique<NeugContextScalarFunction>(*this);
  }
};
}  // namespace function
}  // namespace neug
