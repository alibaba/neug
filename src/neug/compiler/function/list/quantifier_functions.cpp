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

#include "neug/compiler/common/types/types.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/function.h"
#include "neug/compiler/function/list/vector_list_functions.h"

using namespace gs::common;

namespace gs {
namespace function {

void execQuantifierFunc(
    quantifier_handler handler,
    const std::vector<std::shared_ptr<common::ValueVector>>& input,
    const std::vector<common::SelectionVector*>& inputSelVectors,
    common::ValueVector& result, common::SelectionVector* resultSelVector,
    void* bindData) {}

std::unique_ptr<FunctionBindData> bindQuantifierFunc(
    const ScalarBindFuncInput& input) {
  std::vector<common::LogicalType> paramTypes;
  paramTypes.push_back(input.arguments[0]->getDataType().copy());
  paramTypes.push_back(input.arguments[1]->getDataType().copy());
  return std::make_unique<FunctionBindData>(std::move(paramTypes),
                                            common::LogicalType::BOOL());
}

}  // namespace function
}  // namespace gs
