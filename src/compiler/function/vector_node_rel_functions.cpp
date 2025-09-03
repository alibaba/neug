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

#include "neug/compiler/function/schema/vector_node_rel_functions.h"

#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/compiler/function/schema/offset_functions.h"
#include "neug/compiler/function/unary_function_executor.h"

using namespace gs::common;

namespace gs {
namespace function {

static void execFunc(
    const std::vector<std::shared_ptr<common::ValueVector>>& params,
    const std::vector<common::SelectionVector*>& paramSelVectors,
    common::ValueVector& result, common::SelectionVector* resultSelVector,
    void* /*dataPtr*/ = nullptr) {
  KU_ASSERT(params.size() == 1);
  UnaryFunctionExecutor::execute<internalID_t, int64_t, Offset>(
      *params[0], paramSelVectors[0], result, resultSelVector);
}

function_set OffsetFunction::getFunctionSet() {
  function_set functionSet;
  functionSet.push_back(make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::INTERNAL_ID},
      LogicalTypeID::INT64, execFunc));
  return functionSet;
}

}  // namespace function
}  // namespace gs
