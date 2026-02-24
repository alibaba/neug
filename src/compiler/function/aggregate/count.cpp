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

#include "neug/compiler/function/aggregate/count.h"

#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/expression/rel_expression.h"

using namespace neug::common;
using namespace neug::storage;
using namespace neug::binder;

namespace neug {
namespace function {

void CountFunction::updateAll(uint8_t* state_, ValueVector* input,
                              uint64_t multiplicity,
                              InMemOverflowBuffer* /*overflowBuffer*/) {
  auto state = reinterpret_cast<CountState*>(state_);
  state->count += multiplicity * input->countNonNull();
}

void CountFunction::paramRewriteFunc(binder::expression_vector& arguments) {
  NEUG_ASSERT(arguments.size() == 1);
  if (ExpressionUtil::isNodePattern(*arguments[0])) {
    auto node = (NodeExpression*) arguments[0].get();
    arguments[0] = node->getInternalID();
  } else if (ExpressionUtil::isRelPattern(*arguments[0])) {
    auto rel = (RelExpression*) arguments[0].get();
    arguments[0] = rel->getInternalIDProperty();
  }
}

function_set CountFunction::getFunctionSet() {
  function_set result;
  for (auto& type : LogicalTypeUtils::getAllValidLogicTypeIDs()) {
    for (auto isDistinct : std::vector<bool>{true, false}) {
      result.push_back(AggregateFunctionUtils::getAggFunc<CountFunction>(
          name, type, LogicalTypeID::INT64, isDistinct, paramRewriteFunc));
    }
  }
  return result;
}

}  // namespace function
}  // namespace neug
