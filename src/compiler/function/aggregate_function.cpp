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

#include "neug/compiler/function/aggregate_function.h"

#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/function/aggregate/avg.h"
#include "neug/compiler/function/aggregate/sum.h"

using namespace neug::common;
using namespace neug::function;

namespace neug {
namespace function {

AggregateFunction::AggregateFunction(const AggregateFunction& other)
    : ScalarOrAggregateFunction{other.name, other.parameterTypeIDs,
                                other.returnTypeID, other.bindFunc} {
  isDistinct = other.isDistinct;
  initializeFunc = other.initializeFunc;
  updateAllFunc = other.updateAllFunc;
  updatePosFunc = other.updatePosFunc;
  combineFunc = other.combineFunc;
  finalizeFunc = other.finalizeFunc;
  paramRewriteFunc = other.paramRewriteFunc;
  initialNullAggregateState = createInitialNullAggregateState();
}

template void AggregateFunctionUtils::appendSumOrAvgFuncs<AvgFunction>(
    std::string name, common::LogicalTypeID inputType, function_set& result);
template void AggregateFunctionUtils::appendSumOrAvgFuncs<SumFunction>(
    std::string name, common::LogicalTypeID inputType, function_set& result);

template <template <typename, typename> class FunctionType>
void AggregateFunctionUtils::appendSumOrAvgFuncs(
    std::string name, common::LogicalTypeID inputType, function_set& result) {
  std::unique_ptr<AggregateFunction> aggFunc;
  for (auto isDistinct : std::vector<bool>{true, false}) {
    TypeUtils::visit(
        LogicalType{inputType},
        [&]<IntegerTypes T>(T) {
          LogicalTypeID resultType = LogicalTypeID::INT64;
          // For avg aggregate functions, the result type is always double.
          if constexpr (std::is_same_v<FunctionType<T, int128_t>,
                                       AvgFunction<T, int128_t>>) {
            resultType = LogicalTypeID::DOUBLE;
          }
          aggFunc =
              AggregateFunctionUtils::getAggFunc<FunctionType<T, int128_t>>(
                  name, inputType, resultType, isDistinct);
        },
        [&]<FloatingPointTypes T>(T) {
          aggFunc = AggregateFunctionUtils::getAggFunc<FunctionType<T, double>>(
              name, inputType, LogicalTypeID::DOUBLE, isDistinct);
        },
        [](auto) { NEUG_UNREACHABLE; });
    result.push_back(std::move(aggFunc));
  }
}

}  // namespace function
}  // namespace neug
