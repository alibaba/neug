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

#include "neug/compiler/function/aggregate/avg.h"

namespace neug {
namespace function {

using namespace neug::common;

function_set AggregateAvgFunction::getFunctionSet() {
  function_set result;
  for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
    AggregateFunctionUtils::appendSumOrAvgFuncs<AvgFunction>(name, typeID,
                                                             result);
  }
  return result;
}

}  // namespace function
}  // namespace neug
