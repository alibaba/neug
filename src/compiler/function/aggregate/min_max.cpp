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

#include "neug/compiler/function/aggregate/min_max.h"

#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/function/comparison/comparison_functions.h"

namespace neug {
namespace function {

using namespace neug::common;

template <typename FUNC>
static void getMinMaxFunction(std::string name, function_set& set) {
  std::unique_ptr<AggregateFunction> func;
  for (auto& type : LogicalTypeUtils::getAllValidComparableLogicalTypes()) {
    auto inputTypes = std::vector<common::LogicalTypeID>{type};
    for (auto isDistinct : std::vector<bool>{true, false}) {
      common::TypeUtils::visit(
          LogicalType::getPhysicalType(type),
          [&]<ComparableTypes T>(T) {
            func = std::make_unique<AggregateFunction>(
                name, inputTypes, type, MinMaxFunction<T>::initialize,
                MinMaxFunction<T>::template updateAll<FUNC>,
                MinMaxFunction<T>::template updatePos<FUNC>,
                MinMaxFunction<T>::template combine<FUNC>,
                MinMaxFunction<T>::finalize, isDistinct);
          },
          [](auto) { NEUG_UNREACHABLE; });
      set.push_back(std::move(func));
    }
  }
}

function_set AggregateMinFunction::getFunctionSet() {
  function_set result;
  getMinMaxFunction<LessThan>(AggregateMinFunction::name, result);
  return result;
}

function_set AggregateMaxFunction::getFunctionSet() {
  function_set result;
  getMinMaxFunction<GreaterThan>(AggregateMaxFunction::name, result);
  return result;
}

}  // namespace function
}  // namespace neug
