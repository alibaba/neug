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

#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/function/list/functions/list_position_function.h"
#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/scalar_function.h"

using namespace neug::common;
using namespace neug::binder;

namespace neug {
namespace function {

template <typename T>
static void execContains(
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    const std::vector<SelectionVector*>& parameterSelVectors,
    ValueVector& result, SelectionVector* resultSelVector, void*) {
  auto& lists = *parameters[0];
  auto& needles = *parameters[1];
  auto* elements = ListVector::getDataVector(&lists);
  for (auto i = 0u; i < resultSelVector->getSelSize(); ++i) {
    auto resultPos = (*resultSelVector)[i];
    auto listPos = (*parameterSelVectors[0])[lists.state->isFlat() ? 0 : i];
    auto needlePos = (*parameterSelVectors[1])[needles.state->isFlat() ? 0 : i];
    result.setNull(resultPos, false);
    result.setValue<uint8_t>(resultPos, false);
    if (lists.isNull(listPos)) {
      result.setNull(resultPos, true);
      continue;
    }
    auto list = lists.getValue<list_entry_t>(listPos);
    if (list.size == 0) {
      continue;
    }
    if (needles.isNull(needlePos)) {
      result.setNull(resultPos, true);
      continue;
    }
    bool hasNull = false;
    bool found = false;
    auto& needle = needles.getValue<T>(needlePos);
    for (auto j = 0u; j < list.size; ++j) {
      auto pos = list.offset + j;
      if (elements->isNull(pos)) {
        hasNull = true;
        continue;
      }
      uint8_t equal = 0;
      Equals::operation(elements->getValue<T>(pos), needle, equal, elements,
                        &needles);
      if (equal) {
        found = true;
        break;
      }
    }
    result.setValue<uint8_t>(resultPos, found);
    result.setNull(resultPos, !found && hasNull);
  }
}

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  auto scalarFunction = input.definition->ptrCast<ScalarFunction>();
  // for list_contains(list, input), we expect input and list child have the
  // same type, if list is empty, we use in the input type. Otherwise, we use
  // list child type because casting list is more expensive.
  std::vector<DataType> paramTypes;
  DataType listType, childType;
  if (ExpressionUtil::isEmptyList(*input.arguments[0]) ||
      input.arguments[0]->getDataType().containsAny()) {
    childType = input.arguments[1]->getDataType().copy();
    listType = DataType::List(childType.copy());
  } else {
    listType = input.arguments[0]->getDataType().copy();
    childType = listType.id() == DataTypeId::kArray
                    ? ArrayType::GetChildType(listType).copy()
                    : ListType::GetChildType(listType).copy();
  }
  paramTypes.push_back(listType.copy());
  paramTypes.push_back(childType.copy());
  TypeUtils::visit(getPhysicalType(childType.id()),
                   [&scalarFunction]<typename T>(T) {
                     scalarFunction->execFunc = execContains<T>;
                   });
  return std::make_unique<FunctionBindData>(std::move(paramTypes),
                                            DataType(DataTypeId::kBoolean));
}

function_set ListContainsFunction::getFunctionSet() {
  function_set result;
  auto function = std::make_unique<ScalarFunction>(
      name, std::vector<DataTypeId>{DataTypeId::kList, DataTypeId::kUnknown},
      DataTypeId::kBoolean);
  function->bindFunc = bindFunc;
  result.push_back(std::move(function));
  return result;
}

}  // namespace function
}  // namespace neug
