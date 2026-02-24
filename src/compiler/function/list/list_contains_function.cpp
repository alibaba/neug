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

struct ListContains {
  template <typename T>
  static void operation(common::list_entry_t& list, T& element, uint8_t& result,
                        common::ValueVector& listVector,
                        common::ValueVector& elementVector,
                        common::ValueVector& resultVector) {
    int64_t pos = 0;
    ListPosition::operation(list, element, pos, listVector, elementVector,
                            resultVector);
    result = (pos != 0);
  }
};

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  auto scalarFunction = input.definition->ptrCast<ScalarFunction>();
  // for list_contains(list, input), we expect input and list child have the
  // same type, if list is empty, we use in the input type. Otherwise, we use
  // list child type because casting list is more expensive.
  std::vector<LogicalType> paramTypes;
  LogicalType listType, childType;
  if (ExpressionUtil::isEmptyList(*input.arguments[0])) {
    childType = input.arguments[1]->getDataType().copy();
    listType = LogicalType::LIST(childType.copy());
  } else {
    listType = input.arguments[0]->getDataType().copy();
    childType = ListType::getChildType(listType).copy();
  }
  paramTypes.push_back(listType.copy());
  paramTypes.push_back(childType.copy());
  TypeUtils::visit(
      childType.getPhysicalType(), [&scalarFunction]<typename T>(T) {
        scalarFunction->execFunc =
            ScalarFunction::BinaryExecListStructFunction<list_entry_t, T,
                                                         uint8_t, ListContains>;
      });
  return std::make_unique<FunctionBindData>(std::move(paramTypes),
                                            LogicalType::BOOL());
}

function_set ListContainsFunction::getFunctionSet() {
  function_set result;
  auto function = std::make_unique<ScalarFunction>(
      name, std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY},
      LogicalTypeID::BOOL);
  function->bindFunc = bindFunc;
  result.push_back(std::move(function));
  return result;
}

}  // namespace function
}  // namespace neug
