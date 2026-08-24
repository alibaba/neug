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

#include "neug/compiler/function/list/functions/list_append_function.h"

#include "neug/compiler/function/list/functions/list_function_utils.h"
#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;

namespace neug {
namespace function {

namespace {

std::unique_ptr<FunctionBindData> bindAppend(const ScalarBindFuncInput& input) {
  if (input.arguments.size() != 2) {
    THROW_BINDER_EXCEPTION("LIST_APPEND expects exactly 2 arguments.");
  }
  const auto& listType = input.arguments[0]->getDataType();
  if (listType.id() != DataTypeId::kList &&
      listType.id() != DataTypeId::kArray) {
    THROW_BINDER_EXCEPTION(
        "LIST_APPEND expects its first argument to be LIST or ARRAY, got " +
        listType.ToString() + ".");
  }
  const auto& inputElementType = ListFunctionUtils::getElementType(listType);
  const auto& appendType = input.arguments[1]->getDataType();
  DataType elementType;
  if (!LogicalTypeUtils::tryGetMaxLogicalType(inputElementType, appendType,
                                              elementType)) {
    THROW_BINDER_EXCEPTION(
        "LIST_APPEND cannot find a common element type for " +
        inputElementType.ToString() + " and " + appendType.ToString() + ".");
  }
  auto resultType = DataType::List(elementType.copy());
  std::vector<DataType> paramTypes;
  paramTypes.push_back(
      ListFunctionUtils::getCoercedInputType(listType, elementType));
  paramTypes.push_back(elementType.copy());
  return std::make_unique<FunctionBindData>(std::move(paramTypes),
                                            std::move(resultType));
}

}  // namespace

Value ListAppend::operation(const std::vector<Value>& args) {
  if (args.size() != 2) {
    THROW_RUNTIME_ERROR("LIST_APPEND expects exactly 2 arguments.");
  }
  if (args[0].IsNull()) {
    return Value(DataType::List(args[1].type().copy()));
  }
  const auto& input = ListFunctionUtils::getChildren(args[0]);
  std::vector<Value> children;
  children.reserve(input.size() + 1);
  children.insert(children.end(), input.begin(), input.end());
  children.push_back(args[1]);
  const auto& inputElementType =
      ListFunctionUtils::getElementType(args[0].type());
  const auto& resultElementType = inputElementType.id() == DataTypeId::kUnknown
                                      ? args[1].type()
                                      : inputElementType;
  return Value::LIST(resultElementType, std::move(children));
}

function_set ListAppendFunction::getFunctionSet() {
  function_set result;
  auto function = std::make_unique<NeugScalarFunction>(
      name, std::vector<DataTypeId>{DataTypeId::kUnknown, DataTypeId::kUnknown},
      DataTypeId::kList, ListAppend::operation);
  function->bindFunc = bindAppend;
  result.push_back(std::move(function));
  return result;
}

}  // namespace function
}  // namespace neug
