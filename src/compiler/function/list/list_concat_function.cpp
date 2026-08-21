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

#include "neug/compiler/function/list/functions/list_concat_function.h"

#include "neug/common/types/value.h"
#include "neug/compiler/function/list/functions/list_function_utils.h"
#include "neug/compiler/function/list/vector_list_functions.h"
#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/exception/message.h"

using namespace neug::common;

namespace neug {
namespace function {

namespace {

std::unique_ptr<FunctionBindData> bindConcat(const ScalarBindFuncInput& input) {
  if (input.arguments.size() != 2) {
    THROW_BINDER_EXCEPTION("LIST_CONCAT expects exactly 2 arguments.");
  }
  const auto& leftType = input.arguments[0]->getDataType();
  const auto& rightType = input.arguments[1]->getDataType();
  if (!ListFunctionUtils::isListLike(leftType) ||
      !ListFunctionUtils::isListLike(rightType)) {
    THROW_BINDER_EXCEPTION("LIST_CONCAT expects LIST or ARRAY arguments, got " +
                           leftType.ToString() + " and " +
                           rightType.ToString() + ".");
  }
  DataType elementType;
  if (!LogicalTypeUtils::tryGetMaxLogicalType(
          ListFunctionUtils::getElementType(leftType),
          ListFunctionUtils::getElementType(rightType), elementType)) {
    THROW_BINDER_EXCEPTION(
        "LIST_CONCAT cannot find a common element type for " +
        ListFunctionUtils::getElementType(leftType).ToString() + " and " +
        ListFunctionUtils::getElementType(rightType).ToString() + ".");
  }
  auto resultType = DataType::List(elementType.copy());
  std::vector<DataType> paramTypes;
  paramTypes.push_back(
      ListFunctionUtils::getCoercedInputType(leftType, elementType));
  paramTypes.push_back(
      ListFunctionUtils::getCoercedInputType(rightType, elementType));
  return std::make_unique<FunctionBindData>(std::move(paramTypes),
                                            std::move(resultType));
}

Value concatValues(const std::vector<Value>& args) {
  if (args.size() != 2) {
    THROW_RUNTIME_ERROR("LIST_CONCAT expects exactly 2 arguments.");
  }
  if (args[0].IsNull() || args[1].IsNull()) {
    if (ListFunctionUtils::isListLike(args[0].type())) {
      return Value(DataType::List(
          ListFunctionUtils::getElementType(args[0].type()).copy()));
    }
    if (ListFunctionUtils::isListLike(args[1].type())) {
      return Value(DataType::List(
          ListFunctionUtils::getElementType(args[1].type()).copy()));
    }
    return Value(DataType::List(DataType(DataTypeId::kUnknown)));
  }
  const auto& left = ListFunctionUtils::getChildren(args[0]);
  const auto& right = ListFunctionUtils::getChildren(args[1]);
  std::vector<Value> children;
  children.reserve(left.size() + right.size());
  children.insert(children.end(), left.begin(), left.end());
  children.insert(children.end(), right.begin(), right.end());
  const auto& leftElementType =
      ListFunctionUtils::getElementType(args[0].type());
  const auto& rightElementType =
      ListFunctionUtils::getElementType(args[1].type());
  const auto& resultElementType = leftElementType.id() == DataTypeId::kUnknown
                                      ? rightElementType
                                      : leftElementType;
  return Value::LIST(resultElementType, std::move(children));
}

}  // namespace

void ListConcat::operation(common::list_entry_t& left,
                           common::list_entry_t& right,
                           common::list_entry_t& result,
                           common::ValueVector& leftVector,
                           common::ValueVector& rightVector,
                           common::ValueVector& resultVector) {
  result = common::ListVector::addList(&resultVector, left.size + right.size);
  auto resultDataVector = common::ListVector::getDataVector(&resultVector);
  auto resultPos = result.offset;
  auto leftDataVector = common::ListVector::getDataVector(&leftVector);
  auto leftPos = left.offset;
  for (auto i = 0u; i < left.size; i++) {
    resultDataVector->copyFromVectorData(resultPos++, leftDataVector,
                                         leftPos++);
  }
  auto rightDataVector = common::ListVector::getDataVector(&rightVector);
  auto rightPos = right.offset;
  for (auto i = 0u; i < right.size; i++) {
    resultDataVector->copyFromVectorData(resultPos++, rightDataVector,
                                         rightPos++);
  }
}

function_set ListConcatFunction::getFunctionSet() {
  function_set result;
  auto function = std::make_unique<NeugScalarFunction>(
      name, std::vector<DataTypeId>{DataTypeId::kUnknown, DataTypeId::kUnknown},
      DataTypeId::kList, concatValues);
  function->bindFunc = bindConcat;
  result.push_back(std::move(function));
  return result;
}

}  // namespace function
}  // namespace neug
