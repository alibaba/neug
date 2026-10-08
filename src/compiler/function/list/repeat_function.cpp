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

#include "neug/compiler/function/list/vector_list_functions.h"

#include <optional>

#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/binder/expression/parameter_expression.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/function/list/functions/list_function_utils.h"
#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;

namespace neug {
namespace function {
namespace {

using binder::Expression;
using binder::LiteralExpression;
using binder::ParameterExpression;
using binder::ScalarFunctionExpression;

bool isInteger(const DataTypeId type) {
  switch (type) {
  case DataTypeId::kInt8:
  case DataTypeId::kInt16:
  case DataTypeId::kInt32:
  case DataTypeId::kInt64:
  case DataTypeId::kUInt8:
  case DataTypeId::kUInt16:
  case DataTypeId::kUInt32:
  case DataTypeId::kUInt64:
    return true;
  default:
    return false;
  }
}

std::optional<uint64_t> getStaticCount(const Expression& expression) {
  std::optional<compiler_impl::Value> value;
  switch (expression.expressionType) {
  case ExpressionType::LITERAL:
    value = expression.constCast<LiteralExpression>().getValue();
    break;
  case ExpressionType::PARAMETER:
    value = expression.constCast<ParameterExpression>().getValue();
    break;
  default:
    return std::nullopt;
  }
  if (value->isNull()) {
    THROW_BINDER_EXCEPTION("REPEAT count cannot be NULL.");
  }
  switch (value->getDataType().id()) {
  case DataTypeId::kInt8: {
    const auto count = value->getValue<int8_t>();
    if (count < 0) {
      THROW_BINDER_EXCEPTION("REPEAT count cannot be negative.");
    }
    return static_cast<uint64_t>(count);
  }
  case DataTypeId::kInt16: {
    const auto count = value->getValue<int16_t>();
    if (count < 0) {
      THROW_BINDER_EXCEPTION("REPEAT count cannot be negative.");
    }
    return static_cast<uint64_t>(count);
  }
  case DataTypeId::kInt32: {
    const auto count = value->getValue<int32_t>();
    if (count < 0) {
      THROW_BINDER_EXCEPTION("REPEAT count cannot be negative.");
    }
    return static_cast<uint64_t>(count);
  }
  case DataTypeId::kInt64: {
    const auto count = value->getValue<int64_t>();
    if (count < 0) {
      THROW_BINDER_EXCEPTION("REPEAT count cannot be negative.");
    }
    return static_cast<uint64_t>(count);
  }
  case DataTypeId::kUInt8:
    return value->getValue<uint8_t>();
  case DataTypeId::kUInt16:
    return value->getValue<uint16_t>();
  case DataTypeId::kUInt32:
    return value->getValue<uint32_t>();
  case DataTypeId::kUInt64:
    return value->getValue<uint64_t>();
  default:
    return std::nullopt;
  }
}

std::optional<uint64_t> getStaticCollectionSize(const Expression& expression) {
  const auto& type = expression.getDataType();
  if (type.id() == DataTypeId::kArray) {
    return ArrayType::GetNumElements(type);
  }
  if (expression.expressionType == ExpressionType::LITERAL) {
    const auto value = expression.constCast<LiteralExpression>().getValue();
    if (!value.isNull() && ListFunctionUtils::isListLike(value.getDataType())) {
      return value.getChildrenSize();
    }
    return std::nullopt;
  }
  if (expression.expressionType == ExpressionType::PARAMETER) {
    const auto value = expression.constCast<ParameterExpression>().getValue();
    if (!value.isNull() && ListFunctionUtils::isListLike(value.getDataType())) {
      return value.getChildrenSize();
    }
    return std::nullopt;
  }
  if (expression.expressionType != ExpressionType::FUNCTION) {
    return std::nullopt;
  }
  const auto& functionExpression =
      expression.constCast<ScalarFunctionExpression>();
  if (functionExpression.getFunction().name == ListCreationFunction::name) {
    return functionExpression.getNumChildren();
  }
  return std::nullopt;
}

uint64_t getRepeatCount(const Value& value) {
  if (value.IsNull()) {
    THROW_RUNTIME_ERROR("REPEAT count cannot be NULL.");
  }
  if (value.type().id() != DataTypeId::kInt64) {
    THROW_RUNTIME_ERROR("REPEAT count must be an integer.");
  }
  const auto count = value.GetValue<int64_t>();
  if (count < 0) {
    THROW_RUNTIME_ERROR("REPEAT count cannot be negative.");
  }
  return static_cast<uint64_t>(count);
}

std::unique_ptr<FunctionBindData> bindRepeat(const ScalarBindFuncInput& input) {
  if (input.arguments.size() != 2) {
    THROW_BINDER_EXCEPTION("REPEAT expects exactly 2 arguments.");
  }
  const auto& inputType = input.arguments[0]->getDataType();
  if (!ListFunctionUtils::isListLike(inputType)) {
    THROW_BINDER_EXCEPTION(
        "REPEAT expects its first argument to be LIST or ARRAY, got " +
        inputType.ToString() + ".");
  }
  const auto& countType = input.arguments[1]->getDataType();
  if (!isInteger(countType.id())) {
    THROW_BINDER_EXCEPTION(
        "REPEAT expects its second argument to be an integer, got " +
        countType.ToString() + ".");
  }
  const auto count = getStaticCount(*input.arguments[1]);
  const auto unitSize = getStaticCollectionSize(*input.arguments[0]);
  if (count.has_value() && unitSize.has_value() && *count != 0 &&
      *unitSize > MAX_LIST_ELEMENTS / *count) {
    THROW_BINDER_EXCEPTION(
        "REPEAT result length exceeds maximum supported length of " +
        std::to_string(MAX_LIST_ELEMENTS) + ".");
  }
  std::vector<DataType> paramTypes;
  paramTypes.push_back(inputType.copy());
  // Normalize all accepted integer widths to INT64 so the engine-side eval
  // path has one stable representation for the repeat count.
  paramTypes.emplace_back(DataTypeId::kInt64);
  auto resultType =
      DataType::List(ListFunctionUtils::getElementType(inputType).copy());
  return std::make_unique<FunctionBindData>(std::move(paramTypes),
                                            std::move(resultType));
}

Value repeatValues(const std::vector<Value>& args) {
  if (args.size() != 2) {
    THROW_RUNTIME_ERROR("REPEAT expects exactly 2 arguments.");
  }
  if (!ListFunctionUtils::isListLike(args[0].type())) {
    THROW_RUNTIME_ERROR(
        "REPEAT expects its first argument to be LIST or ARRAY.");
  }
  const auto& elementType = ListFunctionUtils::getElementType(args[0].type());
  if (args[0].IsNull()) {
    return Value(DataType::List(elementType.copy()));
  }
  const auto count = getRepeatCount(args[1]);
  const auto& input = ListFunctionUtils::getChildren(args[0]);
  if (count != 0 && input.size() > MAX_LIST_ELEMENTS / count) {
    THROW_RUNTIME_ERROR(
        "REPEAT result length exceeds maximum supported length of " +
        std::to_string(MAX_LIST_ELEMENTS) + ".");
  }
  if (count == 0 || input.empty()) {
    return Value::LIST(elementType, {});
  }
  const auto resultSize = input.size() * count;
  std::vector<Value> children;
  children.reserve(resultSize);
  for (uint64_t i = 0; i < count; ++i) {
    children.insert(children.end(), input.begin(), input.end());
  }
  return Value::LIST(elementType, std::move(children));
}

}  // namespace

std::optional<uint64_t> RepeatFunction::tryGetResultSize(
    const Expression& expression) {
  if (expression.expressionType != ExpressionType::FUNCTION) {
    return std::nullopt;
  }
  const auto& functionExpression =
      expression.constCast<ScalarFunctionExpression>();
  if (functionExpression.getFunction().name != name ||
      functionExpression.getNumChildren() != 2) {
    return std::nullopt;
  }
  const auto unitSize =
      getStaticCollectionSize(*functionExpression.getChild(0));
  const auto count = getStaticCount(*functionExpression.getChild(1));
  if (!unitSize.has_value() || !count.has_value()) {
    return std::nullopt;
  }
  if (*count != 0 && *unitSize > MAX_LIST_ELEMENTS / *count) {
    return std::nullopt;
  }
  return *unitSize * *count;
}

function_set RepeatFunction::getFunctionSet() {
  function_set result;
  auto function = std::make_unique<NeugScalarFunction>(
      name, std::vector<DataTypeId>{DataTypeId::kUnknown, DataTypeId::kUnknown},
      DataTypeId::kList, repeatValues);
  function->bindFunc = bindRepeat;
  result.push_back(std::move(function));
  return result;
}

}  // namespace function
}  // namespace neug
