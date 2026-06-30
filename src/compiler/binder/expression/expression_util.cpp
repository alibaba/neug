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

#include <algorithm>

#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/binder/expression/node_rel_expression.h"
#include "neug/compiler/binder/expression/parameter_expression.h"
#include "neug/compiler/common/types/value/nested.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;

namespace neug {
namespace binder {

expression_vector ExpressionUtil::getExpressionsWithDataType(
    const expression_vector& expressions, DataTypeId dataTypeID) {
  expression_vector result;
  for (auto& expression : expressions) {
    if (expression->dataType.id() == dataTypeID) {
      result.push_back(expression);
    }
  }
  return result;
}

uint32_t ExpressionUtil::find(const Expression* target,
                              const expression_vector& expressions) {
  for (auto i = 0u; i < expressions.size(); ++i) {
    if (target->getUniqueName() == expressions[i]->getUniqueName()) {
      return i;
    }
  }
  return INVALID_IDX;
}

std::string ExpressionUtil::toString(const expression_vector& expressions) {
  if (expressions.empty()) {
    return std::string{};
  }
  auto result = expressions[0]->toString();
  for (auto i = 1u; i < expressions.size(); ++i) {
    result += "," + expressions[i]->toString();
  }
  return result;
}

std::string ExpressionUtil::toStringOrdered(
    const expression_vector& expressions) {
  auto expressions_ = expressions;
  std::sort(expressions_.begin(), expressions_.end(),
            [](const std::shared_ptr<Expression>& a,
               const std::shared_ptr<Expression>& b) {
              return a->toString() < b->toString();
            });
  return toString(expressions_);
}

std::string ExpressionUtil::toString(
    const std::vector<expression_pair>& expressionPairs) {
  if (expressionPairs.empty()) {
    return std::string{};
  }
  auto result = toString(expressionPairs[0]);
  for (auto i = 1u; i < expressionPairs.size(); ++i) {
    result += "," + toString(expressionPairs[i]);
  }
  return result;
}

std::string ExpressionUtil::toString(const expression_pair& expressionPair) {
  return expressionPair.first->toString() + "=" +
         expressionPair.second->toString();
}

std::string ExpressionUtil::getUniqueName(
    const expression_vector& expressions) {
  if (expressions.empty()) {
    return std::string();
  }
  auto result = expressions[0]->getUniqueName();
  for (auto i = 1u; i < expressions.size(); ++i) {
    result += "," + expressions[i]->getUniqueName();
  }
  return result;
}

expression_vector ExpressionUtil::excludeExpression(
    const expression_vector& exprs, const Expression& exprToExclude) {
  expression_vector result;
  for (auto& expr : exprs) {
    if (*expr != exprToExclude) {
      result.push_back(expr);
    }
  }
  return result;
}

expression_vector ExpressionUtil::excludeExpressions(
    const expression_vector& expressions,
    const expression_vector& expressionsToExclude) {
  expression_set excludeSet;
  for (auto& expression : expressionsToExclude) {
    excludeSet.insert(expression);
  }
  expression_vector result;
  for (auto& expression : expressions) {
    if (!excludeSet.contains(expression)) {
      result.push_back(expression);
    }
  }
  return result;
}

std::vector<DataType> ExpressionUtil::getDataTypes(
    const expression_vector& expressions) {
  std::vector<DataType> result;
  result.reserve(expressions.size());
  for (auto& expression : expressions) {
    result.push_back(expression->getDataType().copy());
  }
  return result;
}

expression_vector ExpressionUtil::removeDuplication(
    const expression_vector& expressions) {
  expression_vector result;
  expression_set expressionSet;
  for (auto& expression : expressions) {
    if (expressionSet.contains(expression)) {
      continue;
    }
    result.push_back(expression);
    expressionSet.insert(expression);
  }
  return result;
}

bool ExpressionUtil::isEmptyPattern(const Expression& expression) {
  if (expression.expressionType != ExpressionType::PATTERN) {
    return false;
  }
  return expression.constCast<NodeOrRelExpression>().isEmpty();
}

bool ExpressionUtil::isNodePattern(const Expression& expression) {
  if (expression.expressionType != ExpressionType::PATTERN) {
    return false;
  }
  return expression.dataType.id() == DataTypeId::kVertex;
};

bool ExpressionUtil::isRelPattern(const Expression& expression) {
  if (expression.expressionType != ExpressionType::PATTERN) {
    return false;
  }
  return expression.dataType.id() == DataTypeId::kEdge;
}

bool ExpressionUtil::isRecursiveRelPattern(const Expression& expression) {
  if (expression.expressionType != ExpressionType::PATTERN) {
    return false;
  }
  return expression.dataType.id() == DataTypeId::kPath;
}

bool ExpressionUtil::isNullLiteral(const Expression& expression) {
  if (expression.expressionType != ExpressionType::LITERAL) {
    return false;
  }
  return expression.constCast<LiteralExpression>().getValue().isNull();
}

bool ExpressionUtil::isBoolLiteral(const Expression& expression) {
  if (expression.expressionType != ExpressionType::LITERAL) {
    return false;
  }
  return expression.dataType == DataType(DataTypeId::kBoolean);
}

bool ExpressionUtil::isFalseLiteral(const Expression& expression) {
  if (expression.expressionType != ExpressionType::LITERAL) {
    return false;
  }
  return expression.constCast<LiteralExpression>()
             .getValue()
             .getValue<bool>() == false;
}

bool ExpressionUtil::isEmptyList(const Expression& expression) {
  auto val = Value::createNullValue();
  switch (expression.expressionType) {
  case ExpressionType::LITERAL: {
    val = expression.constCast<LiteralExpression>().getValue();
  } break;
  case ExpressionType::PARAMETER: {
    val = expression.constCast<ParameterExpression>().getValue();
  } break;
  default:
    return false;
  }
  if (val.getDataType().id() != DataTypeId::kList) {
    return false;
  }
  return val.getChildrenSize() == 0;
}

void ExpressionUtil::validateExpressionType(const Expression& expr,
                                            ExpressionType expectedType) {
  if (expr.expressionType == expectedType) {
    return;
  }
  THROW_BINDER_EXCEPTION(
      stringFormat("{} has type {} but {} was expected.", expr.toString(),
                   ExpressionTypeUtil::toString(expr.expressionType),
                   ExpressionTypeUtil::toString(expectedType)));
}

void ExpressionUtil::validateExpressionType(
    const Expression& expr, std::vector<ExpressionType> expectedType) {
  if (std::find(expectedType.begin(), expectedType.end(),
                expr.expressionType) != expectedType.end()) {
    return;
  }
  std::string expectedTypesStr = "";
  std::for_each(expectedType.begin(), expectedType.end(),
                [&expectedTypesStr](ExpressionType type) {
                  expectedTypesStr +=
                      expectedTypesStr.empty()
                          ? ExpressionTypeUtil::toString(type)
                          : "," + ExpressionTypeUtil::toString(type);
                });
  THROW_BINDER_EXCEPTION(stringFormat(
      "{} has type {} but {} was expected.", expr.toString(),
      ExpressionTypeUtil::toString(expr.expressionType), expectedTypesStr));
}

void ExpressionUtil::validateDataType(const Expression& expr,
                                      const DataType& expectedType) {
  if (expr.getDataType() == expectedType) {
    return;
  }
  THROW_BINDER_EXCEPTION(
      stringFormat("{} has data type {} but {} was expected.", expr.toString(),
                   expr.getDataType().ToString(), expectedType.ToString()));
}

void ExpressionUtil::validateDataType(const Expression& expr,
                                      DataTypeId expectedTypeID) {
  if (expr.getDataType().id() == expectedTypeID) {
    return;
  }
  THROW_BINDER_EXCEPTION(
      stringFormat("{} has data type {} but {} was expected.", expr.toString(),
                   expr.getDataType().ToString(),
                   LogicalTypeUtils::toString(expectedTypeID)));
}

void ExpressionUtil::validateDataType(
    const Expression& expr, const std::vector<DataTypeId>& expectedTypeIDs) {
  auto targetsSet = std::unordered_set<DataTypeId>{expectedTypeIDs.begin(),
                                                   expectedTypeIDs.end()};
  if (targetsSet.contains(expr.getDataType().id())) {
    return;
  }
  THROW_BINDER_EXCEPTION(
      stringFormat("{} has data type {} but {} was expected.", expr.toString(),
                   expr.getDataType().ToString(),
                   LogicalTypeUtils::toString(expectedTypeIDs)));
}

template <>
uint64_t ExpressionUtil::getLiteralValue(const Expression& expr) {
  validateExpressionType(expr, ExpressionType::LITERAL);
  validateDataType(expr, DataType(DataTypeId::kUInt64));
  return expr.constCast<LiteralExpression>().getValue().getValue<uint64_t>();
}
template <>
int64_t ExpressionUtil::getLiteralValue(const Expression& expr) {
  validateExpressionType(expr, ExpressionType::LITERAL);
  validateDataType(expr, DataType(DataTypeId::kInt64));
  return expr.constCast<LiteralExpression>().getValue().getValue<int64_t>();
}
template <>
bool ExpressionUtil::getLiteralValue(const Expression& expr) {
  validateExpressionType(expr, ExpressionType::LITERAL);
  validateDataType(expr, DataType(DataTypeId::kBoolean));
  return expr.constCast<LiteralExpression>().getValue().getValue<bool>();
}
template <>
std::string ExpressionUtil::getLiteralValue(const Expression& expr) {
  validateExpressionType(expr, ExpressionType::LITERAL);
  validateDataType(expr, DataType::Varchar());
  return expr.constCast<LiteralExpression>().getValue().getValue<std::string>();
}
template <>
double ExpressionUtil::getLiteralValue(const Expression& expr) {
  validateExpressionType(expr, ExpressionType::LITERAL);
  validateDataType(expr, DataType(DataTypeId::kDouble));
  return expr.constCast<LiteralExpression>().getValue().getValue<double>();
}

// For primitive types, two types are compatible if they have the same id.
// For nested types, two types are compatible if they have the same id and their
// children are also compatible. E.g. [NULL] is compatible with [1,2] E.g. {a:
// NULL, b: NULL} is compatible with {a: [1,2], b: ['c']}
static bool compatible(const DataType& type, const DataType& target) {
  if (!type.isInternalType()) {
    return false;
  }
  if (type.id() == DataTypeId::kUnknown) {
    return true;
  }
  if (type.id() != target.id()) {
    return false;
  }
  switch (type.id()) {
  case DataTypeId::kList: {
    return compatible(ListType::GetChildType(type),
                      ListType::GetChildType(target));
  }
  case DataTypeId::kArray: {
    if (ArrayType::GetNumElements(type) != ArrayType::GetNumElements(target)) {
      return false;
    }
    return compatible(ArrayType::GetChildType(type),
                      ArrayType::GetChildType(target));
  }
  case DataTypeId::kStruct: {
    if (StructType::GetNumFields(type) != StructType::GetNumFields(target)) {
      return false;
    }
    for (auto i = 0u; i < StructType::GetNumFields(type); ++i) {
      if (!compatible(StructType::GetChildType(type, i),
                      StructType::GetChildType(target, i))) {
        return false;
      }
    }
    return true;
  }
  case DataTypeId::kMap:
  case DataTypeId::kVertex:
  case DataTypeId::kEdge:
  case DataTypeId::kPath:
    return false;
  default:
    return true;
  }
}

// Handle special cases where value can be compatible to a type. This happens
// when a value is a nested value but does not have any child. E.g. [] is
// compatible with [1,2]
static bool compatible(const Value& value, const DataType& targetType) {
  if (value.isNull()) {  // Value is null. We can safely change its type.
    return true;
  }
  if (value.getDataType().id() != targetType.id()) {
    return false;
  }
  switch (value.getDataType().id()) {
  case DataTypeId::kList: {
    if (!value.hasNoneNullChildren()) {  // Empty list free to change.
      return true;
    }
    for (auto i = 0u; i < NestedVal::getChildrenSize(&value); ++i) {
      if (!compatible(*NestedVal::getChildVal(&value, i),
                      ListType::GetChildType(targetType))) {
        return false;
      }
    }
    return true;
  }
  case DataTypeId::kArray: {
    if (NestedVal::getChildrenSize(&value) !=
        ArrayType::GetNumElements(targetType)) {
      return false;
    }
    if (!value.hasNoneNullChildren()) {  // Empty array free to change.
      return true;
    }
    for (auto i = 0u; i < NestedVal::getChildrenSize(&value); ++i) {
      if (!compatible(*NestedVal::getChildVal(&value, i),
                      ArrayType::GetChildType(targetType))) {
        return false;
      }
    }
    return true;
  }
  case DataTypeId::kMap: {
    if (!value.hasNoneNullChildren()) {  // Empty map free to change.
      return true;
    }
    const auto& keyType = MapType::GetKeyType(targetType);
    const auto& valType = MapType::GetValueType(targetType);
    for (auto i = 0u; i < NestedVal::getChildrenSize(&value); ++i) {
      auto childVal = NestedVal::getChildVal(&value, i);
      NEUG_ASSERT(NestedVal::getChildrenSize(childVal) == 2);
      auto key = NestedVal::getChildVal(childVal, 0);
      auto val = NestedVal::getChildVal(childVal, 1);
      if (!compatible(*key, keyType) || !compatible(*val, valType)) {
        return false;
      }
    }
    return true;
  }
  default:
    break;
  }
  return compatible(value.getDataType(), targetType);
}

bool ExpressionUtil::tryCombineDataType(const expression_vector& expressions,
                                        DataType& result) {
  std::vector<Value> secondaryValues;
  std::vector<DataType> primaryTypes;
  bool propKeyValues = false;
  if (expressions.size() == 2 &&
          expressions.at(0)->expressionType == ExpressionType::PROPERTY &&
          expressions.at(1)->expressionType == ExpressionType::LITERAL ||
      expressions.at(0)->expressionType == ExpressionType::LITERAL &&
          expressions.at(1)->expressionType == ExpressionType::PROPERTY) {
    propKeyValues = true;
  }
  for (auto& expr : expressions) {
    if (expr->expressionType != ExpressionType::LITERAL) {
      primaryTypes.push_back(expr->getDataType().copy());
      continue;
    }
    // if expressions are all property key values, i.e. {a.id = 10, a.age = 20},
    // then infer combine type from non literal expressions, the literal
    // expression type will be aligned with the non literal expression type.
    // i.e. a.length = 12345, the combined type will be int32 if length is
    // int32 in schema, even though the literal expression '12345' is int64,
    // which has a wider range.
    if (!propKeyValues) {
      auto literalExpr = expr->constPtrCast<LiteralExpression>();
      if (literalExpr->getValue().allowTypeChange()) {
        secondaryValues.push_back(literalExpr->getValue());
        continue;
      }
      primaryTypes.push_back(expr->getDataType().copy());
    }
  }
  if (!LogicalTypeUtils::tryGetMaxLogicalType(primaryTypes, result)) {
    return false;
  }
  if (!propKeyValues) {
    for (auto& value : secondaryValues) {
      if (compatible(value, result)) {
        continue;
      }
      if (!LogicalTypeUtils::tryGetMaxLogicalType(result, value.getDataType(),
                                                  result)) {
        return false;
      }
    }
  }
  return true;
}

bool ExpressionUtil::canCastStatically(const Expression& expr,
                                       const DataType& targetType) {
  switch (expr.expressionType) {
  case ExpressionType::LITERAL: {
    auto value = expr.constPtrCast<LiteralExpression>()->getValue();
    return compatible(value, targetType);
  }
  case ExpressionType::PARAMETER: {
    auto value = expr.constPtrCast<ParameterExpression>()->getValue();
    return compatible(value, targetType);
  }
  default:
    return compatible(expr.getDataType(), targetType);
  }
}

bool ExpressionUtil::canEvaluateAsLiteral(const Expression& expr) {
  return (expr.expressionType == ExpressionType::LITERAL ||
          (expr.expressionType == ExpressionType::PARAMETER &&
           expr.getDataType().id() != DataTypeId::kUnknown));
}

Value ExpressionUtil::evaluateAsLiteralValue(const Expression& expr) {
  NEUG_ASSERT(canEvaluateAsLiteral(expr));
  auto value = Value::createDefaultValue(expr.dataType);
  switch (expr.expressionType) {
  case ExpressionType::LITERAL: {
    value = expr.constCast<LiteralExpression>().getValue();
  } break;
  case ExpressionType::PARAMETER: {
    value = expr.constCast<ParameterExpression>().getValue();
  } break;
  default:
    NEUG_UNREACHABLE;
  }
  return value;
}

template <typename T>
T ExpressionUtil::evaluateLiteral(const Expression& expression,
                                  const common::DataType& type,
                                  validate_param_func<T> validateParamFunc) {
  if (!canEvaluateAsLiteral(expression)) {
    std::string errMsg;
    switch (expression.expressionType) {
    case common::ExpressionType::PARAMETER: {
      errMsg = "The query is a parameter expression. Please assign it a value.";
    } break;
    default: {
      errMsg = "The query must be a parameter/literal expression.";
    } break;
    }
    THROW_RUNTIME_ERROR(errMsg);
  }
  auto value = evaluateAsLiteralValue(expression);
  if (value.getDataType() != type) {
    THROW_RUNTIME_ERROR(
        common::stringFormat("Parameter: {} must be a {} literal.",
                             expression.getAlias(), type.ToString()));
  }
  T val = value.getValue<T>();
  if (validateParamFunc != nullptr) {
    validateParamFunc(val);
  }
  return val;
}

template NEUG_API std::string ExpressionUtil::evaluateLiteral<std::string>(
    const Expression& expression, const common::DataType& type,
    validate_param_func<std::string> validateParamFunc);

template NEUG_API double ExpressionUtil::evaluateLiteral<double>(
    const Expression& expression, const common::DataType& type,
    validate_param_func<double> validateParamFunc);

template NEUG_API int64_t ExpressionUtil::evaluateLiteral<int64_t>(
    const Expression& expression, const common::DataType& type,
    validate_param_func<int64_t> validateParamFunc);

template NEUG_API bool ExpressionUtil::evaluateLiteral<bool>(
    const Expression& expression, const common::DataType& type,
    validate_param_func<bool> validateParamFunc);

}  // namespace binder
}  // namespace neug
