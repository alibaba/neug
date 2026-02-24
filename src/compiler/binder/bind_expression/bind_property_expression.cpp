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

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression/node_rel_expression.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/binder/expression_binder.h"
#include "neug/compiler/common/cast.h"
#include "neug/compiler/common/string_format.h"
#include "neug/compiler/function/struct/vector_struct_functions.h"
#include "neug/compiler/parser/expression/parsed_property_expression.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::parser;
using namespace neug::catalog;

namespace neug {
namespace binder {

static bool isNodeOrRelPattern(const Expression& expression) {
  return ExpressionUtil::isNodePattern(expression) ||
         ExpressionUtil::isRelPattern(expression);
}

static bool isStructPattern(const Expression& expression) {
  auto logicalTypeID = expression.getDataType().getLogicalTypeID();
  return logicalTypeID == LogicalTypeID::NODE ||
         logicalTypeID == LogicalTypeID::REL ||
         logicalTypeID == LogicalTypeID::STRUCT;
}

expression_vector ExpressionBinder::bindPropertyStarExpression(
    const parser::ParsedExpression& parsedExpression) {
  auto child = bindExpression(*parsedExpression.getChild(0));
  if (isNodeOrRelPattern(*child)) {
    return bindNodeOrRelPropertyStarExpression(*child);
  } else if (isStructPattern(*child)) {
    return bindStructPropertyStarExpression(child);
  } else {
    THROW_BINDER_EXCEPTION(
        stringFormat("Cannot bind property for expression {} with type {}.",
                     child->toString(),
                     ExpressionTypeUtil::toString(child->expressionType)));
  }
}

expression_vector ExpressionBinder::bindNodeOrRelPropertyStarExpression(
    const Expression& child) {
  expression_vector result;
  auto& nodeOrRel = child.constCast<NodeOrRelExpression>();
  for (auto& expression : nodeOrRel.getPropertyExprsRef()) {
    auto propertyExpression = expression->ptrCast<PropertyExpression>();
    if (Binder::reservedInPropertyLookup(
            propertyExpression->getPropertyName())) {
      continue;
    }
    result.push_back(expression->copy());
  }
  return result;
}

expression_vector ExpressionBinder::bindStructPropertyStarExpression(
    const std::shared_ptr<Expression>& child) {
  expression_vector result;
  const auto& childType = child->getDataType();
  for (auto& field : neug::common::StructType::getFields(childType)) {
    result.push_back(bindStructPropertyExpression(child, field.getName()));
  }
  return result;
}

std::shared_ptr<Expression> ExpressionBinder::bindPropertyExpression(
    const ParsedExpression& parsedExpression) {
  auto& propertyExpression =
      parsedExpression.constCast<ParsedPropertyExpression>();
  if (propertyExpression.isStar()) {
    THROW_BINDER_EXCEPTION(
        stringFormat("Cannot bind {} as a single property expression.",
                     parsedExpression.toString()));
  }
  auto propertyName = propertyExpression.getPropertyName();
  auto child = bindExpression(*parsedExpression.getChild(0));
  ExpressionUtil::validateDataType(
      *child,
      std::vector<LogicalTypeID>{LogicalTypeID::NODE, LogicalTypeID::REL,
                                 LogicalTypeID::STRUCT, LogicalTypeID::ANY});
  // Neug can support node or rel pattern as order key directly, we don't
  // need to convert it to struct property expression.

  // if (config.bindOrderByAfterAggregate) {
  //   // See the declaration of this field for more information.
  //   return bindStructPropertyExpression(child, propertyName);
  // }
  if (isNodeOrRelPattern(*child)) {
    if (Binder::reservedInPropertyLookup(propertyName)) {
      // Note we don't expose direct access to internal properties in case user
      // tries to modify them. However, we can expose indirect read-only access
      // through function e.g. ID().
      THROW_BINDER_EXCEPTION(
          propertyName +
          " is reserved for system usage. External access is not allowed.");
    }
    return bindNodeOrRelPropertyExpression(*child, propertyName);
  } else if (isStructPattern(*child)) {
    return bindStructPropertyExpression(child, propertyName);
  } else if (child->getDataType().getLogicalTypeID() == LogicalTypeID::ANY) {
    return createVariableExpression(LogicalType::ANY(),
                                    binder->getUniqueExpressionName(""));
  } else {
    THROW_BINDER_EXCEPTION(
        stringFormat("Cannot bind property for expression {} with type {}.",
                     child->toString(),
                     ExpressionTypeUtil::toString(child->expressionType)));
  }
}

std::shared_ptr<Expression> ExpressionBinder::bindNodeOrRelPropertyExpression(
    const Expression& child, const std::string& propertyName) {
  auto& nodeOrRel = child.constCast<NodeOrRelExpression>();
  // TODO(Xiyang): we should be able to remove l97-l100 after removing
  // propertyDataExprs from node & rel expression.
  if (propertyName == InternalKeyword::ID &&
      child.dataType.getLogicalTypeID() == common::LogicalTypeID::NODE) {
    auto& node = neug_dynamic_cast<const NodeExpression&>(child);
    return node.getInternalID();
  }
  if (!nodeOrRel.hasPropertyExpression(propertyName)) {
    THROW_PROPERTY_NOT_FOUND("Cannot find property " + propertyName + " for " +
                             child.toString() + ".");
  }
  return nodeOrRel.getPropertyExpression(propertyName);
}

std::shared_ptr<Expression> ExpressionBinder::bindStructPropertyExpression(
    std::shared_ptr<Expression> child, const std::string& propertyName) {
  auto children = expression_vector{std::move(child),
                                    createLiteralExpression(propertyName)};
  return bindScalarFunctionExpression(children,
                                      function::StructExtractFunctions::name);
}

}  // namespace binder
}  // namespace neug
