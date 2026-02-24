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
#include "neug/compiler/binder/expression/aggregate_function_expression.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/binder/expression_binder.h"
#include "neug/compiler/binder/expression_visitor.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/catalog/catalog_entry/rel_group_catalog_entry.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/function/built_in_function_utils.h"
#include "neug/compiler/function/cast/vector_cast_functions.h"
#include "neug/compiler/function/rewrite_function.h"
#include "neug/compiler/function/scalar_macro_function.h"
#include "neug/compiler/main/client_context.h"
#include "neug/compiler/parser/expression/parsed_expression_visitor.h"
#include "neug/compiler/parser/expression/parsed_function_expression.h"
#include "neug/utils/exception/exception.h"

using namespace neug::common;
using namespace neug::parser;
using namespace neug::function;
using namespace neug::catalog;

namespace neug {
namespace binder {

std::shared_ptr<Expression> ExpressionBinder::bindFunctionExpression(
    const ParsedExpression& expr) {
  auto funcExpr = expr.constPtrCast<ParsedFunctionExpression>();
  auto functionName = funcExpr->getNormalizedFunctionName();
  auto entry = context->getCatalog()->getFunctionEntry(
      context->getTransaction(), functionName);
  switch (entry->getType()) {
  case CatalogEntryType::SCALAR_FUNCTION_ENTRY:
    return bindScalarFunctionExpression(expr, functionName);
  case CatalogEntryType::REWRITE_FUNCTION_ENTRY:
    return bindRewriteFunctionExpression(expr);
  case CatalogEntryType::AGGREGATE_FUNCTION_ENTRY:
    return bindAggregateFunctionExpression(expr, functionName,
                                           funcExpr->getIsDistinct());
  case CatalogEntryType::SCALAR_MACRO_ENTRY:
    return bindMacroExpression(expr, functionName);
  default:
    THROW_BINDER_EXCEPTION(stringFormat(
        "{} is a {}. Scalar function, aggregate function or macro was "
        "expected. ",
        functionName, CatalogEntryTypeUtils::toString(entry->getType())));
  }
}

std::shared_ptr<Expression> ExpressionBinder::bindScalarFunctionExpression(
    const ParsedExpression& parsedExpression, const std::string& functionName) {
  expression_vector children;
  for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
    auto expr = bindExpression(*parsedExpression.getChild(i));
    if (parsedExpression.getChild(i)->hasAlias()) {
      expr->setAlias(parsedExpression.getChild(i)->getAlias());
    }
    children.push_back(expr);
  }
  return bindScalarFunctionExpression(
      children, functionName,
      parsedExpression.constCast<ParsedFunctionExpression>()
          .getOptionalArguments());
}

static std::vector<LogicalType> getTypes(const expression_vector& exprs) {
  std::vector<LogicalType> result;
  for (auto& expr : exprs) {
    result.push_back(expr->getDataType().copy());
  }
  return result;
}

std::shared_ptr<Expression> ExpressionBinder::bindScalarFunctionExpression(
    const expression_vector& children, const std::string& functionName,
    std::vector<std::string> optionalArguments) {
  auto catalog = context->getCatalog();
  auto transaction = context->getTransaction();
  auto childrenTypes = getTypes(children);

  auto entry = catalog->getFunctionEntry(transaction, functionName);

  auto function =
      BuiltInFunctionsUtils::matchFunction(
          functionName, childrenTypes, entry->ptrCast<FunctionCatalogEntry>())
          ->ptrCast<ScalarFunction>()
          ->copy();
  if (children.size() == 2 &&
      children[1]->expressionType == ExpressionType::LAMBDA) {
    if (!function->isListLambda) {
      THROW_BINDER_EXCEPTION(
          stringFormat("{} does not support lambda input.", functionName));
    }
    bindLambdaExpression(*children[0], *children[1]);
  }
  expression_vector childrenAfterCast;
  std::unique_ptr<function::FunctionBindData> bindData;
  auto bindInput = ScalarBindFuncInput{children, function.get(), context,
                                       std::move(optionalArguments)};
  if (functionName == CastAnyFunction::name) {
    bindData = function->bindFunc(bindInput);
    if (bindData == nullptr) {  // No need to cast.
      // TODO(Xiyang): We should return a deep copy otherwise the same
      // expression might appear in the final projection list repeatedly. E.g.
      // RETURN cast([NULL], "INT64[1][]"), cast([NULL], "INT64[1][][]")
      return children[0];
    }
    // run cast function in compiling phase for literal expression
    if (children[0]->expressionType == ExpressionType::LITERAL &&
        function->execFunc) {
      auto child = children[0];
      if (child->getDataType().getLogicalTypeID() == LogicalTypeID::ANY) {
        child = implicitCastIfNecessary(child, LogicalType::STRING());
      }
      childrenAfterCast.push_back(std::move(child));
    } else {
      // convert to extension function
      for (auto child : children) {
        if (child->getDataType().getLogicalTypeID() == LogicalTypeID::ANY) {
          child = implicitCastIfNecessary(child, LogicalType::STRING());
        }
        childrenAfterCast.push_back(std::move(child));
      }
    }
  } else {
    if (function->bindFunc) {
      bindData = function->bindFunc(bindInput);
    } else {
      bindData = std::make_unique<FunctionBindData>(
          LogicalType(function->returnTypeID));
    }
    if (bindData == nullptr) {
      THROW_BINDER_EXCEPTION("Failed to bind function " + functionName +
                             ". Invalid input types: {}." +
                             common::LogicalTypeUtils::toString(childrenTypes));
    }
    if (!bindData->paramTypes.empty()) {
      for (auto i = 0u; i < children.size(); ++i) {
        childrenAfterCast.push_back(
            implicitCastIfNecessary(children[i], bindData->paramTypes[i]));
      }
    } else {
      for (auto i = 0u; i < children.size(); ++i) {
        auto id = function->isVarLength ? function->parameterTypeIDs[0]
                                        : function->parameterTypeIDs[i];
        auto type = LogicalType(id);
        childrenAfterCast.push_back(implicitCastIfNecessary(children[i], type));
      }
    }
  }
  auto uniqueExpressionName = ScalarFunctionExpression::getUniqueName(
      function->name, childrenAfterCast);
  return std::make_shared<ScalarFunctionExpression>(
      ExpressionType::FUNCTION, std::move(function), std::move(bindData),
      std::move(childrenAfterCast), uniqueExpressionName);
}

std::shared_ptr<Expression> ExpressionBinder::bindRewriteFunctionExpression(
    const parser::ParsedExpression& expr) {
  auto& funcExpr = expr.constCast<ParsedFunctionExpression>();
  expression_vector children;
  for (auto i = 0u; i < expr.getNumChildren(); ++i) {
    children.push_back(bindExpression(*expr.getChild(i)));
  }
  auto childrenTypes = getTypes(children);
  auto functionName = funcExpr.getNormalizedFunctionName();
  auto entry = context->getCatalog()->getFunctionEntry(
      context->getTransaction(), functionName);
  auto match = BuiltInFunctionsUtils::matchFunction(
      functionName, childrenTypes, entry->ptrCast<FunctionCatalogEntry>());
  auto function = match->constPtrCast<RewriteFunction>();
  NEUG_ASSERT(function->rewriteFunc != nullptr);
  auto input = RewriteFunctionBindInput(context, this, children);
  return function->rewriteFunc(input);
}

std::shared_ptr<Expression> ExpressionBinder::bindAggregateFunctionExpression(
    const ParsedExpression& parsedExpression, const std::string& functionName,
    bool isDistinct) {
  std::vector<LogicalType> childrenTypes;
  expression_vector children;
  for (auto i = 0u; i < parsedExpression.getNumChildren(); ++i) {
    auto child = bindExpression(*parsedExpression.getChild(i));
    childrenTypes.push_back(child->dataType.copy());
    children.push_back(std::move(child));
  }
  auto entry = context->getCatalog()->getFunctionEntry(
      context->getTransaction(), functionName);
  auto function = BuiltInFunctionsUtils::matchAggregateFunction(
                      functionName, childrenTypes, isDistinct,
                      entry->ptrCast<FunctionCatalogEntry>())
                      ->copy();
  if (functionName == CollectFunction::name && parsedExpression.hasAlias() &&
      children[0]->getDataType().getLogicalTypeID() == LogicalTypeID::NODE) {
    auto& node = children[0]->constCast<NodeExpression>();
    binder->scope.memorizeTableEntries(parsedExpression.getAlias(),
                                       node.getEntries());
  }
  auto uniqueExpressionName = AggregateFunctionExpression::getUniqueName(
      function.name, children, function.isDistinct);
  if (children.empty()) {
    uniqueExpressionName =
        binder->getUniqueExpressionName(uniqueExpressionName);
  }
  std::unique_ptr<FunctionBindData> bindData;
  if (function.bindFunc) {
    auto bindInput =
        ScalarBindFuncInput{children, &function, context,
                            std::vector<std::string>{} /* optionalParams */};
    bindData = function.bindFunc(bindInput);
  } else {
    bindData = std::make_unique<function::FunctionBindData>(
        LogicalType(function.returnTypeID));
  }
  return std::make_shared<AggregateFunctionExpression>(
      std::move(function), std::move(bindData), std::move(children),
      uniqueExpressionName);
}

std::shared_ptr<Expression> ExpressionBinder::bindMacroExpression(
    const ParsedExpression& parsedExpression, const std::string& macroName) {
  auto scalarMacroFunction = context->getCatalog()->getScalarMacroFunction(
      context->getTransaction(), macroName);
  auto macroExpr = scalarMacroFunction->expression->copy();
  auto parameterVals = scalarMacroFunction->getDefaultParameterVals();
  auto& parsedFuncExpr = parsedExpression.constCast<ParsedFunctionExpression>();
  auto positionalArgs = scalarMacroFunction->getPositionalArgs();
  if (parsedFuncExpr.getNumChildren() > scalarMacroFunction->getNumArgs() ||
      parsedFuncExpr.getNumChildren() < positionalArgs.size()) {
    THROW_BINDER_EXCEPTION("Invalid number of arguments for macro " +
                           macroName + ".");
  }
  // Bind positional arguments.
  for (auto i = 0u; i < positionalArgs.size(); i++) {
    parameterVals[positionalArgs[i]] = parsedFuncExpr.getChild(i);
  }
  // Bind arguments with default values.
  for (auto i = positionalArgs.size(); i < parsedFuncExpr.getNumChildren();
       i++) {
    auto parameterName =
        scalarMacroFunction->getDefaultParameterName(i - positionalArgs.size());
    parameterVals[parameterName] = parsedFuncExpr.getChild(i);
  }
  auto replacer = MacroParameterReplacer(parameterVals);
  return bindExpression(*replacer.replace(std::move(macroExpr)));
}

}  // namespace binder
}  // namespace neug
