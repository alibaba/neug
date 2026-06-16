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
#include "neug/compiler/binder/bound_table_scan_info.h"
#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression/literal_expression.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/function/built_in_function_utils.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/main/client_context.h"

using namespace neug::common;
using namespace neug::function;

namespace neug {
namespace binder {

BoundTableScanInfo Binder::bindTableFunc(
    const std::string& tableFuncName, const parser::ParsedExpression& expr,
    std::vector<parser::YieldVariable> yieldVariables) {
  auto entry = clientContext->getCatalog()->getFunctionEntry(
      clientContext->getTransaction(), tableFuncName,
      clientContext->useInternalCatalogEntry());
  expression_vector positionalParams;
  std::vector<DataType> positionalParamTypes;
  optional_params_t optionalParams;
  expression_vector optionalParamsLegacy;
  for (auto i = 0u; i < expr.getNumChildren(); i++) {
    auto& childExpr = *expr.getChild(i);
    auto param = expressionBinder.bindExpression(childExpr);
    if (!childExpr.hasAlias()) {
      ExpressionUtil::validateExpressionType(
          *param, {ExpressionType::LITERAL, ExpressionType::PARAMETER});
      positionalParams.push_back(param);
      positionalParamTypes.push_back(param->getDataType().copy());
    } else {
      ExpressionUtil::validateExpressionType(
          *param, {ExpressionType::LITERAL, ExpressionType::PARAMETER});
      if (param->expressionType == ExpressionType::LITERAL) {
        auto literalExpr = param->constPtrCast<LiteralExpression>();
        optionalParams.emplace(childExpr.getAlias(), literalExpr->getValue());
      }
      param->setAlias(expr.getChild(i)->getAlias());
      optionalParamsLegacy.push_back(param);
    }
  }
  auto func = BuiltInFunctionsUtils::matchFunction(
      tableFuncName, positionalParamTypes,
      entry->ptrCast<catalog::FunctionCatalogEntry>());
  auto callFunc = func->constPtrCast<NeugCallFunction>();
  std::vector<common::DataType> inputTypes;
  // For functions which don't have nested type parameters, we can simply use
  // the types declared in the function signature.
  for (auto i = 0u; i < callFunc->parameterTypeIDs.size(); i++) {
    inputTypes.push_back(common::DataType(callFunc->parameterTypeIDs[i]));
  }
  for (auto i = 0u; i < positionalParams.size(); ++i) {
    auto parameterTypeID = callFunc->parameterTypeIDs[i];
    if (positionalParams[i]->expressionType == ExpressionType::LITERAL &&
        parameterTypeID != DataTypeId::kUnknown) {
      positionalParams[i] = expressionBinder.implicitCastIfNecessary(
          positionalParams[i], inputTypes[i]);
      // A bare LITERAL already holds its final value — fold would only
      // re-materialize it through a ValueVector. We skip that round-trip
      // because the literal-fold path currently trips NEUG_UNREACHABLE in
      // DataType::getPhysicalType (types.cpp:990) for at least
      // JSON_SCAN's STRING parameter. When the cast above wrapped the
      // literal in a CAST function expression we still need to fold to
      // evaluate the cast.
      if (positionalParams[i]->expressionType != ExpressionType::LITERAL) {
        positionalParams[i] =
            expressionBinder.foldExpression(positionalParams[i]);
      }
    }
  }
  expression_vector outputColumns;
  for (auto& outputColumn : callFunc->outputColumns) {
    // add ouput columns to scope if exists
    outputColumns.push_back(
        createVariable(outputColumn.first, outputColumn.second));
  }
  auto bindData = std::make_unique<TableFuncBindData>(
      std::move(outputColumns), callFunc->outputColumns.size(),
      std::move(positionalParams));
  return BoundTableScanInfo{*callFunc, std::move(bindData)};
}

}  // namespace binder
}  // namespace neug
