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

#include <cstdint>
#include <memory>
#include "neug/compiler/binder/bound_statement_rewriter.h"
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression/scalar_function_expression.h"
#include "neug/compiler/catalog/catalog.h"
#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/compiler/common/constants.h"
#include "neug/compiler/common/copier_config/csv_reader_config.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/built_in_function_utils.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/function/schema/vector_node_rel_functions.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/bind_input.h"
#include "neug/compiler/function/table/scan_file_function.h"
#include "neug/compiler/function/table/table_function.h"
#include "neug/utils/exception/exception.h"

using namespace neug::catalog;
using namespace neug::common;
using namespace neug::function;
using namespace neug::parser;

namespace neug {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bind(const Statement& statement) {
  std::unique_ptr<BoundStatement> boundStatement;
  switch (statement.getStatementType()) {
  case StatementType::CREATE_TABLE: {
    boundStatement = bindCreateTable(statement);
  } break;
  case StatementType::CREATE_TYPE: {
    boundStatement = bindCreateType(statement);
  } break;
  case StatementType::CREATE_SEQUENCE: {
    boundStatement = bindCreateSequence(statement);
  } break;
  case StatementType::COPY_FROM: {
    boundStatement = bindCopyFromClause(statement);
  } break;
  case StatementType::COPY_TO: {
    boundStatement = bindCopyToClause(statement);
  } break;
  case StatementType::DROP: {
    boundStatement = bindDrop(statement);
  } break;
  case StatementType::ALTER: {
    boundStatement = bindAlter(statement);
  } break;
  case StatementType::QUERY: {
    boundStatement = bindQuery(statement);
  } break;
  case StatementType::STANDALONE_CALL: {
    boundStatement = bindStandaloneCall(statement);
  } break;
  case StatementType::STANDALONE_CALL_FUNCTION: {
    boundStatement = bindStandaloneCallFunction(statement);
  } break;
  case StatementType::EXPLAIN: {
    boundStatement = bindExplain(statement);
  } break;
  case StatementType::CREATE_MACRO: {
    boundStatement = bindCreateMacro(statement);
  } break;
  case StatementType::TRANSACTION: {
    boundStatement = bindTransaction(statement);
  } break;
  case StatementType::EXTENSION: {
    boundStatement = bindExtension(statement);
  } break;
  case StatementType::EXPORT_DATABASE: {
    boundStatement = bindExportDatabaseClause(statement);
  } break;
  case StatementType::IMPORT_DATABASE: {
    boundStatement = bindImportDatabaseClause(statement);
  } break;
  case StatementType::ATTACH_DATABASE: {
    boundStatement = bindAttachDatabase(statement);
  } break;
  case StatementType::DETACH_DATABASE: {
    boundStatement = bindDetachDatabase(statement);
  } break;
  case StatementType::USE_DATABASE: {
    boundStatement = bindUseDatabase(statement);
  } break;
  default: {
    NEUG_UNREACHABLE;
  }
  }
  BoundStatementRewriter::rewrite(*boundStatement, *clientContext);
  return boundStatement;
}

std::shared_ptr<Expression> Binder::bindWhereExpression(
    const ParsedExpression& parsedExpression) {
  auto whereExpression = expressionBinder.bindExpression(parsedExpression);
  expressionBinder.implicitCastIfNecessary(whereExpression,
                                           LogicalType::BOOL());
  return whereExpression;
}

std::shared_ptr<Expression> Binder::createVariable(std::string_view name,
                                                   LogicalTypeID typeID) {
  return createVariable(std::string(name), LogicalType{typeID});
}

std::shared_ptr<Expression> Binder::createVariable(
    const std::string& name, LogicalTypeID logicalTypeID) {
  return createVariable(name, LogicalType{logicalTypeID});
}

std::shared_ptr<Expression> Binder::createVariable(
    const std::string& name, const LogicalType& dataType) {
  if (scope.contains(name)) {
    return scope.getExpression(name);
  }
  auto expression =
      expressionBinder.createVariableExpression(dataType.copy(), name);
  expression->setAlias(name);
  addToScope(name, expression);
  return expression;
}

std::shared_ptr<Expression> Binder::createInvisibleVariable(
    const std::string& name, const LogicalType& dataType) const {
  auto expression =
      expressionBinder.createVariableExpression(dataType.copy(), name);
  expression->setAlias(name);
  return expression;
}

expression_vector Binder::createVariables(
    const std::vector<std::string>& names,
    const std::vector<common::LogicalType>& types) {
  NEUG_ASSERT(names.size() == types.size());
  expression_vector variables;
  for (auto i = 0u; i < names.size(); ++i) {
    variables.push_back(createVariable(names[i], types[i]));
  }
  return variables;
}

expression_vector Binder::createInvisibleVariables(
    const std::vector<std::string>& names,
    const std::vector<LogicalType>& types) const {
  NEUG_ASSERT(names.size() == types.size());
  expression_vector variables;
  for (auto i = 0u; i < names.size(); ++i) {
    variables.push_back(createInvisibleVariable(names[i], types[i]));
  }
  return variables;
}

void Binder::validateOrderByFollowedBySkipOrLimitInWithClause(
    const BoundProjectionBody& boundProjectionBody) {
  auto hasSkipOrLimit =
      boundProjectionBody.hasSkip() || boundProjectionBody.hasLimit();
  if (boundProjectionBody.hasOrderByExpressions() && !hasSkipOrLimit) {
    THROW_BINDER_EXCEPTION(
        "In WITH clause, ORDER BY must be followed by SKIP or LIMIT.");
  }
}

std::string Binder::getUniqueExpressionName(const std::string& name) {
  return "_" + std::to_string(lastExpressionId++) + "_" + name;
}

struct ReservedNames {
  static std::unordered_set<std::string> getColumnNames() {
    return {
        InternalKeyword::ID,
        InternalKeyword::LABEL,
        InternalKeyword::SRC,
        InternalKeyword::DST,
        InternalKeyword::DIRECTION,
        InternalKeyword::LENGTH,
        InternalKeyword::NODES,
        InternalKeyword::RELS,
        InternalKeyword::PLACE_HOLDER,
        StringUtils::getUpper(InternalKeyword::ROW_OFFSET),
        StringUtils::getUpper(InternalKeyword::SRC_OFFSET),
        StringUtils::getUpper(InternalKeyword::DST_OFFSET),
    };
  }

  static std::unordered_set<std::string> getPropertyLookupName() {
    return {InternalKeyword::ID, InternalKeyword::LABEL, InternalKeyword::SRC,
            InternalKeyword::DST};
  }
};

bool Binder::reservedInColumnName(const std::string& name) {
  auto normalizedName = StringUtils::getUpper(name);
  return ReservedNames::getColumnNames().contains(normalizedName);
}

bool Binder::reservedInPropertyLookup(const std::string& name) {
  auto normalizedName = StringUtils::getUpper(name);
  return ReservedNames::getPropertyLookupName().contains(normalizedName);
}

void Binder::addToScope(const std::vector<std::string>& names,
                        const expression_vector& exprs) {
  NEUG_ASSERT(names.size() == exprs.size());
  for (auto i = 0u; i < names.size(); ++i) {
    addToScope(names[i], exprs[i]);
  }
}

void Binder::addToScope(const std::string& name,
                        std::shared_ptr<Expression> expr) {
  scope.addExpression(name, std::move(expr));
}

BinderScope Binder::saveScope() const { return scope.copy(); }

void Binder::restoreScope(BinderScope prevScope) {
  scope = std::move(prevScope);
}

void Binder::replaceExpressionInScope(const std::string& oldName,
                                      const std::string& newName,
                                      std::shared_ptr<Expression> expression) {
  scope.replaceExpression(oldName, newName, expression);
}

/**
 * @brief Get the scan function bind data by file type info
 * Create ScanFileBindData specifically if file type is CSV (CSV will be
 * integrated into extension framework later), Otherwise, create
 * TableFuncBindData for other common cases.
 * @param input
 * @return std::unique_ptr<function::TableFuncBindData>
 */
std::unique_ptr<function::TableFuncBindData> Binder::getScanFuncBindData(
    const function::TableFuncBindInput* input) const {
  auto& extraInput = input->extraInput;
  // todo: check if extraInput is of type ExtraScanTableFuncBindInput
  auto scanInput = extraInput->constPtrCast<ExtraScanTableFuncBindInput>();
  auto vars = input->binder->createVariables(scanInput->expectedColumnNames,
                                             scanInput->expectedColumnTypes);
  return std::make_unique<function::ScanFileBindData>(
      vars, vars.size(), std::move(scanInput->fileScanInfo.copy()),
      clientContext);
}

/**
 * @brief Get the scan function by file type info
 * Create table function directly if file type is CSV (CSV will be integrated
 * into extension framework later), Otherwise, get the function from catalog
 * which has been registered into extension system.
 * @param typeInfo
 * @param fileScanInfo
 * @return function::TableFunction
 */
function::TableFunction* Binder::getScanFunction(
    const common::FileTypeInfo& typeInfo,
    const common::FileScanInfo& fileScanInfo) const {
  auto fileTypeStr = typeInfo.fileTypeStr;
  std::transform(fileTypeStr.begin(), fileTypeStr.end(), fileTypeStr.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  auto name = stringFormat("{}_SCAN", fileTypeStr);
  // TODO: consider about other parameters of data source except input file
  std::vector<LogicalType> inputTypes;
  inputTypes.push_back(LogicalType::STRING());
  auto catalog = clientContext->getCatalog();
  auto transaction = clientContext->getTransaction();
  auto entry = catalog->getFunctionEntry(transaction, name);
  auto func = BuiltInFunctionsUtils::matchFunction(
      name, inputTypes, entry->ptrCast<FunctionCatalogEntry>());
  return func->ptrCast<function::TableFunction>();
}

std::shared_ptr<binder::NodeExpression> Binder::createChildNodeExpr(
    std::shared_ptr<binder::Expression> inputExpr,
    const common::LogicalType& outDataType, const std::string& uniqueName,
    const std::string& aliasName) {
  if (outDataType.getLogicalTypeID() != common::LogicalTypeID::NODE) {
    THROW_EXCEPTION_WITH_FILE_LINE(
        "Cannot create child node expression for non-node type: " +
        outDataType.toString());
  }
  bool startNode = true;
  while (inputExpr) {
    if (inputExpr->expressionType == common::ExpressionType::FUNCTION) {
      auto funcExpr = inputExpr->ptrCast<binder::ScalarFunctionExpression>();
      auto func = funcExpr->getFunction();
      if (func.name == function::EndNodeFunction::name) {
        startNode = false;
      }
    } else if (inputExpr->expressionType == common::ExpressionType::PATTERN) {
      auto typeID = inputExpr->getDataType().getLogicalTypeID();
      if (typeID == common::LogicalTypeID::REL) {
        auto relExpr = inputExpr->ptrCast<RelExpression>();
        if (startNode) {
          inputExpr = relExpr->getSrcNode();
        } else {
          inputExpr = relExpr->getDstNode();
        }
      } else if (typeID == common::LogicalTypeID::RECURSIVE_REL) {
        inputExpr =
            inputExpr->ptrCast<RelExpression>()->getRecursiveInfo()->node;
      }
      break;
    }
    if (!inputExpr->getNumChildren()) {
      break;
    }
    inputExpr = inputExpr->getChild(0);
  }
  std::vector<catalog::TableCatalogEntry*> entries;
  if (inputExpr &&
      inputExpr->expressionType == common::ExpressionType::PATTERN &&
      inputExpr->getDataType().getLogicalTypeID() ==
          common::LogicalTypeID::NODE) {
    auto nodeExpr = inputExpr->ptrCast<binder::NodeExpression>();
    entries = nodeExpr->getEntries();
  }
  auto nodeExpr = std::make_shared<binder::NodeExpression>(
      outDataType.copy(), uniqueName, aliasName, std::move(entries));
  bindQueryNodeProperties(*nodeExpr);
  nodeExpr->setAlias(aliasName);
  auto internalID = PropertyExpression::construct(
      LogicalType::INTERNAL_ID(), InternalKeyword::ID, *nodeExpr);
  nodeExpr->setInternalID(std::move(internalID));
  return nodeExpr;
}

}  // namespace binder
}  // namespace neug
