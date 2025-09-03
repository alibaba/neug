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

#include <memory>
#include "neug/compiler/binder/bound_statement_rewriter.h"
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/common/constants.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/bind_input.h"
#include "neug/compiler/function/table/scan_file_function.h"
#include "neug/utils/exception/exception.h"

using namespace gs::catalog;
using namespace gs::common;
using namespace gs::function;
using namespace gs::parser;

namespace gs {
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
    KU_UNREACHABLE;
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
    THROW_BINDER_EXCEPTION("Variable " + name + " already exists.");
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
  KU_ASSERT(names.size() == types.size());
  expression_vector variables;
  for (auto i = 0u; i < names.size(); ++i) {
    variables.push_back(createVariable(names[i], types[i]));
  }
  return variables;
}

expression_vector Binder::createInvisibleVariables(
    const std::vector<std::string>& names,
    const std::vector<LogicalType>& types) const {
  KU_ASSERT(names.size() == types.size());
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
  KU_ASSERT(names.size() == exprs.size());
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

static std::unique_ptr<TableFuncBindData> scanBindFunc(
    main::ClientContext* context, const TableFuncBindInput* input) {
  auto& extraInput = input->extraInput;
  // todo: check if extraInput is of type ExtraScanTableFuncBindInput
  auto scanInput = extraInput->constPtrCast<ExtraScanTableFuncBindInput>();
  auto vars = input->binder->createVariables(scanInput->expectedColumnNames,
                                             scanInput->expectedColumnTypes);
  return std::make_unique<function::ScanFileBindData>(
      vars, vars.size(), scanInput->fileScanInfo.copy(), context);
}

TableFunction Binder::getScanFunction(const FileTypeInfo& typeInfo,
                                      const FileScanInfo& fileScanInfo) const {
  TableFunction scanFunction;
  scanFunction.bindFunc = scanBindFunc;
  return scanFunction;
}

}  // namespace binder
}  // namespace gs
