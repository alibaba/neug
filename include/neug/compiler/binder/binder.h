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

#pragma once

#include "neug/compiler/binder/binder_scope.h"
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/binder/expression_binder.h"
#include "neug/compiler/binder/query/bound_regular_query.h"
#include "neug/compiler/binder/query/query_graph.h"
#include "neug/compiler/catalog/catalog_entry/table_catalog_entry.h"
#include "neug/compiler/common/copier_config/file_scan_info.h"
#include "neug/compiler/function/export/export_function.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/parser/ddl/parsed_property_definition.h"
#include "neug/compiler/parser/query/graph_pattern/pattern_element.h"

namespace neug {
namespace parser {
class ProjectionBody;
class ReturnClause;
class WithClause;
class UpdatingClause;
class ReadingClause;
class QueryPart;
class SingleQuery;
struct CreateTableInfo;
struct BaseScanSource;
struct JoinHintNode;
class Statement;
struct YieldVariable;
}  // namespace parser

namespace catalog {
class NodeTableCatalogEntry;
class RelTableCatalogEntry;
class Catalog;
}  // namespace catalog

namespace main {
class ClientContext;
class MetadataManager;
}  // namespace main

namespace function {
struct TableFunction;
struct ExportFuncBindInput;
struct ExportFuncBindData;
}  // namespace function

namespace transaction {
class Transaction;
}  // namespace transaction

namespace binder {
struct BoundBaseScanSource;
struct BoundCreateTableInfo;
struct BoundInsertInfo;
struct BoundSetPropertyInfo;
struct BoundDeleteInfo;
class BoundWithClause;
class BoundReturnClause;
struct ExportedTableData;
struct BoundJoinHintNode;
struct BoundCopyFromInfo;
struct BoundTableScanInfo;

// BinderScope keeps track of expressions in scope and their aliases. We
// maintain the order of expressions in

class Binder {
  friend class ExpressionBinder;

 public:
  explicit Binder(main::ClientContext* clientContext)
      : lastExpressionId{0},
        scope{},
        expressionBinder{this, clientContext},
        clientContext{clientContext} {}

  std::unique_ptr<BoundStatement> bind(const parser::Statement& statement);

  void setInputParameters(
      std::unordered_map<std::string, std::shared_ptr<common::Value>>
          parameters) {
    expressionBinder.parameterMap = std::move(parameters);
  }

  std::unordered_map<std::string, std::shared_ptr<common::Value>>
  getParameterMap() {
    return expressionBinder.parameterMap;
  }

  bool bindExportTableData(ExportedTableData& tableData,
                           const catalog::TableCatalogEntry& entry,
                           const catalog::Catalog& catalog,
                           const transaction::Transaction* transaction);

  NEUG_API std::shared_ptr<Expression> createVariable(
      const std::string& name, const common::LogicalType& dataType);
  NEUG_API std::shared_ptr<Expression> createInvisibleVariable(
      const std::string& name, const common::LogicalType& dataType) const;
  NEUG_API expression_vector
  createVariables(const std::vector<std::string>& names,
                  const std::vector<common::LogicalType>& types);
  NEUG_API expression_vector
  createInvisibleVariables(const std::vector<std::string>& names,
                           const std::vector<common::LogicalType>& types) const;

  std::shared_ptr<Expression> bindWhereExpression(
      const parser::ParsedExpression& parsedExpression);

  std::shared_ptr<Expression> createVariable(std::string_view name,
                                             common::LogicalTypeID typeID);
  std::shared_ptr<Expression> createVariable(
      const std::string& name, common::LogicalTypeID logicalTypeID);

  /*** bind DDL ***/
  BoundCreateTableInfo bindCreateTableInfo(const parser::CreateTableInfo* info);
  BoundCreateTableInfo bindCreateNodeTableInfo(
      const parser::CreateTableInfo* info);
  BoundCreateTableInfo bindCreateRelTableInfo(
      const parser::CreateTableInfo* info);
  BoundCreateTableInfo bindCreateRelTableInfo(
      const parser::CreateTableInfo* info,
      const parser::options_t& parsedOptions);
  BoundCreateTableInfo bindCreateRelTableGroupInfo(
      const parser::CreateTableInfo* info);
  std::unique_ptr<BoundStatement> bindCreateTable(
      const parser::Statement& statement);
  std::unique_ptr<BoundStatement> bindCreateType(
      const parser::Statement& statement) const;
  std::unique_ptr<BoundStatement> bindCreateSequence(
      const parser::Statement& statement) const;

  static std::unique_ptr<BoundStatement> bindDrop(
      const parser::Statement& statement);
  std::unique_ptr<BoundStatement> bindAlter(const parser::Statement& statement);
  std::unique_ptr<BoundStatement> bindRenameTable(
      const parser::Statement& statement) const;
  std::unique_ptr<BoundStatement> bindAddProperty(
      const parser::Statement& statement);
  std::unique_ptr<BoundStatement> bindDropProperty(
      const parser::Statement& statement) const;
  std::unique_ptr<BoundStatement> bindRenameProperty(
      const parser::Statement& statement) const;
  std::unique_ptr<BoundStatement> bindCommentOn(
      const parser::Statement& statement) const;

  std::vector<PropertyDefinition> bindPropertyDefinitions(
      const std::vector<parser::ParsedPropertyDefinition>& parsedDefinitions,
      const std::string& tableName);

  std::unique_ptr<parser::ParsedExpression> resolvePropertyDefault(
      parser::ParsedExpression* parsedDefault, const common::LogicalType& type,
      const std::string& tableName, const std::string& propertyName);

  /*** bind copy ***/
  std::unique_ptr<BoundStatement> bindCopyFromClause(
      const parser::Statement& statement);
  std::unique_ptr<BoundStatement> bindCopyNodeFrom(
      const parser::Statement& statement,
      catalog::NodeTableCatalogEntry* nodeTableEntry);
  std::unique_ptr<BoundStatement> bindCopyRelFrom(
      const parser::Statement& statement,
      catalog::RelTableCatalogEntry* relTableEntry);

  std::unique_ptr<BoundStatement> bindCopyToClause(
      const parser::Statement& statement);

  static std::unique_ptr<BoundStatement> bindAttachDatabase(
      const parser::Statement& statement);
  static std::unique_ptr<BoundStatement> bindDetachDatabase(
      const parser::Statement& statement);
  static std::unique_ptr<BoundStatement> bindUseDatabase(
      const parser::Statement& statement);

  /*** bind scan source ***/
  std::unique_ptr<BoundBaseScanSource> bindScanSource(
      const parser::BaseScanSource* source, const parser::options_t& options,
      const std::vector<std::string>& columnNames,
      const std::vector<common::LogicalType>& columnTypes);
  std::unique_ptr<BoundBaseScanSource> bindFileScanSource(
      const parser::BaseScanSource& scanSource,
      const parser::options_t& options,
      const std::vector<std::string>& columnNames,
      const std::vector<common::LogicalType>& columnTypes);
  std::unique_ptr<BoundBaseScanSource> bindQueryScanSource(
      const parser::BaseScanSource& scanSource,
      const parser::options_t& options,
      const std::vector<std::string>& columnNames,
      const std::vector<common::LogicalType>& columnTypes);
  std::unique_ptr<BoundBaseScanSource> bindObjectScanSource(
      const parser::BaseScanSource& scanSource,
      const parser::options_t& options,
      const std::vector<std::string>& columnNames,
      const std::vector<common::LogicalType>& columnTypes);
  std::unique_ptr<BoundBaseScanSource> bindTableFuncScanSource(
      const parser::BaseScanSource& scanSource,
      const parser::options_t& options,
      const std::vector<std::string>& columnNames,
      const std::vector<common::LogicalType>& columnTypes);

  common::case_insensitive_map_t<common::Value> bindParsingOptions(
      const parser::options_t& parsingOptions);
  common::FileTypeInfo bindFileTypeInfo(
      const std::vector<std::string>& filePaths) const;
  std::vector<std::string> bindFilePaths(
      const std::vector<std::string>& filePaths) const;

  /*** bind query ***/
  std::unique_ptr<BoundRegularQuery> bindQuery(
      const parser::Statement& statement);
  NormalizedSingleQuery bindSingleQuery(const parser::SingleQuery& singleQuery);
  NormalizedQueryPart bindQueryPart(const parser::QueryPart& queryPart);

  /*** bind standalone call ***/
  std::unique_ptr<BoundStatement> bindStandaloneCall(
      const parser::Statement& statement);

  /*** bind standalone call function ***/
  std::unique_ptr<BoundStatement> bindStandaloneCallFunction(
      const parser::Statement& statement);

  /*** bind table function ***/
  BoundTableScanInfo bindTableFunc(
      const std::string& tableFuncName, const parser::ParsedExpression& expr,
      std::vector<parser::YieldVariable> yieldVariables);

  /*** bind create macro ***/
  std::unique_ptr<BoundStatement> bindCreateMacro(
      const parser::Statement& statement) const;

  /*** bind transaction ***/
  static std::unique_ptr<BoundStatement> bindTransaction(
      const parser::Statement& statement);

  /*** bind extension ***/
  static std::unique_ptr<BoundStatement> bindExtension(
      const parser::Statement& statement);

  /*** bind explain ***/
  std::unique_ptr<BoundStatement> bindExplain(
      const parser::Statement& statement);

  /*** bind reading clause ***/
  std::unique_ptr<BoundReadingClause> bindReadingClause(
      const parser::ReadingClause& readingClause);
  std::unique_ptr<BoundReadingClause> bindMatchClause(
      const parser::ReadingClause& readingClause);
  std::shared_ptr<BoundJoinHintNode> bindJoinHint(
      const QueryGraphCollection& queryGraphCollection,
      const parser::JoinHintNode& joinHintNode);
  std::shared_ptr<BoundJoinHintNode> bindJoinNode(
      const parser::JoinHintNode& joinHintNode);
  void rewriteMatchPattern(BoundGraphPattern& boundGraphPattern);
  std::unique_ptr<BoundReadingClause> bindUnwindClause(
      const parser::ReadingClause& readingClause);
  std::unique_ptr<BoundReadingClause> bindInQueryCall(
      const parser::ReadingClause& readingClause);
  std::unique_ptr<BoundReadingClause> bindLoadFrom(
      const parser::ReadingClause& readingClause);

  /*** bind updating clause ***/
  std::unique_ptr<BoundUpdatingClause> bindUpdatingClause(
      const parser::UpdatingClause& updatingClause);
  std::unique_ptr<BoundUpdatingClause> bindInsertClause(
      const parser::UpdatingClause& updatingClause);
  std::unique_ptr<BoundUpdatingClause> bindMergeClause(
      const parser::UpdatingClause& updatingClause);
  std::unique_ptr<BoundUpdatingClause> bindSetClause(
      const parser::UpdatingClause& updatingClause);
  std::unique_ptr<BoundUpdatingClause> bindDeleteClause(
      const parser::UpdatingClause& updatingClause);

  std::vector<BoundInsertInfo> bindInsertInfos(
      QueryGraphCollection& queryGraphCollection,
      const std::unordered_set<std::string>& patternsInScope_);
  void bindInsertNode(std::shared_ptr<NodeExpression> node,
                      std::vector<BoundInsertInfo>& infos);
  void bindInsertRel(std::shared_ptr<RelExpression> rel,
                     std::vector<BoundInsertInfo>& infos);
  expression_vector bindInsertColumnDataExprs(
      const common::case_insensitive_map_t<std::shared_ptr<Expression>>&
          propertyDataExprs,
      const std::vector<PropertyDefinition>& propertyDefinitions);

  BoundSetPropertyInfo bindSetPropertyInfo(
      const parser::ParsedExpression* column,
      const parser::ParsedExpression* columnData);
  expression_pair bindSetItem(const parser::ParsedExpression* column,
                              const parser::ParsedExpression* columnData);

  /*** bind projection clause ***/
  BoundWithClause bindWithClause(const parser::WithClause& withClause);
  BoundReturnClause bindReturnClause(const parser::ReturnClause& returnClause);

  std::pair<expression_vector, std::vector<std::string>> bindProjectionList(
      const parser::ProjectionBody& projectionBody);
  BoundProjectionBody bindProjectionBody(
      const parser::ProjectionBody& projectionBody,
      const expression_vector& projectionExprs,
      const std::vector<std::string>& aliases);

  expression_vector bindOrderByExpressions(
      const std::vector<std::unique_ptr<parser::ParsedExpression>>&
          parsedExprs);
  std::shared_ptr<Expression> bindSkipLimitExpression(
      const parser::ParsedExpression& expression);

  /*** bind graph pattern ***/
  BoundGraphPattern bindGraphPattern(
      const std::vector<parser::PatternElement>& graphPattern);

  QueryGraph bindPatternElement(const parser::PatternElement& patternElement);
  std::shared_ptr<Expression> createPath(const std::string& pathName,
                                         const expression_vector& children);

  std::shared_ptr<RelExpression> bindQueryRel(
      const parser::RelPattern& relPattern,
      const std::shared_ptr<NodeExpression>& leftNode,
      const std::shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph);
  std::shared_ptr<RelExpression> createNonRecursiveQueryRel(
      const std::string& parsedName,
      const std::vector<catalog::TableCatalogEntry*>& entries,
      std::shared_ptr<NodeExpression> srcNode,
      std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType);
  std::shared_ptr<RelExpression> createRecursiveQueryRel(
      const parser::RelPattern& relPattern,
      const std::vector<catalog::TableCatalogEntry*>& entries,
      std::shared_ptr<NodeExpression> srcNode,
      std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType);
  expression_vector bindRecursivePatternNodeProjectionList(
      const parser::RecursiveRelPatternInfo& info,
      const NodeOrRelExpression& expr);
  expression_vector bindRecursivePatternRelProjectionList(
      const parser::RecursiveRelPatternInfo& info,
      const NodeOrRelExpression& expr);
  std::pair<uint64_t, uint64_t> bindVariableLengthRelBound(
      const parser::RelPattern& relPattern);
  static void bindQueryRelProperties(RelExpression& rel);

  std::shared_ptr<NodeExpression> bindQueryNode(
      const parser::NodePattern& nodePattern, QueryGraph& queryGraph);
  std::shared_ptr<NodeExpression> createQueryNode(
      const parser::NodePattern& nodePattern);
  NEUG_API std::shared_ptr<NodeExpression> createQueryNode(
      const std::string& parsedName,
      const std::vector<catalog::TableCatalogEntry*>& entries);
  static void bindQueryNodeProperties(NodeExpression& node);

  /*** bind table entries ***/
  std::vector<catalog::TableCatalogEntry*> bindNodeTableEntries(
      const std::vector<std::string>& tableNames) const;
  catalog::TableCatalogEntry* bindNodeTableEntry(const std::string& name) const;
  std::vector<catalog::TableCatalogEntry*> bindRelTableEntries(
      const std::vector<std::string>& tableNames) const;

  /*** validations ***/
  static void validateOrderByFollowedBySkipOrLimitInWithClause(
      const BoundProjectionBody& boundProjectionBody);
  static bool isOrderByKeyTypeSupported(const common::LogicalType& dataType);

  NEUG_API static void validateTableExistence(
      const main::ClientContext& context, const std::string& tableName);
  NEUG_API static void validateNodeTableType(
      const catalog::TableCatalogEntry* entry);
  NEUG_API static void validateColumnExistence(
      const catalog::TableCatalogEntry* entry, const std::string& columnName);

  void validateNoIndexOnProperty(const std::string& tableName,
                                 const std::string& propertyName) const;
  /*** helpers ***/
  std::string getUniqueExpressionName(const std::string& name);

  static bool reservedInColumnName(const std::string& name);
  static bool reservedInPropertyLookup(const std::string& name);

  void addToScope(const std::vector<std::string>& names,
                  const expression_vector& exprs);
  NEUG_API void addToScope(const std::string& name,
                           std::shared_ptr<Expression> expr);
  BinderScope saveScope() const;
  void restoreScope(BinderScope prevScope);
  void replaceExpressionInScope(const std::string& oldName,
                                const std::string& newName,
                                std::shared_ptr<Expression> expression);

  function::TableFunction* getScanFunction(
      const common::FileTypeInfo& typeInfo,
      const common::FileScanInfo& fileScanInfo) const;

  std::unique_ptr<function::TableFuncBindData> getScanFuncBindData(
      const function::TableFuncBindInput* input) const;

  function::ExportFunction getExportFunction(
      const common::FileTypeInfo& typeInfo);

  std::unique_ptr<function::ExportFuncBindData> getExportFuncBindData(
      const common::FileTypeInfo& typeInfo,
      const function::ExportFuncBindInput& bindInput);

  ExpressionBinder* getExpressionBinder() { return &expressionBinder; }

  std::shared_ptr<Expression> createAlias(
      const std::string& name, const common::LogicalType& dataType,
      std::shared_ptr<binder::Expression> boundExpr);

  std::shared_ptr<binder::NodeExpression> createChildNodeExpr(
      std::shared_ptr<binder::Expression> inputExpr,
      const common::LogicalType& outDataType, const std::string& uniqueName,
      const std::string& aliasName);

 private:
  common::idx_t lastExpressionId;
  BinderScope scope;
  ExpressionBinder expressionBinder;
  main::ClientContext* clientContext;
};

}  // namespace binder
}  // namespace neug
