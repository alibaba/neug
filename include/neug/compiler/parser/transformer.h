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

// ANTLR4 generates code with unused parameters.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "antlr4_cypher/include/cypher_parser.h"
#pragma GCC diagnostic pop

#include "neug/compiler/parser/ddl/parsed_property_definition.h"
#include "statement.h"

namespace neug {
namespace main {
class ClientContext;
}
namespace parser {

class RegularQuery;
class SingleQuery;
class QueryPart;
class UpdatingClause;
class ReadingClause;
class WithClause;
class ReturnClause;
class ProjectionBody;
class PatternElement;
class NodePattern;
class PatternElementChain;
class RelPattern;
struct ParsedCaseAlternative;
struct BaseScanSource;
struct JoinHintNode;
struct YieldVariable;

class Transformer {
 public:
  explicit Transformer(CypherParser::Neug_StatementsContext& root)
      : root{root} {}

  std::vector<std::shared_ptr<Statement>> transform();

 private:
  std::unique_ptr<Statement> transformStatement(
      CypherParser::OC_StatementContext& ctx);

  std::unique_ptr<ParsedExpression> transformWhere(
      CypherParser::OC_WhereContext& ctx);

  std::string transformVariable(CypherParser::OC_VariableContext& ctx);
  std::string transformSchemaName(CypherParser::OC_SchemaNameContext& ctx);
  std::string transformSymbolicName(CypherParser::OC_SymbolicNameContext& ctx);
  std::string transformStringLiteral(antlr4::tree::TerminalNode& stringLiteral);

  std::unique_ptr<Statement> transformCallUnionQuery(
      CypherParser::OC_CallUnionQueryContext& ctx);

  // Transform copy statement.
  std::unique_ptr<Statement> transformCopyTo(
      CypherParser::NEUG_CopyTOContext& ctx);
  std::unique_ptr<Statement> transformCopyFrom(
      CypherParser::NEUG_CopyFromContext& ctx);
  std::unique_ptr<Statement> transformCopyFromByColumn(
      CypherParser::NEUG_CopyFromByColumnContext& ctx);
  std::vector<std::string> transformColumnNames(
      CypherParser::NEUG_ColumnNamesContext& ctx);
  std::vector<std::string> transformFilePaths(
      const std::vector<antlr4::tree::TerminalNode*>& stringLiteral);
  std::unique_ptr<BaseScanSource> transformScanSource(
      CypherParser::NEUG_ScanSourceContext& ctx);
  options_t transformOptions(CypherParser::NEUG_OptionsContext& ctx);

  std::unique_ptr<Statement> transformExportDatabase(
      CypherParser::NEUG_ExportDatabaseContext& ctx);
  std::unique_ptr<Statement> transformImportDatabase(
      CypherParser::NEUG_ImportDatabaseContext& ctx);

  // Transform query statement.
  std::unique_ptr<Statement> transformQuery(CypherParser::OC_QueryContext& ctx);
  std::unique_ptr<Statement> transformRegularQuery(
      CypherParser::OC_RegularQueryContext& ctx);
  SingleQuery transformSingleQuery(CypherParser::OC_SingleQueryContext& ctx);
  SingleQuery transformSinglePartQuery(
      CypherParser::OC_SinglePartQueryContext& ctx);
  QueryPart transformQueryPart(CypherParser::NEUG_QueryPartContext& ctx);

  // Transform updating.
  std::unique_ptr<UpdatingClause> transformUpdatingClause(
      CypherParser::OC_UpdatingClauseContext& ctx);
  std::unique_ptr<UpdatingClause> transformCreate(
      CypherParser::OC_CreateContext& ctx);
  std::unique_ptr<UpdatingClause> transformMerge(
      CypherParser::OC_MergeContext& ctx);
  std::unique_ptr<UpdatingClause> transformSet(
      CypherParser::OC_SetContext& ctx);
  parsed_expr_pair transformSetItem(CypherParser::OC_SetItemContext& ctx);
  std::unique_ptr<UpdatingClause> transformDelete(
      CypherParser::OC_DeleteContext& ctx);

  // Transform reading.
  std::unique_ptr<ReadingClause> transformReadingClause(
      CypherParser::OC_ReadingClauseContext& ctx);
  std::unique_ptr<ReadingClause> transformMatch(
      CypherParser::OC_MatchContext& ctx);
  std::unique_ptr<ReadingClause> transformUnwind(
      CypherParser::OC_UnwindContext& ctx);
  std::vector<YieldVariable> transformYieldVariables(
      CypherParser::OC_YieldItemsContext& ctx);
  std::unique_ptr<ReadingClause> transformInQueryCall(
      CypherParser::NEUG_InQueryCallContext& ctx);
  std::unique_ptr<ReadingClause> transformLoadFrom(
      CypherParser::NEUG_LoadFromContext& ctx);
  std::shared_ptr<JoinHintNode> transformJoinHint(
      CypherParser::NEUG_JoinNodeContext& ctx);

  // Transform projection.
  WithClause transformWith(CypherParser::OC_WithContext& ctx);
  ReturnClause transformReturn(CypherParser::OC_ReturnContext& ctx);
  ProjectionBody transformProjectionBody(
      CypherParser::OC_ProjectionBodyContext& ctx);
  std::vector<std::unique_ptr<ParsedExpression>> transformProjectionItems(
      CypherParser::OC_ProjectionItemsContext& ctx);
  std::unique_ptr<ParsedExpression> transformProjectionItem(
      CypherParser::OC_ProjectionItemContext& ctx);

  // Transform graph pattern.
  std::vector<PatternElement> transformPattern(
      CypherParser::OC_PatternContext& ctx);
  PatternElement transformPatternPart(CypherParser::OC_PatternPartContext& ctx);
  PatternElement transformAnonymousPatternPart(
      CypherParser::OC_AnonymousPatternPartContext& ctx);
  PatternElement transformPatternElement(
      CypherParser::OC_PatternElementContext& ctx);
  NodePattern transformNodePattern(CypherParser::OC_NodePatternContext& ctx);
  PatternElementChain transformPatternElementChain(
      CypherParser::OC_PatternElementChainContext& ctx);
  RelPattern transformRelationshipPattern(
      CypherParser::OC_RelationshipPatternContext& ctx);
  std::vector<s_parsed_expr_pair> transformProperties(
      CypherParser::NEUG_PropertiesContext& ctx);
  std::vector<std::string> transformRelTypes(
      CypherParser::OC_RelationshipTypesContext& ctx);
  std::vector<std::string> transformNodeLabels(
      CypherParser::OC_NodeLabelsContext& ctx);
  std::string transformNodeLabel(CypherParser::OC_NodeLabelContext& ctx);
  std::string transformLabelName(CypherParser::OC_LabelNameContext& ctx);
  std::string transformRelTypeName(CypherParser::OC_RelTypeNameContext& ctx);

  // Transform expression.
  std::unique_ptr<ParsedExpression> transformExpression(
      CypherParser::OC_ExpressionContext& ctx);
  std::unique_ptr<ParsedExpression> transformOrExpression(
      CypherParser::OC_OrExpressionContext& ctx);
  std::unique_ptr<ParsedExpression> transformXorExpression(
      CypherParser::OC_XorExpressionContext& ctx);
  std::unique_ptr<ParsedExpression> transformAndExpression(
      CypherParser::OC_AndExpressionContext& ctx);
  std::unique_ptr<ParsedExpression> transformNotExpression(
      CypherParser::OC_NotExpressionContext& ctx);
  std::unique_ptr<ParsedExpression> transformComparisonExpression(
      CypherParser::OC_ComparisonExpressionContext& ctx);
  std::unique_ptr<ParsedExpression> transformBitwiseOrOperatorExpression(
      CypherParser::NEUG_BitwiseOrOperatorExpressionContext& ctx);
  std::unique_ptr<ParsedExpression> transformBitwiseAndOperatorExpression(
      CypherParser::NEUG_BitwiseAndOperatorExpressionContext& ctx);
  std::unique_ptr<ParsedExpression> transformBitShiftOperatorExpression(
      CypherParser::NEUG_BitShiftOperatorExpressionContext& ctx);
  std::unique_ptr<ParsedExpression> transformAddOrSubtractExpression(
      CypherParser::OC_AddOrSubtractExpressionContext& ctx);
  std::unique_ptr<ParsedExpression> transformMultiplyDivideModuloExpression(
      CypherParser::OC_MultiplyDivideModuloExpressionContext& ctx);
  std::unique_ptr<ParsedExpression> transformPowerOfExpression(
      CypherParser::OC_PowerOfExpressionContext& ctx);
  std::unique_ptr<ParsedExpression>
  transformUnaryAddSubtractOrFactorialExpression(
      CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext& ctx);
  std::unique_ptr<ParsedExpression> transformStringListNullOperatorExpression(
      CypherParser::OC_StringListNullOperatorExpressionContext& ctx);
  std::unique_ptr<ParsedExpression> transformStringOperatorExpression(
      CypherParser::OC_StringOperatorExpressionContext& ctx,
      std::unique_ptr<ParsedExpression> propertyExpression);
  std::unique_ptr<ParsedExpression> transformListOperatorExpression(
      CypherParser::OC_ListOperatorExpressionContext& ctx,
      std::unique_ptr<ParsedExpression> childExpression);
  std::unique_ptr<ParsedExpression> transformNullOperatorExpression(
      CypherParser::OC_NullOperatorExpressionContext& ctx,
      std::unique_ptr<ParsedExpression> propertyExpression);
  std::unique_ptr<ParsedExpression> transformPropertyOrLabelsExpression(
      CypherParser::OC_PropertyOrLabelsExpressionContext& ctx);
  std::unique_ptr<ParsedExpression> transformAtom(
      CypherParser::OC_AtomContext& ctx);
  std::unique_ptr<ParsedExpression> transformLiteral(
      CypherParser::OC_LiteralContext& ctx);
  std::unique_ptr<ParsedExpression> transformBooleanLiteral(
      CypherParser::OC_BooleanLiteralContext& ctx);
  std::unique_ptr<ParsedExpression> transformListLiteral(
      CypherParser::OC_ListLiteralContext& ctx);
  std::unique_ptr<ParsedExpression> transformStructLiteral(
      CypherParser::NEUG_StructLiteralContext& ctx);
  std::unique_ptr<ParsedExpression> transformParameterExpression(
      CypherParser::OC_ParameterContext& ctx);
  std::unique_ptr<ParsedExpression> transformParenthesizedExpression(
      CypherParser::OC_ParenthesizedExpressionContext& ctx);
  std::unique_ptr<ParsedExpression> transformFunctionInvocation(
      CypherParser::OC_FunctionInvocationContext& ctx);
  std::string transformFunctionName(CypherParser::OC_FunctionNameContext& ctx);
  std::vector<std::string> transformLambdaVariables(
      CypherParser::NEUG_LambdaVarsContext& ctx);
  std::unique_ptr<ParsedExpression> transformLambdaParameter(
      CypherParser::NEUG_LambdaParameterContext& ctx);
  std::unique_ptr<ParsedExpression> transformFunctionParameterExpression(
      CypherParser::NEUG_FunctionParameterContext& ctx);
  std::unique_ptr<ParsedExpression> transformPathPattern(
      CypherParser::OC_PathPatternsContext& ctx);
  std::unique_ptr<ParsedExpression> transformExistCountSubquery(
      CypherParser::OC_ExistCountSubqueryContext& ctx);
  std::unique_ptr<ParsedExpression> transformOcQuantifier(
      CypherParser::OC_QuantifierContext& ctx);
  std::unique_ptr<ParsedExpression> createPropertyExpression(
      CypherParser::OC_PropertyLookupContext& ctx,
      std::unique_ptr<ParsedExpression> child);
  std::unique_ptr<ParsedExpression> transformCaseExpression(
      CypherParser::OC_CaseExpressionContext& ctx);
  ParsedCaseAlternative transformCaseAlternative(
      CypherParser::OC_CaseAlternativeContext& ctx);
  std::unique_ptr<ParsedExpression> transformNumberLiteral(
      CypherParser::OC_NumberLiteralContext& ctx);
  std::unique_ptr<ParsedExpression> transformProperty(
      CypherParser::OC_PropertyExpressionContext& ctx);
  std::string transformPropertyKeyName(
      CypherParser::OC_PropertyKeyNameContext& ctx);
  std::unique_ptr<ParsedExpression> transformIntegerLiteral(
      CypherParser::OC_IntegerLiteralContext& ctx);
  std::unique_ptr<ParsedExpression> transformDoubleLiteral(
      CypherParser::OC_DoubleLiteralContext& ctx);

  // Transform ddl.
  std::unique_ptr<Statement> transformAlterTable(
      CypherParser::NEUG_AlterTableContext& ctx);
  std::unique_ptr<Statement> transformCreateNodeTable(
      CypherParser::NEUG_CreateNodeTableContext& ctx);
  std::unique_ptr<Statement> transformCreateRelTable(
      CypherParser::NEUG_CreateRelTableContext& ctx);
  std::unique_ptr<Statement> transformCreateSequence(
      CypherParser::NEUG_CreateSequenceContext& ctx);
  std::unique_ptr<Statement> transformCreateType(
      CypherParser::NEUG_CreateTypeContext& ctx);
  std::unique_ptr<Statement> transformDrop(CypherParser::NEUG_DropContext& ctx);
  std::unique_ptr<Statement> transformRenameTable(
      CypherParser::NEUG_AlterTableContext& ctx);
  std::unique_ptr<Statement> transformAddProperty(
      CypherParser::NEUG_AlterTableContext& ctx);
  std::unique_ptr<Statement> transformDropProperty(
      CypherParser::NEUG_AlterTableContext& ctx);
  std::unique_ptr<Statement> transformRenameProperty(
      CypherParser::NEUG_AlterTableContext& ctx);
  std::unique_ptr<Statement> transformCommentOn(
      CypherParser::NEUG_CommentOnContext& ctx);
  std::string transformDataType(CypherParser::NEUG_DataTypeContext& ctx);
  std::string getPKName(CypherParser::NEUG_CreateNodeTableContext& ctx);
  std::string transformPrimaryKey(
      CypherParser::NEUG_CreateNodeConstraintContext& ctx);
  std::string transformPrimaryKey(
      CypherParser::NEUG_ColumnDefinitionContext& ctx);
  std::vector<ParsedColumnDefinition> transformColumnDefinitions(
      CypherParser::NEUG_ColumnDefinitionsContext& ctx);
  ParsedColumnDefinition transformColumnDefinition(
      CypherParser::NEUG_ColumnDefinitionContext& ctx);
  std::vector<ParsedPropertyDefinition> transformPropertyDefinitions(
      CypherParser::NEUG_PropertyDefinitionsContext& ctx);

  // Transform standalone call.
  std::unique_ptr<Statement> transformStandaloneCall(
      CypherParser::NEUG_StandaloneCallContext& ctx);

  // Transform create macro.
  std::unique_ptr<Statement> transformCreateMacro(
      CypherParser::NEUG_CreateMacroContext& ctx);
  std::vector<std::string> transformPositionalArgs(
      CypherParser::NEUG_PositionalArgsContext& ctx);

  // Transform transaction.
  std::unique_ptr<Statement> transformTransaction(
      CypherParser::NEUG_TransactionContext& ctx);

  // Transform extension.
  std::unique_ptr<Statement> transformExtension(
      CypherParser::NEUG_ExtensionContext& ctx);

  // Transform attach/detach/use database.
  std::unique_ptr<Statement> transformAttachDatabase(
      CypherParser::NEUG_AttachDatabaseContext& ctx);
  std::unique_ptr<Statement> transformDetachDatabase(
      CypherParser::NEUG_DetachDatabaseContext& ctx);
  std::unique_ptr<Statement> transformUseDatabase(
      CypherParser::NEUG_UseDatabaseContext& ctx);

  std::vector<std::unique_ptr<ParsedExpression>> transformCallScope(
      CypherParser::OC_CallUnionScopeContext& scope);

 private:
  CypherParser::Neug_StatementsContext& root;
};

}  // namespace parser
}  // namespace neug
