
// Generated from Cypher.g4 by ANTLR 4.13.1

#pragma once


#include "antlr4-runtime.h"




class  CypherParser : public antlr4::Parser {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, T__21 = 22, T__22 = 23, T__23 = 24, T__24 = 25, T__25 = 26, 
    T__26 = 27, T__27 = 28, T__28 = 29, T__29 = 30, T__30 = 31, T__31 = 32, 
    T__32 = 33, T__33 = 34, T__34 = 35, T__35 = 36, T__36 = 37, T__37 = 38, 
    T__38 = 39, T__39 = 40, T__40 = 41, T__41 = 42, T__42 = 43, T__43 = 44, 
    T__44 = 45, T__45 = 46, ACYCLIC = 47, ANY = 48, ADD = 49, ALL = 50, 
    ALTER = 51, AND = 52, AS = 53, ASC = 54, ASCENDING = 55, ATTACH = 56, 
    BEGIN = 57, BY = 58, CALL = 59, CASE = 60, CAST = 61, CHECKPOINT = 62, 
    COLUMN = 63, COMMENT = 64, COMMIT = 65, COMMIT_SKIP_CHECKPOINT = 66, 
    CONTAINS = 67, COPY = 68, COUNT = 69, CREATE = 70, CYCLE = 71, DATABASE = 72, 
    DBTYPE = 73, DEFAULT = 74, DELETE = 75, DESC = 76, DESCENDING = 77, 
    DETACH = 78, DISTINCT = 79, DROP = 80, ELSE = 81, END = 82, ENDS = 83, 
    EXISTS = 84, EXPLAIN = 85, EXPORT = 86, EXTENSION = 87, FROM = 88, GLOB = 89, 
    GRAPH = 90, GROUP = 91, HEADERS = 92, HINT = 93, IMPORT = 94, IF = 95, 
    IN = 96, INCREMENT = 97, INSTALL = 98, IS = 99, JOIN = 100, KEY = 101, 
    LIMIT = 102, LOAD = 103, LOGICAL = 104, MACRO = 105, MATCH = 106, MAXVALUE = 107, 
    MERGE = 108, MINVALUE = 109, MULTI_JOIN = 110, NO = 111, NODE = 112, 
    NOT = 113, NONE = 114, NULL_ = 115, ON = 116, ONLY = 117, OPTIONAL = 118, 
    OR = 119, ORDER = 120, PRIMARY = 121, PROFILE = 122, PROJECT = 123, 
    READ = 124, REL = 125, RENAME = 126, RETURN = 127, ROLLBACK = 128, ROLLBACK_SKIP_CHECKPOINT = 129, 
    SEQUENCE = 130, SET = 131, SHORTEST = 132, START = 133, STARTS = 134, 
    TABLE = 135, THEN = 136, TO = 137, TRAIL = 138, TRANSACTION = 139, TYPE = 140, 
    UNINSTALL = 141, UNION = 142, UNWIND = 143, USE = 144, WHEN = 145, WHERE = 146, 
    WITH = 147, WRITE = 148, WSHORTEST = 149, XOR = 150, SINGLE = 151, YIELD = 152, 
    DECIMAL = 153, STAR = 154, L_SKIP = 155, INVALID_NOT_EQUAL = 156, MINUS = 157, 
    FACTORIAL = 158, COLON = 159, BTRUE = 160, BFALSE = 161, StringLiteral = 162, 
    EscapedChar = 163, DecimalInteger = 164, HexLetter = 165, HexDigit = 166, 
    Digit = 167, NonZeroDigit = 168, NonZeroOctDigit = 169, ZeroDigit = 170, 
    ExponentDecimalReal = 171, RegularDecimalReal = 172, UnescapedSymbolicName = 173, 
    IdentifierStart = 174, IdentifierPart = 175, EscapedSymbolicName = 176, 
    SP = 177, WHITESPACE = 178, CypherComment = 179, Unknown = 180
  };

  enum {
    RuleNeug_Statements = 0, RuleOC_Cypher = 1, RuleOC_Statement = 2, RuleNEUG_CopyFrom = 3, 
    RuleNEUG_ColumnNames = 4, RuleNEUG_ScanSource = 5, RuleNEUG_CopyFromByColumn = 6, 
    RuleNEUG_CopyTO = 7, RuleNEUG_ExportDatabase = 8, RuleNEUG_ImportDatabase = 9, 
    RuleNEUG_AttachDatabase = 10, RuleNEUG_Option = 11, RuleNEUG_Options = 12, 
    RuleNEUG_DetachDatabase = 13, RuleNEUG_UseDatabase = 14, RuleNEUG_StandaloneCall = 15, 
    RuleNEUG_CommentOn = 16, RuleNEUG_CreateMacro = 17, RuleNEUG_PositionalArgs = 18, 
    RuleNEUG_DefaultArg = 19, RuleNEUG_FilePaths = 20, RuleNEUG_IfNotExists = 21, 
    RuleNEUG_CreateNodeTable = 22, RuleNEUG_CreateRelTable = 23, RuleNEUG_FromToConnections = 24, 
    RuleNEUG_FromToConnection = 25, RuleNEUG_CreateSequence = 26, RuleNEUG_CreateType = 27, 
    RuleNEUG_SequenceOptions = 28, RuleNEUG_IncrementBy = 29, RuleNEUG_MinValue = 30, 
    RuleNEUG_MaxValue = 31, RuleNEUG_StartWith = 32, RuleNEUG_Cycle = 33, 
    RuleNEUG_IfExists = 34, RuleNEUG_Drop = 35, RuleNEUG_AlterTable = 36, 
    RuleNEUG_AlterOptions = 37, RuleNEUG_AddProperty = 38, RuleNEUG_Default = 39, 
    RuleNEUG_DropProperty = 40, RuleNEUG_RenameTable = 41, RuleNEUG_RenameProperty = 42, 
    RuleNEUG_ColumnDefinitions = 43, RuleNEUG_ColumnDefinition = 44, RuleNEUG_PropertyDefinitions = 45, 
    RuleNEUG_PropertyDefinition = 46, RuleNEUG_CreateNodeConstraint = 47, 
    RuleNEUG_DataType = 48, RuleNEUG_ListIdentifiers = 49, RuleNEUG_ListIdentifier = 50, 
    RuleOC_AnyCypherOption = 51, RuleOC_Explain = 52, RuleOC_Profile = 53, 
    RuleNEUG_Transaction = 54, RuleNEUG_Extension = 55, RuleNEUG_LoadExtension = 56, 
    RuleNEUG_InstallExtension = 57, RuleNEUG_UninstallExtension = 58, RuleOC_Query = 59, 
    RuleOC_RegularQuery = 60, RuleOC_Union = 61, RuleOC_CallUnionQuery = 62, 
    RuleOC_CallUnion = 63, RuleOC_CallUnionScope = 64, RuleOC_SingleQuery = 65, 
    RuleOC_SinglePartQuery = 66, RuleOC_MultiPartQuery = 67, RuleNEUG_QueryPart = 68, 
    RuleOC_UpdatingClause = 69, RuleOC_ReadingClause = 70, RuleNEUG_LoadFrom = 71, 
    RuleOC_YieldItem = 72, RuleOC_YieldItems = 73, RuleNEUG_InQueryCall = 74, 
    RuleOC_Match = 75, RuleNEUG_Hint = 76, RuleNEUG_JoinNode = 77, RuleOC_Unwind = 78, 
    RuleOC_Create = 79, RuleOC_Merge = 80, RuleOC_MergeAction = 81, RuleOC_Set = 82, 
    RuleOC_SetItem = 83, RuleOC_Delete = 84, RuleOC_With = 85, RuleOC_Return = 86, 
    RuleOC_ProjectionBody = 87, RuleOC_ProjectionItems = 88, RuleOC_ProjectionItem = 89, 
    RuleOC_Order = 90, RuleOC_Skip = 91, RuleOC_Limit = 92, RuleOC_SortItem = 93, 
    RuleOC_Where = 94, RuleOC_Pattern = 95, RuleOC_PatternPart = 96, RuleOC_AnonymousPatternPart = 97, 
    RuleOC_PatternElement = 98, RuleOC_NodePattern = 99, RuleOC_PatternElementChain = 100, 
    RuleOC_RelationshipPattern = 101, RuleOC_RelationshipDetail = 102, RuleNEUG_Properties = 103, 
    RuleOC_RelationshipTypes = 104, RuleOC_NodeLabels = 105, RuleOC_NodeLabel = 106, 
    RuleNEUG_RecursiveDetail = 107, RuleNEUG_RecursiveType = 108, RuleOC_RangeLiteral = 109, 
    RuleNEUG_RecursiveComprehension = 110, RuleNEUG_RecursiveProjectionItems = 111, 
    RuleOC_LowerBound = 112, RuleOC_UpperBound = 113, RuleOC_LabelName = 114, 
    RuleOC_RelTypeName = 115, RuleOC_Expression = 116, RuleOC_OrExpression = 117, 
    RuleOC_XorExpression = 118, RuleOC_AndExpression = 119, RuleOC_NotExpression = 120, 
    RuleOC_ComparisonExpression = 121, RuleNEUG_ComparisonOperator = 122, 
    RuleNEUG_BitwiseOrOperatorExpression = 123, RuleNEUG_BitwiseAndOperatorExpression = 124, 
    RuleNEUG_BitShiftOperatorExpression = 125, RuleNEUG_BitShiftOperator = 126, 
    RuleOC_AddOrSubtractExpression = 127, RuleNEUG_AddOrSubtractOperator = 128, 
    RuleOC_MultiplyDivideModuloExpression = 129, RuleNEUG_MultiplyDivideModuloOperator = 130, 
    RuleOC_PowerOfExpression = 131, RuleOC_UnaryAddSubtractOrFactorialExpression = 132, 
    RuleOC_StringListNullOperatorExpression = 133, RuleOC_ListOperatorExpression = 134, 
    RuleOC_StringOperatorExpression = 135, RuleOC_RegularExpression = 136, 
    RuleOC_NullOperatorExpression = 137, RuleOC_PropertyOrLabelsExpression = 138, 
    RuleOC_Atom = 139, RuleOC_Quantifier = 140, RuleOC_FilterExpression = 141, 
    RuleOC_IdInColl = 142, RuleOC_Literal = 143, RuleOC_BooleanLiteral = 144, 
    RuleOC_ListLiteral = 145, RuleNEUG_ListEntry = 146, RuleNEUG_StructLiteral = 147, 
    RuleNEUG_StructField = 148, RuleOC_ParenthesizedExpression = 149, RuleOC_FunctionInvocation = 150, 
    RuleOC_FunctionName = 151, RuleNEUG_FunctionParameter = 152, RuleNEUG_LambdaParameter = 153, 
    RuleNEUG_LambdaVars = 154, RuleOC_PathPatterns = 155, RuleOC_ExistCountSubquery = 156, 
    RuleOC_PropertyLookup = 157, RuleOC_CaseExpression = 158, RuleOC_CaseAlternative = 159, 
    RuleOC_Variable = 160, RuleOC_NumberLiteral = 161, RuleOC_Parameter = 162, 
    RuleOC_PropertyExpression = 163, RuleOC_PropertyKeyName = 164, RuleOC_IntegerLiteral = 165, 
    RuleOC_DoubleLiteral = 166, RuleOC_SchemaName = 167, RuleOC_SymbolicName = 168, 
    RuleNEUG_NonReservedKeywords = 169, RuleOC_LeftArrowHead = 170, RuleOC_RightArrowHead = 171, 
    RuleOC_Dash = 172
  };

  explicit CypherParser(antlr4::TokenStream *input);

  CypherParser(antlr4::TokenStream *input, const antlr4::atn::ParserATNSimulatorOptions &options);

  ~CypherParser() override;

  std::string getGrammarFileName() const override;

  const antlr4::atn::ATN& getATN() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;


  class Neug_StatementsContext;
  class OC_CypherContext;
  class OC_StatementContext;
  class NEUG_CopyFromContext;
  class NEUG_ColumnNamesContext;
  class NEUG_ScanSourceContext;
  class NEUG_CopyFromByColumnContext;
  class NEUG_CopyTOContext;
  class NEUG_ExportDatabaseContext;
  class NEUG_ImportDatabaseContext;
  class NEUG_AttachDatabaseContext;
  class NEUG_OptionContext;
  class NEUG_OptionsContext;
  class NEUG_DetachDatabaseContext;
  class NEUG_UseDatabaseContext;
  class NEUG_StandaloneCallContext;
  class NEUG_CommentOnContext;
  class NEUG_CreateMacroContext;
  class NEUG_PositionalArgsContext;
  class NEUG_DefaultArgContext;
  class NEUG_FilePathsContext;
  class NEUG_IfNotExistsContext;
  class NEUG_CreateNodeTableContext;
  class NEUG_CreateRelTableContext;
  class NEUG_FromToConnectionsContext;
  class NEUG_FromToConnectionContext;
  class NEUG_CreateSequenceContext;
  class NEUG_CreateTypeContext;
  class NEUG_SequenceOptionsContext;
  class NEUG_IncrementByContext;
  class NEUG_MinValueContext;
  class NEUG_MaxValueContext;
  class NEUG_StartWithContext;
  class NEUG_CycleContext;
  class NEUG_IfExistsContext;
  class NEUG_DropContext;
  class NEUG_AlterTableContext;
  class NEUG_AlterOptionsContext;
  class NEUG_AddPropertyContext;
  class NEUG_DefaultContext;
  class NEUG_DropPropertyContext;
  class NEUG_RenameTableContext;
  class NEUG_RenamePropertyContext;
  class NEUG_ColumnDefinitionsContext;
  class NEUG_ColumnDefinitionContext;
  class NEUG_PropertyDefinitionsContext;
  class NEUG_PropertyDefinitionContext;
  class NEUG_CreateNodeConstraintContext;
  class NEUG_DataTypeContext;
  class NEUG_ListIdentifiersContext;
  class NEUG_ListIdentifierContext;
  class OC_AnyCypherOptionContext;
  class OC_ExplainContext;
  class OC_ProfileContext;
  class NEUG_TransactionContext;
  class NEUG_ExtensionContext;
  class NEUG_LoadExtensionContext;
  class NEUG_InstallExtensionContext;
  class NEUG_UninstallExtensionContext;
  class OC_QueryContext;
  class OC_RegularQueryContext;
  class OC_UnionContext;
  class OC_CallUnionQueryContext;
  class OC_CallUnionContext;
  class OC_CallUnionScopeContext;
  class OC_SingleQueryContext;
  class OC_SinglePartQueryContext;
  class OC_MultiPartQueryContext;
  class NEUG_QueryPartContext;
  class OC_UpdatingClauseContext;
  class OC_ReadingClauseContext;
  class NEUG_LoadFromContext;
  class OC_YieldItemContext;
  class OC_YieldItemsContext;
  class NEUG_InQueryCallContext;
  class OC_MatchContext;
  class NEUG_HintContext;
  class NEUG_JoinNodeContext;
  class OC_UnwindContext;
  class OC_CreateContext;
  class OC_MergeContext;
  class OC_MergeActionContext;
  class OC_SetContext;
  class OC_SetItemContext;
  class OC_DeleteContext;
  class OC_WithContext;
  class OC_ReturnContext;
  class OC_ProjectionBodyContext;
  class OC_ProjectionItemsContext;
  class OC_ProjectionItemContext;
  class OC_OrderContext;
  class OC_SkipContext;
  class OC_LimitContext;
  class OC_SortItemContext;
  class OC_WhereContext;
  class OC_PatternContext;
  class OC_PatternPartContext;
  class OC_AnonymousPatternPartContext;
  class OC_PatternElementContext;
  class OC_NodePatternContext;
  class OC_PatternElementChainContext;
  class OC_RelationshipPatternContext;
  class OC_RelationshipDetailContext;
  class NEUG_PropertiesContext;
  class OC_RelationshipTypesContext;
  class OC_NodeLabelsContext;
  class OC_NodeLabelContext;
  class NEUG_RecursiveDetailContext;
  class NEUG_RecursiveTypeContext;
  class OC_RangeLiteralContext;
  class NEUG_RecursiveComprehensionContext;
  class NEUG_RecursiveProjectionItemsContext;
  class OC_LowerBoundContext;
  class OC_UpperBoundContext;
  class OC_LabelNameContext;
  class OC_RelTypeNameContext;
  class OC_ExpressionContext;
  class OC_OrExpressionContext;
  class OC_XorExpressionContext;
  class OC_AndExpressionContext;
  class OC_NotExpressionContext;
  class OC_ComparisonExpressionContext;
  class NEUG_ComparisonOperatorContext;
  class NEUG_BitwiseOrOperatorExpressionContext;
  class NEUG_BitwiseAndOperatorExpressionContext;
  class NEUG_BitShiftOperatorExpressionContext;
  class NEUG_BitShiftOperatorContext;
  class OC_AddOrSubtractExpressionContext;
  class NEUG_AddOrSubtractOperatorContext;
  class OC_MultiplyDivideModuloExpressionContext;
  class NEUG_MultiplyDivideModuloOperatorContext;
  class OC_PowerOfExpressionContext;
  class OC_UnaryAddSubtractOrFactorialExpressionContext;
  class OC_StringListNullOperatorExpressionContext;
  class OC_ListOperatorExpressionContext;
  class OC_StringOperatorExpressionContext;
  class OC_RegularExpressionContext;
  class OC_NullOperatorExpressionContext;
  class OC_PropertyOrLabelsExpressionContext;
  class OC_AtomContext;
  class OC_QuantifierContext;
  class OC_FilterExpressionContext;
  class OC_IdInCollContext;
  class OC_LiteralContext;
  class OC_BooleanLiteralContext;
  class OC_ListLiteralContext;
  class NEUG_ListEntryContext;
  class NEUG_StructLiteralContext;
  class NEUG_StructFieldContext;
  class OC_ParenthesizedExpressionContext;
  class OC_FunctionInvocationContext;
  class OC_FunctionNameContext;
  class NEUG_FunctionParameterContext;
  class NEUG_LambdaParameterContext;
  class NEUG_LambdaVarsContext;
  class OC_PathPatternsContext;
  class OC_ExistCountSubqueryContext;
  class OC_PropertyLookupContext;
  class OC_CaseExpressionContext;
  class OC_CaseAlternativeContext;
  class OC_VariableContext;
  class OC_NumberLiteralContext;
  class OC_ParameterContext;
  class OC_PropertyExpressionContext;
  class OC_PropertyKeyNameContext;
  class OC_IntegerLiteralContext;
  class OC_DoubleLiteralContext;
  class OC_SchemaNameContext;
  class OC_SymbolicNameContext;
  class NEUG_NonReservedKeywordsContext;
  class OC_LeftArrowHeadContext;
  class OC_RightArrowHeadContext;
  class OC_DashContext; 

  class  Neug_StatementsContext : public antlr4::ParserRuleContext {
  public:
    Neug_StatementsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_CypherContext *> oC_Cypher();
    OC_CypherContext* oC_Cypher(size_t i);
    antlr4::tree::TerminalNode *EOF();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  Neug_StatementsContext* neug_Statements();

  class  OC_CypherContext : public antlr4::ParserRuleContext {
  public:
    OC_CypherContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_StatementContext *oC_Statement();
    OC_AnyCypherOptionContext *oC_AnyCypherOption();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_CypherContext* oC_Cypher();

  class  OC_StatementContext : public antlr4::ParserRuleContext {
  public:
    OC_StatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_QueryContext *oC_Query();
    NEUG_CreateNodeTableContext *nEUG_CreateNodeTable();
    NEUG_CreateRelTableContext *nEUG_CreateRelTable();
    NEUG_CreateSequenceContext *nEUG_CreateSequence();
    NEUG_CreateTypeContext *nEUG_CreateType();
    NEUG_DropContext *nEUG_Drop();
    NEUG_AlterTableContext *nEUG_AlterTable();
    NEUG_CopyFromContext *nEUG_CopyFrom();
    NEUG_CopyFromByColumnContext *nEUG_CopyFromByColumn();
    NEUG_CopyTOContext *nEUG_CopyTO();
    NEUG_StandaloneCallContext *nEUG_StandaloneCall();
    NEUG_CreateMacroContext *nEUG_CreateMacro();
    NEUG_CommentOnContext *nEUG_CommentOn();
    NEUG_TransactionContext *nEUG_Transaction();
    NEUG_ExtensionContext *nEUG_Extension();
    NEUG_ExportDatabaseContext *nEUG_ExportDatabase();
    NEUG_ImportDatabaseContext *nEUG_ImportDatabase();
    NEUG_AttachDatabaseContext *nEUG_AttachDatabase();
    NEUG_DetachDatabaseContext *nEUG_DetachDatabase();
    NEUG_UseDatabaseContext *nEUG_UseDatabase();

   
  };

  OC_StatementContext* oC_Statement();

  class  NEUG_CopyFromContext : public antlr4::ParserRuleContext {
  public:
    NEUG_CopyFromContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COPY();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_SchemaNameContext *oC_SchemaName();
    antlr4::tree::TerminalNode *FROM();
    NEUG_ScanSourceContext *nEUG_ScanSource();
    NEUG_ColumnNamesContext *nEUG_ColumnNames();
    NEUG_OptionsContext *nEUG_Options();

   
  };

  NEUG_CopyFromContext* nEUG_CopyFrom();

  class  NEUG_ColumnNamesContext : public antlr4::ParserRuleContext {
  public:
    NEUG_ColumnNamesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<OC_SchemaNameContext *> oC_SchemaName();
    OC_SchemaNameContext* oC_SchemaName(size_t i);

   
  };

  NEUG_ColumnNamesContext* nEUG_ColumnNames();

  class  NEUG_ScanSourceContext : public antlr4::ParserRuleContext {
  public:
    NEUG_ScanSourceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NEUG_FilePathsContext *nEUG_FilePaths();
    OC_QueryContext *oC_Query();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_VariableContext *oC_Variable();
    OC_SchemaNameContext *oC_SchemaName();
    OC_FunctionInvocationContext *oC_FunctionInvocation();

   
  };

  NEUG_ScanSourceContext* nEUG_ScanSource();

  class  NEUG_CopyFromByColumnContext : public antlr4::ParserRuleContext {
  public:
    NEUG_CopyFromByColumnContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COPY();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_SchemaNameContext *oC_SchemaName();
    antlr4::tree::TerminalNode *FROM();
    std::vector<antlr4::tree::TerminalNode *> StringLiteral();
    antlr4::tree::TerminalNode* StringLiteral(size_t i);
    antlr4::tree::TerminalNode *BY();
    antlr4::tree::TerminalNode *COLUMN();

   
  };

  NEUG_CopyFromByColumnContext* nEUG_CopyFromByColumn();

  class  NEUG_CopyTOContext : public antlr4::ParserRuleContext {
  public:
    NEUG_CopyTOContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COPY();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_QueryContext *oC_Query();
    antlr4::tree::TerminalNode *TO();
    antlr4::tree::TerminalNode *StringLiteral();
    NEUG_OptionsContext *nEUG_Options();

   
  };

  NEUG_CopyTOContext* nEUG_CopyTO();

  class  NEUG_ExportDatabaseContext : public antlr4::ParserRuleContext {
  public:
    NEUG_ExportDatabaseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EXPORT();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *DATABASE();
    antlr4::tree::TerminalNode *StringLiteral();
    NEUG_OptionsContext *nEUG_Options();

   
  };

  NEUG_ExportDatabaseContext* nEUG_ExportDatabase();

  class  NEUG_ImportDatabaseContext : public antlr4::ParserRuleContext {
  public:
    NEUG_ImportDatabaseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IMPORT();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *DATABASE();
    antlr4::tree::TerminalNode *StringLiteral();

   
  };

  NEUG_ImportDatabaseContext* nEUG_ImportDatabase();

  class  NEUG_AttachDatabaseContext : public antlr4::ParserRuleContext {
  public:
    NEUG_AttachDatabaseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ATTACH();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *StringLiteral();
    antlr4::tree::TerminalNode *DBTYPE();
    OC_SymbolicNameContext *oC_SymbolicName();
    antlr4::tree::TerminalNode *AS();
    OC_SchemaNameContext *oC_SchemaName();
    NEUG_OptionsContext *nEUG_Options();

   
  };

  NEUG_AttachDatabaseContext* nEUG_AttachDatabase();

  class  NEUG_OptionContext : public antlr4::ParserRuleContext {
  public:
    NEUG_OptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SymbolicNameContext *oC_SymbolicName();
    OC_LiteralContext *oC_Literal();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  NEUG_OptionContext* nEUG_Option();

  class  NEUG_OptionsContext : public antlr4::ParserRuleContext {
  public:
    NEUG_OptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NEUG_OptionContext *> nEUG_Option();
    NEUG_OptionContext* nEUG_Option(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  NEUG_OptionsContext* nEUG_Options();

  class  NEUG_DetachDatabaseContext : public antlr4::ParserRuleContext {
  public:
    NEUG_DetachDatabaseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DETACH();
    antlr4::tree::TerminalNode *SP();
    OC_SchemaNameContext *oC_SchemaName();

   
  };

  NEUG_DetachDatabaseContext* nEUG_DetachDatabase();

  class  NEUG_UseDatabaseContext : public antlr4::ParserRuleContext {
  public:
    NEUG_UseDatabaseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *USE();
    antlr4::tree::TerminalNode *SP();
    OC_SchemaNameContext *oC_SchemaName();

   
  };

  NEUG_UseDatabaseContext* nEUG_UseDatabase();

  class  NEUG_StandaloneCallContext : public antlr4::ParserRuleContext {
  public:
    NEUG_StandaloneCallContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CALL();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_SymbolicNameContext *oC_SymbolicName();
    OC_ExpressionContext *oC_Expression();
    OC_FunctionInvocationContext *oC_FunctionInvocation();

   
  };

  NEUG_StandaloneCallContext* nEUG_StandaloneCall();

  class  NEUG_CommentOnContext : public antlr4::ParserRuleContext {
  public:
    NEUG_CommentOnContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COMMENT();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *ON();
    antlr4::tree::TerminalNode *TABLE();
    OC_SchemaNameContext *oC_SchemaName();
    antlr4::tree::TerminalNode *IS();
    antlr4::tree::TerminalNode *StringLiteral();

   
  };

  NEUG_CommentOnContext* nEUG_CommentOn();

  class  NEUG_CreateMacroContext : public antlr4::ParserRuleContext {
  public:
    NEUG_CreateMacroContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *MACRO();
    OC_FunctionNameContext *oC_FunctionName();
    antlr4::tree::TerminalNode *AS();
    OC_ExpressionContext *oC_Expression();
    NEUG_PositionalArgsContext *nEUG_PositionalArgs();
    std::vector<NEUG_DefaultArgContext *> nEUG_DefaultArg();
    NEUG_DefaultArgContext* nEUG_DefaultArg(size_t i);

   
  };

  NEUG_CreateMacroContext* nEUG_CreateMacro();

  class  NEUG_PositionalArgsContext : public antlr4::ParserRuleContext {
  public:
    NEUG_PositionalArgsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_SymbolicNameContext *> oC_SymbolicName();
    OC_SymbolicNameContext* oC_SymbolicName(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  NEUG_PositionalArgsContext* nEUG_PositionalArgs();

  class  NEUG_DefaultArgContext : public antlr4::ParserRuleContext {
  public:
    NEUG_DefaultArgContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SymbolicNameContext *oC_SymbolicName();
    antlr4::tree::TerminalNode *COLON();
    OC_LiteralContext *oC_Literal();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  NEUG_DefaultArgContext* nEUG_DefaultArg();

  class  NEUG_FilePathsContext : public antlr4::ParserRuleContext {
  public:
    NEUG_FilePathsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> StringLiteral();
    antlr4::tree::TerminalNode* StringLiteral(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *GLOB();

   
  };

  NEUG_FilePathsContext* nEUG_FilePaths();

  class  NEUG_IfNotExistsContext : public antlr4::ParserRuleContext {
  public:
    NEUG_IfNotExistsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IF();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *NOT();
    antlr4::tree::TerminalNode *EXISTS();

   
  };

  NEUG_IfNotExistsContext* nEUG_IfNotExists();

  class  NEUG_CreateNodeTableContext : public antlr4::ParserRuleContext {
  public:
    NEUG_CreateNodeTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *NODE();
    antlr4::tree::TerminalNode *TABLE();
    OC_SchemaNameContext *oC_SchemaName();
    NEUG_PropertyDefinitionsContext *nEUG_PropertyDefinitions();
    NEUG_IfNotExistsContext *nEUG_IfNotExists();
    NEUG_CreateNodeConstraintContext *nEUG_CreateNodeConstraint();

   
  };

  NEUG_CreateNodeTableContext* nEUG_CreateNodeTable();

  class  NEUG_CreateRelTableContext : public antlr4::ParserRuleContext {
  public:
    NEUG_CreateRelTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *REL();
    antlr4::tree::TerminalNode *TABLE();
    OC_SchemaNameContext *oC_SchemaName();
    NEUG_FromToConnectionsContext *nEUG_FromToConnections();
    antlr4::tree::TerminalNode *GROUP();
    NEUG_IfNotExistsContext *nEUG_IfNotExists();
    NEUG_PropertyDefinitionsContext *nEUG_PropertyDefinitions();
    OC_SymbolicNameContext *oC_SymbolicName();
    antlr4::tree::TerminalNode *WITH();
    NEUG_OptionsContext *nEUG_Options();

   
  };

  NEUG_CreateRelTableContext* nEUG_CreateRelTable();

  class  NEUG_FromToConnectionsContext : public antlr4::ParserRuleContext {
  public:
    NEUG_FromToConnectionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NEUG_FromToConnectionContext *> nEUG_FromToConnection();
    NEUG_FromToConnectionContext* nEUG_FromToConnection(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  NEUG_FromToConnectionsContext* nEUG_FromToConnections();

  class  NEUG_FromToConnectionContext : public antlr4::ParserRuleContext {
  public:
    NEUG_FromToConnectionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FROM();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<OC_SchemaNameContext *> oC_SchemaName();
    OC_SchemaNameContext* oC_SchemaName(size_t i);
    antlr4::tree::TerminalNode *TO();

   
  };

  NEUG_FromToConnectionContext* nEUG_FromToConnection();

  class  NEUG_CreateSequenceContext : public antlr4::ParserRuleContext {
  public:
    NEUG_CreateSequenceContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *SEQUENCE();
    OC_SchemaNameContext *oC_SchemaName();
    NEUG_IfNotExistsContext *nEUG_IfNotExists();
    std::vector<NEUG_SequenceOptionsContext *> nEUG_SequenceOptions();
    NEUG_SequenceOptionsContext* nEUG_SequenceOptions(size_t i);

   
  };

  NEUG_CreateSequenceContext* nEUG_CreateSequence();

  class  NEUG_CreateTypeContext : public antlr4::ParserRuleContext {
  public:
    NEUG_CreateTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *TYPE();
    OC_SchemaNameContext *oC_SchemaName();
    antlr4::tree::TerminalNode *AS();
    NEUG_DataTypeContext *nEUG_DataType();

   
  };

  NEUG_CreateTypeContext* nEUG_CreateType();

  class  NEUG_SequenceOptionsContext : public antlr4::ParserRuleContext {
  public:
    NEUG_SequenceOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NEUG_IncrementByContext *nEUG_IncrementBy();
    NEUG_MinValueContext *nEUG_MinValue();
    NEUG_MaxValueContext *nEUG_MaxValue();
    NEUG_StartWithContext *nEUG_StartWith();
    NEUG_CycleContext *nEUG_Cycle();

   
  };

  NEUG_SequenceOptionsContext* nEUG_SequenceOptions();

  class  NEUG_IncrementByContext : public antlr4::ParserRuleContext {
  public:
    NEUG_IncrementByContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INCREMENT();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_IntegerLiteralContext *oC_IntegerLiteral();
    antlr4::tree::TerminalNode *BY();
    antlr4::tree::TerminalNode *MINUS();

   
  };

  NEUG_IncrementByContext* nEUG_IncrementBy();

  class  NEUG_MinValueContext : public antlr4::ParserRuleContext {
  public:
    NEUG_MinValueContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *NO();
    antlr4::tree::TerminalNode *SP();
    antlr4::tree::TerminalNode *MINVALUE();
    OC_IntegerLiteralContext *oC_IntegerLiteral();
    antlr4::tree::TerminalNode *MINUS();

   
  };

  NEUG_MinValueContext* nEUG_MinValue();

  class  NEUG_MaxValueContext : public antlr4::ParserRuleContext {
  public:
    NEUG_MaxValueContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *NO();
    antlr4::tree::TerminalNode *SP();
    antlr4::tree::TerminalNode *MAXVALUE();
    OC_IntegerLiteralContext *oC_IntegerLiteral();
    antlr4::tree::TerminalNode *MINUS();

   
  };

  NEUG_MaxValueContext* nEUG_MaxValue();

  class  NEUG_StartWithContext : public antlr4::ParserRuleContext {
  public:
    NEUG_StartWithContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *START();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_IntegerLiteralContext *oC_IntegerLiteral();
    antlr4::tree::TerminalNode *WITH();
    antlr4::tree::TerminalNode *MINUS();

   
  };

  NEUG_StartWithContext* nEUG_StartWith();

  class  NEUG_CycleContext : public antlr4::ParserRuleContext {
  public:
    NEUG_CycleContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CYCLE();
    antlr4::tree::TerminalNode *NO();
    antlr4::tree::TerminalNode *SP();

   
  };

  NEUG_CycleContext* nEUG_Cycle();

  class  NEUG_IfExistsContext : public antlr4::ParserRuleContext {
  public:
    NEUG_IfExistsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *SP();
    antlr4::tree::TerminalNode *EXISTS();

   
  };

  NEUG_IfExistsContext* nEUG_IfExists();

  class  NEUG_DropContext : public antlr4::ParserRuleContext {
  public:
    NEUG_DropContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DROP();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_SchemaNameContext *oC_SchemaName();
    antlr4::tree::TerminalNode *TABLE();
    antlr4::tree::TerminalNode *SEQUENCE();
    NEUG_IfExistsContext *nEUG_IfExists();

   
  };

  NEUG_DropContext* nEUG_Drop();

  class  NEUG_AlterTableContext : public antlr4::ParserRuleContext {
  public:
    NEUG_AlterTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ALTER();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *TABLE();
    OC_SchemaNameContext *oC_SchemaName();
    NEUG_AlterOptionsContext *nEUG_AlterOptions();

   
  };

  NEUG_AlterTableContext* nEUG_AlterTable();

  class  NEUG_AlterOptionsContext : public antlr4::ParserRuleContext {
  public:
    NEUG_AlterOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NEUG_AddPropertyContext *nEUG_AddProperty();
    NEUG_DropPropertyContext *nEUG_DropProperty();
    NEUG_RenameTableContext *nEUG_RenameTable();
    NEUG_RenamePropertyContext *nEUG_RenameProperty();

   
  };

  NEUG_AlterOptionsContext* nEUG_AlterOptions();

  class  NEUG_AddPropertyContext : public antlr4::ParserRuleContext {
  public:
    NEUG_AddPropertyContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ADD();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_PropertyKeyNameContext *oC_PropertyKeyName();
    NEUG_DataTypeContext *nEUG_DataType();
    NEUG_IfNotExistsContext *nEUG_IfNotExists();
    NEUG_DefaultContext *nEUG_Default();

   
  };

  NEUG_AddPropertyContext* nEUG_AddProperty();

  class  NEUG_DefaultContext : public antlr4::ParserRuleContext {
  public:
    NEUG_DefaultContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DEFAULT();
    antlr4::tree::TerminalNode *SP();
    OC_ExpressionContext *oC_Expression();

   
  };

  NEUG_DefaultContext* nEUG_Default();

  class  NEUG_DropPropertyContext : public antlr4::ParserRuleContext {
  public:
    NEUG_DropPropertyContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DROP();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_PropertyKeyNameContext *oC_PropertyKeyName();
    NEUG_IfExistsContext *nEUG_IfExists();

   
  };

  NEUG_DropPropertyContext* nEUG_DropProperty();

  class  NEUG_RenameTableContext : public antlr4::ParserRuleContext {
  public:
    NEUG_RenameTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RENAME();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *TO();
    OC_SchemaNameContext *oC_SchemaName();

   
  };

  NEUG_RenameTableContext* nEUG_RenameTable();

  class  NEUG_RenamePropertyContext : public antlr4::ParserRuleContext {
  public:
    NEUG_RenamePropertyContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RENAME();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<OC_PropertyKeyNameContext *> oC_PropertyKeyName();
    OC_PropertyKeyNameContext* oC_PropertyKeyName(size_t i);
    antlr4::tree::TerminalNode *TO();

   
  };

  NEUG_RenamePropertyContext* nEUG_RenameProperty();

  class  NEUG_ColumnDefinitionsContext : public antlr4::ParserRuleContext {
  public:
    NEUG_ColumnDefinitionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NEUG_ColumnDefinitionContext *> nEUG_ColumnDefinition();
    NEUG_ColumnDefinitionContext* nEUG_ColumnDefinition(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  NEUG_ColumnDefinitionsContext* nEUG_ColumnDefinitions();

  class  NEUG_ColumnDefinitionContext : public antlr4::ParserRuleContext {
  public:
    NEUG_ColumnDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_PropertyKeyNameContext *oC_PropertyKeyName();
    antlr4::tree::TerminalNode *SP();
    NEUG_DataTypeContext *nEUG_DataType();

   
  };

  NEUG_ColumnDefinitionContext* nEUG_ColumnDefinition();

  class  NEUG_PropertyDefinitionsContext : public antlr4::ParserRuleContext {
  public:
    NEUG_PropertyDefinitionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NEUG_PropertyDefinitionContext *> nEUG_PropertyDefinition();
    NEUG_PropertyDefinitionContext* nEUG_PropertyDefinition(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  NEUG_PropertyDefinitionsContext* nEUG_PropertyDefinitions();

  class  NEUG_PropertyDefinitionContext : public antlr4::ParserRuleContext {
  public:
    NEUG_PropertyDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NEUG_ColumnDefinitionContext *nEUG_ColumnDefinition();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    NEUG_DefaultContext *nEUG_Default();
    antlr4::tree::TerminalNode *PRIMARY();
    antlr4::tree::TerminalNode *KEY();

   
  };

  NEUG_PropertyDefinitionContext* nEUG_PropertyDefinition();

  class  NEUG_CreateNodeConstraintContext : public antlr4::ParserRuleContext {
  public:
    NEUG_CreateNodeConstraintContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PRIMARY();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *KEY();
    OC_PropertyKeyNameContext *oC_PropertyKeyName();

   
  };

  NEUG_CreateNodeConstraintContext* nEUG_CreateNodeConstraint();

  class  NEUG_DataTypeContext : public antlr4::ParserRuleContext {
  public:
    NEUG_DataTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SymbolicNameContext *oC_SymbolicName();
    antlr4::tree::TerminalNode *UNION();
    NEUG_ColumnDefinitionsContext *nEUG_ColumnDefinitions();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<NEUG_DataTypeContext *> nEUG_DataType();
    NEUG_DataTypeContext* nEUG_DataType(size_t i);
    antlr4::tree::TerminalNode *DECIMAL();
    std::vector<OC_IntegerLiteralContext *> oC_IntegerLiteral();
    OC_IntegerLiteralContext* oC_IntegerLiteral(size_t i);
    NEUG_ListIdentifiersContext *nEUG_ListIdentifiers();

   
  };

  NEUG_DataTypeContext* nEUG_DataType();
  NEUG_DataTypeContext* nEUG_DataType(int precedence);
  class  NEUG_ListIdentifiersContext : public antlr4::ParserRuleContext {
  public:
    NEUG_ListIdentifiersContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NEUG_ListIdentifierContext *> nEUG_ListIdentifier();
    NEUG_ListIdentifierContext* nEUG_ListIdentifier(size_t i);

   
  };

  NEUG_ListIdentifiersContext* nEUG_ListIdentifiers();

  class  NEUG_ListIdentifierContext : public antlr4::ParserRuleContext {
  public:
    NEUG_ListIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_IntegerLiteralContext *oC_IntegerLiteral();

   
  };

  NEUG_ListIdentifierContext* nEUG_ListIdentifier();

  class  OC_AnyCypherOptionContext : public antlr4::ParserRuleContext {
  public:
    OC_AnyCypherOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_ExplainContext *oC_Explain();
    OC_ProfileContext *oC_Profile();

   
  };

  OC_AnyCypherOptionContext* oC_AnyCypherOption();

  class  OC_ExplainContext : public antlr4::ParserRuleContext {
  public:
    OC_ExplainContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EXPLAIN();
    antlr4::tree::TerminalNode *SP();
    antlr4::tree::TerminalNode *LOGICAL();

   
  };

  OC_ExplainContext* oC_Explain();

  class  OC_ProfileContext : public antlr4::ParserRuleContext {
  public:
    OC_ProfileContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PROFILE();

   
  };

  OC_ProfileContext* oC_Profile();

  class  NEUG_TransactionContext : public antlr4::ParserRuleContext {
  public:
    NEUG_TransactionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *BEGIN();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *TRANSACTION();
    antlr4::tree::TerminalNode *READ();
    antlr4::tree::TerminalNode *ONLY();
    antlr4::tree::TerminalNode *COMMIT();
    antlr4::tree::TerminalNode *ROLLBACK();
    antlr4::tree::TerminalNode *CHECKPOINT();

   
  };

  NEUG_TransactionContext* nEUG_Transaction();

  class  NEUG_ExtensionContext : public antlr4::ParserRuleContext {
  public:
    NEUG_ExtensionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NEUG_LoadExtensionContext *nEUG_LoadExtension();
    NEUG_InstallExtensionContext *nEUG_InstallExtension();
    NEUG_UninstallExtensionContext *nEUG_UninstallExtension();

   
  };

  NEUG_ExtensionContext* nEUG_Extension();

  class  NEUG_LoadExtensionContext : public antlr4::ParserRuleContext {
  public:
    NEUG_LoadExtensionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LOAD();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *StringLiteral();
    OC_VariableContext *oC_Variable();
    antlr4::tree::TerminalNode *EXTENSION();

   
  };

  NEUG_LoadExtensionContext* nEUG_LoadExtension();

  class  NEUG_InstallExtensionContext : public antlr4::ParserRuleContext {
  public:
    NEUG_InstallExtensionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INSTALL();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_VariableContext *oC_Variable();
    antlr4::tree::TerminalNode *FROM();
    antlr4::tree::TerminalNode *StringLiteral();

   
  };

  NEUG_InstallExtensionContext* nEUG_InstallExtension();

  class  NEUG_UninstallExtensionContext : public antlr4::ParserRuleContext {
  public:
    NEUG_UninstallExtensionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UNINSTALL();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *StringLiteral();
    OC_VariableContext *oC_Variable();
    antlr4::tree::TerminalNode *EXTENSION();

   
  };

  NEUG_UninstallExtensionContext* nEUG_UninstallExtension();

  class  OC_QueryContext : public antlr4::ParserRuleContext {
  public:
    OC_QueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_RegularQueryContext *oC_RegularQuery();

   
  };

  OC_QueryContext* oC_Query();

  class  OC_RegularQueryContext : public antlr4::ParserRuleContext {
  public:
    OC_RegularQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SingleQueryContext *oC_SingleQuery();
    std::vector<OC_UnionContext *> oC_Union();
    OC_UnionContext* oC_Union(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<OC_ReturnContext *> oC_Return();
    OC_ReturnContext* oC_Return(size_t i);
    OC_CallUnionQueryContext *oC_CallUnionQuery();

   
  };

  OC_RegularQueryContext* oC_RegularQuery();

  class  OC_UnionContext : public antlr4::ParserRuleContext {
  public:
    OC_UnionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UNION();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *ALL();
    OC_SingleQueryContext *oC_SingleQuery();

   
  };

  OC_UnionContext* oC_Union();

  class  OC_CallUnionQueryContext : public antlr4::ParserRuleContext {
  public:
    OC_CallUnionQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_CallUnionContext *oC_CallUnion();
    std::vector<NEUG_QueryPartContext *> nEUG_QueryPart();
    NEUG_QueryPartContext* nEUG_QueryPart(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_SingleQueryContext *oC_SingleQuery();

   
  };

  OC_CallUnionQueryContext* oC_CallUnionQuery();

  class  OC_CallUnionContext : public antlr4::ParserRuleContext {
  public:
    OC_CallUnionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_CallUnionScopeContext *oC_CallUnionScope();
    OC_SingleQueryContext *oC_SingleQuery();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<OC_UnionContext *> oC_Union();
    OC_UnionContext* oC_Union(size_t i);

   
  };

  OC_CallUnionContext* oC_CallUnion();

  class  OC_CallUnionScopeContext : public antlr4::ParserRuleContext {
  public:
    OC_CallUnionScopeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CALL();
    std::vector<OC_ExpressionContext *> oC_Expression();
    OC_ExpressionContext* oC_Expression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_CallUnionScopeContext* oC_CallUnionScope();

  class  OC_SingleQueryContext : public antlr4::ParserRuleContext {
  public:
    OC_SingleQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SinglePartQueryContext *oC_SinglePartQuery();
    OC_MultiPartQueryContext *oC_MultiPartQuery();

   
  };

  OC_SingleQueryContext* oC_SingleQuery();

  class  OC_SinglePartQueryContext : public antlr4::ParserRuleContext {
  public:
    OC_SinglePartQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_ReturnContext *oC_Return();
    std::vector<OC_ReadingClauseContext *> oC_ReadingClause();
    OC_ReadingClauseContext* oC_ReadingClause(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<OC_UpdatingClauseContext *> oC_UpdatingClause();
    OC_UpdatingClauseContext* oC_UpdatingClause(size_t i);

   
  };

  OC_SinglePartQueryContext* oC_SinglePartQuery();

  class  OC_MultiPartQueryContext : public antlr4::ParserRuleContext {
  public:
    OC_MultiPartQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SinglePartQueryContext *oC_SinglePartQuery();
    std::vector<NEUG_QueryPartContext *> nEUG_QueryPart();
    NEUG_QueryPartContext* nEUG_QueryPart(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_MultiPartQueryContext* oC_MultiPartQuery();

  class  NEUG_QueryPartContext : public antlr4::ParserRuleContext {
  public:
    NEUG_QueryPartContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_WithContext *oC_With();
    std::vector<OC_ReadingClauseContext *> oC_ReadingClause();
    OC_ReadingClauseContext* oC_ReadingClause(size_t i);
    std::vector<OC_UpdatingClauseContext *> oC_UpdatingClause();
    OC_UpdatingClauseContext* oC_UpdatingClause(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  NEUG_QueryPartContext* nEUG_QueryPart();

  class  OC_UpdatingClauseContext : public antlr4::ParserRuleContext {
  public:
    OC_UpdatingClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_CreateContext *oC_Create();
    OC_MergeContext *oC_Merge();
    OC_SetContext *oC_Set();
    OC_DeleteContext *oC_Delete();

   
  };

  OC_UpdatingClauseContext* oC_UpdatingClause();

  class  OC_ReadingClauseContext : public antlr4::ParserRuleContext {
  public:
    OC_ReadingClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_MatchContext *oC_Match();
    OC_UnwindContext *oC_Unwind();
    NEUG_InQueryCallContext *nEUG_InQueryCall();
    NEUG_LoadFromContext *nEUG_LoadFrom();

   
  };

  OC_ReadingClauseContext* oC_ReadingClause();

  class  NEUG_LoadFromContext : public antlr4::ParserRuleContext {
  public:
    NEUG_LoadFromContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LOAD();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *FROM();
    NEUG_ScanSourceContext *nEUG_ScanSource();
    antlr4::tree::TerminalNode *WITH();
    antlr4::tree::TerminalNode *HEADERS();
    NEUG_ColumnDefinitionsContext *nEUG_ColumnDefinitions();
    NEUG_OptionsContext *nEUG_Options();
    OC_WhereContext *oC_Where();

   
  };

  NEUG_LoadFromContext* nEUG_LoadFrom();

  class  OC_YieldItemContext : public antlr4::ParserRuleContext {
  public:
    OC_YieldItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_VariableContext *> oC_Variable();
    OC_VariableContext* oC_Variable(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *AS();

   
  };

  OC_YieldItemContext* oC_YieldItem();

  class  OC_YieldItemsContext : public antlr4::ParserRuleContext {
  public:
    OC_YieldItemsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_YieldItemContext *> oC_YieldItem();
    OC_YieldItemContext* oC_YieldItem(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_YieldItemsContext* oC_YieldItems();

  class  NEUG_InQueryCallContext : public antlr4::ParserRuleContext {
  public:
    NEUG_InQueryCallContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CALL();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_FunctionInvocationContext *oC_FunctionInvocation();
    OC_WhereContext *oC_Where();
    antlr4::tree::TerminalNode *YIELD();
    OC_YieldItemsContext *oC_YieldItems();

   
  };

  NEUG_InQueryCallContext* nEUG_InQueryCall();

  class  OC_MatchContext : public antlr4::ParserRuleContext {
  public:
    OC_MatchContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MATCH();
    OC_PatternContext *oC_Pattern();
    antlr4::tree::TerminalNode *OPTIONAL();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_WhereContext *oC_Where();
    NEUG_HintContext *nEUG_Hint();

   
  };

  OC_MatchContext* oC_Match();

  class  NEUG_HintContext : public antlr4::ParserRuleContext {
  public:
    NEUG_HintContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *HINT();
    antlr4::tree::TerminalNode *SP();
    NEUG_JoinNodeContext *nEUG_JoinNode();

   
  };

  NEUG_HintContext* nEUG_Hint();

  class  NEUG_JoinNodeContext : public antlr4::ParserRuleContext {
  public:
    NEUG_JoinNodeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NEUG_JoinNodeContext *> nEUG_JoinNode();
    NEUG_JoinNodeContext* nEUG_JoinNode(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<OC_SchemaNameContext *> oC_SchemaName();
    OC_SchemaNameContext* oC_SchemaName(size_t i);
    antlr4::tree::TerminalNode *JOIN();
    std::vector<antlr4::tree::TerminalNode *> MULTI_JOIN();
    antlr4::tree::TerminalNode* MULTI_JOIN(size_t i);

   
  };

  NEUG_JoinNodeContext* nEUG_JoinNode();
  NEUG_JoinNodeContext* nEUG_JoinNode(int precedence);
  class  OC_UnwindContext : public antlr4::ParserRuleContext {
  public:
    OC_UnwindContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UNWIND();
    OC_ExpressionContext *oC_Expression();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *AS();
    OC_VariableContext *oC_Variable();

   
  };

  OC_UnwindContext* oC_Unwind();

  class  OC_CreateContext : public antlr4::ParserRuleContext {
  public:
    OC_CreateContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    OC_PatternContext *oC_Pattern();
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_CreateContext* oC_Create();

  class  OC_MergeContext : public antlr4::ParserRuleContext {
  public:
    OC_MergeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MERGE();
    OC_PatternContext *oC_Pattern();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<OC_MergeActionContext *> oC_MergeAction();
    OC_MergeActionContext* oC_MergeAction(size_t i);

   
  };

  OC_MergeContext* oC_Merge();

  class  OC_MergeActionContext : public antlr4::ParserRuleContext {
  public:
    OC_MergeActionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ON();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *MATCH();
    OC_SetContext *oC_Set();
    antlr4::tree::TerminalNode *CREATE();

   
  };

  OC_MergeActionContext* oC_MergeAction();

  class  OC_SetContext : public antlr4::ParserRuleContext {
  public:
    OC_SetContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SET();
    std::vector<OC_SetItemContext *> oC_SetItem();
    OC_SetItemContext* oC_SetItem(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_SetContext* oC_Set();

  class  OC_SetItemContext : public antlr4::ParserRuleContext {
  public:
    OC_SetItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_PropertyExpressionContext *oC_PropertyExpression();
    OC_ExpressionContext *oC_Expression();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_SetItemContext* oC_SetItem();

  class  OC_DeleteContext : public antlr4::ParserRuleContext {
  public:
    OC_DeleteContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DELETE();
    std::vector<OC_ExpressionContext *> oC_Expression();
    OC_ExpressionContext* oC_Expression(size_t i);
    antlr4::tree::TerminalNode *DETACH();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_DeleteContext* oC_Delete();

  class  OC_WithContext : public antlr4::ParserRuleContext {
  public:
    OC_WithContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WITH();
    OC_ProjectionBodyContext *oC_ProjectionBody();
    OC_WhereContext *oC_Where();
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_WithContext* oC_With();

  class  OC_ReturnContext : public antlr4::ParserRuleContext {
  public:
    OC_ReturnContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RETURN();
    OC_ProjectionBodyContext *oC_ProjectionBody();

   
  };

  OC_ReturnContext* oC_Return();

  class  OC_ProjectionBodyContext : public antlr4::ParserRuleContext {
  public:
    OC_ProjectionBodyContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_ProjectionItemsContext *oC_ProjectionItems();
    antlr4::tree::TerminalNode *DISTINCT();
    OC_OrderContext *oC_Order();
    OC_SkipContext *oC_Skip();
    OC_LimitContext *oC_Limit();

   
  };

  OC_ProjectionBodyContext* oC_ProjectionBody();

  class  OC_ProjectionItemsContext : public antlr4::ParserRuleContext {
  public:
    OC_ProjectionItemsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *STAR();
    std::vector<OC_ProjectionItemContext *> oC_ProjectionItem();
    OC_ProjectionItemContext* oC_ProjectionItem(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_ProjectionItemsContext* oC_ProjectionItems();

  class  OC_ProjectionItemContext : public antlr4::ParserRuleContext {
  public:
    OC_ProjectionItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_ExpressionContext *oC_Expression();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *AS();
    OC_VariableContext *oC_Variable();

   
  };

  OC_ProjectionItemContext* oC_ProjectionItem();

  class  OC_OrderContext : public antlr4::ParserRuleContext {
  public:
    OC_OrderContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ORDER();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *BY();
    std::vector<OC_SortItemContext *> oC_SortItem();
    OC_SortItemContext* oC_SortItem(size_t i);

   
  };

  OC_OrderContext* oC_Order();

  class  OC_SkipContext : public antlr4::ParserRuleContext {
  public:
    OC_SkipContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *L_SKIP();
    antlr4::tree::TerminalNode *SP();
    OC_ExpressionContext *oC_Expression();

   
  };

  OC_SkipContext* oC_Skip();

  class  OC_LimitContext : public antlr4::ParserRuleContext {
  public:
    OC_LimitContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LIMIT();
    antlr4::tree::TerminalNode *SP();
    OC_ExpressionContext *oC_Expression();

   
  };

  OC_LimitContext* oC_Limit();

  class  OC_SortItemContext : public antlr4::ParserRuleContext {
  public:
    OC_SortItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_ExpressionContext *oC_Expression();
    antlr4::tree::TerminalNode *ASCENDING();
    antlr4::tree::TerminalNode *ASC();
    antlr4::tree::TerminalNode *DESCENDING();
    antlr4::tree::TerminalNode *DESC();
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_SortItemContext* oC_SortItem();

  class  OC_WhereContext : public antlr4::ParserRuleContext {
  public:
    OC_WhereContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WHERE();
    antlr4::tree::TerminalNode *SP();
    OC_ExpressionContext *oC_Expression();

   
  };

  OC_WhereContext* oC_Where();

  class  OC_PatternContext : public antlr4::ParserRuleContext {
  public:
    OC_PatternContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_PatternPartContext *> oC_PatternPart();
    OC_PatternPartContext* oC_PatternPart(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_PatternContext* oC_Pattern();

  class  OC_PatternPartContext : public antlr4::ParserRuleContext {
  public:
    OC_PatternPartContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_VariableContext *oC_Variable();
    OC_AnonymousPatternPartContext *oC_AnonymousPatternPart();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_PatternPartContext* oC_PatternPart();

  class  OC_AnonymousPatternPartContext : public antlr4::ParserRuleContext {
  public:
    OC_AnonymousPatternPartContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_PatternElementContext *oC_PatternElement();

   
  };

  OC_AnonymousPatternPartContext* oC_AnonymousPatternPart();

  class  OC_PatternElementContext : public antlr4::ParserRuleContext {
  public:
    OC_PatternElementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_NodePatternContext *oC_NodePattern();
    std::vector<OC_PatternElementChainContext *> oC_PatternElementChain();
    OC_PatternElementChainContext* oC_PatternElementChain(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_PatternElementContext *oC_PatternElement();

   
  };

  OC_PatternElementContext* oC_PatternElement();

  class  OC_NodePatternContext : public antlr4::ParserRuleContext {
  public:
    OC_NodePatternContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_VariableContext *oC_Variable();
    OC_NodeLabelsContext *oC_NodeLabels();
    NEUG_PropertiesContext *nEUG_Properties();

   
  };

  OC_NodePatternContext* oC_NodePattern();

  class  OC_PatternElementChainContext : public antlr4::ParserRuleContext {
  public:
    OC_PatternElementChainContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_RelationshipPatternContext *oC_RelationshipPattern();
    OC_NodePatternContext *oC_NodePattern();
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_PatternElementChainContext* oC_PatternElementChain();

  class  OC_RelationshipPatternContext : public antlr4::ParserRuleContext {
  public:
    OC_RelationshipPatternContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_LeftArrowHeadContext *oC_LeftArrowHead();
    std::vector<OC_DashContext *> oC_Dash();
    OC_DashContext* oC_Dash(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_RelationshipDetailContext *oC_RelationshipDetail();
    OC_RightArrowHeadContext *oC_RightArrowHead();

   
  };

  OC_RelationshipPatternContext* oC_RelationshipPattern();

  class  OC_RelationshipDetailContext : public antlr4::ParserRuleContext {
  public:
    OC_RelationshipDetailContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_VariableContext *oC_Variable();
    OC_RelationshipTypesContext *oC_RelationshipTypes();
    NEUG_RecursiveDetailContext *nEUG_RecursiveDetail();
    NEUG_PropertiesContext *nEUG_Properties();

   
  };

  OC_RelationshipDetailContext* oC_RelationshipDetail();

  class  NEUG_PropertiesContext : public antlr4::ParserRuleContext {
  public:
    NEUG_PropertiesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<OC_PropertyKeyNameContext *> oC_PropertyKeyName();
    OC_PropertyKeyNameContext* oC_PropertyKeyName(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COLON();
    antlr4::tree::TerminalNode* COLON(size_t i);
    std::vector<OC_ExpressionContext *> oC_Expression();
    OC_ExpressionContext* oC_Expression(size_t i);

   
  };

  NEUG_PropertiesContext* nEUG_Properties();

  class  OC_RelationshipTypesContext : public antlr4::ParserRuleContext {
  public:
    OC_RelationshipTypesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> COLON();
    antlr4::tree::TerminalNode* COLON(size_t i);
    std::vector<OC_RelTypeNameContext *> oC_RelTypeName();
    OC_RelTypeNameContext* oC_RelTypeName(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_RelationshipTypesContext* oC_RelationshipTypes();

  class  OC_NodeLabelsContext : public antlr4::ParserRuleContext {
  public:
    OC_NodeLabelsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_NodeLabelContext *> oC_NodeLabel();
    OC_NodeLabelContext* oC_NodeLabel(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_NodeLabelsContext* oC_NodeLabels();

  class  OC_NodeLabelContext : public antlr4::ParserRuleContext {
  public:
    OC_NodeLabelContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COLON();
    OC_LabelNameContext *oC_LabelName();
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_NodeLabelContext* oC_NodeLabel();

  class  NEUG_RecursiveDetailContext : public antlr4::ParserRuleContext {
  public:
    NEUG_RecursiveDetailContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *STAR();
    NEUG_RecursiveTypeContext *nEUG_RecursiveType();
    OC_RangeLiteralContext *oC_RangeLiteral();
    NEUG_RecursiveComprehensionContext *nEUG_RecursiveComprehension();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  NEUG_RecursiveDetailContext* nEUG_RecursiveDetail();

  class  NEUG_RecursiveTypeContext : public antlr4::ParserRuleContext {
  public:
    NEUG_RecursiveTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WSHORTEST();
    OC_PropertyKeyNameContext *oC_PropertyKeyName();
    antlr4::tree::TerminalNode *ALL();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *SHORTEST();
    antlr4::tree::TerminalNode *TRAIL();
    antlr4::tree::TerminalNode *ACYCLIC();

   
  };

  NEUG_RecursiveTypeContext* nEUG_RecursiveType();

  class  OC_RangeLiteralContext : public antlr4::ParserRuleContext {
  public:
    OC_RangeLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_LowerBoundContext *oC_LowerBound();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_UpperBoundContext *oC_UpperBound();
    OC_IntegerLiteralContext *oC_IntegerLiteral();

   
  };

  OC_RangeLiteralContext* oC_RangeLiteral();

  class  NEUG_RecursiveComprehensionContext : public antlr4::ParserRuleContext {
  public:
    NEUG_RecursiveComprehensionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_VariableContext *> oC_Variable();
    OC_VariableContext* oC_Variable(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_WhereContext *oC_Where();
    std::vector<NEUG_RecursiveProjectionItemsContext *> nEUG_RecursiveProjectionItems();
    NEUG_RecursiveProjectionItemsContext* nEUG_RecursiveProjectionItems(size_t i);

   
  };

  NEUG_RecursiveComprehensionContext* nEUG_RecursiveComprehension();

  class  NEUG_RecursiveProjectionItemsContext : public antlr4::ParserRuleContext {
  public:
    NEUG_RecursiveProjectionItemsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_ProjectionItemsContext *oC_ProjectionItems();

   
  };

  NEUG_RecursiveProjectionItemsContext* nEUG_RecursiveProjectionItems();

  class  OC_LowerBoundContext : public antlr4::ParserRuleContext {
  public:
    OC_LowerBoundContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DecimalInteger();

   
  };

  OC_LowerBoundContext* oC_LowerBound();

  class  OC_UpperBoundContext : public antlr4::ParserRuleContext {
  public:
    OC_UpperBoundContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DecimalInteger();

   
  };

  OC_UpperBoundContext* oC_UpperBound();

  class  OC_LabelNameContext : public antlr4::ParserRuleContext {
  public:
    OC_LabelNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SchemaNameContext *oC_SchemaName();

   
  };

  OC_LabelNameContext* oC_LabelName();

  class  OC_RelTypeNameContext : public antlr4::ParserRuleContext {
  public:
    OC_RelTypeNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SchemaNameContext *oC_SchemaName();

   
  };

  OC_RelTypeNameContext* oC_RelTypeName();

  class  OC_ExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_ExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_OrExpressionContext *oC_OrExpression();

   
  };

  OC_ExpressionContext* oC_Expression();

  class  OC_OrExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_OrExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_XorExpressionContext *> oC_XorExpression();
    OC_XorExpressionContext* oC_XorExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<antlr4::tree::TerminalNode *> OR();
    antlr4::tree::TerminalNode* OR(size_t i);

   
  };

  OC_OrExpressionContext* oC_OrExpression();

  class  OC_XorExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_XorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_AndExpressionContext *> oC_AndExpression();
    OC_AndExpressionContext* oC_AndExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<antlr4::tree::TerminalNode *> XOR();
    antlr4::tree::TerminalNode* XOR(size_t i);

   
  };

  OC_XorExpressionContext* oC_XorExpression();

  class  OC_AndExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_AndExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_NotExpressionContext *> oC_NotExpression();
    OC_NotExpressionContext* oC_NotExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<antlr4::tree::TerminalNode *> AND();
    antlr4::tree::TerminalNode* AND(size_t i);

   
  };

  OC_AndExpressionContext* oC_AndExpression();

  class  OC_NotExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_NotExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_ComparisonExpressionContext *oC_ComparisonExpression();
    std::vector<antlr4::tree::TerminalNode *> NOT();
    antlr4::tree::TerminalNode* NOT(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_NotExpressionContext* oC_NotExpression();

  class  OC_ComparisonExpressionContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *invalid_not_equalToken = nullptr;
    OC_ComparisonExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NEUG_BitwiseOrOperatorExpressionContext *> nEUG_BitwiseOrOperatorExpression();
    NEUG_BitwiseOrOperatorExpressionContext* nEUG_BitwiseOrOperatorExpression(size_t i);
    std::vector<NEUG_ComparisonOperatorContext *> nEUG_ComparisonOperator();
    NEUG_ComparisonOperatorContext* nEUG_ComparisonOperator(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *INVALID_NOT_EQUAL();

   
  };

  OC_ComparisonExpressionContext* oC_ComparisonExpression();

  class  NEUG_ComparisonOperatorContext : public antlr4::ParserRuleContext {
  public:
    NEUG_ComparisonOperatorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;

   
  };

  NEUG_ComparisonOperatorContext* nEUG_ComparisonOperator();

  class  NEUG_BitwiseOrOperatorExpressionContext : public antlr4::ParserRuleContext {
  public:
    NEUG_BitwiseOrOperatorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NEUG_BitwiseAndOperatorExpressionContext *> nEUG_BitwiseAndOperatorExpression();
    NEUG_BitwiseAndOperatorExpressionContext* nEUG_BitwiseAndOperatorExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  NEUG_BitwiseOrOperatorExpressionContext* nEUG_BitwiseOrOperatorExpression();

  class  NEUG_BitwiseAndOperatorExpressionContext : public antlr4::ParserRuleContext {
  public:
    NEUG_BitwiseAndOperatorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NEUG_BitShiftOperatorExpressionContext *> nEUG_BitShiftOperatorExpression();
    NEUG_BitShiftOperatorExpressionContext* nEUG_BitShiftOperatorExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  NEUG_BitwiseAndOperatorExpressionContext* nEUG_BitwiseAndOperatorExpression();

  class  NEUG_BitShiftOperatorExpressionContext : public antlr4::ParserRuleContext {
  public:
    NEUG_BitShiftOperatorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_AddOrSubtractExpressionContext *> oC_AddOrSubtractExpression();
    OC_AddOrSubtractExpressionContext* oC_AddOrSubtractExpression(size_t i);
    std::vector<NEUG_BitShiftOperatorContext *> nEUG_BitShiftOperator();
    NEUG_BitShiftOperatorContext* nEUG_BitShiftOperator(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  NEUG_BitShiftOperatorExpressionContext* nEUG_BitShiftOperatorExpression();

  class  NEUG_BitShiftOperatorContext : public antlr4::ParserRuleContext {
  public:
    NEUG_BitShiftOperatorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;

   
  };

  NEUG_BitShiftOperatorContext* nEUG_BitShiftOperator();

  class  OC_AddOrSubtractExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_AddOrSubtractExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_MultiplyDivideModuloExpressionContext *> oC_MultiplyDivideModuloExpression();
    OC_MultiplyDivideModuloExpressionContext* oC_MultiplyDivideModuloExpression(size_t i);
    std::vector<NEUG_AddOrSubtractOperatorContext *> nEUG_AddOrSubtractOperator();
    NEUG_AddOrSubtractOperatorContext* nEUG_AddOrSubtractOperator(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_AddOrSubtractExpressionContext* oC_AddOrSubtractExpression();

  class  NEUG_AddOrSubtractOperatorContext : public antlr4::ParserRuleContext {
  public:
    NEUG_AddOrSubtractOperatorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MINUS();

   
  };

  NEUG_AddOrSubtractOperatorContext* nEUG_AddOrSubtractOperator();

  class  OC_MultiplyDivideModuloExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_MultiplyDivideModuloExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_PowerOfExpressionContext *> oC_PowerOfExpression();
    OC_PowerOfExpressionContext* oC_PowerOfExpression(size_t i);
    std::vector<NEUG_MultiplyDivideModuloOperatorContext *> nEUG_MultiplyDivideModuloOperator();
    NEUG_MultiplyDivideModuloOperatorContext* nEUG_MultiplyDivideModuloOperator(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_MultiplyDivideModuloExpressionContext* oC_MultiplyDivideModuloExpression();

  class  NEUG_MultiplyDivideModuloOperatorContext : public antlr4::ParserRuleContext {
  public:
    NEUG_MultiplyDivideModuloOperatorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *STAR();

   
  };

  NEUG_MultiplyDivideModuloOperatorContext* nEUG_MultiplyDivideModuloOperator();

  class  OC_PowerOfExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_PowerOfExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_UnaryAddSubtractOrFactorialExpressionContext *> oC_UnaryAddSubtractOrFactorialExpression();
    OC_UnaryAddSubtractOrFactorialExpressionContext* oC_UnaryAddSubtractOrFactorialExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_PowerOfExpressionContext* oC_PowerOfExpression();

  class  OC_UnaryAddSubtractOrFactorialExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_UnaryAddSubtractOrFactorialExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_StringListNullOperatorExpressionContext *oC_StringListNullOperatorExpression();
    std::vector<antlr4::tree::TerminalNode *> MINUS();
    antlr4::tree::TerminalNode* MINUS(size_t i);
    antlr4::tree::TerminalNode *FACTORIAL();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_UnaryAddSubtractOrFactorialExpressionContext* oC_UnaryAddSubtractOrFactorialExpression();

  class  OC_StringListNullOperatorExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_StringListNullOperatorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_PropertyOrLabelsExpressionContext *oC_PropertyOrLabelsExpression();
    OC_StringOperatorExpressionContext *oC_StringOperatorExpression();
    OC_NullOperatorExpressionContext *oC_NullOperatorExpression();
    std::vector<OC_ListOperatorExpressionContext *> oC_ListOperatorExpression();
    OC_ListOperatorExpressionContext* oC_ListOperatorExpression(size_t i);

   
  };

  OC_StringListNullOperatorExpressionContext* oC_StringListNullOperatorExpression();

  class  OC_ListOperatorExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_ListOperatorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *IN();
    OC_PropertyOrLabelsExpressionContext *oC_PropertyOrLabelsExpression();
    std::vector<OC_ExpressionContext *> oC_Expression();
    OC_ExpressionContext* oC_Expression(size_t i);
    antlr4::tree::TerminalNode *COLON();

   
  };

  OC_ListOperatorExpressionContext* oC_ListOperatorExpression();

  class  OC_StringOperatorExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_StringOperatorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_PropertyOrLabelsExpressionContext *oC_PropertyOrLabelsExpression();
    OC_RegularExpressionContext *oC_RegularExpression();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *STARTS();
    antlr4::tree::TerminalNode *WITH();
    antlr4::tree::TerminalNode *ENDS();
    antlr4::tree::TerminalNode *CONTAINS();

   
  };

  OC_StringOperatorExpressionContext* oC_StringOperatorExpression();

  class  OC_RegularExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_RegularExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_RegularExpressionContext* oC_RegularExpression();

  class  OC_NullOperatorExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_NullOperatorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *IS();
    antlr4::tree::TerminalNode *NULL_();
    antlr4::tree::TerminalNode *NOT();

   
  };

  OC_NullOperatorExpressionContext* oC_NullOperatorExpression();

  class  OC_PropertyOrLabelsExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_PropertyOrLabelsExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_AtomContext *oC_Atom();
    std::vector<OC_PropertyLookupContext *> oC_PropertyLookup();
    OC_PropertyLookupContext* oC_PropertyLookup(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_PropertyOrLabelsExpressionContext* oC_PropertyOrLabelsExpression();

  class  OC_AtomContext : public antlr4::ParserRuleContext {
  public:
    OC_AtomContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_LiteralContext *oC_Literal();
    OC_ParameterContext *oC_Parameter();
    OC_CaseExpressionContext *oC_CaseExpression();
    OC_ParenthesizedExpressionContext *oC_ParenthesizedExpression();
    OC_FunctionInvocationContext *oC_FunctionInvocation();
    OC_PathPatternsContext *oC_PathPatterns();
    OC_ExistCountSubqueryContext *oC_ExistCountSubquery();
    OC_VariableContext *oC_Variable();
    OC_QuantifierContext *oC_Quantifier();

   
  };

  OC_AtomContext* oC_Atom();

  class  OC_QuantifierContext : public antlr4::ParserRuleContext {
  public:
    OC_QuantifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ALL();
    OC_FilterExpressionContext *oC_FilterExpression();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *ANY();
    antlr4::tree::TerminalNode *NONE();
    antlr4::tree::TerminalNode *SINGLE();

   
  };

  OC_QuantifierContext* oC_Quantifier();

  class  OC_FilterExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_FilterExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_IdInCollContext *oC_IdInColl();
    antlr4::tree::TerminalNode *SP();
    OC_WhereContext *oC_Where();

   
  };

  OC_FilterExpressionContext* oC_FilterExpression();

  class  OC_IdInCollContext : public antlr4::ParserRuleContext {
  public:
    OC_IdInCollContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_VariableContext *oC_Variable();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *IN();
    OC_ExpressionContext *oC_Expression();

   
  };

  OC_IdInCollContext* oC_IdInColl();

  class  OC_LiteralContext : public antlr4::ParserRuleContext {
  public:
    OC_LiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_NumberLiteralContext *oC_NumberLiteral();
    antlr4::tree::TerminalNode *StringLiteral();
    OC_BooleanLiteralContext *oC_BooleanLiteral();
    antlr4::tree::TerminalNode *NULL_();
    OC_ListLiteralContext *oC_ListLiteral();
    NEUG_StructLiteralContext *nEUG_StructLiteral();

   
  };

  OC_LiteralContext* oC_Literal();

  class  OC_BooleanLiteralContext : public antlr4::ParserRuleContext {
  public:
    OC_BooleanLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *BTRUE();
    antlr4::tree::TerminalNode *BFALSE();

   
  };

  OC_BooleanLiteralContext* oC_BooleanLiteral();

  class  OC_ListLiteralContext : public antlr4::ParserRuleContext {
  public:
    OC_ListLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_ExpressionContext *oC_Expression();
    std::vector<NEUG_ListEntryContext *> nEUG_ListEntry();
    NEUG_ListEntryContext* nEUG_ListEntry(size_t i);

   
  };

  OC_ListLiteralContext* oC_ListLiteral();

  class  NEUG_ListEntryContext : public antlr4::ParserRuleContext {
  public:
    NEUG_ListEntryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SP();
    OC_ExpressionContext *oC_Expression();

   
  };

  NEUG_ListEntryContext* nEUG_ListEntry();

  class  NEUG_StructLiteralContext : public antlr4::ParserRuleContext {
  public:
    NEUG_StructLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NEUG_StructFieldContext *> nEUG_StructField();
    NEUG_StructFieldContext* nEUG_StructField(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  NEUG_StructLiteralContext* nEUG_StructLiteral();

  class  NEUG_StructFieldContext : public antlr4::ParserRuleContext {
  public:
    NEUG_StructFieldContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COLON();
    OC_ExpressionContext *oC_Expression();
    OC_SymbolicNameContext *oC_SymbolicName();
    antlr4::tree::TerminalNode *StringLiteral();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  NEUG_StructFieldContext* nEUG_StructField();

  class  OC_ParenthesizedExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_ParenthesizedExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_ExpressionContext *oC_Expression();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_ParenthesizedExpressionContext* oC_ParenthesizedExpression();

  class  OC_FunctionInvocationContext : public antlr4::ParserRuleContext {
  public:
    OC_FunctionInvocationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COUNT();
    antlr4::tree::TerminalNode *STAR();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *CAST();
    std::vector<NEUG_FunctionParameterContext *> nEUG_FunctionParameter();
    NEUG_FunctionParameterContext* nEUG_FunctionParameter(size_t i);
    antlr4::tree::TerminalNode *AS();
    NEUG_DataTypeContext *nEUG_DataType();
    OC_FunctionNameContext *oC_FunctionName();
    antlr4::tree::TerminalNode *DISTINCT();

   
  };

  OC_FunctionInvocationContext* oC_FunctionInvocation();

  class  OC_FunctionNameContext : public antlr4::ParserRuleContext {
  public:
    OC_FunctionNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SymbolicNameContext *oC_SymbolicName();

   
  };

  OC_FunctionNameContext* oC_FunctionName();

  class  NEUG_FunctionParameterContext : public antlr4::ParserRuleContext {
  public:
    NEUG_FunctionParameterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_ExpressionContext *oC_Expression();
    OC_SymbolicNameContext *oC_SymbolicName();
    antlr4::tree::TerminalNode *COLON();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    NEUG_LambdaParameterContext *nEUG_LambdaParameter();

   
  };

  NEUG_FunctionParameterContext* nEUG_FunctionParameter();

  class  NEUG_LambdaParameterContext : public antlr4::ParserRuleContext {
  public:
    NEUG_LambdaParameterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NEUG_LambdaVarsContext *nEUG_LambdaVars();
    antlr4::tree::TerminalNode *MINUS();
    OC_ExpressionContext *oC_Expression();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  NEUG_LambdaParameterContext* nEUG_LambdaParameter();

  class  NEUG_LambdaVarsContext : public antlr4::ParserRuleContext {
  public:
    NEUG_LambdaVarsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_SymbolicNameContext *> oC_SymbolicName();
    OC_SymbolicNameContext* oC_SymbolicName(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  NEUG_LambdaVarsContext* nEUG_LambdaVars();

  class  OC_PathPatternsContext : public antlr4::ParserRuleContext {
  public:
    OC_PathPatternsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_NodePatternContext *oC_NodePattern();
    std::vector<OC_PatternElementChainContext *> oC_PatternElementChain();
    OC_PatternElementChainContext* oC_PatternElementChain(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_PathPatternsContext* oC_PathPatterns();

  class  OC_ExistCountSubqueryContext : public antlr4::ParserRuleContext {
  public:
    OC_ExistCountSubqueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MATCH();
    OC_PatternContext *oC_Pattern();
    antlr4::tree::TerminalNode *EXISTS();
    antlr4::tree::TerminalNode *COUNT();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_WhereContext *oC_Where();
    NEUG_HintContext *nEUG_Hint();

   
  };

  OC_ExistCountSubqueryContext* oC_ExistCountSubquery();

  class  OC_PropertyLookupContext : public antlr4::ParserRuleContext {
  public:
    OC_PropertyLookupContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_PropertyKeyNameContext *oC_PropertyKeyName();
    antlr4::tree::TerminalNode *STAR();
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_PropertyLookupContext* oC_PropertyLookup();

  class  OC_CaseExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_CaseExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *END();
    antlr4::tree::TerminalNode *ELSE();
    std::vector<OC_ExpressionContext *> oC_Expression();
    OC_ExpressionContext* oC_Expression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *CASE();
    std::vector<OC_CaseAlternativeContext *> oC_CaseAlternative();
    OC_CaseAlternativeContext* oC_CaseAlternative(size_t i);

   
  };

  OC_CaseExpressionContext* oC_CaseExpression();

  class  OC_CaseAlternativeContext : public antlr4::ParserRuleContext {
  public:
    OC_CaseAlternativeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WHEN();
    std::vector<OC_ExpressionContext *> oC_Expression();
    OC_ExpressionContext* oC_Expression(size_t i);
    antlr4::tree::TerminalNode *THEN();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_CaseAlternativeContext* oC_CaseAlternative();

  class  OC_VariableContext : public antlr4::ParserRuleContext {
  public:
    OC_VariableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SymbolicNameContext *oC_SymbolicName();

   
  };

  OC_VariableContext* oC_Variable();

  class  OC_NumberLiteralContext : public antlr4::ParserRuleContext {
  public:
    OC_NumberLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_DoubleLiteralContext *oC_DoubleLiteral();
    OC_IntegerLiteralContext *oC_IntegerLiteral();

   
  };

  OC_NumberLiteralContext* oC_NumberLiteral();

  class  OC_ParameterContext : public antlr4::ParserRuleContext {
  public:
    OC_ParameterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SymbolicNameContext *oC_SymbolicName();
    antlr4::tree::TerminalNode *DecimalInteger();

   
  };

  OC_ParameterContext* oC_Parameter();

  class  OC_PropertyExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_PropertyExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_AtomContext *oC_Atom();
    OC_PropertyLookupContext *oC_PropertyLookup();
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_PropertyExpressionContext* oC_PropertyExpression();

  class  OC_PropertyKeyNameContext : public antlr4::ParserRuleContext {
  public:
    OC_PropertyKeyNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SchemaNameContext *oC_SchemaName();

   
  };

  OC_PropertyKeyNameContext* oC_PropertyKeyName();

  class  OC_IntegerLiteralContext : public antlr4::ParserRuleContext {
  public:
    OC_IntegerLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DecimalInteger();

   
  };

  OC_IntegerLiteralContext* oC_IntegerLiteral();

  class  OC_DoubleLiteralContext : public antlr4::ParserRuleContext {
  public:
    OC_DoubleLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ExponentDecimalReal();
    antlr4::tree::TerminalNode *RegularDecimalReal();

   
  };

  OC_DoubleLiteralContext* oC_DoubleLiteral();

  class  OC_SchemaNameContext : public antlr4::ParserRuleContext {
  public:
    OC_SchemaNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SymbolicNameContext *oC_SymbolicName();

   
  };

  OC_SchemaNameContext* oC_SchemaName();

  class  OC_SymbolicNameContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *escapedsymbolicnameToken = nullptr;
    OC_SymbolicNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UnescapedSymbolicName();
    antlr4::tree::TerminalNode *EscapedSymbolicName();
    antlr4::tree::TerminalNode *HexLetter();
    NEUG_NonReservedKeywordsContext *nEUG_NonReservedKeywords();

   
  };

  OC_SymbolicNameContext* oC_SymbolicName();

  class  NEUG_NonReservedKeywordsContext : public antlr4::ParserRuleContext {
  public:
    NEUG_NonReservedKeywordsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COMMENT();
    antlr4::tree::TerminalNode *ADD();
    antlr4::tree::TerminalNode *ALTER();
    antlr4::tree::TerminalNode *AS();
    antlr4::tree::TerminalNode *ATTACH();
    antlr4::tree::TerminalNode *BEGIN();
    antlr4::tree::TerminalNode *BY();
    antlr4::tree::TerminalNode *CALL();
    antlr4::tree::TerminalNode *CHECKPOINT();
    antlr4::tree::TerminalNode *COMMIT();
    antlr4::tree::TerminalNode *CONTAINS();
    antlr4::tree::TerminalNode *COPY();
    antlr4::tree::TerminalNode *COUNT();
    antlr4::tree::TerminalNode *CYCLE();
    antlr4::tree::TerminalNode *DATABASE();
    antlr4::tree::TerminalNode *DECIMAL();
    antlr4::tree::TerminalNode *DELETE();
    antlr4::tree::TerminalNode *DETACH();
    antlr4::tree::TerminalNode *DROP();
    antlr4::tree::TerminalNode *EXPLAIN();
    antlr4::tree::TerminalNode *EXPORT();
    antlr4::tree::TerminalNode *EXTENSION();
    antlr4::tree::TerminalNode *GRAPH();
    antlr4::tree::TerminalNode *IF();
    antlr4::tree::TerminalNode *IS();
    antlr4::tree::TerminalNode *IMPORT();
    antlr4::tree::TerminalNode *INCREMENT();
    antlr4::tree::TerminalNode *KEY();
    antlr4::tree::TerminalNode *LOAD();
    antlr4::tree::TerminalNode *LOGICAL();
    antlr4::tree::TerminalNode *MATCH();
    antlr4::tree::TerminalNode *MAXVALUE();
    antlr4::tree::TerminalNode *MERGE();
    antlr4::tree::TerminalNode *MINVALUE();
    antlr4::tree::TerminalNode *NO();
    antlr4::tree::TerminalNode *NODE();
    antlr4::tree::TerminalNode *PROJECT();
    antlr4::tree::TerminalNode *READ();
    antlr4::tree::TerminalNode *REL();
    antlr4::tree::TerminalNode *RENAME();
    antlr4::tree::TerminalNode *RETURN();
    antlr4::tree::TerminalNode *ROLLBACK();
    antlr4::tree::TerminalNode *SEQUENCE();
    antlr4::tree::TerminalNode *SET();
    antlr4::tree::TerminalNode *START();
    antlr4::tree::TerminalNode *L_SKIP();
    antlr4::tree::TerminalNode *LIMIT();
    antlr4::tree::TerminalNode *TRANSACTION();
    antlr4::tree::TerminalNode *TYPE();
    antlr4::tree::TerminalNode *USE();
    antlr4::tree::TerminalNode *WRITE();
    antlr4::tree::TerminalNode *FROM();
    antlr4::tree::TerminalNode *TO();
    antlr4::tree::TerminalNode *YIELD();

   
  };

  NEUG_NonReservedKeywordsContext* nEUG_NonReservedKeywords();

  class  OC_LeftArrowHeadContext : public antlr4::ParserRuleContext {
  public:
    OC_LeftArrowHeadContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;

   
  };

  OC_LeftArrowHeadContext* oC_LeftArrowHead();

  class  OC_RightArrowHeadContext : public antlr4::ParserRuleContext {
  public:
    OC_RightArrowHeadContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;

   
  };

  OC_RightArrowHeadContext* oC_RightArrowHead();

  class  OC_DashContext : public antlr4::ParserRuleContext {
  public:
    OC_DashContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MINUS();

   
  };

  OC_DashContext* oC_Dash();


  bool sempred(antlr4::RuleContext *_localctx, size_t ruleIndex, size_t predicateIndex) override;

  bool nEUG_DataTypeSempred(NEUG_DataTypeContext *_localctx, size_t predicateIndex);
  bool nEUG_JoinNodeSempred(NEUG_JoinNodeContext *_localctx, size_t predicateIndex);

  // By default the static state used to implement the parser is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:

      virtual void notifyQueryNotConcludeWithReturn(antlr4::Token* startToken) {};
      virtual void notifyNodePatternWithoutParentheses(std::string nodeName, antlr4::Token* startToken) {};
      virtual void notifyInvalidNotEqualOperator(antlr4::Token* startToken) {};
      virtual void notifyEmptyToken(antlr4::Token* startToken) {};
      virtual void notifyReturnNotAtEnd(antlr4::Token* startToken) {};
      virtual void notifyNonBinaryComparison(antlr4::Token* startToken) {};

};

