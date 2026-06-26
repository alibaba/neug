
// Generated from Cypher.g4 by ANTLR 4.13.1

#pragma once


#include "antlr4-runtime.h"




class  CypherLexer : public antlr4::Lexer {
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
    TABLE = 135, TEMP = 136, THEN = 137, TO = 138, TRAIL = 139, TRANSACTION = 140, 
    TYPE = 141, UNINSTALL = 142, UNION = 143, UNWIND = 144, USE = 145, WHEN = 146, 
    WHERE = 147, WITH = 148, WRITE = 149, WSHORTEST = 150, XOR = 151, SINGLE = 152, 
    YIELD = 153, DECIMAL = 154, VARCHAR = 155, STAR = 156, L_SKIP = 157, 
    INVALID_NOT_EQUAL = 158, MINUS = 159, FACTORIAL = 160, COLON = 161, 
    BTRUE = 162, BFALSE = 163, StringLiteral = 164, EscapedChar = 165, DecimalInteger = 166, 
    HexLetter = 167, HexDigit = 168, Digit = 169, NonZeroDigit = 170, NonZeroOctDigit = 171, 
    ZeroDigit = 172, ExponentDecimalReal = 173, RegularDecimalReal = 174, 
    UnescapedSymbolicName = 175, IdentifierStart = 176, IdentifierPart = 177, 
    EscapedSymbolicName = 178, SP = 179, WHITESPACE = 180, CypherComment = 181, 
    Unknown = 182
  };

  explicit CypherLexer(antlr4::CharStream *input);

  ~CypherLexer() override;


  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  // By default the static state used to implement the lexer is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:

  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

};

