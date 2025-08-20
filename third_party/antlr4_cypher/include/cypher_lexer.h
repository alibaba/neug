
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
    EXISTS = 84, EXPLAIN = 85, EXPORT = 86, EXTENSION = 87, FALSE = 88, 
    FROM = 89, GLOB = 90, GRAPH = 91, GROUP = 92, HEADERS = 93, HINT = 94, 
    IMPORT = 95, IF = 96, IN = 97, INCREMENT = 98, INSTALL = 99, IS = 100, 
    JOIN = 101, KEY = 102, LIMIT = 103, LOAD = 104, LOGICAL = 105, MACRO = 106, 
    MATCH = 107, MAXVALUE = 108, MERGE = 109, MINVALUE = 110, MULTI_JOIN = 111, 
    NO = 112, NODE = 113, NOT = 114, NONE = 115, NULL_ = 116, ON = 117, 
    ONLY = 118, OPTIONAL = 119, OR = 120, ORDER = 121, PRIMARY = 122, PROFILE = 123, 
    PROJECT = 124, READ = 125, REL = 126, RENAME = 127, RETURN = 128, ROLLBACK = 129, 
    ROLLBACK_SKIP_CHECKPOINT = 130, SEQUENCE = 131, SET = 132, SHORTEST = 133, 
    START = 134, STARTS = 135, TABLE = 136, THEN = 137, TO = 138, TRAIL = 139, 
    TRANSACTION = 140, TRUE = 141, TYPE = 142, UNION = 143, UNWIND = 144, 
    USE = 145, WHEN = 146, WHERE = 147, WITH = 148, WRITE = 149, WSHORTEST = 150, 
    XOR = 151, SINGLE = 152, YIELD = 153, DECIMAL = 154, STAR = 155, L_SKIP = 156, 
    INVALID_NOT_EQUAL = 157, MINUS = 158, FACTORIAL = 159, COLON = 160, 
    StringLiteral = 161, EscapedChar = 162, DecimalInteger = 163, HexLetter = 164, 
    HexDigit = 165, Digit = 166, NonZeroDigit = 167, NonZeroOctDigit = 168, 
    ZeroDigit = 169, ExponentDecimalReal = 170, RegularDecimalReal = 171, 
    UnescapedSymbolicName = 172, IdentifierStart = 173, IdentifierPart = 174, 
    EscapedSymbolicName = 175, SP = 176, WHITESPACE = 177, CypherComment = 178, 
    Unknown = 179
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

