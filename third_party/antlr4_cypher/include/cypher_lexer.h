
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
    IN = 96, INCREMENT = 97, INDEX = 98, INSTALL = 99, IS = 100, JOIN = 101,
    KEY = 102, LIMIT = 103, LOAD = 104, LOGICAL = 105, MACRO = 106, MATCH = 107,
    MAXVALUE = 108, MERGE = 109, MINVALUE = 110, MULTI_JOIN = 111, NAMESPACE = 112,
    NO = 113, NODE = 114, NOT = 115, NONE = 116, NULL_ = 117, ON = 118,
    ONLY = 119, OPTIONAL = 120, OR = 121, ORDER = 122, PRIMARY = 123, PROFILE = 124,
    PROJECT = 125, READ = 126, REL = 127, RENAME = 128, RETURN = 129, ROLLBACK = 130,
    ROLLBACK_SKIP_CHECKPOINT = 131, SEQUENCE = 132, SET = 133, SHORTEST = 134,
    START = 135, STARTS = 136, TABLE = 137, TEMP = 138, THEN = 139, TO = 140,
    TRAIL = 141, TRANSACTION = 142, TYPE = 143, UNINSTALL = 144, UNION = 145,
    USING = 146, UNWIND = 147, USE = 148, WHEN = 149, WHERE = 150, WITH = 151,
    WRITE = 152, WSHORTEST = 153, XOR = 154, SINGLE = 155, YIELD = 156,
    DECIMAL = 157, VARCHAR = 158, STAR = 159, L_SKIP = 160, INVALID_NOT_EQUAL = 161,
    MINUS = 162, FACTORIAL = 163, COLON = 164, BTRUE = 165, BFALSE = 166,
    StringLiteral = 167, EscapedChar = 168, DecimalInteger = 169, HexLetter = 170,
    HexDigit = 171, Digit = 172, NonZeroDigit = 173, NonZeroOctDigit = 174,
    ZeroDigit = 175, ExponentDecimalReal = 176, RegularDecimalReal = 177,
    UnescapedSymbolicName = 178, IdentifierStart = 179, IdentifierPart = 180,
    EscapedSymbolicName = 181, SP = 182, WHITESPACE = 183, CypherComment = 184,
    Unknown = 185
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

