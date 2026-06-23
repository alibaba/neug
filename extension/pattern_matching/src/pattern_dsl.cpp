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
 */

// ============================================================================
// Pattern DSL → pattern_matching JSON translator.
//
// Grammar (case-insensitive keywords; whitespace and `--` line comments
// ignored):
//
//   program     := MATCH pattern ( ',' pattern )*
//                  ( WHERE predicate ( AND predicate )* )?
//                  ( RETURN proj ( ',' proj )* )?
//                  EOF
//
//   pattern     := node ( rel node )*
//   node        := '(' var? ( ':' label )? prop_map? ')'
//   rel         := '-' rel_detail? '->'   |   '<-' rel_detail? '-'
//   rel_detail  := '[' var? ( ':' label )? prop_map? ']'
//   prop_map    := '{' prop_kv ( ',' prop_kv )* '}'
//   prop_kv     := IDENT ':' literal
//
//   predicate   := var '.' IDENT op literal
//   op          := '=' | '>' | '>=' | '<' | '<='
//
//   proj        := var ( '.' IDENT )?
//   literal     := INT | FLOAT | STRING | TRUE | FALSE
//
// What is intentionally rejected (each with a pointed error):
//   - undirected edges ('--'), variable-length ('*1..N'), OPTIONAL MATCH
//   - multi-label nodes ('(a:A:B)')
//   - WHERE OR / NOT / XOR, cross-variable comparisons, '<>', '!=', IN
//   - function calls, path variables, WITH / UNWIND / CREATE / etc.
//
// The matcher itself supports `in` / `not_in` only as runtime no-ops (see
// PatternMatchingTest.InOperatorIsNoOp), so we don't expose them through the
// DSL — surfacing them would be a footgun.
// ============================================================================

#include "pattern_dsl.h"

#include <cctype>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace neug {
namespace pattern_matching {

namespace {

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

enum class TokKind {
  Ident,       // identifier or keyword
  IntLit,
  FloatLit,
  StringLit,
  LParen,      // (
  RParen,      // )
  LBracket,    // [
  RBracket,    // ]
  LBrace,      // {
  RBrace,      // }
  Comma,       // ,
  Colon,       // :
  Dot,         // .
  Dash,        // -
  ArrowRight,  // ->
  ArrowLeft,   // <-
  Eq,          // =
  Lt,          // <
  Gt,          // >
  Leq,         // <=
  Geq,         // >=
  Neq,         // <>  (parsed only so we can reject with a clear message)
  Star,        // *   (only used to detect and reject variable-length paths)
  End,
};

struct Token {
  TokKind kind;
  std::string text;  // raw text for idents/literals or the punctuation glyph
  // For string/numeric literals, value-bearing form; for booleans, IsBool/
  // BoolValue carry the parsed bool.
  int line = 1;
  int col = 1;
};

struct ParseError : std::runtime_error {
  int line;
  int col;
  ParseError(int l, int c, const std::string& m)
      : std::runtime_error(m), line(l), col(c) {}
};

// Case-insensitive equals for ASCII (DSL keywords are ASCII-only).
bool IEquals(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(a[i])) !=
        std::tolower(static_cast<unsigned char>(b[i]))) {
      return false;
    }
  }
  return true;
}

bool IsIdentStart(char c) {
  return std::isalpha(static_cast<unsigned char>(c)) || c == '_';
}
bool IsIdentCont(char c) {
  return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

class Lexer {
 public:
  explicit Lexer(std::string_view src) : src_(src) {}

  std::vector<Token> Lex() {
    std::vector<Token> out;
    while (!Eof()) {
      SkipTrivia();
      if (Eof()) break;
      int sl = line_, sc = col_;
      char c = Peek();
      if (IsIdentStart(c)) {
        out.push_back(LexIdent(sl, sc));
      } else if (std::isdigit(static_cast<unsigned char>(c))) {
        out.push_back(LexNumber(sl, sc));
      } else if (c == '\'' || c == '"') {
        out.push_back(LexString(sl, sc));
      } else {
        out.push_back(LexPunct(sl, sc));
      }
    }
    out.push_back(Token{TokKind::End, "", line_, col_});
    return out;
  }

 private:
  void SkipTrivia() {
    while (!Eof()) {
      char c = Peek();
      if (c == ' ' || c == '\t' || c == '\r') {
        Advance();
      } else if (c == '\n') {
        Advance();
      } else if (c == '-' && pos_ + 1 < src_.size() && src_[pos_ + 1] == '-') {
        // Line comment, swallow until newline.
        while (!Eof() && Peek() != '\n') Advance();
      } else {
        break;
      }
    }
  }

  Token LexIdent(int sl, int sc) {
    std::string s;
    while (!Eof() && IsIdentCont(Peek())) {
      s.push_back(Peek());
      Advance();
    }
    return Token{TokKind::Ident, std::move(s), sl, sc};
  }

  Token LexNumber(int sl, int sc) {
    std::string s;
    while (!Eof() && std::isdigit(static_cast<unsigned char>(Peek()))) {
      s.push_back(Peek());
      Advance();
    }
    bool is_float = false;
    if (!Eof() && Peek() == '.') {
      // Only consume the dot if followed by another digit, to leave bare
      // `a.prop` style accesses alone.
      if (pos_ + 1 < src_.size() &&
          std::isdigit(static_cast<unsigned char>(src_[pos_ + 1]))) {
        is_float = true;
        s.push_back(Peek());
        Advance();
        while (!Eof() && std::isdigit(static_cast<unsigned char>(Peek()))) {
          s.push_back(Peek());
          Advance();
        }
      }
    }
    if (!Eof() && (Peek() == 'e' || Peek() == 'E')) {
      is_float = true;
      s.push_back(Peek());
      Advance();
      if (!Eof() && (Peek() == '+' || Peek() == '-')) {
        s.push_back(Peek());
        Advance();
      }
      if (Eof() || !std::isdigit(static_cast<unsigned char>(Peek()))) {
        throw ParseError(sl, sc, "malformed numeric literal '" + s + "'");
      }
      while (!Eof() && std::isdigit(static_cast<unsigned char>(Peek()))) {
        s.push_back(Peek());
        Advance();
      }
    }
    return Token{is_float ? TokKind::FloatLit : TokKind::IntLit,
                 std::move(s), sl, sc};
  }

  Token LexString(int sl, int sc) {
    char q = Peek();
    Advance();
    std::string s;
    while (!Eof() && Peek() != q) {
      char c = Peek();
      if (c == '\\') {
        Advance();
        if (Eof()) {
          throw ParseError(sl, sc, "unterminated string literal");
        }
        char esc = Peek();
        Advance();
        switch (esc) {
          case '\\': s.push_back('\\'); break;
          case '\'': s.push_back('\''); break;
          case '"':  s.push_back('"');  break;
          case 'n':  s.push_back('\n'); break;
          case 't':  s.push_back('\t'); break;
          case 'r':  s.push_back('\r'); break;
          default:
            throw ParseError(line_, col_,
                             std::string("unrecognized escape '\\") + esc +
                                 "'");
        }
      } else {
        s.push_back(c);
        Advance();
      }
    }
    if (Eof()) {
      throw ParseError(sl, sc, "unterminated string literal");
    }
    Advance();  // closing quote
    return Token{TokKind::StringLit, std::move(s), sl, sc};
  }

  Token LexPunct(int sl, int sc) {
    char c = Peek();
    Advance();
    switch (c) {
      case '(': return Token{TokKind::LParen,   "(", sl, sc};
      case ')': return Token{TokKind::RParen,   ")", sl, sc};
      case '[': return Token{TokKind::LBracket, "[", sl, sc};
      case ']': return Token{TokKind::RBracket, "]", sl, sc};
      case '{': return Token{TokKind::LBrace,   "{", sl, sc};
      case '}': return Token{TokKind::RBrace,   "}", sl, sc};
      case ',': return Token{TokKind::Comma,    ",", sl, sc};
      case ':': return Token{TokKind::Colon,    ":", sl, sc};
      case '.': return Token{TokKind::Dot,      ".", sl, sc};
      case '*': return Token{TokKind::Star,     "*", sl, sc};
      case '=': return Token{TokKind::Eq,       "=", sl, sc};
      case '-':
        if (!Eof() && Peek() == '>') {
          Advance();
          return Token{TokKind::ArrowRight, "->", sl, sc};
        }
        return Token{TokKind::Dash, "-", sl, sc};
      case '<':
        if (!Eof() && Peek() == '-') {
          Advance();
          return Token{TokKind::ArrowLeft, "<-", sl, sc};
        }
        if (!Eof() && Peek() == '=') {
          Advance();
          return Token{TokKind::Leq, "<=", sl, sc};
        }
        if (!Eof() && Peek() == '>') {
          Advance();
          return Token{TokKind::Neq, "<>", sl, sc};
        }
        return Token{TokKind::Lt, "<", sl, sc};
      case '>':
        if (!Eof() && Peek() == '=') {
          Advance();
          return Token{TokKind::Geq, ">=", sl, sc};
        }
        return Token{TokKind::Gt, ">", sl, sc};
    }
    throw ParseError(sl, sc,
                     std::string("unexpected character '") + c + "'");
  }

  bool Eof() const { return pos_ >= src_.size(); }
  char Peek() const { return src_[pos_]; }
  void Advance() {
    if (Eof()) return;
    if (src_[pos_] == '\n') {
      line_++;
      col_ = 1;
    } else {
      col_++;
    }
    pos_++;
  }

  std::string_view src_;
  size_t pos_ = 0;
  int line_ = 1;
  int col_ = 1;
};

// ---------------------------------------------------------------------------
// Intermediate representation + JSON emit
// ---------------------------------------------------------------------------

struct ConstraintEntry {
  std::string property;
  std::string op;
  rapidjson::Value value;
};

struct VertexInfo {
  int id = -1;
  std::string variable;
  std::string label;
  std::vector<ConstraintEntry> constraints;
  std::vector<std::string> required_props;
};

struct EdgeInfo {
  int idx = -1;
  std::string label;
  int src = -1;
  int dst = -1;
  std::string variable;
  std::vector<ConstraintEntry> constraints;
  std::vector<std::string> required_props;
};

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

class Parser {
 public:
  Parser(std::vector<Token> toks, rapidjson::Document::AllocatorType& alloc)
      : toks_(std::move(toks)), alloc_(alloc) {}

  void ParseProgram() {
    ExpectKeyword("MATCH");
    ParsePatternList();
    if (PeekIsKeyword("WHERE")) {
      Advance();
      ParseWhereClause();
    }
    if (PeekIsKeyword("RETURN")) {
      Advance();
      ParseReturnClause();
    }
    Expect(TokKind::End, "expected end of input");
  }

  std::string EmitJson() {
    rapidjson::Document doc(&alloc_);
    doc.SetObject();

    rapidjson::Value v_arr(rapidjson::kArrayType);
    for (const auto& v : vertices_) {
      if (v.label.empty()) {
        throw ParseError(0, 0,
                         "pattern node '" + RevLookupVar(v.id) +
                             "' has no label; every node must declare one "
                             "(e.g. '(a:Person)')");
      }
      rapidjson::Value obj(rapidjson::kObjectType);
      obj.AddMember("id", v.id, alloc_);
      if (!v.variable.empty() && v.variable.rfind("__anon_v", 0) != 0) {
        obj.AddMember("alias", MakeString(v.variable), alloc_);
      }
      obj.AddMember("label", MakeString(v.label), alloc_);
      AddConstraints(obj, v.constraints);
      AddRequiredProps(obj, v.required_props);
      v_arr.PushBack(obj, alloc_);
    }
    doc.AddMember("vertices", v_arr, alloc_);

    rapidjson::Value e_arr(rapidjson::kArrayType);
    for (const auto& e : edges_) {
      rapidjson::Value obj(rapidjson::kObjectType);
      obj.AddMember("source", e.src, alloc_);
      obj.AddMember("target", e.dst, alloc_);
      if (!e.variable.empty()) {
        obj.AddMember("alias", MakeString(e.variable), alloc_);
      }
      if (!e.label.empty()) {
        obj.AddMember("label", MakeString(e.label), alloc_);
      }
      AddConstraints(obj, e.constraints);
      AddRequiredProps(obj, e.required_props);
      e_arr.PushBack(obj, alloc_);
    }
    doc.AddMember("edges", e_arr, alloc_);

    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
    doc.Accept(writer);
    return buf.GetString();
  }

 private:
  // ---- pattern parsing ----

  void ParsePatternList() {
    ParsePattern();
    while (Peek().kind == TokKind::Comma) {
      Advance();
      ParsePattern();
    }
  }

  void ParsePattern() {
    int prev = ParseNode();
    while (PeekIsRelStart()) {
      auto rel = ParseRel();
      int cur = ParseNode();
      int src = rel.direction_right ? prev : cur;
      int dst = rel.direction_right ? cur : prev;
      AddEdge(rel, src, dst);
      prev = cur;
    }
  }

  bool PeekIsRelStart() {
    return Peek().kind == TokKind::Dash ||
           Peek().kind == TokKind::ArrowLeft;
  }

  struct RelInfo {
    bool direction_right;
    std::string variable;
    std::string label;
    std::vector<ConstraintEntry> constraints;
    Token at;
  };

  RelInfo ParseRel() {
    Token at = Peek();
    RelInfo info;
    info.at = at;

    if (Peek().kind == TokKind::Dash) {
      Advance();
      ParseOptionalRelDetail(info);
      // Now expect either '->' (right) or '-' (undirected, rejected).
      if (Peek().kind == TokKind::ArrowRight) {
        Advance();
        info.direction_right = true;
        return info;
      }
      if (Peek().kind == TokKind::Dash) {
        throw ParseError(Peek().line, Peek().col,
                         "undirected relationships ('--') are not "
                         "supported; use '->' or '<-'");
      }
      throw ParseError(Peek().line, Peek().col,
                       "expected '->' to close the relationship at this "
                       "position, found '" +
                           Peek().text + "'");
    }
    // ArrowLeft
    Advance();
    ParseOptionalRelDetail(info);
    Expect(TokKind::Dash,
           "expected '-' after relationship pattern starting with '<-'");
    info.direction_right = false;
    return info;
  }

  void ParseOptionalRelDetail(RelInfo& info) {
    if (Peek().kind != TokKind::LBracket) return;
    Advance();
    // [var? (':' label)? prop_map?]
    if (Peek().kind == TokKind::Ident) {
      info.variable = Peek().text;
      Advance();
    }
    if (Peek().kind == TokKind::Colon) {
      Advance();
      if (Peek().kind != TokKind::Ident) {
        throw ParseError(Peek().line, Peek().col,
                         "expected relationship label after ':' inside "
                         "'[...]'");
      }
      info.label = Peek().text;
      Advance();
      if (Peek().kind == TokKind::Star) {
        throw ParseError(Peek().line, Peek().col,
                         "variable-length relationships ('*1..N') are not "
                         "supported");
      }
    }
    if (Peek().kind == TokKind::LBrace) {
      ParsePropMap(info.constraints);
    }
    Expect(TokKind::RBracket, "expected ']' to close relationship pattern");
  }

  int ParseNode() {
    Token at = Peek();
    Expect(TokKind::LParen, "expected '(' to start a node pattern");

    std::string var;
    std::string label;
    std::vector<ConstraintEntry> inline_props;

    if (Peek().kind == TokKind::Ident) {
      var = Peek().text;
      Advance();
    }
    if (Peek().kind == TokKind::Colon) {
      Advance();
      if (Peek().kind != TokKind::Ident) {
        throw ParseError(Peek().line, Peek().col,
                         "expected node label after ':' inside '(...)'");
      }
      label = Peek().text;
      Advance();
      if (Peek().kind == TokKind::Colon) {
        throw ParseError(Peek().line, Peek().col,
                         "multi-label nodes ('(a:A:B)') are not supported");
      }
    }
    if (Peek().kind == TokKind::LBrace) {
      ParsePropMap(inline_props);
    }
    Expect(TokKind::RParen, "expected ')' to close node pattern");

    // Anonymous node — synthesize a unique name so it gets its own id.
    if (var.empty()) {
      var = "__anon_v" + std::to_string(anon_counter_++);
      // Append in-place (anonymous nodes can't be referenced again).
      return MakeVertex(var, label, std::move(inline_props), at);
    }
    auto it = var_to_vertex_.find(var);
    if (it != var_to_vertex_.end()) {
      // Re-use the existing vertex; tolerate empty repeat labels but reject
      // contradictions.
      VertexInfo& existing = vertices_[it->second];
      if (!label.empty()) {
        if (existing.label.empty()) {
          existing.label = label;
        } else if (existing.label != label) {
          throw ParseError(at.line, at.col,
                           "variable '" + var + "' has conflicting labels '" +
                               existing.label + "' and '" + label + "'");
        }
      }
      MergeConstraints(existing.constraints, std::move(inline_props));
      return existing.id;
    }
    return MakeVertex(var, label, std::move(inline_props), at);
  }

  int MakeVertex(const std::string& var, const std::string& label,
                 std::vector<ConstraintEntry>&& inline_props,
                 const Token& at) {
    (void)at;
    VertexInfo v;
    v.id = static_cast<int>(vertices_.size());
    v.variable = var;
    v.label = label;
    v.constraints = std::move(inline_props);
    var_to_vertex_[var] = v.id;
    vertex_to_var_[v.id] = var;
    vertices_.push_back(std::move(v));
    return vertices_.back().id;
  }

  void AddEdge(const RelInfo& rel, int src, int dst) {
    EdgeInfo e;
    e.idx = static_cast<int>(edges_.size());
    e.src = src;
    e.dst = dst;
    e.label = rel.label;
    e.variable = rel.variable;
    e.constraints = std::move(const_cast<RelInfo&>(rel).constraints);
    if (!e.variable.empty()) {
      if (var_to_edge_.count(e.variable)) {
        throw ParseError(rel.at.line, rel.at.col,
                         "relationship variable '" + e.variable +
                             "' is reused — distinct edges only");
      }
      if (var_to_vertex_.count(e.variable)) {
        throw ParseError(rel.at.line, rel.at.col,
                         "variable '" + e.variable +
                             "' is already used as a node variable");
      }
      var_to_edge_[e.variable] = e.idx;
    }
    edges_.push_back(std::move(e));
  }

  void ParsePropMap(std::vector<ConstraintEntry>& sink) {
    Expect(TokKind::LBrace, "expected '{'");
    bool first = true;
    while (Peek().kind != TokKind::RBrace) {
      if (!first) Expect(TokKind::Comma, "expected ',' between properties");
      first = false;
      if (Peek().kind != TokKind::Ident) {
        throw ParseError(Peek().line, Peek().col,
                         "expected property name inside '{...}'");
      }
      std::string prop = Peek().text;
      Token at = Peek();
      Advance();
      Expect(TokKind::Colon, "expected ':' after property name (inline "
                             "property maps use '{prop: literal}')");
      ConstraintEntry c;
      c.property = std::move(prop);
      c.op = "=";
      c.value = ParseLiteral(at);
      sink.push_back(std::move(c));
    }
    Advance();  // '}'
  }

  // ---- WHERE ----

  void ParseWhereClause() {
    ParsePredicate();
    while (PeekIsKeyword("AND")) {
      Advance();
      ParsePredicate();
    }
    if (PeekIsKeyword("OR") || PeekIsKeyword("NOT") ||
        PeekIsKeyword("XOR")) {
      throw ParseError(Peek().line, Peek().col,
                       "WHERE only accepts conjunctions of comparisons "
                       "(found '" +
                           Peek().text + "')");
    }
  }

  void ParsePredicate() {
    if (Peek().kind != TokKind::Ident) {
      throw ParseError(Peek().line, Peek().col,
                       "WHERE predicate must start with a variable");
    }
    Token var_tok = Peek();
    std::string var = var_tok.text;
    Advance();
    Expect(TokKind::Dot,
           "WHERE predicates use the form 'var.property OP literal'");
    if (Peek().kind != TokKind::Ident) {
      throw ParseError(Peek().line, Peek().col,
                       "expected property name after '.'");
    }
    std::string prop = Peek().text;
    Advance();

    std::string op;
    switch (Peek().kind) {
      case TokKind::Eq:  op = "=";  break;
      case TokKind::Gt:  op = ">";  break;
      case TokKind::Geq: op = ">="; break;
      case TokKind::Lt:  op = "<";  break;
      case TokKind::Leq: op = "<="; break;
      case TokKind::Neq:
        throw ParseError(Peek().line, Peek().col,
                         "'<>' is not supported; the matcher has no "
                         "inequality operator");
      default:
        // Catch 'IN' specifically since users might try it.
        if (Peek().kind == TokKind::Ident && IEquals(Peek().text, "in")) {
          throw ParseError(Peek().line, Peek().col,
                           "'IN' is not supported (the matcher treats it as "
                           "a runtime no-op)");
        }
        throw ParseError(Peek().line, Peek().col,
                         "expected comparison operator (=, >, >=, <, <=)");
    }
    Token op_tok = Peek();
    Advance();

    // Cross-variable comparison guard: RHS must be a literal.
    if (Peek().kind == TokKind::Ident) {
      throw ParseError(Peek().line, Peek().col,
                       "right-hand side of '" + op +
                           "' must be a literal (no cross-variable or "
                           "computed comparisons)");
    }
    ConstraintEntry c;
    c.property = std::move(prop);
    c.op = std::move(op);
    c.value = ParseLiteral(op_tok);

    AttachConstraint(var, var_tok, std::move(c));
  }

  void AttachConstraint(const std::string& var, const Token& at,
                        ConstraintEntry c) {
    auto v_it = var_to_vertex_.find(var);
    if (v_it != var_to_vertex_.end()) {
      vertices_[v_it->second].constraints.push_back(std::move(c));
      return;
    }
    auto e_it = var_to_edge_.find(var);
    if (e_it != var_to_edge_.end()) {
      edges_[e_it->second].constraints.push_back(std::move(c));
      return;
    }
    throw ParseError(at.line, at.col,
                     "unknown variable '" + var + "' in WHERE");
  }

  // ---- RETURN ----

  void ParseReturnClause() {
    ParseProjection();
    while (Peek().kind == TokKind::Comma) {
      Advance();
      ParseProjection();
    }
  }

  void ParseProjection() {
    if (Peek().kind != TokKind::Ident) {
      throw ParseError(Peek().line, Peek().col,
                       "RETURN entry must start with a variable name");
    }
    Token var_tok = Peek();
    std::string var = var_tok.text;
    Advance();
    if (Peek().kind == TokKind::Dot) {
      Advance();
      if (Peek().kind != TokKind::Ident) {
        throw ParseError(Peek().line, Peek().col,
                         "expected property name after '.' in RETURN");
      }
      std::string prop = Peek().text;
      Advance();
      auto v_it = var_to_vertex_.find(var);
      if (v_it != var_to_vertex_.end()) {
        vertices_[v_it->second].required_props.push_back(prop);
        return;
      }
      auto e_it = var_to_edge_.find(var);
      if (e_it != var_to_edge_.end()) {
        edges_[e_it->second].required_props.push_back(prop);
        return;
      }
      throw ParseError(var_tok.line, var_tok.col,
                       "RETURN references unknown variable '" + var + "'");
    }
    // Bare `RETURN var` — accept and ignore (no property is requested).
    if (var_to_vertex_.count(var) == 0 && var_to_edge_.count(var) == 0) {
      throw ParseError(var_tok.line, var_tok.col,
                       "RETURN references unknown variable '" + var + "'");
    }
  }

  // ---- literals ----

  rapidjson::Value ParseLiteral(const Token& at_token_for_errors) {
    (void)at_token_for_errors;
    Token t = Peek();
    rapidjson::Value v;
    switch (t.kind) {
      case TokKind::IntLit: {
        Advance();
        try {
          long long i = std::stoll(t.text);
          if (i > std::numeric_limits<int32_t>::max() ||
              i < std::numeric_limits<int32_t>::min()) {
            v.SetInt64(static_cast<int64_t>(i));
          } else {
            v.SetInt(static_cast<int>(i));
          }
        } catch (const std::exception&) {
          throw ParseError(t.line, t.col,
                           "integer literal '" + t.text + "' out of range");
        }
        return v;
      }
      case TokKind::FloatLit: {
        Advance();
        try {
          v.SetDouble(std::stod(t.text));
        } catch (const std::exception&) {
          throw ParseError(t.line, t.col,
                           "malformed floating literal '" + t.text + "'");
        }
        return v;
      }
      case TokKind::StringLit: {
        Advance();
        v.SetString(t.text.c_str(),
                    static_cast<rapidjson::SizeType>(t.text.size()), alloc_);
        return v;
      }
      case TokKind::Dash: {
        // Allow a leading minus on a numeric literal: '- 1' or '-1' both
        // valid since the lexer doesn't bind '-' to numbers.
        Advance();
        Token next = Peek();
        if (next.kind == TokKind::IntLit) {
          Advance();
          try {
            long long i = -std::stoll(next.text);
            if (i > std::numeric_limits<int32_t>::max() ||
                i < std::numeric_limits<int32_t>::min()) {
              v.SetInt64(static_cast<int64_t>(i));
            } else {
              v.SetInt(static_cast<int>(i));
            }
          } catch (const std::exception&) {
            throw ParseError(next.line, next.col,
                             "integer literal '-" + next.text +
                                 "' out of range");
          }
          return v;
        }
        if (next.kind == TokKind::FloatLit) {
          Advance();
          try {
            v.SetDouble(-std::stod(next.text));
          } catch (const std::exception&) {
            throw ParseError(next.line, next.col,
                             "malformed floating literal '-" + next.text +
                                 "'");
          }
          return v;
        }
        throw ParseError(t.line, t.col,
                         "expected numeric literal after '-'");
      }
      case TokKind::Ident: {
        if (IEquals(t.text, "true")) {
          Advance();
          v.SetBool(true);
          return v;
        }
        if (IEquals(t.text, "false")) {
          Advance();
          v.SetBool(false);
          return v;
        }
        throw ParseError(t.line, t.col,
                         "expected a literal value, found identifier '" +
                             t.text + "'");
      }
      default:
        throw ParseError(t.line, t.col,
                         "expected a literal value, found '" + t.text + "'");
    }
  }

  // ---- token plumbing ----

  const Token& Peek() const { return toks_[pos_]; }
  void Advance() {
    if (toks_[pos_].kind != TokKind::End) pos_++;
  }
  bool PeekIsKeyword(const char* kw) {
    return Peek().kind == TokKind::Ident && IEquals(Peek().text, kw);
  }
  void Expect(TokKind k, const std::string& msg) {
    if (Peek().kind != k) {
      throw ParseError(Peek().line, Peek().col,
                       msg + " (found '" + Peek().text + "')");
    }
    Advance();
  }
  void ExpectKeyword(const char* kw) {
    if (!PeekIsKeyword(kw)) {
      throw ParseError(Peek().line, Peek().col,
                       std::string("expected keyword '") + kw +
                           "' at top of query, found '" + Peek().text + "'");
    }
    Advance();
  }

  // ---- emit helpers ----

  rapidjson::Value MakeString(const std::string& s) {
    rapidjson::Value v;
    v.SetString(s.c_str(), static_cast<rapidjson::SizeType>(s.size()),
                alloc_);
    return v;
  }
  void AddConstraints(rapidjson::Value& obj,
                      const std::vector<ConstraintEntry>& cs) {
    if (cs.empty()) return;
    rapidjson::Value arr(rapidjson::kArrayType);
    for (const auto& c : cs) {
      rapidjson::Value cobj(rapidjson::kObjectType);
      cobj.AddMember("property", MakeString(c.property), alloc_);
      cobj.AddMember("operator", MakeString(c.op), alloc_);
      rapidjson::Value vv;
      vv.CopyFrom(c.value, alloc_);
      cobj.AddMember("value", vv, alloc_);
      arr.PushBack(cobj, alloc_);
    }
    obj.AddMember("constraints", arr, alloc_);
  }
  void AddRequiredProps(rapidjson::Value& obj,
                        const std::vector<std::string>& props) {
    if (props.empty()) return;
    rapidjson::Value arr(rapidjson::kArrayType);
    for (const auto& p : props) arr.PushBack(MakeString(p), alloc_);
    obj.AddMember("required_props", arr, alloc_);
  }
  void MergeConstraints(std::vector<ConstraintEntry>& dst,
                        std::vector<ConstraintEntry>&& src) {
    for (auto& c : src) dst.push_back(std::move(c));
  }
  std::string RevLookupVar(int id) const {
    auto it = vertex_to_var_.find(id);
    return it == vertex_to_var_.end() ? std::to_string(id) : it->second;
  }

  std::vector<Token> toks_;
  size_t pos_ = 0;
  rapidjson::Document::AllocatorType& alloc_;
  std::vector<VertexInfo> vertices_;
  std::vector<EdgeInfo> edges_;
  std::unordered_map<std::string, int> var_to_vertex_;
  std::unordered_map<int, std::string> vertex_to_var_;
  std::unordered_map<std::string, int> var_to_edge_;
  int anon_counter_ = 0;
};

}  // namespace

std::string TranslatePatternDslToJson(std::string_view dsl) {
  try {
    rapidjson::Document doc;
    auto& alloc = doc.GetAllocator();
    Lexer lex(dsl);
    auto toks = lex.Lex();
    Parser p(std::move(toks), alloc);
    p.ParseProgram();
    return p.EmitJson();
  } catch (const ParseError& e) {
    LOG(WARNING) << "[SAMPLED_PATTERN_MATCH_DSL] parse error at " << e.line << ":"
                 << e.col << " — " << e.what();
    return "";
  } catch (const std::exception& e) {
    LOG(WARNING) << "[SAMPLED_PATTERN_MATCH_DSL] " << e.what();
    return "";
  } catch (...) {
    LOG(WARNING) << "[SAMPLED_PATTERN_MATCH_DSL] unknown error during translation";
    return "";
  }
}

}  // namespace pattern_matching
}  // namespace neug
