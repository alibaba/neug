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

#include "pattern_cypher_translator.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "neug/compiler/common/enums/clause_type.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/common/enums/query_rel_type.h"
#include "neug/compiler/common/enums/statement_type.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/parser/expression/parsed_expression.h"
#include "neug/compiler/parser/expression/parsed_function_expression.h"
#include "neug/compiler/parser/expression/parsed_literal_expression.h"
#include "neug/compiler/parser/expression/parsed_property_expression.h"
#include "neug/compiler/parser/expression/parsed_variable_expression.h"
#include "neug/compiler/parser/parser.h"
#include "neug/compiler/parser/query/graph_pattern/node_pattern.h"
#include "neug/compiler/parser/query/graph_pattern/pattern_element.h"
#include "neug/compiler/parser/query/graph_pattern/rel_pattern.h"
#include "neug/compiler/parser/query/reading_clause/match_clause.h"
#include "neug/compiler/parser/query/regular_query.h"
#include "neug/compiler/parser/query/return_with_clause/projection_body.h"
#include "neug/compiler/parser/query/return_with_clause/return_clause.h"
#include "neug/compiler/parser/query/single_query.h"

namespace neug {
namespace pattern_matching {

namespace {

using common::DataTypeId;
using common::ExpressionType;
using parser::MatchClause;
using parser::NodePattern;
using parser::ParsedExpression;
using parser::PatternElement;
using parser::RelPattern;

std::string TrimCopy(std::string_view text) {
  size_t begin = 0;
  while (begin < text.size() &&
         std::isspace(static_cast<unsigned char>(text[begin]))) {
    ++begin;
  }
  size_t end = text.size();
  while (end > begin &&
         std::isspace(static_cast<unsigned char>(text[end - 1]))) {
    --end;
  }
  return std::string(text.substr(begin, end - begin));
}

bool IStartsWith(std::string_view text, std::string_view prefix) {
  if (text.size() < prefix.size()) {
    return false;
  }
  for (size_t i = 0; i < prefix.size(); ++i) {
    if (std::toupper(static_cast<unsigned char>(text[i])) !=
        std::toupper(static_cast<unsigned char>(prefix[i]))) {
      return false;
    }
  }
  return true;
}

bool IsIdentChar(char ch) {
  return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_';
}

bool ContainsKeywordOutsideString(std::string_view text,
                                  std::string_view keyword) {
  bool in_single = false;
  bool in_double = false;
  bool in_backtick = false;
  for (size_t i = 0; i < text.size(); ++i) {
    const char ch = text[i];
    // Backslash escapes apply only inside '...'/"..." string literals; Cypher
    // backtick-quoted identifiers escape a literal backtick by doubling it, so
    // a stray backslash there must not consume the next character.
    if (ch == '\\' && (in_single || in_double)) {
      ++i;
      continue;
    }
    if (!in_double && !in_backtick && ch == '\'') {
      in_single = !in_single;
      continue;
    }
    if (!in_single && !in_backtick && ch == '"') {
      in_double = !in_double;
      continue;
    }
    // Backtick-quoted identifiers (e.g. `RETURN` as a property name) are not
    // keywords; skip their contents so they don't trip the keyword scan.
    if (!in_single && !in_double && ch == '`') {
      in_backtick = !in_backtick;
      continue;
    }
    if (in_single || in_double || in_backtick) {
      continue;
    }
    if (i > 0 && IsIdentChar(text[i - 1])) {
      continue;
    }
    if (i + keyword.size() < text.size() &&
        IsIdentChar(text[i + keyword.size()])) {
      continue;
    }
    if (i + keyword.size() > text.size()) {
      continue;
    }
    bool match = true;
    for (size_t k = 0; k < keyword.size(); ++k) {
      if (std::toupper(static_cast<unsigned char>(text[i + k])) !=
          std::toupper(static_cast<unsigned char>(keyword[k]))) {
        match = false;
        break;
      }
    }
    if (match) {
      return true;
    }
  }
  return false;
}

std::string NormalizePatternCypherForParser(std::string_view cypher) {
  std::string normalized = TrimCopy(cypher);
  while (!normalized.empty() && normalized.back() == ';') {
    normalized.pop_back();
    normalized = TrimCopy(normalized);
  }
  // Allow a bare pattern such as "(a)-[r:R]->(b)" without the leading MATCH
  // keyword: prepend "MATCH " when the input does not already begin with a
  // MATCH (or OPTIONAL MATCH) clause. This lets callers write
  // PATTERN_MATCH('(a)-[r:R]->(b)') instead of repeating "MATCH". The full
  // "MATCH ..." form is still accepted unchanged.
  if (!IStartsWith(normalized, "MATCH") &&
      !IStartsWith(normalized, "OPTIONAL")) {
    normalized = "MATCH " + normalized;
  }
  if ((IStartsWith(normalized, "MATCH") ||
       IStartsWith(normalized, "OPTIONAL")) &&
      !ContainsKeywordOutsideString(normalized, "RETURN")) {
    normalized += " RETURN *";
  }
  return normalized;
}

struct PatternConstraint {
  std::string property;
  std::string op;
  rapidjson::Value value;
};

struct PatternVertex {
  int id = -1;
  std::string variable;
  std::string label;
  std::vector<PatternConstraint> constraints;
  std::vector<std::string> required_props;
};

struct PatternEdge {
  int id = -1;
  int src = -1;
  int dst = -1;
  std::string variable;
  std::string label;
  std::vector<PatternConstraint> constraints;
  std::vector<std::string> required_props;
};

struct PatternOrderBy {
  std::string variable;
  std::string property;
  bool ascending = true;
};

class OfficialCypherPatternTranslator {
 public:
  OfficialCypherPatternTranslator() { doc_.SetObject(); }

  bool Translate(const MatchClause& match_clause,
                 const parser::SingleQuery& single_query) {
    if (match_clause.getMatchClauseType() != common::MatchClauseType::MATCH) {
      return Unsupported("OPTIONAL MATCH is not supported by PATTERN_MATCH");
    }
    if (match_clause.hasHint()) {
      return Unsupported(
          "join hints inside PATTERN_MATCH input are not "
          "supported");
    }
    for (const auto& pattern_element : match_clause.getPatternElementsRef()) {
      if (!ExtractPatternElement(pattern_element)) {
        return false;
      }
    }
    if (match_clause.hasWherePredicate() &&
        !ExtractWhereExpression(*match_clause.getWherePredicate())) {
      return false;
    }
    if (single_query.hasReturnClause() &&
        !ExtractReturnClause(*single_query.getReturnClause())) {
      return false;
    }
    return true;
  }

  std::string EmitJson() {
    doc_.RemoveAllMembers();
    auto& alloc = doc_.GetAllocator();
    rapidjson::Value vertices(rapidjson::kArrayType);
    for (const auto& vertex : vertices_) {
      rapidjson::Value obj(rapidjson::kObjectType);
      obj.AddMember("id", vertex.id, alloc);
      obj.AddMember("label", MakeString(vertex.label), alloc);
      if (!vertex.variable.empty()) {
        obj.AddMember("alias", MakeString(vertex.variable), alloc);
      }
      AddConstraints(obj, vertex.constraints);
      AddRequiredProps(obj, vertex.required_props);
      vertices.PushBack(obj, alloc);
    }
    doc_.AddMember("vertices", vertices, alloc);

    rapidjson::Value edges(rapidjson::kArrayType);
    for (const auto& edge : edges_) {
      rapidjson::Value obj(rapidjson::kObjectType);
      obj.AddMember("source", edge.src, alloc);
      obj.AddMember("target", edge.dst, alloc);
      obj.AddMember("label", MakeString(edge.label), alloc);
      if (!edge.variable.empty()) {
        obj.AddMember("alias", MakeString(edge.variable), alloc);
      }
      AddConstraints(obj, edge.constraints);
      AddRequiredProps(obj, edge.required_props);
      edges.PushBack(obj, alloc);
    }
    doc_.AddMember("edges", edges, alloc);

    if (!order_by_.empty()) {
      rapidjson::Value order_by(rapidjson::kArrayType);
      for (const auto& item : order_by_) {
        rapidjson::Value obj(rapidjson::kObjectType);
        obj.AddMember("variable", MakeString(item.variable), alloc);
        obj.AddMember("property", MakeString(item.property), alloc);
        obj.AddMember("ascending", item.ascending, alloc);
        order_by.PushBack(obj, alloc);
      }
      doc_.AddMember("order_by", order_by, alloc);
    }
    if (has_skip_) {
      doc_.AddMember("skip", skip_, alloc);
    }
    if (has_limit_) {
      doc_.AddMember("limit", limit_, alloc);
    }

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc_.Accept(writer);
    return buffer.GetString();
  }

  const std::string& error() const { return error_; }

 private:
  bool Unsupported(std::string message) {
    if (error_.empty()) {
      error_ = std::move(message);
    }
    return false;
  }

  rapidjson::Value MakeString(const std::string& text) {
    rapidjson::Value value;
    value.SetString(text.c_str(), static_cast<rapidjson::SizeType>(text.size()),
                    doc_.GetAllocator());
    return value;
  }

  void AddConstraints(rapidjson::Value& obj,
                      const std::vector<PatternConstraint>& constraints) {
    if (constraints.empty()) {
      return;
    }
    auto& alloc = doc_.GetAllocator();
    rapidjson::Value arr(rapidjson::kArrayType);
    for (const auto& constraint : constraints) {
      rapidjson::Value cobj(rapidjson::kObjectType);
      cobj.AddMember("property", MakeString(constraint.property), alloc);
      cobj.AddMember("operator", MakeString(constraint.op), alloc);
      rapidjson::Value copied;
      copied.CopyFrom(constraint.value, alloc);
      cobj.AddMember("value", copied, alloc);
      arr.PushBack(cobj, alloc);
    }
    obj.AddMember("constraints", arr, alloc);
  }

  void AddRequiredProps(rapidjson::Value& obj,
                        const std::vector<std::string>& props) {
    if (props.empty()) {
      return;
    }
    auto& alloc = doc_.GetAllocator();
    rapidjson::Value arr(rapidjson::kArrayType);
    for (const auto& prop : props) {
      arr.PushBack(MakeString(prop), alloc);
    }
    obj.AddMember("required_props", arr, alloc);
  }

  bool ConvertValueToJson(const common::Value& value, rapidjson::Value* out,
                          bool negate = false) {
    if (value.isNull()) {
      return Unsupported(
          "NULL literal is not supported in PATTERN_MATCH "
          "property constraints");
    }
    switch (value.getDataType().id()) {
    case DataTypeId::kBoolean:
      if (negate) {
        return Unsupported(
            "boolean literals cannot be negated in "
            "PATTERN_MATCH property constraints");
      }
      out->SetBool(value.getValue<bool>());
      return true;
    case DataTypeId::kInt8:
      out->SetInt(negate ? -static_cast<int>(value.getValue<int8_t>())
                         : static_cast<int>(value.getValue<int8_t>()));
      return true;
    case DataTypeId::kInt16:
      out->SetInt(negate ? -static_cast<int>(value.getValue<int16_t>())
                         : static_cast<int>(value.getValue<int16_t>()));
      return true;
    case DataTypeId::kInt32:
      out->SetInt(negate ? -value.getValue<int32_t>()
                         : value.getValue<int32_t>());
      return true;
    case DataTypeId::kInt64: {
      const int64_t raw = value.getValue<int64_t>();
      if (negate && raw == std::numeric_limits<int64_t>::min()) {
        return Unsupported("integer literal is out of range after negation");
      }
      out->SetInt64(negate ? -raw : raw);
      return true;
    }
    case DataTypeId::kUInt8:
      if (negate) {
        out->SetInt(-static_cast<int>(value.getValue<uint8_t>()));
      } else {
        out->SetUint(static_cast<unsigned>(value.getValue<uint8_t>()));
      }
      return true;
    case DataTypeId::kUInt16:
      if (negate) {
        out->SetInt(-static_cast<int>(value.getValue<uint16_t>()));
      } else {
        out->SetUint(static_cast<unsigned>(value.getValue<uint16_t>()));
      }
      return true;
    case DataTypeId::kUInt32: {
      const uint32_t raw = value.getValue<uint32_t>();
      if (negate) {
        if (raw > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
          out->SetInt64(-static_cast<int64_t>(raw));
        } else {
          out->SetInt(-static_cast<int>(raw));
        }
      } else {
        out->SetUint(raw);
      }
      return true;
    }
    case DataTypeId::kUInt64: {
      const uint64_t raw = value.getValue<uint64_t>();
      if (negate) {
        if (raw > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
          return Unsupported(
              "unsigned integer literal is out of range after "
              "negation");
        }
        out->SetInt64(-static_cast<int64_t>(raw));
      } else {
        out->SetUint64(raw);
      }
      return true;
    }
    case DataTypeId::kFloat:
      out->SetDouble(negate ? -static_cast<double>(value.getValue<float>())
                            : static_cast<double>(value.getValue<float>()));
      return true;
    case DataTypeId::kDouble:
      out->SetDouble(negate ? -value.getValue<double>()
                            : value.getValue<double>());
      return true;
    case DataTypeId::kVarchar: {
      if (negate) {
        return Unsupported(
            "string literals cannot be negated in "
            "PATTERN_MATCH property constraints");
      }
      const auto str = value.getValue<std::string>();
      out->SetString(str.c_str(), static_cast<rapidjson::SizeType>(str.size()),
                     doc_.GetAllocator());
      return true;
    }
    default:
      return Unsupported(
          "only boolean, numeric, and string literals are "
          "supported in PATTERN_MATCH property constraints");
    }
  }

  bool ConvertLiteralExpressionToJson(const ParsedExpression& expr,
                                      rapidjson::Value* out) {
    if (expr.getExpressionType() == ExpressionType::LITERAL) {
      const auto& literal = expr.constCast<parser::ParsedLiteralExpression>();
      return ConvertValueToJson(literal.getValue(), out);
    }
    if (expr.getExpressionType() == ExpressionType::FUNCTION) {
      const auto& fn = expr.constCast<parser::ParsedFunctionExpression>();
      if (fn.getFunctionName() == "NEGATE" && fn.getNumChildren() == 1 &&
          fn.getChild(0)->getExpressionType() == ExpressionType::LITERAL) {
        const auto& literal =
            fn.getChild(0)->constCast<parser::ParsedLiteralExpression>();
        return ConvertValueToJson(literal.getValue(), out, true);
      }
    }
    return Unsupported(
        "PATTERN_MATCH property constraints only support "
        "literal values");
  }

  bool AppendEqualityConstraints(
      std::vector<PatternConstraint>* constraints,
      const std::vector<parser::s_parsed_expr_pair>& property_key_vals) {
    for (const auto& [property, expr] : property_key_vals) {
      rapidjson::Value value;
      if (!ConvertLiteralExpressionToJson(*expr, &value)) {
        return false;
      }
      constraints->push_back(
          PatternConstraint{property, "=", std::move(value)});
    }
    return true;
  }

  bool ExtractPatternElement(const PatternElement& pattern_element) {
    if (pattern_element.hasPathName()) {
      return Unsupported(
          "path variables such as 'p = (...)' are not "
          "supported by PATTERN_MATCH");
    }
    int left = -1;
    if (!GetOrCreateVertex(*pattern_element.getFirstNodePattern(), &left)) {
      return false;
    }
    for (uint32_t i = 0; i < pattern_element.getNumPatternElementChains();
         ++i) {
      const auto* chain = pattern_element.getPatternElementChain(i);
      int right = -1;
      if (!GetOrCreateVertex(*chain->getNodePattern(), &right)) {
        return false;
      }
      if (!AddEdge(*chain->getRelPattern(), left, right)) {
        return false;
      }
      left = right;
    }
    return true;
  }

  bool GetOrCreateVertex(const NodePattern& node, int* out_id) {
    const auto labels = node.getTableNames();
    if (labels.empty()) {
      return Unsupported(
          "unlabeled node patterns are not supported by "
          "PATTERN_MATCH");
    }
    if (labels.size() != 1) {
      return Unsupported(
          "multi-label node patterns are not supported by "
          "PATTERN_MATCH");
    }

    const std::string variable = node.getVariableName();
    if (!variable.empty() && edge_name_to_id_.contains(variable)) {
      return Unsupported("variable '" + variable +
                         "' is already used as a relationship variable");
    }
    if (!variable.empty()) {
      auto it = vertex_name_to_id_.find(variable);
      if (it != vertex_name_to_id_.end()) {
        auto& existing = vertices_[it->second];
        if (existing.label != labels[0]) {
          return Unsupported("node variable '" + variable +
                             "' has conflicting labels");
        }
        if (!AppendEqualityConstraints(&existing.constraints,
                                       node.getPropertyKeyVals())) {
          return false;
        }
        *out_id = existing.id;
        return true;
      }
    }

    PatternVertex vertex;
    vertex.id = static_cast<int>(vertices_.size());
    vertex.variable = variable;
    vertex.label = labels[0];
    if (!AppendEqualityConstraints(&vertex.constraints,
                                   node.getPropertyKeyVals())) {
      return false;
    }
    if (!variable.empty()) {
      vertex_name_to_id_[variable] = vertex.id;
    }
    vertices_.push_back(std::move(vertex));
    *out_id = vertices_.back().id;
    return true;
  }

  bool AddEdge(const RelPattern& rel, int left, int right) {
    if (rel.getRelType() != common::QueryRelType::NON_RECURSIVE) {
      return Unsupported(
          "variable-length or recursive relationships are not "
          "supported by PATTERN_MATCH");
    }
    const auto rel_types = rel.getTableNames();
    if (rel_types.empty()) {
      return Unsupported(
          "untyped relationship patterns are not supported by "
          "PATTERN_MATCH");
    }
    if (rel_types.size() != 1) {
      return Unsupported(
          "multi-type relationship patterns are not supported "
          "by PATTERN_MATCH");
    }
    if (rel.getDirection() == parser::ArrowDirection::BOTH) {
      return Unsupported(
          "undirected relationship patterns are not supported "
          "by PATTERN_MATCH; use '->' or '<-'");
    }

    const std::string variable = rel.getVariableName();
    if (!variable.empty()) {
      if (vertex_name_to_id_.contains(variable)) {
        return Unsupported("variable '" + variable +
                           "' is already used as a node variable");
      }
      if (edge_name_to_id_.contains(variable)) {
        return Unsupported("relationship variable '" + variable +
                           "' is reused; distinct edges must use distinct "
                           "variables");
      }
    }

    PatternEdge edge;
    edge.id = static_cast<int>(edges_.size());
    edge.src =
        rel.getDirection() == parser::ArrowDirection::RIGHT ? left : right;
    edge.dst =
        rel.getDirection() == parser::ArrowDirection::RIGHT ? right : left;
    edge.variable = variable;
    edge.label = rel_types[0];
    if (!AppendEqualityConstraints(&edge.constraints,
                                   rel.getPropertyKeyVals())) {
      return false;
    }
    if (!variable.empty()) {
      edge_name_to_id_[variable] = edge.id;
    }
    edges_.push_back(std::move(edge));
    return true;
  }

  std::optional<std::pair<std::string, std::string>> ExtractPropertyRef(
      const ParsedExpression& expr) {
    if (expr.getExpressionType() != ExpressionType::PROPERTY) {
      return std::nullopt;
    }
    const auto& property = expr.constCast<parser::ParsedPropertyExpression>();
    if (property.isStar() || property.getNumChildren() != 1 ||
        property.getChild(0)->getExpressionType() != ExpressionType::VARIABLE) {
      return std::nullopt;
    }
    const auto& variable =
        property.getChild(0)->constCast<parser::ParsedVariableExpression>();
    return std::make_pair(variable.getVariableName(),
                          property.getPropertyName());
  }

  bool AddConstraintToVariable(std::string variable, std::string property,
                               std::string op, rapidjson::Value value) {
    auto vertex_it = vertex_name_to_id_.find(variable);
    if (vertex_it != vertex_name_to_id_.end()) {
      vertices_[vertex_it->second].constraints.push_back(PatternConstraint{
          std::move(property), std::move(op), std::move(value)});
      return true;
    }
    auto edge_it = edge_name_to_id_.find(variable);
    if (edge_it != edge_name_to_id_.end()) {
      edges_[edge_it->second].constraints.push_back(PatternConstraint{
          std::move(property), std::move(op), std::move(value)});
      return true;
    }
    return Unsupported("WHERE references unknown pattern variable '" +
                       variable + "'");
  }

  std::string OpString(ExpressionType type) const {
    switch (type) {
    case ExpressionType::EQUALS:
      return "=";
    case ExpressionType::GREATER_THAN:
      return ">";
    case ExpressionType::GREATER_THAN_EQUALS:
      return ">=";
    case ExpressionType::LESS_THAN:
      return "<";
    case ExpressionType::LESS_THAN_EQUALS:
      return "<=";
    default:
      return "";
    }
  }

  std::string ReverseOpString(ExpressionType type) const {
    switch (type) {
    case ExpressionType::EQUALS:
      return "=";
    case ExpressionType::GREATER_THAN:
      return "<";
    case ExpressionType::GREATER_THAN_EQUALS:
      return "<=";
    case ExpressionType::LESS_THAN:
      return ">";
    case ExpressionType::LESS_THAN_EQUALS:
      return ">=";
    default:
      return "";
    }
  }

  bool ExtractWhereExpression(const ParsedExpression& expr) {
    if (expr.getExpressionType() == ExpressionType::AND) {
      if (expr.getNumChildren() != 2) {
        return Unsupported("malformed AND expression in PATTERN_MATCH WHERE");
      }
      return ExtractWhereExpression(*expr.getChild(0)) &&
             ExtractWhereExpression(*expr.getChild(1));
    }
    if (expr.getExpressionType() == ExpressionType::OR ||
        expr.getExpressionType() == ExpressionType::XOR ||
        expr.getExpressionType() == ExpressionType::NOT) {
      return Unsupported(
          "PATTERN_MATCH WHERE only supports AND-combined "
          "comparisons");
    }
    if (expr.getExpressionType() == ExpressionType::NOT_EQUALS) {
      return Unsupported(
          "'<'>' comparisons are not supported by "
          "PATTERN_MATCH");
    }
    const std::string op = OpString(expr.getExpressionType());
    const std::string reverse_op = ReverseOpString(expr.getExpressionType());
    if (op.empty() || expr.getNumChildren() != 2) {
      return Unsupported(
          "PATTERN_MATCH WHERE only supports comparisons of "
          "the form var.property OP literal");
    }

    const auto lhs_property = ExtractPropertyRef(*expr.getChild(0));
    const auto rhs_property = ExtractPropertyRef(*expr.getChild(1));
    if (lhs_property.has_value() && rhs_property.has_value()) {
      return Unsupported(
          "cross-variable property comparisons are not "
          "supported by PATTERN_MATCH");
    }
    rapidjson::Value value;
    if (lhs_property.has_value()) {
      if (!ConvertLiteralExpressionToJson(*expr.getChild(1), &value)) {
        return false;
      }
      return AddConstraintToVariable(lhs_property->first, lhs_property->second,
                                     op, std::move(value));
    }
    if (rhs_property.has_value()) {
      if (!ConvertLiteralExpressionToJson(*expr.getChild(0), &value)) {
        return false;
      }
      return AddConstraintToVariable(rhs_property->first, rhs_property->second,
                                     reverse_op, std::move(value));
    }
    return Unsupported(
        "PATTERN_MATCH WHERE only supports comparisons of the "
        "form var.property OP literal");
  }

  bool AddRequiredProp(std::string variable, std::string property,
                       const char* clause) {
    auto vertex_it = vertex_name_to_id_.find(variable);
    if (vertex_it != vertex_name_to_id_.end()) {
      vertices_[vertex_it->second].required_props.push_back(
          std::move(property));
      return true;
    }
    auto edge_it = edge_name_to_id_.find(variable);
    if (edge_it != edge_name_to_id_.end()) {
      edges_[edge_it->second].required_props.push_back(std::move(property));
      return true;
    }
    return Unsupported(std::string(clause) +
                       " references unknown pattern variable '" + variable +
                       "'");
  }

  bool ValidateProjectionVariable(const std::string& variable) {
    if (vertex_name_to_id_.contains(variable) ||
        edge_name_to_id_.contains(variable)) {
      return true;
    }
    return Unsupported("RETURN references unknown pattern variable '" +
                       variable + "'");
  }

  bool ExtractNonNegativeInteger(const ParsedExpression& expr, uint64_t* out,
                                 const char* clause) {
    if (expr.getExpressionType() != ExpressionType::LITERAL) {
      return Unsupported(std::string("PATTERN_MATCH ") + clause +
                         " only supports non-negative integer literals");
    }
    const auto& literal = expr.constCast<parser::ParsedLiteralExpression>();
    const auto value = literal.getValue();
    if (value.isNull()) {
      return Unsupported(std::string("PATTERN_MATCH ") + clause +
                         " cannot be NULL");
    }
    switch (value.getDataType().id()) {
    case DataTypeId::kInt8: {
      auto raw = value.getValue<int8_t>();
      if (raw < 0)
        return Unsupported(std::string("PATTERN_MATCH ") + clause +
                           " cannot be negative");
      *out = static_cast<uint64_t>(raw);
      return true;
    }
    case DataTypeId::kInt16: {
      auto raw = value.getValue<int16_t>();
      if (raw < 0)
        return Unsupported(std::string("PATTERN_MATCH ") + clause +
                           " cannot be negative");
      *out = static_cast<uint64_t>(raw);
      return true;
    }
    case DataTypeId::kInt32: {
      auto raw = value.getValue<int32_t>();
      if (raw < 0)
        return Unsupported(std::string("PATTERN_MATCH ") + clause +
                           " cannot be negative");
      *out = static_cast<uint64_t>(raw);
      return true;
    }
    case DataTypeId::kInt64: {
      auto raw = value.getValue<int64_t>();
      if (raw < 0)
        return Unsupported(std::string("PATTERN_MATCH ") + clause +
                           " cannot be negative");
      *out = static_cast<uint64_t>(raw);
      return true;
    }
    case DataTypeId::kUInt8:
      *out = static_cast<uint64_t>(value.getValue<uint8_t>());
      return true;
    case DataTypeId::kUInt16:
      *out = static_cast<uint64_t>(value.getValue<uint16_t>());
      return true;
    case DataTypeId::kUInt32:
      *out = static_cast<uint64_t>(value.getValue<uint32_t>());
      return true;
    case DataTypeId::kUInt64:
      *out = value.getValue<uint64_t>();
      return true;
    default:
      return Unsupported(std::string("PATTERN_MATCH ") + clause +
                         " only supports non-negative integer literals");
    }
  }

  bool ExtractOrderByExpression(const ParsedExpression& expr, bool ascending) {
    auto property_ref = ExtractPropertyRef(expr);
    if (!property_ref.has_value()) {
      return Unsupported(
          "PATTERN_MATCH ORDER BY only supports var.property expressions");
    }
    if (!AddRequiredProp(property_ref->first, property_ref->second,
                         "ORDER BY")) {
      return false;
    }
    order_by_.push_back(
        PatternOrderBy{property_ref->first, property_ref->second, ascending});
    return true;
  }

  bool ExtractProjectionBody(const parser::ProjectionBody& body) {
    if (body.getIsDistinct()) {
      return Unsupported(
          "DISTINCT inside PATTERN_MATCH input is not "
          "supported");
    }
    for (const auto& expr : body.getProjectionExpressions()) {
      if (expr->getExpressionType() == ExpressionType::STAR) {
        continue;
      }
      if (expr->getExpressionType() == ExpressionType::VARIABLE) {
        const auto& variable =
            expr->constCast<parser::ParsedVariableExpression>();
        if (!ValidateProjectionVariable(variable.getVariableName())) {
          return false;
        }
        continue;
      }
      auto property_ref = ExtractPropertyRef(*expr);
      if (property_ref.has_value()) {
        if (!AddRequiredProp(property_ref->first, property_ref->second,
                             "RETURN")) {
          return false;
        }
        continue;
      }
      return Unsupported(
          "PATTERN_MATCH input RETURN only supports '*', "
          "variables, or var.property");
    }
    if (body.hasOrderByExpressions()) {
      auto sort_orders = body.getSortOrders();
      const auto& order_exprs = body.getOrderByExpressions();
      if (sort_orders.size() != order_exprs.size()) {
        return Unsupported("malformed ORDER BY expression in PATTERN_MATCH");
      }
      for (size_t i = 0; i < order_exprs.size(); ++i) {
        if (!ExtractOrderByExpression(*order_exprs[i], sort_orders[i])) {
          return false;
        }
      }
    }
    if (body.hasSkipExpression()) {
      if (!ExtractNonNegativeInteger(*body.getSkipExpression(), &skip_,
                                     "SKIP")) {
        return false;
      }
      has_skip_ = true;
    }
    if (body.hasLimitExpression()) {
      if (!ExtractNonNegativeInteger(*body.getLimitExpression(), &limit_,
                                     "LIMIT")) {
        return false;
      }
      has_limit_ = true;
    }
    return true;
  }

  bool ExtractReturnClause(const parser::ReturnClause& return_clause) {
    return ExtractProjectionBody(*return_clause.getProjectionBody());
  }

  rapidjson::Document doc_;
  std::vector<PatternVertex> vertices_;
  std::vector<PatternEdge> edges_;
  std::vector<PatternOrderBy> order_by_;
  uint64_t skip_ = 0;
  uint64_t limit_ = 0;
  bool has_skip_ = false;
  bool has_limit_ = false;
  std::unordered_map<std::string, int> vertex_name_to_id_;
  std::unordered_map<std::string, int> edge_name_to_id_;
  std::string error_;
};

const MatchClause* ExtractSingleMatchClause(
    const std::vector<std::shared_ptr<parser::Statement>>& statements,
    const parser::SingleQuery** single_query_out, std::string* error) {
  if (statements.size() != 1) {
    *error = "PATTERN_MATCH input must contain exactly one Cypher statement";
    return nullptr;
  }
  const auto& statement = statements[0];
  if (statement->getStatementType() != common::StatementType::QUERY) {
    *error = "PATTERN_MATCH input must be a MATCH query";
    return nullptr;
  }
  const auto* regular_query =
      dynamic_cast<const parser::RegularQuery*>(statement.get());
  if (regular_query == nullptr) {
    *error = "PATTERN_MATCH input is not a regular query";
    return nullptr;
  }
  if (!regular_query->getPreQueryPart().empty() ||
      !regular_query->getPreQueryExprs().empty() ||
      regular_query->getPostSingleQuery() != nullptr ||
      regular_query->getNumSingleQueries() != 1) {
    *error =
        "UNION, CALL subqueries, and multi-part Cypher queries are not "
        "supported by PATTERN_MATCH";
    return nullptr;
  }
  const auto* single_query = regular_query->getSingleQuery(0);
  if (single_query->getNumQueryParts() != 0 ||
      single_query->getNumUpdatingClauses() != 0) {
    *error = "WITH and updating clauses are not supported by PATTERN_MATCH";
    return nullptr;
  }
  if (single_query->getNumReadingClauses() != 1) {
    *error = "PATTERN_MATCH input must contain exactly one MATCH clause";
    return nullptr;
  }
  const auto* reading_clause = single_query->getReadingClause(0);
  if (reading_clause->getClauseType() != common::ClauseType::MATCH) {
    *error = "PATTERN_MATCH input must contain a MATCH clause";
    return nullptr;
  }
  *single_query_out = single_query;
  return &reading_clause->constCast<MatchClause>();
}

}  // namespace

std::string TranslatePatternCypherToJson(std::string_view dsl) {
  const std::string cypher = NormalizePatternCypherForParser(dsl);
  try {
    auto statements = parser::Parser::parseQuery(cypher);
    const parser::SingleQuery* single_query = nullptr;
    std::string error;
    const MatchClause* match_clause =
        ExtractSingleMatchClause(statements, &single_query, &error);
    if (match_clause == nullptr) {
      LOG(WARNING) << "[PATTERN_MATCH_CYPHER] unsupported input: " << error;
      return "";
    }
    OfficialCypherPatternTranslator translator;
    if (!translator.Translate(*match_clause, *single_query)) {
      LOG(WARNING) << "[PATTERN_MATCH_CYPHER] unsupported input: "
                   << translator.error();
      return "";
    }
    return translator.EmitJson();
  } catch (const std::exception& e) {
    LOG(WARNING) << "[PATTERN_MATCH_CYPHER] parse/translation failed: "
                 << e.what();
    return "";
  } catch (...) {
    LOG(WARNING) << "[PATTERN_MATCH_CYPHER] unknown parse/translation failure";
    return "";
  }
}

}  // namespace pattern_matching
}  // namespace neug
