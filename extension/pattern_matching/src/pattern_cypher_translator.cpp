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
using compiler_impl::Value;
using parser::MatchClause;
using parser::NodePattern;
using parser::ParsedExpression;
using parser::PatternElement;
using parser::RelPattern;

std::string trim_copy(std::string_view text) {
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

bool i_starts_with(std::string_view text, std::string_view prefix) {
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

bool is_ident_char(char ch) {
  return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_';
}

bool contains_keyword_outside_string(std::string_view text,
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
    if (i > 0 && is_ident_char(text[i - 1])) {
      continue;
    }
    if (i + keyword.size() < text.size() &&
        is_ident_char(text[i + keyword.size()])) {
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

std::string normalize_pattern_cypher_for_parser(std::string_view cypher) {
  std::string normalized = trim_copy(cypher);
  while (!normalized.empty() && normalized.back() == ';') {
    normalized.pop_back();
    normalized = trim_copy(normalized);
  }
  // Allow a bare pattern such as "(a)-[r:R]->(b)" without the leading MATCH
  // keyword: prepend "MATCH " when the input does not already begin with a
  // MATCH (or OPTIONAL MATCH) clause. This lets callers write
  // PATTERN_MATCH('(a)-[r:R]->(b)') instead of repeating "MATCH". The full
  // "MATCH ..." form is still accepted unchanged.
  if (!i_starts_with(normalized, "MATCH") &&
      !i_starts_with(normalized, "OPTIONAL")) {
    normalized = "MATCH " + normalized;
  }
  if ((i_starts_with(normalized, "MATCH") ||
       i_starts_with(normalized, "OPTIONAL")) &&
      !contains_keyword_outside_string(normalized, "RETURN")) {
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

  bool translate(const MatchClause& match_clause,
                 const parser::SingleQuery& single_query) {
    if (match_clause.getMatchClauseType() != common::MatchClauseType::MATCH) {
      return unsupported("OPTIONAL MATCH is not supported by PATTERN_MATCH");
    }
    if (match_clause.hasHint()) {
      return unsupported(
          "join hints inside PATTERN_MATCH input are not "
          "supported");
    }
    for (const auto& pattern_element : match_clause.getPatternElementsRef()) {
      if (!extract_pattern_element(pattern_element)) {
        return false;
      }
    }
    if (match_clause.hasWherePredicate() &&
        !extract_where_expression(*match_clause.getWherePredicate())) {
      return false;
    }
    if (single_query.hasReturnClause() &&
        !extract_return_clause(*single_query.getReturnClause())) {
      return false;
    }
    return true;
  }

  std::string emit_json() {
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
      add_constraints(obj, vertex.constraints);
      add_required_props(obj, vertex.required_props);
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
      add_constraints(obj, edge.constraints);
      add_required_props(obj, edge.required_props);
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
  bool unsupported(std::string message) {
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

  void add_constraints(rapidjson::Value& obj,
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

  void add_required_props(rapidjson::Value& obj,
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

  bool convert_value_to_json(const Value& value, rapidjson::Value* out,
                             bool negate = false) {
    if (value.isNull()) {
      return unsupported(
          "NULL literal is not supported in PATTERN_MATCH "
          "property constraints");
    }
    switch (value.getDataType().id()) {
    case DataTypeId::kBoolean:
      if (negate) {
        return unsupported(
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
        return unsupported("integer literal is out of range after negation");
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
          return unsupported(
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
        return unsupported(
            "string literals cannot be negated in "
            "PATTERN_MATCH property constraints");
      }
      const auto str = value.getValue<std::string>();
      out->SetString(str.c_str(), static_cast<rapidjson::SizeType>(str.size()),
                     doc_.GetAllocator());
      return true;
    }
    default:
      return unsupported(
          "only boolean, numeric, and string literals are "
          "supported in PATTERN_MATCH property constraints");
    }
  }

  bool convert_literal_expression_to_json(const ParsedExpression& expr,
                                          rapidjson::Value* out) {
    if (expr.getExpressionType() == ExpressionType::LITERAL) {
      const auto& literal = expr.constCast<parser::ParsedLiteralExpression>();
      return convert_value_to_json(literal.getValue(), out);
    }
    if (expr.getExpressionType() == ExpressionType::FUNCTION) {
      const auto& fn = expr.constCast<parser::ParsedFunctionExpression>();
      if (fn.getFunctionName() == "NEGATE" && fn.getNumChildren() == 1 &&
          fn.getChild(0)->getExpressionType() == ExpressionType::LITERAL) {
        const auto& literal =
            fn.getChild(0)->constCast<parser::ParsedLiteralExpression>();
        return convert_value_to_json(literal.getValue(), out, true);
      }
    }
    return unsupported(
        "PATTERN_MATCH property constraints only support "
        "literal values");
  }

  bool append_equality_constraints(
      std::vector<PatternConstraint>* constraints,
      const std::vector<parser::s_parsed_expr_pair>& property_key_vals) {
    for (const auto& [property, expr] : property_key_vals) {
      rapidjson::Value value;
      if (!convert_literal_expression_to_json(*expr, &value)) {
        return false;
      }
      constraints->push_back(
          PatternConstraint{property, "=", std::move(value)});
    }
    return true;
  }

  bool extract_pattern_element(const PatternElement& pattern_element) {
    if (pattern_element.hasPathName()) {
      return unsupported(
          "path variables such as 'p = (...)' are not "
          "supported by PATTERN_MATCH");
    }
    int left = -1;
    if (!get_or_create_vertex(*pattern_element.getFirstNodePattern(), &left)) {
      return false;
    }
    for (uint32_t i = 0; i < pattern_element.getNumPatternElementChains();
         ++i) {
      const auto* chain = pattern_element.getPatternElementChain(i);
      int right = -1;
      if (!get_or_create_vertex(*chain->getNodePattern(), &right)) {
        return false;
      }
      if (!add_edge(*chain->getRelPattern(), left, right)) {
        return false;
      }
      left = right;
    }
    return true;
  }

  bool get_or_create_vertex(const NodePattern& node, int* out_id) {
    const auto labels = node.getTableNames();
    if (labels.empty()) {
      return unsupported(
          "unlabeled node patterns are not supported by "
          "PATTERN_MATCH");
    }
    if (labels.size() != 1) {
      return unsupported(
          "multi-label node patterns are not supported by "
          "PATTERN_MATCH");
    }

    const std::string variable = node.getVariableName();
    if (!variable.empty() && edge_name_to_id_.contains(variable)) {
      return unsupported("variable '" + variable +
                         "' is already used as a relationship variable");
    }
    if (!variable.empty()) {
      auto it = vertex_name_to_id_.find(variable);
      if (it != vertex_name_to_id_.end()) {
        auto& existing = vertices_[it->second];
        if (existing.label != labels[0]) {
          return unsupported("node variable '" + variable +
                             "' has conflicting labels");
        }
        if (!append_equality_constraints(&existing.constraints,
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
    if (!append_equality_constraints(&vertex.constraints,
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

  bool add_edge(const RelPattern& rel, int left, int right) {
    if (rel.getRelType() != common::QueryRelType::NON_RECURSIVE) {
      return unsupported(
          "variable-length or recursive relationships are not "
          "supported by PATTERN_MATCH");
    }
    const auto rel_types = rel.getTableNames();
    if (rel_types.empty()) {
      return unsupported(
          "untyped relationship patterns are not supported by "
          "PATTERN_MATCH");
    }
    if (rel_types.size() != 1) {
      return unsupported(
          "multi-type relationship patterns are not supported "
          "by PATTERN_MATCH");
    }
    if (rel.getDirection() == parser::ArrowDirection::BOTH) {
      return unsupported(
          "undirected relationship patterns are not supported "
          "by PATTERN_MATCH; use '->' or '<-'");
    }

    const std::string variable = rel.getVariableName();
    if (!variable.empty()) {
      if (vertex_name_to_id_.contains(variable)) {
        return unsupported("variable '" + variable +
                           "' is already used as a node variable");
      }
      if (edge_name_to_id_.contains(variable)) {
        return unsupported("relationship variable '" + variable +
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
    if (!append_equality_constraints(&edge.constraints,
                                     rel.getPropertyKeyVals())) {
      return false;
    }
    if (!variable.empty()) {
      edge_name_to_id_[variable] = edge.id;
    }
    edges_.push_back(std::move(edge));
    return true;
  }

  std::optional<std::pair<std::string, std::string>> extract_property_ref(
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

  bool add_constraint_to_variable(std::string variable, std::string property,
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
    return unsupported("WHERE references unknown pattern variable '" +
                       variable + "'");
  }

  std::string op_string(ExpressionType type) const {
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

  std::string reverse_op_string(ExpressionType type) const {
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

  bool extract_where_expression(const ParsedExpression& expr) {
    if (expr.getExpressionType() == ExpressionType::AND) {
      if (expr.getNumChildren() != 2) {
        return unsupported("malformed AND expression in PATTERN_MATCH WHERE");
      }
      return extract_where_expression(*expr.getChild(0)) &&
             extract_where_expression(*expr.getChild(1));
    }
    if (expr.getExpressionType() == ExpressionType::OR ||
        expr.getExpressionType() == ExpressionType::XOR ||
        expr.getExpressionType() == ExpressionType::NOT) {
      return unsupported(
          "PATTERN_MATCH WHERE only supports AND-combined "
          "comparisons");
    }
    if (expr.getExpressionType() == ExpressionType::NOT_EQUALS) {
      return unsupported(
          "'<'>' comparisons are not supported by "
          "PATTERN_MATCH");
    }
    const std::string op = op_string(expr.getExpressionType());
    const std::string reverse_op = reverse_op_string(expr.getExpressionType());
    if (op.empty() || expr.getNumChildren() != 2) {
      return unsupported(
          "PATTERN_MATCH WHERE only supports comparisons of "
          "the form var.property OP literal");
    }

    const auto lhs_property = extract_property_ref(*expr.getChild(0));
    const auto rhs_property = extract_property_ref(*expr.getChild(1));
    if (lhs_property.has_value() && rhs_property.has_value()) {
      return unsupported(
          "cross-variable property comparisons are not "
          "supported by PATTERN_MATCH");
    }
    rapidjson::Value value;
    if (lhs_property.has_value()) {
      if (!convert_literal_expression_to_json(*expr.getChild(1), &value)) {
        return false;
      }
      return add_constraint_to_variable(
          lhs_property->first, lhs_property->second, op, std::move(value));
    }
    if (rhs_property.has_value()) {
      if (!convert_literal_expression_to_json(*expr.getChild(0), &value)) {
        return false;
      }
      return add_constraint_to_variable(rhs_property->first,
                                        rhs_property->second, reverse_op,
                                        std::move(value));
    }
    return unsupported(
        "PATTERN_MATCH WHERE only supports comparisons of the "
        "form var.property OP literal");
  }

  bool add_required_prop(std::string variable, std::string property,
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
    return unsupported(std::string(clause) +
                       " references unknown pattern variable '" + variable +
                       "'");
  }

  bool validate_projection_variable(const std::string& variable) {
    if (vertex_name_to_id_.contains(variable) ||
        edge_name_to_id_.contains(variable)) {
      return true;
    }
    return unsupported("RETURN references unknown pattern variable '" +
                       variable + "'");
  }

  bool extract_non_negative_integer(const ParsedExpression& expr, uint64_t* out,
                                    const char* clause) {
    if (expr.getExpressionType() != ExpressionType::LITERAL) {
      return unsupported(std::string("PATTERN_MATCH ") + clause +
                         " only supports non-negative integer literals");
    }
    const auto& literal = expr.constCast<parser::ParsedLiteralExpression>();
    const auto value = literal.getValue();
    if (value.isNull()) {
      return unsupported(std::string("PATTERN_MATCH ") + clause +
                         " cannot be NULL");
    }
    switch (value.getDataType().id()) {
    case DataTypeId::kInt8: {
      auto raw = value.getValue<int8_t>();
      if (raw < 0)
        return unsupported(std::string("PATTERN_MATCH ") + clause +
                           " cannot be negative");
      *out = static_cast<uint64_t>(raw);
      return true;
    }
    case DataTypeId::kInt16: {
      auto raw = value.getValue<int16_t>();
      if (raw < 0)
        return unsupported(std::string("PATTERN_MATCH ") + clause +
                           " cannot be negative");
      *out = static_cast<uint64_t>(raw);
      return true;
    }
    case DataTypeId::kInt32: {
      auto raw = value.getValue<int32_t>();
      if (raw < 0)
        return unsupported(std::string("PATTERN_MATCH ") + clause +
                           " cannot be negative");
      *out = static_cast<uint64_t>(raw);
      return true;
    }
    case DataTypeId::kInt64: {
      auto raw = value.getValue<int64_t>();
      if (raw < 0)
        return unsupported(std::string("PATTERN_MATCH ") + clause +
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
      return unsupported(std::string("PATTERN_MATCH ") + clause +
                         " only supports non-negative integer literals");
    }
  }

  bool extract_order_by_expression(const ParsedExpression& expr,
                                   bool ascending) {
    auto property_ref = extract_property_ref(expr);
    if (!property_ref.has_value()) {
      return unsupported(
          "PATTERN_MATCH ORDER BY only supports var.property expressions");
    }
    if (!add_required_prop(property_ref->first, property_ref->second,
                           "ORDER BY")) {
      return false;
    }
    order_by_.push_back(
        PatternOrderBy{property_ref->first, property_ref->second, ascending});
    return true;
  }

  bool extract_projection_body(const parser::ProjectionBody& body) {
    if (body.getIsDistinct()) {
      return unsupported(
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
        if (!validate_projection_variable(variable.getVariableName())) {
          return false;
        }
        continue;
      }
      auto property_ref = extract_property_ref(*expr);
      if (property_ref.has_value()) {
        if (!add_required_prop(property_ref->first, property_ref->second,
                               "RETURN")) {
          return false;
        }
        continue;
      }
      return unsupported(
          "PATTERN_MATCH input RETURN only supports '*', "
          "variables, or var.property");
    }
    if (body.hasOrderByExpressions()) {
      auto sort_orders = body.getSortOrders();
      const auto& order_exprs = body.getOrderByExpressions();
      if (sort_orders.size() != order_exprs.size()) {
        return unsupported("malformed ORDER BY expression in PATTERN_MATCH");
      }
      for (size_t i = 0; i < order_exprs.size(); ++i) {
        if (!extract_order_by_expression(*order_exprs[i], sort_orders[i])) {
          return false;
        }
      }
    }
    if (body.hasSkipExpression()) {
      if (!extract_non_negative_integer(*body.getSkipExpression(), &skip_,
                                        "SKIP")) {
        return false;
      }
      has_skip_ = true;
    }
    if (body.hasLimitExpression()) {
      if (!extract_non_negative_integer(*body.getLimitExpression(), &limit_,
                                        "LIMIT")) {
        return false;
      }
      has_limit_ = true;
    }
    return true;
  }

  bool extract_return_clause(const parser::ReturnClause& return_clause) {
    return extract_projection_body(*return_clause.getProjectionBody());
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

const MatchClause* extract_single_match_clause(
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

std::string translate_pattern_cypher_to_json(std::string_view dsl) {
  const std::string cypher = normalize_pattern_cypher_for_parser(dsl);
  try {
    auto statements = parser::Parser::parseQuery(cypher);
    const parser::SingleQuery* single_query = nullptr;
    std::string error;
    const MatchClause* match_clause =
        extract_single_match_clause(statements, &single_query, &error);
    if (match_clause == nullptr) {
      LOG(WARNING) << "[PATTERN_MATCH_CYPHER] unsupported input: " << error;
      return "";
    }
    OfficialCypherPatternTranslator translator;
    if (!translator.translate(*match_clause, *single_query)) {
      LOG(WARNING) << "[PATTERN_MATCH_CYPHER] unsupported input: "
                   << translator.error();
      return "";
    }
    return translator.emit_json();
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
