/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace gbi {

using EIdType = int64_t;

enum class ValueType {
  kNull,
  kInt32,
  kInt64,
  kDouble,
  kString,
  kBoolean,
};

class Value {
 public:
  Value() : value_(std::monostate{}), is_orgininally_null(true) {}
  explicit Value(int32_t value) : value_(value) {}
  explicit Value(int64_t value) : value_(value) {}
  explicit Value(double value) : value_(value) {}
  explicit Value(std::string value) : value_(std::move(value)) {}

  static Value BOOLEAN(bool value) {
    return Value(static_cast<int32_t>(value ? 1 : 0));
  }
  static Value CreateValue(double value) { return Value(value); }

  bool IsNull() const { return std::holds_alternative<std::monostate>(value_); }

  std::string ToString() const {
    if (std::holds_alternative<std::monostate>(value_))
      return "";
    if (std::holds_alternative<int32_t>(value_))
      return std::to_string(std::get<int32_t>(value_));
    if (std::holds_alternative<int64_t>(value_))
      return std::to_string(std::get<int64_t>(value_));
    if (std::holds_alternative<double>(value_)) {
      std::ostringstream oss;
      oss << std::get<double>(value_);
      return oss.str();
    }
    return std::get<std::string>(value_);
  }

  bool operator==(const Value& other) const {
    if (IsNull() || other.IsNull())
      return IsNull() && other.IsNull();
    double lhs = 0;
    double rhs = 0;
    if (TryDouble(lhs) && other.TryDouble(rhs))
      return lhs == rhs;
    return ToString() == other.ToString();
  }

  bool operator<(const Value& other) const {
    if (IsNull() || other.IsNull())
      return false;
    double lhs = 0;
    double rhs = 0;
    if (TryDouble(lhs) && other.TryDouble(rhs))
      return lhs < rhs;
    return ToString() < other.ToString();
  }

  bool operator>(const Value& other) const { return other < *this; }
  bool operator<=(const Value& other) const { return !(other < *this); }
  bool operator>=(const Value& other) const { return !(*this < other); }

  bool is_orgininally_null = false;

 private:
  bool TryDouble(double& out) const {
    if (std::holds_alternative<int32_t>(value_)) {
      out = static_cast<double>(std::get<int32_t>(value_));
      return true;
    }
    if (std::holds_alternative<int64_t>(value_)) {
      out = static_cast<double>(std::get<int64_t>(value_));
      return true;
    }
    if (std::holds_alternative<double>(value_)) {
      out = std::get<double>(value_);
      return true;
    }
    if (std::holds_alternative<std::string>(value_)) {
      try {
        out = std::stod(std::get<std::string>(value_));
        return true;
      } catch (...) { return false; }
    }
    return false;
  }

  std::variant<std::monostate, int32_t, int64_t, double, std::string> value_;
};

inline Value GetDefaultValue(ValueType type) {
  switch (type) {
  case ValueType::kInt32:
    return Value(int32_t{0});
  case ValueType::kInt64:
    return Value(int64_t{0});
  case ValueType::kDouble:
    return Value(0.0);
  case ValueType::kString:
    return Value(std::string{});
  case ValueType::kBoolean:
    return Value::BOOLEAN(false);
  case ValueType::kNull:
  default:
    return Value();
  }
}

enum class CompType {
  COMP_EQUAL,
  COMP_GREATER,
  COMP_LESS,
  COMP_GREATER_EQUAL,
  COMP_LESS_EQUAL,
  COMP_IN,
  COMP_NOT_IN,
  COMP_IS_NULL,
  COMP_IS_NOT_NULL,
};

struct ExprNode {
  using PropertyResolver =
      std::function<Value(const std::string&, const std::string&)>;
  virtual ~ExprNode() = default;
  virtual Value Evaluate(const PropertyResolver& resolver) const = 0;
};

using ExprNodePtr = std::shared_ptr<ExprNode>;

class PropCons {
 public:
  PropCons() = default;
  PropCons(std::string prop_name, CompType comp_type, Value value)
      : _prop_name(std::move(prop_name)),
        _comp_type(comp_type),
        _value(std::move(value)) {}
  PropCons(ExprNodePtr left_expr, CompType comp_type, ExprNodePtr right_expr)
      : _comp_type(comp_type),
        _use_expr(true),
        _left_expr(std::move(left_expr)),
        _right_expr(std::move(right_expr)) {}

  bool CheckExpr(const ExprNode::PropertyResolver& resolver) const {
    if (!_use_expr || !_left_expr)
      return false;
    Value lhs = _left_expr->Evaluate(resolver);
    if (_comp_type == CompType::COMP_IS_NULL)
      return lhs.IsNull() || lhs.is_orgininally_null;
    if (_comp_type == CompType::COMP_IS_NOT_NULL)
      return !lhs.IsNull() && !lhs.is_orgininally_null;
    if (!_right_expr)
      return false;
    Value rhs = _right_expr->Evaluate(resolver);
    switch (_comp_type) {
    case CompType::COMP_EQUAL:
      return lhs == rhs;
    case CompType::COMP_GREATER:
      return lhs > rhs;
    case CompType::COMP_LESS:
      return lhs < rhs;
    case CompType::COMP_GREATER_EQUAL:
      return lhs >= rhs;
    case CompType::COMP_LESS_EQUAL:
      return lhs <= rhs;
    default:
      return false;
    }
  }

  std::string _prop_name;
  CompType _comp_type = CompType::COMP_EQUAL;
  Value _value;
  bool _use_expr = false;
  ExprNodePtr _left_expr;
  ExprNodePtr _right_expr;
};

struct SchemaGraph {
  std::vector<EIdType> label_vertex;
  std::unordered_map<EIdType, std::unordered_map<std::string, ValueType>>
      vertex_value_type_map;
  std::unordered_map<int, std::map<std::pair<int, int>, EIdType>> label_edge;
  std::unordered_map<EIdType, std::unordered_map<std::string, ValueType>>
      edge_value_type_map;
};

class PatternGraph {};

}  // namespace gbi
