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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "neug/compiler/common/cast.h"
#include "neug/compiler/common/copy_constructors.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/common/types/types.h"

namespace neug {

namespace common {
struct FileInfo;
class Serializer;
class Deserializer;
}  // namespace common

namespace parser {

class ParsedExpression;
class ParsedExpressionChildrenVisitor;
using parsed_expr_vector = std::vector<std::unique_ptr<ParsedExpression>>;
using parsed_expr_pair = std::pair<std::unique_ptr<ParsedExpression>,
                                   std::unique_ptr<ParsedExpression>>;
using s_parsed_expr_pair =
    std::pair<std::string, std::unique_ptr<ParsedExpression>>;

class NEUG_API ParsedExpression {
  friend class ParsedExpressionChildrenVisitor;

 public:
  ParsedExpression(common::ExpressionType type,
                   std::unique_ptr<ParsedExpression> child,
                   std::string rawName);
  ParsedExpression(common::ExpressionType type,
                   std::unique_ptr<ParsedExpression> left,
                   std::unique_ptr<ParsedExpression> right,
                   std::string rawName);
  ParsedExpression(common::ExpressionType type, std::string rawName)
      : type{type}, rawName{std::move(rawName)} {}
  explicit ParsedExpression(common::ExpressionType type) : type{type} {}

  ParsedExpression(common::ExpressionType type, std::string alias,
                   std::string rawName, parsed_expr_vector children)
      : type{type},
        alias{std::move(alias)},
        rawName{std::move(rawName)},
        children{std::move(children)} {}
  DELETE_COPY_DEFAULT_MOVE(ParsedExpression);
  virtual ~ParsedExpression() = default;

  common::ExpressionType getExpressionType() const { return type; }

  void setAlias(std::string name) { alias = std::move(name); }
  bool hasAlias() const { return !alias.empty(); }
  std::string getAlias() const { return alias; }

  std::string getRawName() const { return rawName; }

  common::idx_t getNumChildren() const { return children.size(); }
  ParsedExpression* getChild(common::idx_t idx) const {
    return children[idx].get();
  }
  void setChild(common::idx_t idx, std::unique_ptr<ParsedExpression> child) {
    NEUG_ASSERT(idx < children.size());
    children[idx] = std::move(child);
  }

  std::string toString() const { return rawName; }

  virtual std::unique_ptr<ParsedExpression> copy() const {
    return std::make_unique<ParsedExpression>(type, alias, rawName,
                                              copyVector(children));
  }

  void serialize(common::Serializer& serializer) const;

  static std::unique_ptr<ParsedExpression> deserialize(
      common::Deserializer& deserializer);

  template <class TARGET>
  TARGET& cast() {
    return common::neug_dynamic_cast<TARGET&>(*this);
  }
  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }
  template <class TARGET>
  const TARGET* constPtrCast() const {
    return common::neug_dynamic_cast<const TARGET*>(this);
  }

 private:
  virtual void serializeInternal(common::Serializer&) const {}

 protected:
  common::ExpressionType type;
  std::string alias;
  std::string rawName;
  parsed_expr_vector children;
};

using options_t =
    std::unordered_map<std::string, std::unique_ptr<parser::ParsedExpression>>;

}  // namespace parser
}  // namespace neug
