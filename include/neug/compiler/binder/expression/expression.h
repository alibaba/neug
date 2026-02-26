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

#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/cast.h"
#include "neug/compiler/common/copy_constructors.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/common/types/types.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace binder {

class Expression;
using expression_vector = std::vector<std::shared_ptr<Expression>>;
using expression_pair =
    std::pair<std::shared_ptr<Expression>, std::shared_ptr<Expression>>;

struct ExpressionHasher;
struct ExpressionEquality;
using expression_set = std::unordered_set<std::shared_ptr<Expression>,
                                          ExpressionHasher, ExpressionEquality>;
template <typename T>
using expression_map = std::unordered_map<std::shared_ptr<Expression>, T,
                                          ExpressionHasher, ExpressionEquality>;

class NEUG_API Expression : public std::enable_shared_from_this<Expression> {
  friend class ExpressionChildrenCollector;

 public:
  Expression(common::ExpressionType expressionType,
             common::LogicalType dataType, expression_vector children,
             std::string uniqueName)
      : expressionType{expressionType},
        dataType{std::move(dataType)},
        uniqueName{std::move(uniqueName)},
        children{std::move(children)} {}
  // Create binary expression.
  Expression(common::ExpressionType expressionType,
             common::LogicalType dataType,
             const std::shared_ptr<Expression>& left,
             const std::shared_ptr<Expression>& right, std::string uniqueName)
      : Expression{expressionType, std::move(dataType),
                   expression_vector{left, right}, std::move(uniqueName)} {}
  // Create unary expression.
  Expression(common::ExpressionType expressionType,
             common::LogicalType dataType,
             const std::shared_ptr<Expression>& child, std::string uniqueName)
      : Expression{expressionType, std::move(dataType),
                   expression_vector{child}, std::move(uniqueName)} {}
  // Create leaf expression
  Expression(common::ExpressionType expressionType,
             common::LogicalType dataType, std::string uniqueName)
      : Expression{expressionType, std::move(dataType), expression_vector{},
                   std::move(uniqueName)} {}
  DELETE_COPY_DEFAULT_MOVE(Expression);
  virtual ~Expression() = default;

  void setUniqueName(const std::string& name) { uniqueName = name; }
  std::string getUniqueName() const {
    NEUG_ASSERT(!uniqueName.empty());
    return uniqueName;
  }

  virtual void cast(const common::LogicalType& type);
  const common::LogicalType& getDataType() const { return dataType; }

  void setAlias(const std::string& newAlias) { alias = newAlias; }
  bool hasAlias() const { return !alias.empty(); }
  std::string getAlias() const { return alias; }

  common::idx_t getNumChildren() const { return children.size(); }
  std::shared_ptr<Expression> getChild(common::idx_t idx) const {
    NEUG_ASSERT(idx < children.size());
    return children[idx];
  }
  expression_vector getChildren() const { return children; }
  void setChild(common::idx_t idx, std::shared_ptr<Expression> child) {
    NEUG_ASSERT(idx < children.size());
    children[idx] = std::move(child);
  }

  expression_vector splitOnAND();

  bool operator==(const Expression& rhs) const {
    return uniqueName == rhs.uniqueName;
  }

  std::string toString() const {
    return hasAlias() ? alias : toStringInternal();
  }

  // Returns the original name without alias. Used for data source column
  // naming where the name must match the actual field name in the file.
  std::string rawName() const { return toStringInternal(); }

  virtual std::unique_ptr<Expression> copy() const {
    THROW_INTERNAL_EXCEPTION("Unimplemented expression copy().");
  }

  template <class TARGET>
  TARGET& cast() {
    return common::neug_dynamic_cast<TARGET&>(*this);
  }
  template <class TARGET>
  TARGET* ptrCast() {
    return common::neug_dynamic_cast<TARGET*>(this);
  }
  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }
  template <class TARGET>
  const TARGET* constPtrCast() const {
    return common::neug_dynamic_cast<const TARGET*>(this);
  }

 protected:
  virtual std::string toStringInternal() const = 0;

 public:
  common::ExpressionType expressionType;
  common::LogicalType dataType;

 protected:
  // Name that serves as the unique identifier.
  std::string uniqueName;
  std::string alias;
  expression_vector children;
};

struct ExpressionHasher {
  std::size_t operator()(const std::shared_ptr<Expression>& expression) const {
    return std::hash<std::string>{}(expression->getUniqueName());
  }
};

struct ExpressionEquality {
  bool operator()(const std::shared_ptr<Expression>& left,
                  const std::shared_ptr<Expression>& right) const {
    return left->getUniqueName() == right->getUniqueName();
  }
};

}  // namespace binder
}  // namespace neug
