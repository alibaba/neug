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

#include "expression.h"
#include "neug/compiler/common/case_insensitive_map.h"

namespace neug {
namespace catalog {
class TableCatalogEntry;
}
namespace binder {

class NEUG_API NodeOrRelExpression : public Expression {
  static constexpr common::ExpressionType expressionType_ =
      common::ExpressionType::PATTERN;

 public:
  NodeOrRelExpression(common::LogicalType dataType, std::string uniqueName,
                      std::string variableName,
                      std::vector<catalog::TableCatalogEntry*> entries)
      : Expression{expressionType_, std::move(dataType), std::move(uniqueName)},
        variableName(std::move(variableName)),
        entries{std::move(entries)} {}

  // Note: ideally I would try to remove this function. But for now, we have to
  // create type after expression.
  void setExtraTypeInfo(std::unique_ptr<common::ExtraTypeInfo> info) {
    dataType.setExtraTypeInfo(std::move(info));
  }

  std::string getVariableName() const { return variableName; }

  bool isEmpty() const { return entries.empty(); }
  bool isMultiLabeled() const { return entries.size() > 1; }
  common::idx_t getNumEntries() const { return entries.size(); }
  common::table_id_vector_t getTableIDs() const;
  common::table_id_set_t getTableIDsSet() const;
  const std::vector<catalog::TableCatalogEntry*>& getEntries() const {
    return entries;
  }
  virtual void setEntries(std::vector<catalog::TableCatalogEntry*> entries_) {
    entries = std::move(entries_);
  }
  void addEntries(const std::vector<catalog::TableCatalogEntry*>& entries_);
  catalog::TableCatalogEntry* getSingleEntry() const;

  void addPropertyExpression(const std::string& propertyName,
                             std::unique_ptr<Expression> property);
  bool hasPropertyExpression(const std::string& propertyName) const {
    return propertyNameToIdx.contains(propertyName);
  }
  // Deep copy expression.
  std::shared_ptr<Expression> getPropertyExpression(
      const std::string& propertyName) const;
  const std::vector<std::unique_ptr<Expression>>& getPropertyExprsRef() const {
    return propertyExprs;
  }
  // Deep copy expressions.
  expression_vector getPropertyExprs() const;

  void setLabelExpression(std::shared_ptr<Expression> expression) {
    labelExpression = std::move(expression);
  }
  std::shared_ptr<Expression> getLabelExpression() const {
    return labelExpression;
  }

  void addPropertyDataExpr(std::string propertyName,
                           std::shared_ptr<Expression> expr) {
    propertyDataExprs.insert({propertyName, expr});
  }
  const common::case_insensitive_map_t<std::shared_ptr<Expression>>&
  getPropertyDataExprRef() const {
    return propertyDataExprs;
  }
  bool hasPropertyDataExpr(const std::string& propertyName) const {
    return propertyDataExprs.contains(propertyName);
  }
  std::shared_ptr<Expression> getPropertyDataExpr(
      const std::string& propertyName) const {
    NEUG_ASSERT(propertyDataExprs.contains(propertyName));
    return propertyDataExprs.at(propertyName);
  }

  std::string toStringInternal() const final { return variableName; }

 protected:
  std::string variableName;
  // A pattern may bind to multiple tables.
  std::vector<catalog::TableCatalogEntry*> entries;
  // Index over propertyExprs on property name.
  common::case_insensitive_map_t<common::idx_t> propertyNameToIdx;
  // Property expressions with order (aligned with catalog).
  std::vector<std::unique_ptr<Expression>> propertyExprs;
  std::shared_ptr<Expression> labelExpression;
  // Property data expressions specified by user in the form of "{propertyName :
  // data}"
  common::case_insensitive_map_t<std::shared_ptr<Expression>> propertyDataExprs;
};

}  // namespace binder
}  // namespace neug
