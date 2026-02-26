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

#include "neug/compiler/binder/expression/node_rel_expression.h"
#include "neug/compiler/binder/expression/property_expression.h"
#include "neug/compiler/catalog/catalog_entry/node_table_catalog_entry.h"
#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/enums/expression_type.h"
#include "neug/compiler/common/types/types.h"

namespace neug {
namespace binder {

class NEUG_API NodeExpression final : public NodeOrRelExpression {
 public:
  NodeExpression(common::LogicalType dataType, std::string uniqueName,
                 std::string variableName,
                 std::vector<catalog::TableCatalogEntry*> entries)
      : NodeOrRelExpression{std::move(dataType), std::move(uniqueName),
                            std::move(variableName), std::move(entries)} {}

  ~NodeExpression() override;

  void setInternalID(std::unique_ptr<Expression> expression) {
    internalID = std::move(expression);
  }
  std::shared_ptr<Expression> getInternalID() const {
    NEUG_ASSERT(internalID != nullptr);
    return internalID->copy();
  }

  Expression* getInternalIDRef() const {
    NEUG_ASSERT(internalID != nullptr);
    return internalID.get();
  }

  void setNodeUniqueName(const std::string& uniqueName) {
    this->uniqueName = uniqueName;
    NEUG_ASSERT(internalID &&
                internalID->expressionType == common::ExpressionType::PROPERTY);
    auto propertyExpr = internalID->ptrCast<binder::PropertyExpression>();
    propertyExpr->setUniqueVarName(uniqueName);
    for (auto& expr : this->propertyExprs) {
      auto propertyExpr = expr->ptrCast<binder::PropertyExpression>();
      propertyExpr->setUniqueVarName(uniqueName);
    }
  }

  void setEntries(std::vector<catalog::TableCatalogEntry*> entries_) override;

  // Get primary key property expression for a given table ID.
  std::shared_ptr<Expression> getPrimaryKey(common::table_id_t tableID) const;

 private:
  std::unique_ptr<Expression> internalID;
};

}  // namespace binder
}  // namespace neug
