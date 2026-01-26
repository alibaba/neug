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
#include "neug/compiler/common/constants.h"

namespace neug {
namespace catalog {
class TableCatalogEntry;
}
namespace binder {

struct SingleLabelPropertyInfo {
  bool exists;
  bool isPrimaryKey;

  explicit SingleLabelPropertyInfo(bool exists, bool isPrimaryKey)
      : exists{exists}, isPrimaryKey{isPrimaryKey} {}
  EXPLICIT_COPY_DEFAULT_MOVE(SingleLabelPropertyInfo);

 private:
  SingleLabelPropertyInfo(const SingleLabelPropertyInfo& other)
      : exists{other.exists}, isPrimaryKey{other.isPrimaryKey} {}
};

class PropertyExpression final : public Expression {
  static constexpr common::ExpressionType expressionType_ =
      common::ExpressionType::PROPERTY;

 public:
  PropertyExpression(common::LogicalType dataType, std::string propertyName,
                     std::string uniqueVarName, std::string rawVariableName)
      : Expression{expressionType_, std::move(dataType),
                   uniqueVarName + "." + propertyName},
        propertyName{std::move(propertyName)},
        uniqueVarName{std::move(uniqueVarName)},
        rawVariableName{std::move(rawVariableName)} {}

  PropertyExpression(common::LogicalType dataType, std::string propertyName,
                     std::string uniqueVarName, std::string rawVariableName,
                     common::table_id_map_t<SingleLabelPropertyInfo> infos)
      : Expression{expressionType_, std::move(dataType),
                   uniqueVarName + "." + propertyName},
        propertyName{std::move(propertyName)},
        uniqueVarName{std::move(uniqueVarName)},
        rawVariableName{std::move(rawVariableName)},
        infos{std::move(infos)} {}

  PropertyExpression(const PropertyExpression& other)
      : Expression{expressionType_, other.dataType.copy(), other.uniqueName},
        propertyName{other.propertyName},
        uniqueVarName{other.uniqueVarName},
        rawVariableName{other.rawVariableName},
        infos{copyUnorderedMap(other.infos)} {}

  // Construct from a virtual property, i.e. no propertyID available.
  static std::unique_ptr<PropertyExpression> construct(
      common::LogicalType type, const std::string& propertyName,
      const Expression& child);

  // If this property is primary key on all tables.
  bool isPrimaryKey() const;
  // If this property is primary key for given table.
  bool isPrimaryKey(common::table_id_t tableID) const;

  std::string getPropertyName() const { return propertyName; }
  std::string getVariableName() const { return uniqueVarName; }
  std::string getRawVariableName() const { return rawVariableName; }

  void setUniqueVarName(const std::string& uniqueVarName) {
    this->uniqueVarName = uniqueVarName;
    this->uniqueName = uniqueVarName + "." + propertyName;
  }

  // If this property exists for given table.
  bool hasProperty(common::table_id_t tableID) const;

  common::column_id_t getColumnID(
      const catalog::TableCatalogEntry& entry) const;
  bool isSingleLabel() const { return infos.size() == 1; }
  common::table_id_t getSingleTableID() const { return infos.begin()->first; }

  bool isInternalID() const {
    return getPropertyName() == common::InternalKeyword::ID;
  }

  std::unique_ptr<Expression> copy() const override {
    return make_unique<PropertyExpression>(*this);
  }

  std::string toStringInternal() const override {
    return rawVariableName + "." + propertyName;
  }

 private:
  std::string propertyName;
  // unique identifier references to a node/rel table.
  std::string uniqueVarName;
  // printable identifier references to a node/rel table.
  std::string rawVariableName;
  // The same property name may have different info on each table.
  common::table_id_map_t<SingleLabelPropertyInfo> infos;
};

}  // namespace binder
}  // namespace neug
