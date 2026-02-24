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

#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/parser/expression/parsed_expression.h"

namespace neug {
namespace binder {

struct NEUG_API ColumnDefinition {
  std::string name;
  common::LogicalType type;

  ColumnDefinition() = default;
  ColumnDefinition(std::string name, common::LogicalType type)
      : name{std::move(name)}, type{std::move(type)} {}
  EXPLICIT_COPY_DEFAULT_MOVE(ColumnDefinition);

 private:
  ColumnDefinition(const ColumnDefinition& other)
      : name{other.name}, type{other.type.copy()} {}
};

struct NEUG_API PropertyDefinition {
  ColumnDefinition columnDefinition;
  std::unique_ptr<parser::ParsedExpression> defaultExpr;
  std::shared_ptr<binder::Expression> boundExpr;

  PropertyDefinition() = default;

  PropertyDefinition(ColumnDefinition columnDefinition,
                     std::unique_ptr<parser::ParsedExpression> defaultExpr)
      : columnDefinition{std::move(columnDefinition)},
        defaultExpr{std::move(defaultExpr)} {}

  PropertyDefinition(ColumnDefinition columnDefinition,
                     std::unique_ptr<parser::ParsedExpression> defaultExpr,
                     std::shared_ptr<binder::Expression> boundExpr)
      : columnDefinition{std::move(columnDefinition)},
        defaultExpr{std::move(defaultExpr)},
        boundExpr{std::move(boundExpr)} {}

  EXPLICIT_COPY_DEFAULT_MOVE(PropertyDefinition);

  std::string getName() const { return columnDefinition.name; }
  const common::LogicalType& getType() const { return columnDefinition.type; }
  std::string getDefaultExpressionName() const {
    return defaultExpr->getRawName();
  }
  void rename(const std::string& newName) { columnDefinition.name = newName; }

  void serialize(common::Serializer& serializer) const;
  static PropertyDefinition deserialize(common::Deserializer& deserializer);

 private:
  PropertyDefinition(const PropertyDefinition& other)
      : columnDefinition{other.columnDefinition.copy()},
        defaultExpr{other.defaultExpr->copy()},
        boundExpr{other.boundExpr} {}

 public:
  explicit PropertyDefinition(ColumnDefinition columnDefinition);
};

}  // namespace binder
}  // namespace neug
