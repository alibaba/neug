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

#pragma once

#include <memory>
#include <utility>

#include "neug/compiler/binder/expression/expression.h"
#include "neug/utils/property/property_definition.h"

namespace neug {
namespace binder {

/**
 * Compiler representation of a property definition in a bound DDL statement.
 *
 * CompactLiteralExpression represents the folded form of a high-dimensional
 * LIST or ARRAY as value/repeat-count segments. Keeping that expression here
 * avoids expanding a large vector during compilation and keeps the generated
 * physical plan compact; the execution engine expands it only when a concrete
 * default Value is required. A null defaultExpr means that no explicit DEFAULT
 * clause was specified.
 */
struct BoundPropertyDefinition {
  ColumnDefinition columnDefinition;
  std::shared_ptr<Expression> defaultExpr;

  BoundPropertyDefinition() = default;
  BoundPropertyDefinition(ColumnDefinition columnDefinition,
                          std::shared_ptr<Expression> defaultExpr)
      : columnDefinition{std::move(columnDefinition)},
        defaultExpr{std::move(defaultExpr)} {}

  const std::string& getName() const { return columnDefinition.name; }
  const common::DataType& getType() const { return columnDefinition.type; }
  bool hasDefaultValue() const { return defaultExpr != nullptr; }

  BoundPropertyDefinition copy() const { return *this; }
};

}  // namespace binder
}  // namespace neug
