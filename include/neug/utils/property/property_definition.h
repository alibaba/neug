/** Copyright 2020 Alibaba Group Holding Limited.
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

#include <string>
#include <utility>

#include "neug/common/types/value.h"
#include "neug/utils/property/types.h"

namespace neug {

struct ColumnDefinition {
  std::string name;
  DataType type;

  ColumnDefinition() = default;
  ColumnDefinition(std::string name, DataType type)
      : name(std::move(name)), type(std::move(type)) {}
};

struct PropertyDefinition {
  ColumnDefinition columnDefinition;
  Value defaultExpr;
  bool hasDefault = false;

  PropertyDefinition() = default;
  explicit PropertyDefinition(ColumnDefinition columnDefinition)
      : columnDefinition(std::move(columnDefinition)) {}
  PropertyDefinition(ColumnDefinition columnDefinition, Value defaultExpr,
                     bool hasDefault = false)
      : columnDefinition(std::move(columnDefinition)),
        defaultExpr(std::move(defaultExpr)),
        hasDefault(hasDefault) {}

  std::string getName() const { return columnDefinition.name; }
  const DataType& getType() const { return columnDefinition.type; }
  const Value& getDefaultValue() const { return defaultExpr; }
  bool hasDefaultValue() const { return hasDefault; }
  void rename(const std::string& newName) { columnDefinition.name = newName; }
  PropertyDefinition copy() const { return *this; }
};

}  // namespace neug
