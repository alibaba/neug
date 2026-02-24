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

#include "neug/compiler/parser/expression/parsed_expression.h"

namespace neug {
namespace parser {

struct ParsedColumnDefinition {
  std::string name;
  std::string type;

  ParsedColumnDefinition(std::string name, std::string type)
      : name{std::move(name)}, type{std::move(type)} {}
  EXPLICIT_COPY_DEFAULT_MOVE(ParsedColumnDefinition);

 private:
  ParsedColumnDefinition(const ParsedColumnDefinition& other)
      : name{other.name}, type{other.type} {}
};

struct ParsedPropertyDefinition {
  ParsedColumnDefinition columnDefinition;
  std::unique_ptr<ParsedExpression> defaultExpr;

  ParsedPropertyDefinition(ParsedColumnDefinition columnDefinition,
                           std::unique_ptr<ParsedExpression> defaultExpr)
      : columnDefinition{std::move(columnDefinition)},
        defaultExpr{std::move(defaultExpr)} {}
  EXPLICIT_COPY_DEFAULT_MOVE(ParsedPropertyDefinition);

  std::string getName() const { return columnDefinition.name; }
  std::string getType() const { return columnDefinition.type; }

 private:
  ParsedPropertyDefinition(const ParsedPropertyDefinition& other)
      : columnDefinition{other.columnDefinition.copy()} {
    if (other.defaultExpr) {
      defaultExpr = other.defaultExpr->copy();
    }
  }
};

}  // namespace parser
}  // namespace neug
