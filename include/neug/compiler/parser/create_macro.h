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
#include "neug/compiler/parser/statement.h"

namespace neug {
namespace parser {

using default_macro_args =
    std::vector<std::pair<std::string, std::unique_ptr<ParsedExpression>>>;

class CreateMacro final : public Statement {
 public:
  CreateMacro(std::string macroName,
              std::unique_ptr<ParsedExpression> macroExpression,
              std::vector<std::string> positionalArgs,
              default_macro_args defaultArgs)
      : Statement{common::StatementType::CREATE_MACRO},
        macroName{std::move(macroName)},
        macroExpression{std::move(macroExpression)},
        positionalArgs{std::move(positionalArgs)},
        defaultArgs{std::move(defaultArgs)} {}

  inline std::string getMacroName() const { return macroName; }

  inline ParsedExpression* getMacroExpression() const {
    return macroExpression.get();
  }

  inline std::vector<std::string> getPositionalArgs() const {
    return positionalArgs;
  }

  std::vector<std::pair<std::string, ParsedExpression*>> getDefaultArgs() const;

 public:
  std::string macroName;
  std::unique_ptr<ParsedExpression> macroExpression;
  std::vector<std::string> positionalArgs;
  default_macro_args defaultArgs;
};

}  // namespace parser
}  // namespace neug
