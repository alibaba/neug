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

#include <unordered_map>

#include "neug/compiler/parser/create_macro.h"

namespace neug {
namespace function {

using macro_parameter_value_map =
    std::unordered_map<std::string, parser::ParsedExpression*>;

struct ScalarMacroFunction {
  std::unique_ptr<parser::ParsedExpression> expression;
  std::vector<std::string> positionalArgs;
  parser::default_macro_args defaultArgs;

  ScalarMacroFunction() = default;

  ScalarMacroFunction(std::unique_ptr<parser::ParsedExpression> expression,
                      std::vector<std::string> positionalArgs,
                      parser::default_macro_args defaultArgs)
      : expression{std::move(expression)},
        positionalArgs{std::move(positionalArgs)},
        defaultArgs{std::move(defaultArgs)} {}

  inline std::string getDefaultParameterName(uint64_t idx) const {
    return defaultArgs[idx].first;
  }

  inline uint64_t getNumArgs() const {
    return positionalArgs.size() + defaultArgs.size();
  }

  std::vector<std::string> getPositionalArgs() const { return positionalArgs; }

  macro_parameter_value_map getDefaultParameterVals() const;

  std::unique_ptr<ScalarMacroFunction> copy() const;

  void serialize(common::Serializer& serializer) const;

  std::string toCypher(const std::string& name) const;

  static std::unique_ptr<ScalarMacroFunction> deserialize(
      common::Deserializer& deserializer);
};

}  // namespace function
}  // namespace neug
