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

#include "neug/compiler/binder/bound_statement.h"
#include "neug/compiler/function/scalar_macro_function.h"

namespace neug {
namespace binder {

class BoundCreateMacro final : public BoundStatement {
 public:
  explicit BoundCreateMacro(
      std::string macroName,
      std::unique_ptr<function::ScalarMacroFunction> macro)
      : BoundStatement{common::StatementType::CREATE_MACRO,
                       BoundStatementResult::createSingleStringColumnResult(
                           "result" /* columnName */)},
        macroName{std::move(macroName)},
        macro{std::move(macro)} {}

  inline std::string getMacroName() const { return macroName; }

  inline std::unique_ptr<function::ScalarMacroFunction> getMacro() const {
    return macro->copy();
  }

 private:
  std::string macroName;
  std::unique_ptr<function::ScalarMacroFunction> macro;
};

}  // namespace binder
}  // namespace neug
