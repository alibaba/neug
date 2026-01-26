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

#include <memory>

#include "neug/compiler/common/enums/explain_type.h"
#include "neug/compiler/parser/statement.h"

namespace neug {
namespace parser {

class ExplainStatement : public Statement {
 public:
  ExplainStatement(std::unique_ptr<Statement> statementToExplain,
                   common::ExplainType explainType)
      : Statement{common::StatementType::EXPLAIN},
        statementToExplain{std::move(statementToExplain)},
        explainType{explainType} {}

  inline Statement* getStatementToExplain() const {
    return statementToExplain.get();
  }

  inline common::ExplainType getExplainType() const { return explainType; }

 private:
  std::unique_ptr<Statement> statementToExplain;
  common::ExplainType explainType;
};

}  // namespace parser
}  // namespace neug
