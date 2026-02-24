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

#include "bound_statement_result.h"
#include "neug/compiler/common/copy_constructors.h"
#include "neug/compiler/common/enums/statement_type.h"

namespace neug {
namespace binder {

class BoundStatement {
 public:
  BoundStatement(common::StatementType statementType,
                 BoundStatementResult statementResult)
      : statementType{statementType},
        statementResult{std::move(statementResult)} {}
  DELETE_COPY_DEFAULT_MOVE(BoundStatement);

  virtual ~BoundStatement() = default;

  common::StatementType getStatementType() const { return statementType; }

  const BoundStatementResult* getStatementResult() const {
    return &statementResult;
  }

  BoundStatementResult* getStatementResultUnsafe() { return &statementResult; }

  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }
  template <class TARGET>
  TARGET& cast() {
    return common::neug_dynamic_cast<TARGET&>(*this);
  }

 private:
  common::StatementType statementType;
  BoundStatementResult statementResult;
};

}  // namespace binder
}  // namespace neug
