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

#include "neug/compiler/main/prepared_statement.h"

#include "neug/compiler/binder/bound_statement_result.h"  // IWYU pragma: keep (used to avoid error in destructor)
#include "neug/compiler/common/enums/statement_type.h"
#include "neug/compiler/planner/operator/logical_plan.h"

using namespace neug::common;

namespace neug {
namespace main {

bool PreparedStatement::isTransactionStatement() const {
  return preparedSummary.statementType == StatementType::TRANSACTION;
}

bool PreparedStatement::isSuccess() const { return success; }

std::string PreparedStatement::getErrorMessage() const { return errMsg; }

bool PreparedStatement::isReadOnly() const { return readOnly; }

bool PreparedStatement::isProfile() const { return logicalPlan->isProfile(); }

StatementType PreparedStatement::getStatementType() const {
  return parsedStatement->getStatementType();
}

PreparedStatement::~PreparedStatement() = default;

}  // namespace main
}  // namespace neug
