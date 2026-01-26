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

#include "neug/compiler/main/query_summary.h"

#include "neug/compiler/common/enums/statement_type.h"

using namespace neug::common;

namespace neug {
namespace main {

double QuerySummary::getCompilingTime() const {
  return preparedSummary.compilingTime;
}

double QuerySummary::getExecutionTime() const { return executionTime; }

void QuerySummary::incrementCompilingTime(double increment) {
  preparedSummary.compilingTime += increment;
}

void QuerySummary::incrementExecutionTime(double increment) {
  executionTime += increment;
}

void QuerySummary::setPreparedSummary(PreparedSummary preparedSummary_) {
  preparedSummary = preparedSummary_;
}

bool QuerySummary::isExplain() const {
  return preparedSummary.statementType == StatementType::EXPLAIN;
}

common::StatementType QuerySummary::getStatementType() const {
  return preparedSummary.statementType;
}

}  // namespace main
}  // namespace neug
