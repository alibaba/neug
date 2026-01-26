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
#include <string>
#include <unordered_map>

#include "kuzu_fwd.h"
#include "neug/compiler/parser/statement.h"
#include "neug/utils/api.h"
#include "query_summary.h"

namespace neug {
namespace main {

/**
 * @brief A prepared statement is a parameterized query which can avoid planning
 * the same query for repeated execution.
 */
class PreparedStatement {
  friend class Connection;
  friend class ClientContext;
  friend class testing::TestHelper;
  friend class testing::TestRunner;

 public:
  bool isTransactionStatement() const;
  /**
   * @return the query is prepared successfully or not.
   */
  NEUG_API bool isSuccess() const;
  /**
   * @return the error message if the query is not prepared successfully.
   */
  NEUG_API std::string getErrorMessage() const;
  /**
   * @return the prepared statement is read-only or not.
   */
  NEUG_API bool isReadOnly() const;

  std::unordered_map<std::string, std::shared_ptr<common::Value>>
  getParameterMap() {
    return parameterMap;
  }

  common::StatementType getStatementType() const;

  NEUG_API ~PreparedStatement();

  std::unique_ptr<planner::LogicalPlan> logicalPlan;
  bool success = true;
  std::string errMsg;

 private:
  bool isProfile() const;

 private:
  bool readOnly = false;
  bool useInternalCatalogEntry = false;
  PreparedSummary preparedSummary;
  std::unordered_map<std::string, std::shared_ptr<common::Value>> parameterMap;
  std::unique_ptr<binder::BoundStatementResult> statementResult;
  std::shared_ptr<parser::Statement> parsedStatement;
};

}  // namespace main
}  // namespace neug
