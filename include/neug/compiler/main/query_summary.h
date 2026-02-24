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

#include "kuzu_fwd.h"
#include "neug/utils/api.h"

namespace neug {
namespace main {

/**
 * @brief PreparedSummary stores the compiling time and query options of a
 * query.
 */
struct PreparedSummary {  // NOLINT(*-pro-type-member-init)
  double compilingTime = 0;
  common::StatementType statementType;
};

/**
 * @brief QuerySummary stores the execution time, plan, compiling time and query
 * options of a query.
 */
class QuerySummary {
  friend class ClientContext;
  friend class benchmark::Benchmark;

 public:
  /**
   * @return query compiling time in milliseconds.
   */
  NEUG_API double getCompilingTime() const;
  /**
   * @return query execution time in milliseconds.
   */
  NEUG_API double getExecutionTime() const;

  void incrementCompilingTime(double increment);
  void incrementExecutionTime(double increment);

  void setPreparedSummary(PreparedSummary preparedSummary_);

  /**
   * @return true if the query is executed with EXPLAIN.
   */
  bool isExplain() const;

  /**
   * @return the statement type of the query.
   */
  common::StatementType getStatementType() const;

 private:
  double executionTime = 0;
  PreparedSummary preparedSummary;
};

}  // namespace main
}  // namespace neug
