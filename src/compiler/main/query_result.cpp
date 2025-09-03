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

#include "neug/compiler/main/query_result.h"

#include <memory>

#include "neug/compiler/common/arrow/arrow_converter.h"
#include "neug/utils/exception/exception.h"

using namespace gs::common;

namespace gs {
namespace main {

QueryResult::QueryResult()
    : nextQueryResult{nullptr}, queryResultIterator{this} {}

QueryResult::QueryResult(const PreparedSummary& preparedSummary)
    : nextQueryResult{nullptr}, queryResultIterator{this} {
  querySummary = std::make_unique<QuerySummary>();
  querySummary->setPreparedSummary(preparedSummary);
}

QueryResult::~QueryResult() = default;

bool QueryResult::isSuccess() const { return success; }

std::string QueryResult::getErrorMessage() const { return errMsg; }

size_t QueryResult::getNumColumns() const { return columnDataTypes.size(); }

std::vector<std::string> QueryResult::getColumnNames() const {
  return columnNames;
}

std::vector<LogicalType> QueryResult::getColumnDataTypes() const {
  return LogicalType::copy(columnDataTypes);
}

uint64_t QueryResult::getNumTuples() const { return 0; }

QuerySummary* QueryResult::getQuerySummary() const {
  return querySummary.get();
}

void QueryResult::setColumnHeader(std::vector<std::string> columnNames_,
                                  std::vector<LogicalType> columnTypes_) {
  columnNames = std::move(columnNames_);
  columnDataTypes = std::move(columnTypes_);
}

bool QueryResult::hasNext() const {
  validateQuerySucceed();
  return false;
}

bool QueryResult::hasNextQueryResult() const {
  return queryResultIterator.hasNextQueryResult();
}

QueryResult* QueryResult::getNextQueryResult() {
  if (hasNextQueryResult()) {
    ++queryResultIterator;
    return queryResultIterator.getCurrentResult();
  }
  return nullptr;
}

std::string QueryResult::toString() const {
  std::string result;
  if (isSuccess()) {
    for (auto i = 0u; i < columnNames.size(); ++i) {
      if (i != 0) {
        result += "|";
      }
      result += columnNames[i];
    }
    result += "\n";
  } else {
    result = errMsg;
  }
  return result;
}

void QueryResult::validateQuerySucceed() const {
  if (!success) {
    THROW_EXCEPTION_WITH_FILE_LINE(errMsg);
  }
}

}  // namespace main
}  // namespace gs
