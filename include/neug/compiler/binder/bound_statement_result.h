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

#include "neug/compiler/binder/expression/expression.h"

namespace neug {
namespace binder {

class BoundStatementResult {
 public:
  BoundStatementResult() = default;
  explicit BoundStatementResult(expression_vector columns,
                                std::vector<std::string> columnNames)
      : columns{std::move(columns)}, columnNames{std::move(columnNames)} {}
  EXPLICIT_COPY_DEFAULT_MOVE(BoundStatementResult);

  static BoundStatementResult createEmptyResult() {
    return BoundStatementResult();
  }

  static BoundStatementResult createSingleStringColumnResult(
      const std::string& columnName = "result");

  void addColumn(const std::string& columnName,
                 std::shared_ptr<Expression> column) {
    columns.push_back(std::move(column));
    columnNames.push_back(columnName);
  }
  expression_vector getColumns() const { return columns; }
  std::vector<std::string> getColumnNames() const { return columnNames; }
  std::vector<common::LogicalType> getColumnTypes() const {
    std::vector<common::LogicalType> columnTypes;
    for (auto& column : columns) {
      columnTypes.push_back(column->getDataType().copy());
    }
    return columnTypes;
  }

  std::shared_ptr<Expression> getSingleColumnExpr() const {
    NEUG_ASSERT(columns.size() == 1);
    return columns[0];
  }

 private:
  BoundStatementResult(const BoundStatementResult& other)
      : columns{other.columns}, columnNames{other.columnNames} {}

 private:
  expression_vector columns;
  // ColumnNames might be different from column.toString() because the same
  // column might have different aliases, e.g. RETURN id AS a, id AS b For both
  // columns we currently refer to the same id expr object so we cannot resolve
  // column name properly from expression object.
  std::vector<std::string> columnNames;
};

}  // namespace binder
}  // namespace neug
