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

#include <vector>

#include "neug/compiler/parser/expression/parsed_expression.h"
#include "neug/compiler/parser/scan_source.h"
#include "neug/compiler/parser/statement.h"

namespace neug {
namespace parser {

class Copy : public Statement {
 public:
  explicit Copy(common::StatementType type) : Statement{type} {}

  void setParsingOption(options_t options) {
    parsingOptions = std::move(options);
  }
  const options_t& getParsingOptions() const { return parsingOptions; }

 protected:
  options_t parsingOptions;
};

struct CopyFromColumnInfo {
  bool inputColumnOrder = false;
  std::vector<std::string> columnNames;

  CopyFromColumnInfo() = default;
  CopyFromColumnInfo(bool inputColumnOrder,
                     std::vector<std::string> columnNames)
      : inputColumnOrder{inputColumnOrder},
        columnNames{std::move(columnNames)} {}
};

class CopyFrom : public Copy {
 public:
  CopyFrom(std::unique_ptr<BaseScanSource> source, std::string tableName)
      : Copy{common::StatementType::COPY_FROM},
        byColumn_{false},
        source{std::move(source)},
        tableName{std::move(tableName)} {}

  void setByColumn() { byColumn_ = true; }
  bool byColumn() const { return byColumn_; }

  BaseScanSource* getSource() const { return source.get(); }

  std::string getTableName() const { return tableName; }

  void setColumnInfo(CopyFromColumnInfo columnInfo_) {
    columnInfo = std::move(columnInfo_);
  }
  CopyFromColumnInfo getCopyColumnInfo() const { return columnInfo; }

 private:
  bool byColumn_;
  std::unique_ptr<BaseScanSource> source;
  std::string tableName;
  CopyFromColumnInfo columnInfo;
};

class CopyTo : public Copy {
 public:
  CopyTo(std::string filePath, std::unique_ptr<Statement> statement)
      : Copy{common::StatementType::COPY_TO},
        filePath{std::move(filePath)},
        statement{std::move(statement)} {}

  std::string getFilePath() const { return filePath; }
  const Statement* getStatement() const { return statement.get(); }

 private:
  std::string filePath;
  std::unique_ptr<Statement> statement;
};

}  // namespace parser
}  // namespace neug
