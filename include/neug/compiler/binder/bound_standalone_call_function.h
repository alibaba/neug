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

#include "bound_table_scan_info.h"
#include "neug/compiler/binder/bound_statement.h"
#include "neug/compiler/binder/expression/expression.h"

namespace neug {
namespace binder {

class BoundStandaloneCallFunction final : public BoundStatement {
  static constexpr common::StatementType statementType =
      common::StatementType::STANDALONE_CALL_FUNCTION;

 public:
  explicit BoundStandaloneCallFunction(BoundTableScanInfo info)
      : BoundStatement{statementType,
                       BoundStatementResult::createEmptyResult()},
        info{std::move(info)} {}
  explicit BoundStandaloneCallFunction(BoundTableScanInfo info,
                                       expression_vector outputColumns)
      : BoundStatement{statementType,
                       BoundStatementResult::createEmptyResult()},
        info{std::move(info)},
        outputColumns{std::move(outputColumns)} {}

  const BoundTableScanInfo& getTableScanInfo() const { return info; }

  const function::TableFunction& getTableFunction() const { return info.func; }

  const expression_vector& getOutputColumns() const { return outputColumns; }

  const function::TableFuncBindData* getBindData() const {
    return info.bindData.get();
  }

 private:
  BoundTableScanInfo info;
  expression_vector outputColumns;
};

}  // namespace binder
}  // namespace neug
