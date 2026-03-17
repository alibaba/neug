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

#include "neug/compiler/binder/bound_statement.h"
#include "neug/compiler/function/export/export_function.h"
#include "neug/compiler/function/table/table_function.h"

namespace neug {
namespace binder {

class BoundCopyTo final : public BoundStatement {
 public:
  BoundCopyTo(std::unique_ptr<function::ExportFuncBindData> bindData,
              function::ExportFunction exportFunc,
              std::unique_ptr<BoundStatement> query)
      : BoundStatement{common::StatementType::COPY_TO,
                       BoundStatementResult::createEmptyResult()},
        bindData{std::move(bindData)},
        exportFunc{std::move(exportFunc)},
        query{std::move(query)} {}

  std::unique_ptr<function::ExportFuncBindData> getBindData() const {
    return bindData->copy();
  }

  function::ExportFunction getExportFunc() const { return exportFunc; }

  const BoundStatement* getRegularQuery() const { return query.get(); }

 private:
  std::unique_ptr<function::ExportFuncBindData> bindData;
  function::ExportFunction exportFunc;
  std::unique_ptr<BoundStatement> query;
};

}  // namespace binder
}  // namespace neug
