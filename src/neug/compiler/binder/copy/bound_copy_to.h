#pragma once

#include "neug/compiler/binder/bound_statement.h"
#include "neug/compiler/function/export/export_function.h"

namespace gs {
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
}  // namespace gs
