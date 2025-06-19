#pragma once

#include "src/include/function/table/bind_data.h"
#include "src/include/function/table/table_function.h"

namespace gs {
namespace binder {

struct BoundTableScanInfo {
  function::TableFunction func;
  std::unique_ptr<function::TableFuncBindData> bindData;

  BoundTableScanInfo(function::TableFunction func,
                     std::unique_ptr<function::TableFuncBindData> bindData)
      : func{std::move(func)}, bindData{std::move(bindData)} {}
  EXPLICIT_COPY_DEFAULT_MOVE(BoundTableScanInfo);

 private:
  BoundTableScanInfo(const BoundTableScanInfo& other)
      : func{other.func}, bindData{other.bindData->copy()} {}
};

}  // namespace binder
}  // namespace gs
