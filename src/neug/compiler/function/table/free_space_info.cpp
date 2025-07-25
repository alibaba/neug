#include "neug/compiler/binder/binder.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/simple_table_function.h"
#include "neug/compiler/storage/storage_manager.h"
#include "neug/compiler/storage/store/node_table.h"

namespace gs {
namespace function {

struct FreeSpaceInfoBindData final : TableFuncBindData {
  const main::ClientContext* ctx;
  FreeSpaceInfoBindData(binder::expression_vector columns,
                        common::row_idx_t numRows,
                        const main::ClientContext* ctx)
      : TableFuncBindData{std::move(columns), numRows}, ctx{ctx} {}

  std::unique_ptr<TableFuncBindData> copy() const override {
    return std::make_unique<FreeSpaceInfoBindData>(columns, numRows, ctx);
  }
};

static common::offset_t internalTableFunc(const TableFuncMorsel& morsel,
                                          const TableFuncInput& input,
                                          common::DataChunk& output) {
  return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(
    const main::ClientContext* context, const TableFuncBindInput* input) {
  return nullptr;
}

function_set FreeSpaceInfoFunction::getFunctionSet() {
  function_set functionSet;
  auto function = std::make_unique<TableFunction>(
      name, std::vector<common::LogicalTypeID>{});
  function->tableFunc = SimpleTableFunc::getTableFunc(internalTableFunc);
  function->bindFunc = bindFunc;
  function->initSharedStateFunc = SimpleTableFunc::initSharedState;
  function->initLocalStateFunc = TableFunction::initEmptyLocalState;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace gs
