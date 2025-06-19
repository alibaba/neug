#include "src/include/binder/binder.h"
#include "src/include/catalog/catalog_entry/table_catalog_entry.h"
#include "src/include/common/exception/binder.h"
#include "src/include/function/table/bind_data.h"
#include "src/include/function/table/simple_table_function.h"
#include "src/include/storage/storage_manager.h"
#include "src/include/storage/store/node_table.h"

using namespace gs::catalog;
using namespace gs::common;
using namespace gs::main;

namespace gs {
namespace function {

struct StatsInfoBindData final : TableFuncBindData {
  TableCatalogEntry* tableEntry;
  storage::Table* table;
  const ClientContext* context;

  StatsInfoBindData(binder::expression_vector columns,
                    TableCatalogEntry* tableEntry, storage::Table* table,
                    const ClientContext* context)
      : TableFuncBindData{std::move(columns), 1 /*numRows*/},
        tableEntry{tableEntry},
        table{table},
        context{context} {}

  std::unique_ptr<TableFuncBindData> copy() const override {
    return std::make_unique<StatsInfoBindData>(columns, tableEntry, table,
                                               context);
  }
};

static offset_t internalTableFunc(const TableFuncMorsel& /*morsel*/,
                                  const TableFuncInput& input,
                                  DataChunk& output) {
  return 1;
}

static std::unique_ptr<TableFuncBindData> bindFunc(
    const ClientContext* context, const TableFuncBindInput* input) {
  return nullptr;
}

function_set StatsInfoFunction::getFunctionSet() {
  function_set functionSet;
  auto function =
      std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
  function->tableFunc = SimpleTableFunc::getTableFunc(internalTableFunc);
  function->bindFunc = bindFunc;
  function->initSharedStateFunc = SimpleTableFunc::initSharedState;
  function->initLocalStateFunc = TableFunction::initEmptyLocalState;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace gs
