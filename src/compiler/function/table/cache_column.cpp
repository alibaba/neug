#include "src/include/binder/binder.h"
#include "src/include/catalog/catalog.h"
#include "src/include/catalog/catalog_entry/table_catalog_entry.h"
#include "src/include/common/exception/binder.h"
#include "src/include/function/table/bind_data.h"
#include "src/include/function/table/simple_table_function.h"
#include "src/include/storage/storage_manager.h"
#include "src/include/storage/store/node_table.h"
#include "src/include/storage/store/table.h"
#include "src/include/transaction/transaction.h"

namespace gs::catalog {
class TableCatalogEntry;
}

using namespace gs::common;

namespace gs {
namespace function {

struct CacheArrayColumnBindData final : TableFuncBindData {
  catalog::TableCatalogEntry* tableEntry;
  property_id_t propertyID;

  CacheArrayColumnBindData(catalog::TableCatalogEntry* tableEntry,
                           property_id_t propertyID)
      : tableEntry{tableEntry}, propertyID{propertyID} {}

  std::unique_ptr<TableFuncBindData> copy() const override {
    return std::make_unique<CacheArrayColumnBindData>(tableEntry, propertyID);
  }
};

static void validateArrayColumnType(const catalog::TableCatalogEntry* entry,
                                    property_id_t propertyID) {
  auto& type = entry->getProperty(propertyID).getType();
  if (type.getLogicalTypeID() != LogicalTypeID::ARRAY) {
    throw BinderException{
        stringFormat("Column {} is not of the expected type {}.",
                     entry->getProperty(propertyID).getName(),
                     LogicalTypeUtils::toString(LogicalTypeID::ARRAY))};
  }
}

static std::unique_ptr<TableFuncBindData> bindFunc(
    main::ClientContext* context, const TableFuncBindInput* input) {
  return nullptr;
}

static std::unique_ptr<TableFuncSharedState> initSharedState(
    const TableFuncInitSharedStateInput& input) {
  return nullptr;
}

static std::unique_ptr<TableFuncLocalState> initLocalState(
    const TableFuncInitLocalStateInput& input) {
  return nullptr;
}

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
  return 0;
}

static void finalizeFunc(const processor::ExecutionContext* context,
                         TableFuncSharedState* sharedState) {}

function_set LocalCacheArrayColumnFunction::getFunctionSet() {
  function_set functionSet;
  std::vector inputTypes = {LogicalTypeID::STRING, LogicalTypeID::STRING};
  auto func = std::make_unique<TableFunction>(name, inputTypes);
  func->bindFunc = bindFunc;
  func->initSharedStateFunc = initSharedState;
  func->initLocalStateFunc = initLocalState;
  func->tableFunc = tableFunc;
  func->finalizeFunc = finalizeFunc;
  func->canParallelFunc = [] { return true; };
  functionSet.push_back(std::move(func));
  return functionSet;
}

}  // namespace function
}  // namespace gs
