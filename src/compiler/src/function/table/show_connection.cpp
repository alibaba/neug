#include "binder/binder.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/simple_table_function.h"
#include "main/client_context.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace function {

struct ShowConnectionBindData final : TableFuncBindData {
    std::vector<TableCatalogEntry*> entries;

    ShowConnectionBindData(std::vector<TableCatalogEntry*> entries,
        binder::expression_vector columns, offset_t maxOffset)
        : TableFuncBindData{std::move(columns), maxOffset}, entries{std::move(entries)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShowConnectionBindData>(entries, columns, numRows);
    }
};

static void outputRelTableConnection(DataChunk& outputDataChunk, uint64_t outputPos,
    const ClientContext& context, TableCatalogEntry* entry) {
    const auto catalog = context.getCatalog();
    KU_ASSERT(entry->getType() == CatalogEntryType::REL_TABLE_ENTRY);
    const auto relTableEntry = ku_dynamic_cast<RelTableCatalogEntry*>(entry);

    const auto srcTableID = relTableEntry->getSrcTableID();
    const auto dstTableID = relTableEntry->getDstTableID();
    const auto srcTableEntry = catalog->getTableCatalogEntry(context.getTransaction(), srcTableID);
    const auto dstTableEntry = catalog->getTableCatalogEntry(context.getTransaction(), dstTableID);
    const auto srcTablePrimaryKey =
        srcTableEntry->constCast<NodeTableCatalogEntry>().getPrimaryKeyName();
    const auto dstTablePrimaryKey =
        dstTableEntry->constCast<NodeTableCatalogEntry>().getPrimaryKeyName();
    outputDataChunk.getValueVectorMutable(0).setValue(outputPos, srcTableEntry->getName());
    outputDataChunk.getValueVectorMutable(1).setValue(outputPos, dstTableEntry->getName());
    outputDataChunk.getValueVectorMutable(2).setValue(outputPos, srcTablePrimaryKey);
    outputDataChunk.getValueVectorMutable(3).setValue(outputPos, dstTablePrimaryKey);
}

static offset_t internalTableFunc(const TableFuncMorsel& morsel, const TableFuncInput& input,
    DataChunk& output) {
    return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(const ClientContext* context,
    const TableFuncBindInput* input) {
    return nullptr;
}

function_set ShowConnectionFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
    function->tableFunc = SimpleTableFunc::getTableFunc(internalTableFunc);
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = SimpleTableFunc::initSharedState;
    function->initLocalStateFunc = TableFunction::initEmptyLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
