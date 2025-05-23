#include "binder/binder.h"
#include "common/data_chunk/data_chunk_collection.h"
#include "common/exception/binder.h"
#include "common/type_utils.h"
#include "common/types/interval_t.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/simple_table_function.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"
#include <concepts>

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::main;

namespace kuzu {
namespace function {

struct StorageInfoLocalState final : TableFuncLocalState {
    std::unique_ptr<DataChunkCollection> dataChunkCollection;
    idx_t currChunkIdx;

    explicit StorageInfoLocalState(MemoryManager* mm) : currChunkIdx{0} {
        dataChunkCollection = std::make_unique<DataChunkCollection>(mm);
    }
};

struct StorageInfoBindData final : TableFuncBindData {
    TableCatalogEntry* tableEntry;
    Table* table;
    const ClientContext* context;

    StorageInfoBindData(binder::expression_vector columns, TableCatalogEntry* tableEntry,
        Table* table, const ClientContext* context)
        : TableFuncBindData{std::move(columns), 1 /*maxOffset*/},
          tableEntry{tableEntry}, table{table}, context{context} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<StorageInfoBindData>(columns, tableEntry, table, context);
    }
};

static std::unique_ptr<TableFuncLocalState> initLocalState(
    const TableFuncInitLocalStateInput& input) {
    return nullptr;
}

static void resetOutputIfNecessary(const StorageInfoLocalState* localState,
    DataChunk& outputChunk) {
    if (outputChunk.state->getSelVector().getSelSize() == DEFAULT_VECTOR_CAPACITY) {
        localState->dataChunkCollection->append(outputChunk);
        outputChunk.resetAuxiliaryBuffer();
        outputChunk.state->getSelVectorUnsafe().setSelSize(0);
    }
}

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput& output) {
    return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(const ClientContext* context,
    const TableFuncBindInput* input) {
    return nullptr;
}

function_set StorageInfoFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::STRING});
    function->tableFunc = tableFunc;
    function->bindFunc = bindFunc;
    function->initSharedStateFunc = SimpleTableFunc::initSharedState;
    function->initLocalStateFunc = initLocalState;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
