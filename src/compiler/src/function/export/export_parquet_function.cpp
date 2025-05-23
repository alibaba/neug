#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "common/system_config.h"
#include "function/export/export_function.h"
#include "main/client_context.h"
#include "parquet_types.h"

namespace kuzu {
namespace function {

using namespace common;
using namespace processor;

struct ParquetOptions {
    kuzu_parquet::format::CompressionCodec::type codec =
        kuzu_parquet::format::CompressionCodec::SNAPPY;

    explicit ParquetOptions(case_insensitive_map_t<common::Value> parsingOptions) {
        for (auto& [name, value] : parsingOptions) {
            if (name == "COMPRESSION") {
                setCompression(value);
            } else {
                throw common::RuntimeException{
                    common::stringFormat("Unrecognized parquet option: {}.", name)};
            }
        }
    }

    void setCompression(common::Value& value) {
        if (value.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
            throw common::RuntimeException{
                common::stringFormat("Parquet compression option expects a string value, got: {}.",
                    value.getDataType().toString())};
        }
        auto strVal = common::StringUtils::getUpper(value.getValue<std::string>());
        if (strVal == "UNCOMPRESSED") {
            codec = kuzu_parquet::format::CompressionCodec::UNCOMPRESSED;
        } else if (strVal == "SNAPPY") {
            codec = kuzu_parquet::format::CompressionCodec::SNAPPY;
        } else if (strVal == "ZSTD") {
            codec = kuzu_parquet::format::CompressionCodec::ZSTD;
        } else if (strVal == "GZIP") {
            codec = kuzu_parquet::format::CompressionCodec::GZIP;
        } else if (strVal == "LZ4_RAW") {
            codec = kuzu_parquet::format::CompressionCodec::LZ4_RAW;
        } else {
            throw common::RuntimeException{common::stringFormat(
                "Unrecognized parquet compression option: {}.", value.toString())};
        }
    }
};

struct ExportParquetBindData final : public ExportFuncBindData {
    ParquetOptions parquetOptions;

    ExportParquetBindData(std::vector<std::string> names, std::string fileName,
        ParquetOptions parquetOptions)
        : ExportFuncBindData{std::move(names), std::move(fileName)}, parquetOptions{
                                                                         parquetOptions} {}

    std::unique_ptr<ExportFuncBindData> copy() const override {
        auto bindData =
            std::make_unique<ExportParquetBindData>(columnNames, fileName, parquetOptions);
        bindData->types = LogicalType::copy(types);
        return bindData;
    }
};

struct ExportParquetLocalState final : public ExportFuncLocalState {
    uint64_t numTuplesInFT;
    storage::MemoryManager* mm;

    ExportParquetLocalState(const ExportFuncBindData& bindData, main::ClientContext& context,
        std::vector<bool> isFlatVec)
        : mm{context.getMemoryManager()} {}
};

struct ExportParquetSharedState : public ExportFuncSharedState {

    ExportParquetSharedState() = default;

    void init(main::ClientContext& context, const ExportFuncBindData& bindData) override {
        auto& exportParquetBindData = bindData.constCast<ExportParquetBindData>();
    }
};

static std::unique_ptr<ExportFuncBindData> bindFunc(ExportFuncBindInput& bindInput) {
    ParquetOptions parquetOptions{bindInput.parsingOptions};
    return std::make_unique<ExportParquetBindData>(bindInput.columnNames, bindInput.filePath,
        parquetOptions);
}

static std::unique_ptr<ExportFuncLocalState> initLocalStateFunc(main::ClientContext& context,
    const ExportFuncBindData& bindData, std::vector<bool> isFlatVec) {
    return std::make_unique<ExportParquetLocalState>(bindData, context, isFlatVec);
}

static std::shared_ptr<ExportFuncSharedState> createSharedStateFunc() {
    return std::make_shared<ExportParquetSharedState>();
}

static void initSharedStateFunc(ExportFuncSharedState& sharedState, main::ClientContext& context,
    const ExportFuncBindData& bindData) {
    sharedState.init(context, bindData);
}

static std::vector<ValueVector*> extractSharedPtr(
    std::vector<std::shared_ptr<ValueVector>> inputVectors, uint64_t& numTuplesToAppend) {
    std::vector<ValueVector*> vecs;
    numTuplesToAppend =
        inputVectors.size() > 0 ? inputVectors[0]->state->getSelVector().getSelSize() : 0;
    for (auto& inputVector : inputVectors) {
        if (!inputVector->state->isFlat()) {
            numTuplesToAppend = inputVector->state->getSelVector().getSelSize();
        }
        vecs.push_back(inputVector.get());
    }
    return vecs;
}

static void sinkFunc(ExportFuncSharedState& sharedState, ExportFuncLocalState& localState,
    const ExportFuncBindData& /*bindData*/,
    std::vector<std::shared_ptr<ValueVector>> inputVectors) {}

static void combineFunc(ExportFuncSharedState& sharedState, ExportFuncLocalState& localState) {}

static void finalizeFunc(ExportFuncSharedState& sharedState) {}

function_set ExportParquetFunction::getFunctionSet() {
    function_set functionSet;
    auto exportFunc = std::make_unique<ExportFunction>(name);
    exportFunc->initLocalState = initLocalStateFunc;
    exportFunc->createSharedState = createSharedStateFunc;
    exportFunc->initSharedState = initSharedStateFunc;
    exportFunc->sink = sinkFunc;
    exportFunc->combine = combineFunc;
    exportFunc->finalize = finalizeFunc;
    exportFunc->bind = bindFunc;
    functionSet.push_back(std::move(exportFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
