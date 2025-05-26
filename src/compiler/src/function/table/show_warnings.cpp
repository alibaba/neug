#include "binder/binder.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/simple_table_function.h"
#include "main/client_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct ShowWarningsBindData final : TableFuncBindData {
  std::vector<processor::WarningInfo> warnings;

  ShowWarningsBindData(std::vector<processor::WarningInfo> warnings,
                       binder::expression_vector columns, offset_t maxOffset)
      : TableFuncBindData{std::move(columns), maxOffset},
        warnings{std::move(warnings)} {}

  std::unique_ptr<TableFuncBindData> copy() const override {
    return std::make_unique<ShowWarningsBindData>(warnings, columns, numRows);
  }
};

static offset_t internalTableFunc(const TableFuncMorsel& morsel,
                                  const TableFuncInput& input,
                                  DataChunk& output) {
  const auto warnings =
      input.bindData->constPtrCast<ShowWarningsBindData>()->warnings;
  const auto numWarningsToOutput = morsel.endOffset - morsel.startOffset;
  for (auto i = 0u; i < numWarningsToOutput; i++) {
    const auto tableInfo = warnings[morsel.startOffset + i];
    output.getValueVectorMutable(0).setValue(i, tableInfo.queryID);
    output.getValueVectorMutable(1).setValue(i, tableInfo.warning.message);
    output.getValueVectorMutable(2).setValue(i, tableInfo.warning.filePath);
    output.getValueVectorMutable(3).setValue(i, tableInfo.warning.lineNumber);
    output.getValueVectorMutable(4).setValue(
        i, tableInfo.warning.skippedLineOrRecord);
  }
  return numWarningsToOutput;
}

static std::unique_ptr<TableFuncBindData> bindFunc(
    const main::ClientContext* context, const TableFuncBindInput* input) {
  return nullptr;
}

function_set ShowWarningsFunction::getFunctionSet() {
  function_set functionSet;
  auto function =
      std::make_unique<TableFunction>(name, std::vector<LogicalTypeID>{});
  function->tableFunc = SimpleTableFunc::getTableFunc(internalTableFunc);
  function->bindFunc = bindFunc;
  function->initSharedStateFunc = SimpleTableFunc::initSharedState;
  function->initLocalStateFunc = TableFunction::initEmptyLocalState;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace function
}  // namespace kuzu
