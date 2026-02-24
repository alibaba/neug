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

#include "neug/compiler/common/file_system/virtual_file_system.h"
#include "neug/compiler/function/cast/vector_cast_functions.h"
#include "neug/compiler/function/export/export_function.h"
#include "neug/compiler/function/scalar_function.h"
#include "neug/compiler/main/client_context.h"

namespace neug {
namespace function {

using namespace common;

static std::string addEscapes(char toEscape, char escape,
                              const std::string& val) {
  uint64_t i = 0;
  std::string escapedStr = "";
  auto found = val.find(toEscape);

  while (found != std::string::npos) {
    while (i < found) {
      escapedStr += val[i];
      i++;
    }
    escapedStr += escape;
    found = val.find(toEscape, found + sizeof(escape));
  }
  while (i < val.length()) {
    escapedStr += val[i];
    i++;
  }
  return escapedStr;
}

static bool requireQuotes(const ExportCSVBindData& exportCSVBindData,
                          const uint8_t* str, uint64_t len) {
  // Check if the string is equal to the null string.
  if (len == strlen(ExportCSVConstants::DEFAULT_NULL_STR) &&
      memcmp(str, ExportCSVConstants::DEFAULT_NULL_STR, len) == 0) {
    return true;
  }
  for (auto i = 0u; i < len; i++) {
    if (str[i] == exportCSVBindData.exportOption.quoteChar ||
        str[i] == ExportCSVConstants::DEFAULT_CSV_NEWLINE[0] ||
        str[i] == exportCSVBindData.exportOption.delimiter) {
      return true;
    }
  }
  return false;
}

struct ExportCSVSharedState : public ExportFuncSharedState {
  std::mutex mtx;
  std::unique_ptr<FileInfo> fileInfo;
  offset_t offset = 0;

  ExportCSVSharedState() = default;

  void init(main::ClientContext& context,
            const ExportFuncBindData& bindData) override {
    fileInfo = context.getVFSUnsafe()->openFile(
        bindData.fileName,
        FileOpenFlags(FileFlags::WRITE |
                      FileFlags::CREATE_AND_TRUNCATE_IF_EXISTS),
        &context);
  }

  void writeRows(const uint8_t* data, uint64_t size) {
    std::lock_guard<std::mutex> lck(mtx);
    fileInfo->writeFile(data, size, offset);
    offset += size;
  }
};

struct ExportCSVLocalState final : public ExportFuncLocalState {
  std::unique_ptr<DataChunk> unflatCastDataChunk;
  std::unique_ptr<DataChunk> flatCastDataChunk;
  std::vector<ValueVector*> castVectors;
  std::vector<function::scalar_func_exec_t> castFuncs;

  ExportCSVLocalState(main::ClientContext& context,
                      const ExportFuncBindData& bindData,
                      std::vector<bool> isFlatVec) {
    auto& exportCSVBindData = bindData.constCast<ExportCSVBindData>();
    auto numFlatVectors =
        std::count(isFlatVec.begin(), isFlatVec.end(), true /* isFlat */);
    unflatCastDataChunk =
        std::make_unique<DataChunk>(isFlatVec.size() - numFlatVectors);
    flatCastDataChunk = std::make_unique<DataChunk>(
        numFlatVectors, DataChunkState::getSingleValueDataChunkState());
    uint64_t numInsertedFlatVector = 0;
    castFuncs.resize(exportCSVBindData.types.size());
    for (auto i = 0u; i < exportCSVBindData.types.size(); i++) {
      castFuncs[i] =
          function::CastFunction::bindCastFunction(
              "cast", exportCSVBindData.types[i], LogicalType::STRING())
              ->execFunc;
      auto castVector = std::make_unique<ValueVector>(
          LogicalTypeID::STRING, context.getMemoryManager());
      castVectors.push_back(castVector.get());
      if (isFlatVec[i]) {
        flatCastDataChunk->insert(numInsertedFlatVector, std::move(castVector));
        numInsertedFlatVector++;
      } else {
        unflatCastDataChunk->insert(i - numInsertedFlatVector,
                                    std::move(castVector));
      }
    }
  }
};

static std::unique_ptr<ExportFuncBindData> bindFunc(
    ExportFuncBindInput& bindInput) {
  return std::make_unique<ExportCSVBindData>(
      bindInput.columnNames, bindInput.filePath,
      CSVReaderConfig::construct(bindInput.parsingOptions).option.copy());
}

static std::unique_ptr<ExportFuncLocalState> initLocalStateFunc(
    main::ClientContext& context, const ExportFuncBindData& bindData,
    std::vector<bool> isFlatVec) {
  return std::make_unique<ExportCSVLocalState>(context, bindData, isFlatVec);
}

static std::shared_ptr<ExportFuncSharedState> createSharedStateFunc() {
  return std::make_shared<ExportCSVSharedState>();
}

static void initSharedStateFunc(ExportFuncSharedState& sharedState,
                                main::ClientContext& context,
                                const ExportFuncBindData& bindData) {
  sharedState.init(context, bindData);
}

static void finalizeFunc(ExportFuncSharedState&) {}

function_set ExportCSVFunction::getFunctionSet() {
  function_set functionSet;
  auto exportFunc = std::make_unique<ExportFunction>(name);
  exportFunc->bind = bindFunc;
  exportFunc->initLocalState = initLocalStateFunc;
  exportFunc->createSharedState = createSharedStateFunc;
  exportFunc->initSharedState = initSharedStateFunc;
  exportFunc->finalize = finalizeFunc;
  functionSet.push_back(std::move(exportFunc));
  return functionSet;
}

}  // namespace function
}  // namespace neug