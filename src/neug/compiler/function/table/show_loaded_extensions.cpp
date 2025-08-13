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

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/extension/extension.h"
#include "neug/compiler/extension/extension_manager.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/simple_table_function.h"

using namespace gs::catalog;
using namespace gs::common;
using namespace gs::main;

namespace gs {
namespace function {

struct LoadedExtensionInfo {
  std::string name;
  extension::ExtensionSource extensionSource;
  std::string extensionPath;

  LoadedExtensionInfo(std::string name,
                      extension::ExtensionSource extensionSource,
                      std::string extensionPath)
      : name{std::move(name)},
        extensionSource{extensionSource},
        extensionPath{std::move(extensionPath)} {}
};

struct ShowLoadedExtensionsBindData final : TableFuncBindData {
  std::vector<LoadedExtensionInfo> loadedExtensionInfo;

  ShowLoadedExtensionsBindData(
      std::vector<LoadedExtensionInfo> loadedExtensionInfo,
      binder::expression_vector columns, offset_t maxOffset)
      : TableFuncBindData{std::move(columns), maxOffset},
        loadedExtensionInfo{std::move(loadedExtensionInfo)} {}

  std::unique_ptr<TableFuncBindData> copy() const override {
    return std::make_unique<ShowLoadedExtensionsBindData>(*this);
  }
};

static offset_t internalTableFunc(const TableFuncMorsel& morsel,
                                  const TableFuncInput& input,
                                  DataChunk& output) {
  auto& loadedExtensions =
      input.bindData->constPtrCast<ShowLoadedExtensionsBindData>()
          ->loadedExtensionInfo;
  auto numTuplesToOutput = morsel.getMorselSize();
  for (auto i = 0u; i < numTuplesToOutput; i++) {
    auto loadedExtension = loadedExtensions[morsel.startOffset + i];
    output.getValueVectorMutable(0).setValue(i, loadedExtension.name);
    output.getValueVectorMutable(1).setValue(
        i, extension::ExtensionSourceUtils::toString(
               loadedExtension.extensionSource));
    output.getValueVectorMutable(2).setValue(i, loadedExtension.extensionPath);
  }
  return numTuplesToOutput;
}

static binder::expression_vector bindColumns(const TableFuncBindInput& input) {
  std::vector<std::string> columnNames;
  std::vector<LogicalType> columnTypes;
  columnNames.emplace_back("extension name");
  columnTypes.emplace_back(LogicalType::STRING());
  columnNames.emplace_back("extension source");
  columnTypes.emplace_back(LogicalType::STRING());
  columnNames.emplace_back("extension path");
  columnTypes.emplace_back(LogicalType::STRING());
  columnNames =
      TableFunction::extractYieldVariables(columnNames, input.yieldVariables);
  return input.binder->createVariables(columnNames, columnTypes);
}

static std::unique_ptr<TableFuncBindData> bindFunc(
    const main::ClientContext* context, const TableFuncBindInput* input) {
  auto loadedExtensions = context->getExtensionManager()->getLoadedExtensions();
  std::vector<LoadedExtensionInfo> loadedExtensionInfo;
  for (auto& loadedExtension : loadedExtensions) {
    loadedExtensionInfo.emplace_back(loadedExtension.getExtensionName(),
                                     loadedExtension.getSource(),
                                     loadedExtension.getFullPath());
  }
  return std::make_unique<ShowLoadedExtensionsBindData>(
      loadedExtensionInfo, bindColumns(*input), loadedExtensionInfo.size());
}

function_set ShowLoadedExtensionsFunction::getFunctionSet() {
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
