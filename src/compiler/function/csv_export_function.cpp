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

#include "neug/compiler/function/export/export_function.h"
#include "neug/compiler/function/read_function.h"
#include "neug/utils/writer/writer.h"

namespace neug {
namespace function {

using namespace common;

static void convertFileSchemaOptions(reader::FileSchema& schema) {
  auto& options = schema.options;
  // convert user-specified 'DELIMITER' to 'DELIM' for arrow csv options, all
  // options are case insensitive. Use operator[] so DELIMITER overwrites DELIM
  // when both are set (avoids silently ignoring DELIMITER).
  if (options.contains("DELIMITER")) {
    options["DELIM"] = options.at("DELIMITER");
  }
  if (options.contains("DELIM")) {
    auto value = options.at("DELIM");
    if (value.size() != 1) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Delimiter should be a single character: " + value);
    }
    if (value[0] == '\\') {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Delimiter should not be an escape character: " + value);
    }
  }
}

execution::Context writeExecFunc(
    neug::execution::Context& ctx, reader::FileSchema& schema,
    const std::shared_ptr<reader::EntrySchema>& entry_schema,
    const neug::StorageReadInterface& graph) {
  if (schema.paths.empty()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Schema paths is empty");
  }
  convertFileSchemaOptions(schema);
  LocalFileSystemProvider fsProvider;
  auto fileInfo = fsProvider.provide(schema, false);
  auto writer = std::make_shared<neug::writer::CsvQueryExportWriter>(
      schema, fileInfo.fileSystem, entry_schema);
  auto status = writer->write(ctx, graph);
  if (!status.ok()) {
    THROW_IO_EXCEPTION("Export failed: " + status.ToString());
  }
  ctx.clear();
  return ctx;
}

static std::unique_ptr<ExportFuncBindData> bindFunc(
    ExportFuncBindInput& bindInput) {
  return std::make_unique<ExportFuncBindData>(
      bindInput.columnNames, bindInput.filePath, bindInput.parsingOptions);
}

function_set ExportCSVFunction::getFunctionSet() {
  function_set functionSet;
  auto exportFunc = std::make_unique<ExportFunction>(name);
  exportFunc->bind = bindFunc;
  exportFunc->execFunc = writeExecFunc;
  functionSet.push_back(std::move(exportFunc));
  return functionSet;
}

}  // namespace function
}  // namespace neug
