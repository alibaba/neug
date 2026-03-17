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

#pragma once

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <memory>
#include "neug/compiler/function/function.h"
#include "neug/compiler/function/read_function.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/options.h"
#include "neug/utils/reader/reader.h"
#include "neug/utils/reader/schema.h"
#include "neug/utils/reader/sniffer.h"
namespace neug {
namespace function {
struct CSVReadFunction {
  static constexpr const char* name = "CSV_SCAN";

  static function_set getFunctionSet() {
    auto typeIDs =
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::STRING};
    auto readFunction = std::make_unique<ReadFunction>(name, typeIDs);
    readFunction->execFunc = execFunc;
    readFunction->sniffFunc = sniffFunc;
    function_set functionSet;
    functionSet.push_back(std::move(readFunction));
    return functionSet;
  }

  static void validateAndConvertExecOptions(
      std::shared_ptr<reader::ReadSharedState> state) {
    if (!state) {
      THROW_INVALID_ARGUMENT_EXCEPTION("State is null");
    }
    if (!state->schema.entry) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Entry schema is null");
    }
    const reader::EntrySchema& entrySchema = *state->schema.entry;
    if (entrySchema.columnNames.empty() || entrySchema.columnTypes.empty()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Can not run read function with empty column names and types");
    }
    auto& options = state->schema.file.options;
    // convert user-specified 'DELIMITER' to 'DELIM' for arrow csv options, all
    // options are case insensitive
    auto it = options.find("DELIMITER");
    if (it != options.end()) {
      options["DELIM"] = it->second;
    }
    it = options.find("DELIM");
    if (it != options.end()) {
      auto value = it->second;
      if (value.size() != 1) {
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "Delimiter should be a single character: " + value);
      }
      if (value[0] == '\\') {
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "Delimiter should not be an escape character: " + value);
      }
    }
    // convert user-specified 'HEADER' to 'SKIP_ROWS' for arrow csv options,
    // arrow will skip the first row if 'HEADER' is true
    reader::CSVParseOptions parseOptions;
    if (parseOptions.has_header.get(options)) {
      options.insert({"SKIP_ROWS", "1"});
    }
  }

  static void validateAndConvertSniffOptions(reader::FileSchema& schema) {
    auto& options = schema.options;
    // convert user-specified 'DELIMITER' to 'DELIM' for arrow csv options, all
    // options are case insensitive
    auto it = options.find("DELIMITER");
    if (it != options.end()) {
      options["DELIM"] = it->second;
    }
    it = options.find("DELIM");
    if (it != options.end()) {
      auto value = it->second;
      if (value.size() != 1) {
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "Delimiter should be a single character: " + value);
      }
      if (value[0] == '\\') {
        THROW_INVALID_ARGUMENT_EXCEPTION(
            "Delimiter should not be an escape character: " + value);
      }
    }
    // set 'AUTOGENERATE_COLUMN_NAMES' to 'TRUE' if 'HEADER' is not specified,
    // arrow will generate columns like: f0, f1, f2, ...
    reader::CSVParseOptions parseOptions;
    bool hasHeader = parseOptions.has_header.get(options);
    if (!hasHeader) {
      options.insert({"AUTOGENERATE_COLUMN_NAMES", "TRUE"});
    }
  }

  static execution::Context execFunc(
      std::shared_ptr<reader::ReadSharedState> state) {
    validateAndConvertExecOptions(state);
    const auto& vfs = neug::main::MetadataRegistry::getVFS();
    const auto& fs = vfs->Provide(state->schema.file);
    auto resolvedPaths = std::vector<std::string>();
    for (const auto& path : state->schema.file.paths) {
      const auto& resolved = fs->glob(path);
      resolvedPaths.insert(resolvedPaths.end(), resolved.begin(),
                           resolved.end());
    }
    state->schema.file.paths = std::move(resolvedPaths);
    auto optionsBuilder =
        std::make_unique<reader::ArrowCsvOptionsBuilder>(state);
    auto reader = std::make_unique<reader::ArrowReader>(
        state, std::move(optionsBuilder), fs->toArrowFileSystem());
    execution::Context ctx;
    auto localState = std::make_shared<reader::ReadLocalState>();
    reader->read(localState, ctx);
    return ctx;
  }

  static std::shared_ptr<reader::EntrySchema> sniffFunc(
      const reader::FileSchema& schema) {
    auto state = std::make_shared<reader::ReadSharedState>();
    auto& externalSchema = state->schema;
    // create table entry schema with empty column names and types, which need
    // to be inferred.
    externalSchema.entry = std::make_shared<reader::TableEntrySchema>();
    externalSchema.file = schema;
    validateAndConvertSniffOptions(externalSchema.file);
    const auto& vfs = neug::main::MetadataRegistry::getVFS();
    const auto& fs = vfs->Provide(state->schema.file);
    auto resolvedPaths = std::vector<std::string>();
    for (const auto& path : state->schema.file.paths) {
      const auto& resolved = fs->glob(path);
      resolvedPaths.insert(resolvedPaths.end(), resolved.begin(),
                           resolved.end());
    }
    state->schema.file.paths = std::move(resolvedPaths);
    auto optionsBuilder =
        std::make_unique<reader::ArrowCsvOptionsBuilder>(state);
    auto reader = std::make_shared<reader::ArrowReader>(
        state, std::move(optionsBuilder), fs->toArrowFileSystem());
    auto sniffer = std::make_shared<reader::ArrowSniffer>(reader);
    auto sniffResult = sniffer->sniff();
    if (sniffResult) {
      return sniffResult.value();
    }
    LOG(WARNING) << "Sniff schema warning: " << sniffResult.error().ToString()
                 << ". try to use auto-generate column names";
    // When the header provided by the file contains duplicate column names,
    // switch to auto-generate column names, like: f0, f1, f2 ...
    reader::CSVParseOptions parseOptions;
    auto& options = externalSchema.file.options;
    bool hasHeader = parseOptions.has_header.get(options);
    if (hasHeader) {
      options.insert({"SKIP_ROWS", "1"});
      options.insert({"AUTOGENERATE_COLUMN_NAMES", "TRUE"});
      sniffResult = sniffer->sniff();
      if (sniffResult) {
        return sniffResult.value();
      }
    }
    THROW_IO_EXCEPTION("Failed to sniff schema: " +
                       sniffResult.error().ToString());
  }
};
}  // namespace function
}  // namespace neug