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
#include "json_dataset_builder.h"
#include "json_options.h"
#include "neug/compiler/function/function.h"
#include "neug/compiler/function/read_function.h"
#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/utils/reader/options.h"
#include "neug/utils/reader/reader.h"
#include "neug/utils/reader/schema.h"
#include "neug/utils/reader/sniffer.h"
namespace neug {
namespace function {

struct JsonLReadFunction;

struct JsonReadFunction {
  static constexpr const char* name = "JSON_SCAN";

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

  static execution::Context execFunc(
      std::shared_ptr<reader::ReadSharedState> state) {
    // todo: get file system from vfs manager
    LocalFileSystemProvider fsProvider;
    auto fileInfo = fsProvider.provide(state->schema.file);
    state->schema.file.paths = fileInfo.resolvedPaths;
    auto optionsBuilder =
        std::make_unique<reader::ArrowJsonOptionsBuilder>(state);
    auto reader = std::make_unique<reader::ArrowReader>(
        state, std::move(optionsBuilder), fileInfo.fileSystem,
        std::make_shared<reader::JsonDatasetBuilder>());
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
    // todo: get file system from vfs manager
    LocalFileSystemProvider fsProvider;
    auto fileInfo = fsProvider.provide(state->schema.file);
    state->schema.file.paths = fileInfo.resolvedPaths;
    auto optionsBuilder =
        std::make_unique<reader::ArrowJsonOptionsBuilder>(state);
    auto reader = std::make_shared<reader::ArrowReader>(
        state, std::move(optionsBuilder), fileInfo.fileSystem,
        std::make_shared<reader::JsonDatasetBuilder>());
    auto sniffer = std::make_shared<reader::ArrowSniffer>(reader);
    auto sniffResult = sniffer->sniff();
    if (!sniffResult) {
      THROW_IO_EXCEPTION("Failed to sniff schema: " +
                         sniffResult.error().ToString());
    }
    return sniffResult.value();
  }
};

struct JsonLReadFunction {
  using alias = JsonReadFunction;

  static constexpr const char* name = "JSONL_SCAN";
};
}  // namespace function
}  // namespace neug