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

#include <memory>
#include "neug/compiler/function/function.h"
#include "neug/compiler/function/read_function.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/execution/execute/ops/batch/batch_update_utils.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/read/common/sniffer.h"
#include "neug/utils/io/reader.h"
namespace neug {
namespace function {

struct JsonLReadFunction;

struct JsonReadFunction {
  static constexpr const char* name = "JSON_SCAN";

  static function_set getFunctionSet() {
    auto typeIDs =
        std::vector<common::DataTypeId>{common::DataTypeId::kVarchar};
    auto readFunction = std::make_unique<ReadFunction>(name, typeIDs);
    readFunction->execFunc = jsonExecFunc;
    readFunction->sniffFunc = jsonSniffFunc;
    function_set functionSet;
    functionSet.push_back(std::move(readFunction));
    return functionSet;
  }

  static execution::Context jsonExecFunc(
      std::shared_ptr<reader::ReadSharedState> state) {
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
        std::make_unique<reader::JsonOptionsBuilder>(state, true);
    auto reader =
        std::make_unique<reader::JsonReader>(state, std::move(optionsBuilder));
    execution::Context ctx;
    auto localState = std::make_shared<reader::ReadLocalState>();
    reader->read(localState, ctx);
    return ctx;
  }

  static std::shared_ptr<reader::EntrySchema> jsonSniffFunc(
      const reader::FileSchema& schema) {
    auto state = std::make_shared<reader::ReadSharedState>();
    auto& externalSchema = state->schema;
    externalSchema.entry = std::make_shared<reader::TableEntrySchema>();
    externalSchema.file = schema;
    externalSchema.file.options["BATCH_SIZE"] =
        std::to_string(reader::kSniffBlockSize);
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
        std::make_unique<reader::JsonOptionsBuilder>(state, true);
    auto reader =
        std::make_shared<reader::JsonReader>(state, std::move(optionsBuilder));
    auto sniffer = std::make_shared<reader::JsonSniffer>(reader);
    auto sniffResult = sniffer->sniff();
    if (!sniffResult) {
      THROW_IO_EXCEPTION("Failed to sniff schema: " +
                         sniffResult.error().ToString());
    }
    return sniffResult.value();
  }
};

struct JsonLReadFunction {
  static constexpr const char* name = "JSONL_SCAN";

  static function_set getFunctionSet() {
    auto typeIDs =
        std::vector<common::DataTypeId>{common::DataTypeId::kVarchar};
    auto readFunction = std::make_unique<ReadFunction>(name, typeIDs);
    readFunction->execFunc = jsonLExecFunc;
    readFunction->sniffFunc = jsonLSniffFunc;
    function_set functionSet;
    functionSet.push_back(std::move(readFunction));
    return functionSet;
  }

  static execution::Context jsonLExecFunc(
      std::shared_ptr<reader::ReadSharedState> state) {
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
        std::make_unique<reader::JsonOptionsBuilder>(state, false);
    auto reader =
        std::make_unique<reader::JsonReader>(state, std::move(optionsBuilder));
    execution::Context ctx;
    auto localState = std::make_shared<reader::ReadLocalState>();
    reader->read(localState, ctx);
    return ctx;
  }

  static std::shared_ptr<reader::EntrySchema> jsonLSniffFunc(
      const reader::FileSchema& schema) {
    auto state = std::make_shared<reader::ReadSharedState>();
    auto& externalSchema = state->schema;
    externalSchema.entry = std::make_shared<reader::TableEntrySchema>();
    externalSchema.file = schema;
    externalSchema.file.options["BATCH_SIZE"] =
        std::to_string(reader::kSniffBlockSize);
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
        std::make_unique<reader::JsonOptionsBuilder>(state, false);
    auto reader =
        std::make_shared<reader::JsonReader>(state, std::move(optionsBuilder));
    auto sniffer = std::make_shared<reader::JsonSniffer>(reader);
    auto sniffResult = sniffer->sniff();
    if (!sniffResult) {
      THROW_IO_EXCEPTION("Failed to sniff schema: " +
                         sniffResult.error().ToString());
    }
    return sniffResult.value();
  }
};
}  // namespace function
}  // namespace neug
