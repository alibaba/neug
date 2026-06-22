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
#include "neug/utils/reader/schema.h"
#include "neug/utils/reader/sniffer.h"
#include "parquet/arrow_fs_resolver.h"
#include "parquet/arrow_reader.h"
#include "parquet/arrow_sniffer.h"
#include "parquet_options.h"

namespace neug {
namespace function {

struct ParquetReadFunction {
  static constexpr const char* name = "PARQUET_SCAN";

  static function_set getFunctionSet() {
    auto typeIDs =
        std::vector<::neug::DataTypeId>{::neug::DataTypeId::kVarchar};
    auto readFunction = std::make_unique<ReadFunction>(name, typeIDs);
    readFunction->execFunc = execFunc;
    readFunction->sniffFunc = sniffFunc;
    function_set functionSet;
    functionSet.push_back(std::move(readFunction));
    return functionSet;
  }

  static execution::Context execFunc(
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
        std::make_unique<reader::ArrowParquetOptionsBuilder>(state);

    auto arrowFs = parquet::resolveArrowFileSystem(*fs);
    auto reader = std::make_unique<reader::ArrowReader>(
        state, std::move(optionsBuilder), std::move(arrowFs));

    execution::Context ctx;
    auto localState = std::make_shared<reader::ReadLocalState>();
    reader->read(localState, ctx);
    return ctx;
  }

  static std::shared_ptr<reader::EntrySchema> sniffFunc(
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
        std::make_unique<reader::ArrowParquetOptionsBuilder>(state);

    auto arrowFs = parquet::resolveArrowFileSystem(*fs);
    auto reader = std::make_shared<reader::ArrowReader>(
        state, std::move(optionsBuilder), std::move(arrowFs));

    auto sniffer = std::make_shared<reader::ArrowSniffer>(reader);
    auto sniffResult = sniffer->sniff();

    if (!sniffResult) {
      LOG(ERROR) << "Failed to sniff Parquet schema: "
                 << sniffResult.error().ToString();
      THROW_IO_EXCEPTION("Failed to sniff Parquet schema: " +
                         sniffResult.error().ToString());
    }
    return sniffResult.value();
  }
};

}  // namespace function
}  // namespace neug
