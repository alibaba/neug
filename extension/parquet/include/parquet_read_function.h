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
#include <string>
#include <utility>
#include <vector>

#include "carquet/scan.h"
#include "neug/compiler/function/function.h"
#include "neug/compiler/function/import/import_stream.h"
#include "neug/compiler/function/read_function.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/stream/input_stream.h"

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
    resolvePathsAndStream(state);
    execution::Context ctx;
    parquet::scanCarquet(state, ctx);
    return ctx;
  }

  static std::shared_ptr<reader::EntrySchema> sniffFunc(
      const reader::FileSchema& schema) {
    auto state = std::make_shared<reader::ReadSharedState>();
    state->schema.file = schema;
    resolvePathsAndStream(state);

    const std::string path = state->schema.file.paths.front();
    io::InputStreamFactory inputFactory;
    if (state->stream_opener) {
      inputFactory = io::bindInputStream(state->stream_opener, path);
    } else {
      inputFactory = [path]() { return io::openLocalInputStream(path); };
    }
    auto sniffResult = parquet::sniffCarquet(std::move(inputFactory));
    if (!sniffResult) {
      THROW_IO_EXCEPTION("Failed to sniff Parquet schema: " +
                         sniffResult.error().ToString());
    }
    return std::move(*sniffResult);
  }

 private:
  static void resolvePathsAndStream(
      const std::shared_ptr<reader::ReadSharedState>& state) {
    if (!state) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Parquet read state is null");
    }
    const auto& vfs = main::MetadataRegistry::getVFS();
    const auto& fs = vfs->Provide(state->schema.file);
    std::vector<std::string> resolvedPaths;
    for (const auto& path : state->schema.file.paths) {
      const auto resolved = fs->glob(path);
      resolvedPaths.insert(resolvedPaths.end(), resolved.begin(),
                           resolved.end());
    }
    if (resolvedPaths.empty()) {
      THROW_IO_EXCEPTION("No Parquet files match the input paths");
    }
    state->schema.file.paths = std::move(resolvedPaths);
    state->stream_opener = makeImportStreamOpener(*fs);
  }
};

}  // namespace function
}  // namespace neug
