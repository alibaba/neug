/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <glog/logging.h>
#include <stdint.h>

#include <memory>
#include <utility>

#include "neug/compiler/common/assert.h"
#include "neug/compiler/function/read_function.h"
#include "neug/compiler/main/metadata_registry.h"
#include "neug/execution/common/context.h"
#include "neug/execution/execute/ops/batch/data_source.h"
#include "neug/execution/utils/params.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/reader.h"
#include "neug/utils/result.h"

namespace neug {
class Schema;

namespace execution {
class OprTimer;
namespace ops {

class DataSourceOpr : public IOperator {
 private:
  std::shared_ptr<reader::ReadSharedState> sharedState;
  function::ReadFunction* readFunction;
  std::vector<int> aliases_;

 public:
  DataSourceOpr(const std::shared_ptr<reader::ReadSharedState>& sharedState,
                function::ReadFunction* readFunction, std::vector<int> aliases)
      : sharedState(std::move(sharedState)),
        readFunction(readFunction),
        aliases_(std::move(aliases)) {}

  ~DataSourceOpr() override = default;

  std::string get_operator_name() const override { return "DataSourceOpr"; }

  result<Stream<ContextChunk>> Eval(IStorageInterface& graph,
                                    const ParamsMap& params,
                                    Stream<ContextChunk>&& input,
                                    OprTimer* timer) override {
    NEUG_ASSERT(readFunction != nullptr);
    // Reader initialization may expand globs and normalize options. Never
    // mutate the state captured by the cached operator.
    auto state = std::make_shared<reader::ReadSharedState>(*sharedState);
    auto function = readFunction;
    struct Cursor {
      bool initialized = false;
      std::shared_ptr<IDataChunkSupplier> supplier;
      Stream<ContextChunk> fallback;
    };
    auto cursor = std::make_shared<Cursor>();
    auto raw = Stream<ContextChunk>(
        [state, function,
         cursor]() mutable -> Stream<ContextChunk>::NextResult {
          if (!cursor->initialized) {
            cursor->initialized = true;
            if (function->supplierFunc) {
              cursor->supplier = function->supplierFunc(state);
              if (!cursor->supplier) {
                return tl::unexpected(
                    Status::InternalError("Reader returned a null supplier"));
              }
            } else {
              cursor->fallback = stream_from_context(function->execFunc(state));
            }
          }
          if (!cursor->supplier) {
            return cursor->fallback.Next();
          }
          auto chunk = cursor->supplier->GetNextChunk();
          if (!chunk) {
            return std::optional<ContextChunk>{};
          }
          return std::optional<ContextChunk>(std::in_place, std::move(*chunk));
        });
    if (aliases_.empty())
      return std::move(raw);
    return map_chunks(
        std::move(raw),
        [aliases = aliases_](ContextChunk&& batch) -> result<ContextChunk> {
          if (batch.col_num() != aliases.size()) {
            RETURN_INVALID_ARGUMENT_ERROR(
                "Reader column count does not match source aliases");
          }
          DataChunk output;
          for (size_t i = 0; i < aliases.size(); ++i) {
            output.set(aliases[i], batch.chunk().get(i));
          }
          return ContextChunk(std::move(output), std::move(batch.head()));
        });
  }
};

std::shared_ptr<ReadSharedState> ReadStateBuilder::build(
    const ::physical::DataSource& data_source) {
  auto read_shared_state = std::make_shared<reader::ReadSharedState>();
  auto external_schema = reader::ExternalSchema();
  external_schema.entry = buildEntrySchema(data_source.entry_schema());
  external_schema.file = buildFileSchema(data_source.file_schema());
  read_shared_state->schema = external_schema;
  // Proto field is named skip_columns for historical reasons; it now carries
  // the list of columns to project (include), not skip.
  auto& columns = read_shared_state->projectColumns;
  for (const auto& col : data_source.project_columns()) {
    columns.push_back(col);
  }
  if (data_source.has_skip_rows()) {
    read_shared_state->skipRows =
        std::make_shared<::common::Expression>(data_source.skip_rows());
  }
  return read_shared_state;
}

std::shared_ptr<EntrySchema> ReadStateBuilder::buildEntrySchema(
    const ::physical::EntrySchema& entry_pb) {
  auto entry_schema = std::make_shared<TableEntrySchema>();
  auto& columnNames = entry_schema->columnNames;
  for (auto& column_name : entry_pb.column_names()) {
    columnNames.push_back(column_name);
  }
  auto& columnTypes = entry_schema->columnTypes;
  for (auto& column_type : entry_pb.column_types()) {
    columnTypes.push_back(std::make_shared<::common::DataType>(column_type));
  }
  return entry_schema;
}

FileSchema ReadStateBuilder::buildFileSchema(
    const ::physical::FileSchema& file_schema_pb) {
  FileSchema file_schema;
  auto& paths = file_schema.paths;
  for (auto& path : file_schema_pb.paths()) {
    paths.push_back(path);
  }
  file_schema.format = file_schema_pb.format();
  file_schema.protocol = file_schema_pb.protocol();
  for (auto& option : file_schema_pb.options()) {
    file_schema.options[option.first] = option.second;
  }
  return file_schema;
}

// Build DataSourceOpr from PB, there are two key fields:
// 1. ReadSharedState: can be built from PB, which contains the entry and file
// schema info.
// 2. ReadFunction: can be looked up from catalog, which has been registered in
// extension.
neug::result<OpBuildResultT> DataSourceOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  auto sourcePB = plan.plan(op_idx).opr().source();
  auto stateBuilder = ReadStateBuilder();
  // build read shared state from PB
  auto state = stateBuilder.build(sourcePB);

  // look up read function from catalog
  auto signatureName = sourcePB.extension_name();
  auto gCatalog = neug::main::MetadataRegistry::getCatalog();
  auto func = gCatalog->getFunctionWithSignature(signatureName);
  auto readFunc = func->ptrCast<function::ReadFunction>();
  auto output_meta = ctx_meta;
  std::vector<int> aliases;
  const auto& metadata = plan.plan(op_idx).meta_data();
  for (const auto& column : metadata) {
    output_meta.set(column.alias(), parse_from_ir_data_type(column.type()));
  }
  // Metadata and reader batches use the same projected-column order.
  for (const auto& column : metadata)
    aliases.push_back(column.alias());
  return std::make_pair(
      std::make_unique<DataSourceOpr>(state, readFunc, std::move(aliases)),
      output_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug