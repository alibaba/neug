/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/compiler/function/show_indexes_function.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "neug/common/columns/value_columns.h"
#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/index/storage_index.h"
#include "neug/storages/index/storage_index_manager.h"
#include "neug/utils/exception/exception.h"

namespace neug::function {
namespace {

struct ShowIndexRow {
  IndexMeta meta;
  std::string state;
};

std::string OptionsToJson(const IndexMeta& meta) {
  std::vector<std::pair<std::string, std::string>> options(meta.options.begin(),
                                                           meta.options.end());
  std::sort(options.begin(), options.end());

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  writer.StartObject();
  for (const auto& [key, value] : options) {
    writer.Key(key.data(), static_cast<rapidjson::SizeType>(key.size()));
    writer.String(value.data(), static_cast<rapidjson::SizeType>(value.size()));
  }
  writer.EndObject();
  return buffer.GetString();
}

}  // namespace

function_set ShowIndexesFunction::getFunctionSet() {
  auto varchar = common::DataType(common::DataTypeId::kVarchar);
  auto function = std::make_unique<NeugCallFunction>(
      ShowIndexesFunction::name, call_input_types{},
      call_output_columns{{"name", varchar},
                          {"type", varchar},
                          {"label", varchar},
                          {"property", varchar},
                          {"options", varchar},
                          {"state", varchar}});

  function->bindFunc = [](const Schema&, const execution::ContextMeta&,
                          const ::physical::PhysicalPlan&,
                          int) -> std::unique_ptr<CallFuncInputBase> {
    return std::make_unique<ShowIndexesFuncInput>();
  };

  function->execFunc = [](const CallFuncInputBase&, IStorageInterface& graph) {
    auto* readInterface = dynamic_cast<StorageReadInterface*>(&graph);
    if (!readInterface) {
      THROW_RUNTIME_ERROR("SHOW_INDEXES requires a readable storage interface");
    }

    auto indexesResult = readInterface->GetAllIndexes();
    if (!indexesResult) {
      THROW_RUNTIME_ERROR("SHOW_INDEXES failed: " +
                          indexesResult.error().ToString());
    }
    auto indexes = std::move(indexesResult).value();
    auto pendingResult = readInterface->GetAllPendingIndexes();
    if (!pendingResult) {
      THROW_RUNTIME_ERROR("SHOW_INDEXES failed: " +
                          pendingResult.error().ToString());
    }

    std::vector<ShowIndexRow> rows;
    rows.reserve(indexes.size() + pendingResult->size());
    for (const auto* index : indexes) {
      rows.push_back({index->GetMeta(), "active"});
    }
    for (const auto* pending_index : pendingResult.value()) {
      rows.push_back({pending_index->meta, "pending"});
    }
    std::sort(rows.begin(), rows.end(), [](const auto& lhs, const auto& rhs) {
      return lhs.meta.name < rhs.meta.name;
    });

    ValueColumnBuilder<std::string> nameBuilder;
    ValueColumnBuilder<std::string> typeBuilder;
    ValueColumnBuilder<std::string> labelBuilder;
    ValueColumnBuilder<std::string> propertyBuilder;
    ValueColumnBuilder<std::string> optionsBuilder;
    ValueColumnBuilder<std::string> stateBuilder;
    nameBuilder.reserve(rows.size());
    typeBuilder.reserve(rows.size());
    labelBuilder.reserve(rows.size());
    propertyBuilder.reserve(rows.size());
    optionsBuilder.reserve(rows.size());
    stateBuilder.reserve(rows.size());

    const auto& schema = graph.schema();
    for (const auto& [meta, state] : rows) {
      nameBuilder.push_back_opt(meta.name);
      typeBuilder.push_back_opt(meta.type);
      labelBuilder.push_back_opt(
          schema.get_vertex_label_name(meta.schema.label_id));
      propertyBuilder.push_back_opt(meta.schema.property_name);
      optionsBuilder.push_back_opt(OptionsToJson(meta));
      stateBuilder.push_back_opt(state);
    }

    execution::Context ctx;
    DataChunk chunk;
    chunk.set(0, nameBuilder.finish());
    chunk.set(1, typeBuilder.finish());
    chunk.set(2, labelBuilder.finish());
    chunk.set(3, propertyBuilder.finish());
    chunk.set(4, optionsBuilder.finish());
    chunk.set(5, stateBuilder.finish());
    ctx.append_chunk(std::move(chunk));
    ctx.tag_ids = {0, 1, 2, 3, 4, 5};
    return ctx;
  };

  function_set functionSet;
  functionSet.push_back(std::move(function));
  return functionSet;
}

}  // namespace neug::function
