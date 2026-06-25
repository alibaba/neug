/** Copyright 2020 Alibaba Group Holding Limited.
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

#include "example_index_scan.h"

#include <memory>
#include <vector>

#include "example_index.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"

namespace neug::extension::example_index {
namespace {

std::unique_ptr<function::CallFuncInputBase> BindExampleIndexScan(
    const Schema&, const execution::ContextMeta&,
    const physical::PhysicalPlan& plan, int opIdx) {
  const auto& op = plan.plan(opIdx);
  const auto& indexScan = op.opr().index_scan();
  if (indexScan.target_value().operators_size() != 1 ||
      !indexScan.target_value().operators(0).has_const_() ||
      !indexScan.target_value().operators(0).const_().has_i32()) {
    THROW_RUNTIME_ERROR(
        "EXAMPLE_INDEX_SCAN requires a literal INT32 target value");
  }
  auto input = std::make_unique<ExampleIndexScanFuncInput>();
  input->uniqueIndexName = indexScan.unique_index_name();
  input->targetValue = indexScan.target_value().operators(0).const_().i32();
  input->alias = op.meta_data_size() == 0 ? -1 : op.meta_data(0).alias();
  return input;
}

execution::Context ExecuteExampleIndexScan(
    const function::CallFuncInputBase& baseInput, IStorageInterface& graph) {
  const auto& input = dynamic_cast<const ExampleIndexScanFuncInput&>(baseInput);
  auto* reader = dynamic_cast<StorageReadInterface*>(&graph);
  if (reader == nullptr) {
    THROW_RUNTIME_ERROR("EXAMPLE_INDEX_SCAN requires a readable graph");
  }
  auto* index = reader->GetIndexByName(input.uniqueIndexName);
  if (index == nullptr) {
    THROW_RUNTIME_ERROR("Index not found: " + input.uniqueIndexName);
  }

  ExampleIndexQueryParams query(input.targetValue);
  std::vector<vid_t> results;
  auto status = index->Search(query, IndexFilterParams(*reader), results);
  if (!status.ok()) {
    THROW_RUNTIME_ERROR(status.error_message());
  }

  const auto label = index->GetMeta().schema.label_id;
  execution::MSVertexColumnBuilder builder(label);
  builder.reserve(results.size());
  for (auto vid : results) {
    builder.push_back_opt(vid);
  }
  execution::Context result;
  execution::ContextChunk chunk;
  chunk.set(input.alias, builder.finish());
  result.append_chunk(std::move(chunk));
  return result;
}

}  // namespace

function::function_set ExampleIndexScanFunction::getFunctionSet() {
  auto function = std::make_unique<function::NeugCallFunction>(
      name, std::vector<common::DataTypeId>{}, function::call_output_columns{});
  function->bindFunc = BindExampleIndexScan;
  function->execFunc = ExecuteExampleIndexScan;
  function::function_set result;
  result.push_back(std::move(function));
  return result;
}

}  // namespace neug::extension::example_index
