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

#include "bfs.h"

#include "impl/bfs_impl.h"
#include "impl/bfs_pred_impl.h"
#include "neug/execution/common/context.h"
#include "utils/option_utils.h"
#include "utils/subgraph_utils.h"

namespace neug {
namespace gds {

struct BFSInput : public function::CallFuncInputBase {
  ~BFSInput() = default;

  bool parse_subgraph(const ::physical::Subgraph& subgraph,
                      const execution::ContextMeta& ctx_meta) {
    ParsedSubgraph parsed;
    if (!parse_subgraph_entries(subgraph, ctx_meta, parsed)) {
      return false;
    }
    if (!check_simple_graph_subgraph(parsed, "BFS")) {
      return false;
    }

    vertex_label = parsed.vertex_entries[0].label;
    edge_label = parsed.edge_entries[0].triplet.edge_label;
    vertex_pred = std::move(parsed.vertex_entries[0].predicate);
    edge_pred = std::move(parsed.edge_entries[0].predicate);
    return true;
  }

  label_t vertex_label;
  label_t edge_label;
  std::unique_ptr<execution::ExprBase> vertex_pred;
  std::unique_ptr<execution::ExprBase> edge_pred;
  std::string source;
  int32_t concurrency;
  bool directed;
  int32_t node_alias;
  int32_t distance_alias;
};

std::unique_ptr<function::CallFuncInputBase> BFSFunction::bind(
    const Schema& schema, const execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr();
  const auto& subgraph = opr.gds_algo().sub_graph();
  const auto& options = opr.gds_algo().options();

  auto input = std::make_unique<BFSInput>();
  if (!input->parse_subgraph(subgraph, ctx_meta)) {
    LOG(ERROR) << "Failed to parse subgraph for BFS.";
    THROW_NOT_SUPPORTED_EXCEPTION("Invalid subgraph for BFS");
  }

  input->source = get_option_value<std::string>(options, "source", "");
  input->concurrency = get_option_value<int32_t>(
      options, "concurrency", std::thread::hardware_concurrency());
  input->directed = get_option_value<bool>(options, "directed", false);

  input->node_alias = plan.plan(op_idx).meta_data(0).alias();
  input->distance_alias = plan.plan(op_idx).meta_data(1).alias();

  return input;
}

execution::Context BFSFunction::exec(const function::CallFuncInputBase& input,
                                     neug::IStorageInterface& g) {
  const auto& bfs_input = dynamic_cast<const BFSInput&>(input);

  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);
  // An empty vertex set is handled uniformly below: the source lookup fails
  // and we return an empty context.
  vid_t source_vid;
  if (!try_parse_source_vertex(graph, bfs_input.vertex_label, bfs_input.source,
                               source_vid)) {
    LOG(ERROR) << "BFS: source vertex '" << bfs_input.source
               << "' does not exist; returning empty result.";
    return execution::Context{};
  }

  execution::Context ret;
  // The plain BFS has no predicate support; dispatch to the predicate-aware
  // variant when a vertex or edge predicate is present.
  if (bfs_input.vertex_pred != nullptr || bfs_input.edge_pred != nullptr) {
    BFSPred bfs(graph, bfs_input.vertex_label, bfs_input.edge_label, source_vid,
                bfs_input.directed, bfs_input.concurrency,
                bfs_input.vertex_pred.get(), bfs_input.edge_pred.get());
    bfs.compute();
    bfs.sink(ret, bfs_input.node_alias, bfs_input.distance_alias);
  } else {
    BFS bfs(graph, bfs_input.vertex_label, bfs_input.edge_label, source_vid,
            bfs_input.directed, bfs_input.concurrency);
    bfs.compute();
    bfs.sink(ret, bfs_input.node_alias, bfs_input.distance_alias);
  }
  return ret;
}

function::function_set BFSFunction::getFunctionSet() {
  function::function_set funcSet;
  std::vector<common::DataTypeId> inputTypes = {
      common::DataTypeId::kVarchar, common::DataTypeId::kUnknown};
  function::call_output_columns outputColumns = {
      {"node", common::DataTypeId::kVertex},
      {"distance", common::DataTypeId::kInt64}};

  auto function = std::make_unique<function::GDSAlgoFunction>(name, inputTypes,
                                                              outputColumns);
  function->bindFunc = bind;
  function->execFunc = exec;
  funcSet.emplace_back(std::move(function));
  return funcSet;
}

}  // namespace gds
}  // namespace neug
