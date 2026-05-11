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
#include "neug/execution/common/context.h"
#include "option_utils.h"
#include "subgraph_utils.h"

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

    if (parsed.vertex_entries[0].predicate != nullptr) {
      LOG(ERROR) << "Vertex predicates are not supported in BFS.";
      return false;
    }
    if (parsed.edge_entries[0].predicate != nullptr) {
      LOG(ERROR) << "Edge predicates are not supported in BFS.";
      return false;
    }

    vertex_label = parsed.vertex_entries[0].label;
    edge_label = parsed.edge_entries[0].triplet.edge_label;
    return true;
  }

  label_t vertex_label;
  label_t edge_label;
  vid_t source;
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

  input->source =
      static_cast<vid_t>(get_option_value<int32_t>(options, "source", 0));
  input->concurrency = get_option_value<int32_t>(
      options, "concurrency", std::thread::hardware_concurrency());
  input->directed =
      get_option_value<std::string>(options, "directed", "false") == "true";

  input->node_alias = plan.plan(op_idx).meta_data(0).alias();
  input->distance_alias = plan.plan(op_idx).meta_data(1).alias();

  return input;
}

execution::Context BFSFunction::exec(const function::CallFuncInputBase& input,
                                     neug::IStorageInterface& g,
                                     const neug::execution::Context& ctx) {
  const auto& bfs_input = dynamic_cast<const BFSInput&>(input);

  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);
  auto vertex_count = graph.GetVertexSet(bfs_input.vertex_label).size();
  if (vertex_count == 0) {
    THROW_RUNTIME_ERROR("BFS requires a non-empty vertex set");
  }
  if (bfs_input.source >= static_cast<vid_t>(vertex_count)) {
    THROW_RUNTIME_ERROR("BFS source vertex is out of range");
  }

  BFS bfs(graph, bfs_input.vertex_label, bfs_input.edge_label, bfs_input.source,
          bfs_input.directed, bfs_input.concurrency);
  bfs.compute();
  execution::Context ret;
  bfs.sink(ret, bfs_input.node_alias, bfs_input.distance_alias);
  return ret;
}

function::function_set BFSFunction::getFunctionSet() {
  function::function_set funcSet;
  std::vector<common::LogicalTypeID> inputTypes = {
      common::LogicalTypeID::STRING, common::LogicalTypeID::ANY};
  function::call_output_columns outputColumns = {
      {"node", common::LogicalTypeID::NODE},
      {"distance", common::LogicalTypeID::INT64}};

  auto function = std::make_unique<function::GDSAlgoFunction>(name, inputTypes,
                                                              outputColumns);
  function->bindFunc = bind;
  function->execFunc = exec;
  funcSet.emplace_back(std::move(function));
  return funcSet;
}

}  // namespace gds
}  // namespace neug
