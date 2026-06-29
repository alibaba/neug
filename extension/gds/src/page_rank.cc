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

#include "page_rank.h"

#include "impl/page_rank_directed_impl.h"
#include "impl/page_rank_pred_impl.h"
#include "impl/page_rank_undirected_impl.h"
#include "utils/option_utils.h"
#include "utils/subgraph_utils.h"

namespace neug {
namespace gds {

struct PageRankInput : public function::CallFuncInputBase {
  ~PageRankInput() = default;
  PageRankInput() = default;
  bool parse_subgraph(const ::physical::Subgraph& subgraph,
                      const execution::ContextMeta& ctx_meta) {
    ParsedSubgraph parsed;
    if (!parse_subgraph_entries(subgraph, ctx_meta, parsed)) {
      return false;
    }
    if (!check_simple_graph_subgraph(parsed, "PageRank")) {
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
  int max_iterations;
  double damping_factor;
  int concurrency;
  int32_t node_alias, pr_alias;
  bool directed;
};

std::unique_ptr<function::CallFuncInputBase> PageRankFunction::bind(
    const Schema& schema, const execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr();
  const auto& subgraph = opr.gds_algo().sub_graph();
  const auto& options = opr.gds_algo().options();
  auto input = std::make_unique<PageRankInput>();
  if (!input->parse_subgraph(subgraph, ctx_meta)) {
    LOG(ERROR) << "Failed to parse subgraph for PageRank.";
    THROW_NOT_SUPPORTED_EXCEPTION("Invalid subgraph for PageRank");
  }
  input->node_alias = plan.plan(op_idx).meta_data(0).alias();
  input->pr_alias = plan.plan(op_idx).meta_data(1).alias();
  input->damping_factor =
      get_option_value<double>(options, "damping_factor", 0.85);
  input->max_iterations =
      get_option_value<int32_t>(options, "max_iterations", 20);
  input->concurrency = get_option_value<int32_t>(
      options, "concurrency", std::thread::hardware_concurrency());
  input->directed = get_option_value<bool>(options, "directed", false);

  return input;
}

execution::Context PageRankFunction::exec(
    const function::CallFuncInputBase& input, neug::IStorageInterface& g) {
  const auto& func_input = dynamic_cast<const PageRankInput&>(input);
  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);
  execution::Context ret;
  if (func_input.vertex_pred != nullptr || func_input.edge_pred != nullptr) {
    PageRankPred pagerank(graph, func_input.vertex_label, func_input.edge_label,
                          func_input.damping_factor, func_input.max_iterations,
                          func_input.concurrency, func_input.directed,
                          func_input.vertex_pred.get(),
                          func_input.edge_pred.get());
    pagerank.compute();
    pagerank.sink(ret, func_input.node_alias, func_input.pr_alias);
  } else if (func_input.directed) {
    DirectedPageRank pagerank(graph, func_input.vertex_label,
                              func_input.edge_label, func_input.damping_factor,
                              func_input.concurrency);
    pagerank.compute(func_input.max_iterations);

    pagerank.sink(ret, func_input.node_alias, func_input.pr_alias);
  } else {
    UndirectedPageRank pagerank(
        graph, func_input.vertex_label, func_input.edge_label,
        func_input.damping_factor, func_input.concurrency);
    pagerank.compute(func_input.max_iterations);
    pagerank.sink(ret, func_input.node_alias, func_input.pr_alias);
  }
  return ret;
}

function::function_set PageRankFunction::getFunctionSet() {
  function::function_set funcSet;
  // two input params:
  // 1. subgraph name in string
  // 2. options in map
  std::vector<common::DataTypeId> inputTypes = {common::DataTypeId::kVarchar,
                                                common::DataTypeId::kUnknown};
  // two output columns:
  // 1. node type
  // 2. page rank value in double
  function::call_output_columns outputColumns = {
      {"node", common::DataTypeId::kVertex},
      {"rank", common::DataTypeId::kDouble}};
  auto function = std::make_unique<function::GDSAlgoFunction>(name, inputTypes,
                                                              outputColumns);
  function->bindFunc = bind;
  function->execFunc = exec;
  funcSet.emplace_back(std::move(function));
  return funcSet;
}

}  // namespace gds
}  // namespace neug