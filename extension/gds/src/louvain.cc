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

#include "louvain.h"

#include "impl/louvain_impl.h"
#include "utils/option_utils.h"
#include "utils/subgraph_utils.h"

namespace neug {
namespace gds {

struct LouvainInput : public function::CallFuncInputBase {
  ~LouvainInput() = default;
  LouvainInput() = default;

  bool parse_subgraph(const ::physical::Subgraph& subgraph,
                      const execution::ContextMeta& ctx_meta) {
    ParsedSubgraph parsed;
    if (!parse_subgraph_entries(subgraph, ctx_meta, parsed)) {
      return false;
    }
    if (!check_simple_graph_subgraph(parsed, "louvain")) {
      return false;
    }
    if (parsed.vertex_entries[0].predicate != nullptr) {
      LOG(ERROR) << "Vertex predicates are not supported in louvain.";
      return false;
    }
    if (parsed.edge_entries[0].predicate != nullptr) {
      LOG(ERROR) << "Edge predicates are not supported in louvain.";
      return false;
    }
    vertex_label = parsed.vertex_entries[0].label;
    edge_label = parsed.edge_entries[0].triplet.edge_label;
    return true;
  }

  label_t vertex_label;
  label_t edge_label;
  double resolution = 1.0;
  bool directed = false;
  double threshold = 1e-7;
  int32_t concurrency;
  int32_t node_alias;
  int32_t community_alias;
};

std::unique_ptr<function::CallFuncInputBase> LouvainFunction::bind(
    const Schema& schema, const execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr();
  const auto& subgraph = opr.gds_algo().sub_graph();
  const auto& options = opr.gds_algo().options();

  auto input = std::make_unique<LouvainInput>();
  if (!input->parse_subgraph(subgraph, ctx_meta)) {
    LOG(ERROR) << "Failed to parse subgraph for louvain.";
    THROW_NOT_SUPPORTED_EXCEPTION("Invalid subgraph for louvain");
  }

  input->resolution = get_option_value<double>(options, "resolution", 1.0);
  input->directed = get_option_value<bool>(options, "directed", false);
  input->threshold = get_option_value<double>(options, "threshold", 1e-7);
  input->concurrency = get_option_value<int32_t>(
      options, "concurrency", std::thread::hardware_concurrency());

  input->node_alias = plan.plan(op_idx).meta_data(0).alias();
  input->community_alias = plan.plan(op_idx).meta_data(1).alias();

  return input;
}

execution::Context LouvainFunction::exec(
    const function::CallFuncInputBase& input_base, neug::IStorageInterface& g) {
  const auto& input = dynamic_cast<const LouvainInput&>(input_base);
  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);

  community::Louvain louvain(graph, input.vertex_label, input.edge_label,
                             input.resolution, input.threshold,
                             input.concurrency);
  louvain.compute();

  execution::Context ctx;
  louvain.sink(ctx, input.node_alias, input.community_alias);
  return ctx;
}

function::function_set LouvainFunction::getFunctionSet() {
  function::function_set funcSet;
  std::vector<common::DataTypeId> inputTypes = {
      common::DataTypeId::kVarchar, common::DataTypeId::kUnknown};
  function::call_output_columns outputColumns = {
      {"node", common::DataTypeId::kVertex},
      {"community", common::DataTypeId::kInt64}};
  auto function = std::make_unique<function::GDSAlgoFunction>(name, inputTypes,
                                                              outputColumns);
  function->bindFunc = bind;
  function->execFunc = exec;

  funcSet.emplace_back(std::move(function));
  return funcSet;
}

}  // namespace gds
}  // namespace neug
