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

#include "sssp.h"

#include <thread>

#include "impl/sssp_impl.h"
#include "impl/sssp_pred_impl.h"
#include "utils/option_utils.h"
#include "utils/subgraph_utils.h"

namespace neug {
namespace gds {

struct SSSPInput : public function::CallFuncInputBase {
  ~SSSPInput() = default;

  bool parse_subgraph(const ::physical::Subgraph& subgraph,
                      const execution::ContextMeta& ctx_meta) {
    ParsedSubgraph parsed;
    if (!parse_subgraph_entries(subgraph, ctx_meta, parsed)) {
      return false;
    }
    if (!check_simple_graph_subgraph(parsed, "SSSP")) {
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
  bool directed;
  std::string edge_weight;
  int32_t concurrency;
  int32_t node_alias;
  int32_t distance_alias;
};

std::unique_ptr<function::CallFuncInputBase> SSSPFunction::bind(
    const Schema& schema, const execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr();
  const auto& subgraph = opr.gds_algo().sub_graph();
  const auto& options = opr.gds_algo().options();

  auto input = std::make_unique<SSSPInput>();
  if (!input->parse_subgraph(subgraph, ctx_meta)) {
    LOG(ERROR) << "Failed to parse subgraph for SSSP.";
    THROW_NOT_SUPPORTED_EXCEPTION("Invalid subgraph for SSSP");
  }

  input->source = get_option_value<std::string>(options, "source", "");
  input->directed = get_option_value<bool>(options, "directed", false);
  input->edge_weight = get_option_value<std::string>(options, "weight", "");
  input->concurrency = get_option_value<int32_t>(
      options, "concurrency", std::thread::hardware_concurrency());

  input->node_alias = plan.plan(op_idx).meta_data(0).alias();
  input->distance_alias = plan.plan(op_idx).meta_data(1).alias();

  return input;
}

execution::Context SSSPFunction::exec(const function::CallFuncInputBase& input,
                                      neug::IStorageInterface& g) {
  const auto& sssp_input = dynamic_cast<const SSSPInput&>(input);
  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);

  // An empty vertex set is handled uniformly below: the source lookup fails
  // and we return an empty context.
  vid_t source_vid;
  if (!try_parse_source_vertex(graph, sssp_input.vertex_label,
                               sssp_input.source, source_vid)) {
    LOG(ERROR) << "SSSP: source vertex '" << sssp_input.source
               << "' does not exist; returning empty result.";
    return execution::Context{};
  }

  execution::Context ret;
  // The plain SSSP has no predicate support; dispatch to the predicate-aware
  // variant when a vertex or edge predicate is present.
  if (sssp_input.vertex_pred != nullptr || sssp_input.edge_pred != nullptr) {
    SSSPPred sssp(graph, sssp_input.vertex_label, sssp_input.edge_label,
                  source_vid, sssp_input.directed, sssp_input.edge_weight,
                  sssp_input.concurrency, sssp_input.vertex_pred.get(),
                  sssp_input.edge_pred.get());
    sssp.compute();
    sssp.sink(ret, sssp_input.node_alias, sssp_input.distance_alias);
  } else {
    SSSP sssp(graph, sssp_input.vertex_label, sssp_input.edge_label, source_vid,
              sssp_input.directed, sssp_input.edge_weight,
              sssp_input.concurrency);
    sssp.compute();
    sssp.sink(ret, sssp_input.node_alias, sssp_input.distance_alias);
  }
  return ret;
}

function::function_set SSSPFunction::getFunctionSet() {
  function::function_set func_set;
  std::vector<common::DataTypeId> input_types = {
      common::DataTypeId::kVarchar, common::DataTypeId::kUnknown};
  function::call_output_columns output_columns = {
      {"node", common::DataTypeId::kVertex},
      {"distance", common::DataTypeId::kDouble}};

  auto function = std::make_unique<function::GDSAlgoFunction>(name, input_types,
                                                              output_columns);
  function->bindFunc = bind;
  function->execFunc = exec;
  func_set.emplace_back(std::move(function));
  return func_set;
}

}  // namespace gds
}  // namespace neug
