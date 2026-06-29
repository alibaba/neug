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

#include "lcc.h"

#include <limits>
#include <thread>

#include "impl/lcc_directed_impl.h"
#include "impl/lcc_pred_impl.h"
#include "impl/lcc_undirected_impl.h"
#include "utils/option_utils.h"
#include "utils/subgraph_utils.h"

namespace neug {
namespace gds {

struct LCCInput : public function::CallFuncInputBase {
  ~LCCInput() = default;

  bool parse_subgraph(const ::physical::Subgraph& subgraph,
                      const execution::ContextMeta& ctx_meta) {
    ParsedSubgraph parsed;
    if (!parse_subgraph_entries(subgraph, ctx_meta, parsed)) {
      return false;
    }
    if (!check_simple_graph_subgraph(parsed, "LCC")) {
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
  int32_t degree_threshold;
  int32_t concurrency;
  bool directed;
  int32_t node_alias;
  int32_t lcc_alias;
};

std::unique_ptr<function::CallFuncInputBase> LCCFunction::bind(
    const Schema& schema, const execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr();
  const auto& subgraph = opr.gds_algo().sub_graph();
  const auto& options = opr.gds_algo().options();

  auto input = std::make_unique<LCCInput>();
  if (!input->parse_subgraph(subgraph, ctx_meta)) {
    LOG(ERROR) << "Failed to parse subgraph for LCC.";
    THROW_NOT_SUPPORTED_EXCEPTION("Invalid subgraph for LCC");
  }

  input->directed = get_option_value<bool>(options, "directed", false);

  input->degree_threshold = get_option_value<int32_t>(
      options, "degree_threshold", std::numeric_limits<int32_t>::max());
  input->concurrency = get_option_value<int32_t>(
      options, "concurrency", std::thread::hardware_concurrency());

  input->node_alias = -1;
  input->lcc_alias = -1;
  const auto& meta_data = plan.plan(op_idx);
  for (int i = 0; i < meta_data.meta_data_size(); i++) {
    const auto& meta = meta_data.meta_data(i);
    auto type = parse_from_ir_data_type(meta.type());
    if (type.id() == common::DataTypeId::kVertex) {
      input->node_alias = meta.alias();
    } else if (type.id() == common::DataTypeId::kDouble) {
      input->lcc_alias = meta.alias();
    }
  }

  return input;
}

execution::Context LCCFunction::exec(const function::CallFuncInputBase& input,
                                     neug::IStorageInterface& g) {
  const auto& lcc_input = dynamic_cast<const LCCInput&>(input);
  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);

  execution::Context ret;
  if (lcc_input.vertex_pred != nullptr || lcc_input.edge_pred != nullptr) {
    LCCPred runner(graph, lcc_input.vertex_label, lcc_input.edge_label,
                   lcc_input.directed, lcc_input.concurrency,
                   lcc_input.vertex_pred.get(), lcc_input.edge_pred.get());
    runner.compute();
    runner.sink(ret, lcc_input.node_alias, lcc_input.lcc_alias);
  } else if (lcc_input.directed) {
    LCCDirected directed(graph, lcc_input.vertex_label, lcc_input.edge_label,
                         lcc_input.degree_threshold, lcc_input.concurrency);
    directed.compute();
    directed.sink(ret, lcc_input.node_alias, lcc_input.lcc_alias);
  } else {
    LCCUndirected undirected(graph, lcc_input.vertex_label,
                             lcc_input.edge_label, lcc_input.degree_threshold,
                             lcc_input.concurrency);
    undirected.compute();
    undirected.sink(ret, lcc_input.node_alias, lcc_input.lcc_alias);
  }
  return ret;
}

function::function_set LCCFunction::getFunctionSet() {
  function::function_set func_set;
  std::vector<common::DataTypeId> input_types = {common::DataTypeId::kVarchar,
                                                 common::DataTypeId::kUnknown};
  function::call_output_columns output_columns = {
      {"node", common::DataTypeId::kVertex},
      {"lcc", common::DataTypeId::kDouble}};

  auto function = std::make_unique<function::GDSAlgoFunction>(name, input_types,
                                                              output_columns);
  function->bindFunc = bind;
  function->execFunc = exec;
  func_set.emplace_back(std::move(function));
  return func_set;
}

}  // namespace gds
}  // namespace neug
