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

#include "label_propagation.h"
#include "impl/label_propagation_impl.h"
#include "utils/option_utils.h"
#include "utils/subgraph_utils.h"

#include <string>
#include "neug/utils/exception/exception.h"

namespace neug {
namespace gds {
struct LabelPropagationInput : public function::CallFuncInputBase {
  ~LabelPropagationInput() = default;

  void parse_subgraph(const ::physical::Subgraph& subgraph,
                      const execution::ContextMeta& ctx_meta) {
    ParsedSubgraph parsed;
    if (!parse_subgraph_entries(subgraph, ctx_meta, parsed)) {
      throw std::runtime_error("Failed to parse subgraph entries.");
    }
    if (parsed.vertex_entries.empty()) {
      throw std::runtime_error(
          "LabelPropagation requires exactly one vertex label.");
    }
    vertex_label = parsed.vertex_entries[0].label;
    vertex_pred = std::move(parsed.vertex_entries[0].predicate);

    if (parsed.edge_entries.empty()) {
      throw std::runtime_error(
          "LabelPropagation requires exactly one edge label.");
    }
    edge_triplet = parsed.edge_entries[0].triplet;
    edge_pred = std::move(parsed.edge_entries[0].predicate);
  }

  label_t vertex_label;
  std::unique_ptr<execution::ExprBase> vertex_pred;
  execution::LabelTriplet edge_triplet;
  std::unique_ptr<execution::ExprBase> edge_pred;
  int32_t max_iterations;
  int32_t node_alias, label_alias;
  int32_t concurrency;
};

std::unique_ptr<function::CallFuncInputBase> LabelPropagationFunction::bind(
    const Schema& schema, const execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr();
  const auto& subgraph = opr.gds_algo().sub_graph();
  const auto& options = opr.gds_algo().options();
  auto input = std::make_unique<LabelPropagationInput>();
  input->parse_subgraph(subgraph, ctx_meta);
  input->max_iterations =
      get_option_value<int32_t>(options, "max_iterations", 5);
  input->concurrency = get_option_value<int32_t>(options, "concurrency", 1);

  input->node_alias = plan.plan(op_idx).meta_data(0).alias();
  input->label_alias = plan.plan(op_idx).meta_data(1).alias();
  LOG(INFO) << "LabelPropagationFunction bind with max_iterations = "
            << input->max_iterations;
  return input;
}

execution::Context LabelPropagationFunction::exec(
    const function::CallFuncInputBase& input, neug::IStorageInterface& g,
    const neug::execution::Context& ctx) {
  const auto& lp_input = dynamic_cast<const LabelPropagationInput&>(input);
  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);

  LabelPropagation runner(graph, lp_input.vertex_label, lp_input.edge_triplet,
                          lp_input.max_iterations, lp_input.concurrency,
                          lp_input.vertex_pred.get(), lp_input.edge_pred.get());
  runner.compute();

  execution::Context ret;
  runner.sink(ret, lp_input.node_alias, lp_input.label_alias);
  return ret;
}

function::function_set LabelPropagationFunction::getFunctionSet() {
  function::function_set funcSet;
  // two input params:
  // 1. subgraph name in string
  // 2. options in map
  std::vector<common::LogicalTypeID> inputTypes = {
      common::LogicalTypeID::STRING, common::LogicalTypeID::ANY};
  // two output columns:
  // 1. node type
  // 2. label id in int64
  function::call_output_columns outputColumns = {
      {"node", common::LogicalTypeID::NODE},
      {"label", common::LogicalTypeID::INT64}};
  auto function = std::make_unique<function::GDSAlgoFunction>(name, inputTypes,
                                                              outputColumns);
  function->bindFunc = bind;
  function->execFunc = exec;

  funcSet.emplace_back(std::move(function));
  return funcSet;
}
}  // namespace gds
}  // namespace neug
