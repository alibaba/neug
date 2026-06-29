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

#include "cdlp.h"
#include "impl/cdlp_impl.h"
#include "impl/cdlp_pred_impl.h"
#include "utils/option_utils.h"
#include "utils/subgraph_utils.h"

#include <string>
#include "neug/utils/exception/exception.h"

namespace neug {
namespace gds {
struct CDLPInput : public function::CallFuncInputBase {
  ~CDLPInput() = default;

  void parse_subgraph(const ::physical::Subgraph& subgraph,
                      const execution::ContextMeta& ctx_meta) {
    ParsedSubgraph parsed;
    if (!parse_subgraph_entries(subgraph, ctx_meta, parsed)) {
      throw std::runtime_error("Failed to parse subgraph entries.");
    }
    if (parsed.vertex_entries.size() != 1) {
      throw std::runtime_error(
          "CDLP requires exactly one vertex label, but got " +
          std::to_string(parsed.vertex_entries.size()) + ".");
    }
    vertex_label = parsed.vertex_entries[0].label;
    vertex_pred = std::move(parsed.vertex_entries[0].predicate);

    if (parsed.edge_entries.size() != 1) {
      throw std::runtime_error(
          "CDLP requires exactly one edge label, but got " +
          std::to_string(parsed.edge_entries.size()) + ".");
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

std::unique_ptr<function::CallFuncInputBase> CDLPFunction::bind(
    const Schema& schema, const execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr();
  const auto& subgraph = opr.gds_algo().sub_graph();
  const auto& options = opr.gds_algo().options();
  auto input = std::make_unique<CDLPInput>();
  input->parse_subgraph(subgraph, ctx_meta);
  input->max_iterations =
      get_option_value<int32_t>(options, "max_iterations", 5);
  input->concurrency = get_option_value<int32_t>(options, "concurrency", 1);

  input->node_alias = plan.plan(op_idx).meta_data(0).alias();
  input->label_alias = plan.plan(op_idx).meta_data(1).alias();
  LOG(INFO) << "CDLPFunction bind with max_iterations = "
            << input->max_iterations;
  return input;
}

execution::Context CDLPFunction::exec(const function::CallFuncInputBase& input,
                                      neug::IStorageInterface& g) {
  const auto& lp_input = dynamic_cast<const CDLPInput&>(input);
  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);

  execution::Context ret;
  // The plain CDLP has no predicate support; dispatch to the predicate-aware
  // variant when a vertex or edge predicate is present.
  if (lp_input.vertex_pred != nullptr || lp_input.edge_pred != nullptr) {
    CDLPPred runner(graph, lp_input.vertex_label, lp_input.edge_triplet,
                    lp_input.max_iterations, lp_input.concurrency,
                    lp_input.vertex_pred.get(), lp_input.edge_pred.get());
    runner.compute();
    runner.sink(ret, lp_input.node_alias, lp_input.label_alias);
  } else {
    CDLP runner(graph, lp_input.vertex_label, lp_input.edge_triplet,
                lp_input.max_iterations, lp_input.concurrency);
    runner.compute();
    runner.sink(ret, lp_input.node_alias, lp_input.label_alias);
  }
  return ret;
}

function::function_set CDLPFunction::getFunctionSet() {
  function::function_set funcSet;
  // two input params:
  // 1. subgraph name in string
  // 2. options in map
  std::vector<common::DataTypeId> inputTypes = {common::DataTypeId::kVarchar,
                                                common::DataTypeId::kUnknown};
  // two output columns:
  // 1. node type
  // 2. label id in int64
  function::call_output_columns outputColumns = {
      {"node", common::DataTypeId::kVertex},
      {"label", common::DataTypeId::kInt64}};
  auto function = std::make_unique<function::GDSAlgoFunction>(name, inputTypes,
                                                              outputColumns);
  function->bindFunc = bind;
  function->execFunc = exec;

  funcSet.emplace_back(std::move(function));
  return funcSet;
}
}  // namespace gds
}  // namespace neug
