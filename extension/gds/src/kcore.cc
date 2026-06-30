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

#include "kcore.h"

#include <thread>

#include "impl/kcore_impl.h"
#include "impl/kcore_pred_impl.h"
#include "utils/option_utils.h"
#include "utils/subgraph_utils.h"

namespace neug {
namespace gds {

struct KCoreInput : public function::CallFuncInputBase {
  ~KCoreInput() = default;

  bool parse_subgraph(const ::physical::Subgraph& subgraph,
                      const execution::ContextMeta& ctx_meta) {
    ParsedSubgraph parsed;
    if (!parse_subgraph_entries(subgraph, ctx_meta, parsed)) {
      return false;
    }
    if (!check_simple_graph_subgraph(parsed, "KCore")) {
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
  int32_t k;
  int32_t concurrency;
  int32_t node_alias;
  int32_t core_alias;
};

std::unique_ptr<function::CallFuncInputBase> KCoreFunction::bind(
    const Schema& schema, const execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr();
  const auto& subgraph = opr.gds_algo().sub_graph();
  const auto& options = opr.gds_algo().options();

  auto input = std::make_unique<KCoreInput>();
  if (!input->parse_subgraph(subgraph, ctx_meta)) {
    LOG(ERROR) << "Failed to parse subgraph for KCore.";
    THROW_NOT_SUPPORTED_EXCEPTION("Invalid subgraph for KCore");
  }

  input->k = get_option_value<int32_t>(options, "k", 2);
  if (input->k < 0) {
    THROW_RUNTIME_ERROR("KCore option 'k' must be non-negative");
  }
  input->concurrency = get_option_value<int32_t>(
      options, "concurrency", std::thread::hardware_concurrency());

  input->node_alias = plan.plan(op_idx).meta_data(0).alias();
  input->core_alias = plan.plan(op_idx).meta_data(1).alias();

  return input;
}

execution::Context KCoreFunction::exec(const function::CallFuncInputBase& input,
                                       neug::IStorageInterface& g) {
  const auto& kcore_input = dynamic_cast<const KCoreInput&>(input);

  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);

  execution::Context ret;
  if (kcore_input.vertex_pred != nullptr || kcore_input.edge_pred != nullptr) {
    KCorePred runner(graph, kcore_input.vertex_label, kcore_input.edge_label,
                     kcore_input.k, kcore_input.concurrency,
                     kcore_input.vertex_pred.get(),
                     kcore_input.edge_pred.get());
    runner.compute();
    runner.sink(ret, kcore_input.node_alias, kcore_input.core_alias);
  } else {
    KCore runner(graph, kcore_input.vertex_label, kcore_input.edge_label,
                 kcore_input.k, kcore_input.concurrency);
    runner.compute();
    runner.sink(ret, kcore_input.node_alias, kcore_input.core_alias);
  }
  return ret;
}

function::function_set KCoreFunction::getFunctionSet() {
  function::function_set func_set;
  std::vector<common::DataTypeId> input_types = {common::DataTypeId::kVarchar,
                                                 common::DataTypeId::kUnknown};
  function::call_output_columns output_columns = {
      {"node", common::DataTypeId::kVertex},
      {"core", common::DataTypeId::kInt64}};

  auto function = std::make_unique<function::GDSAlgoFunction>(name, input_types,
                                                              output_columns);
  function->bindFunc = bind;
  function->execFunc = exec;
  func_set.emplace_back(std::move(function));
  return func_set;
}

}  // namespace gds
}  // namespace neug
