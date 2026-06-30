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

#include "wcc.h"

#include "impl/wcc_impl.h"
#include "impl/wcc_pred_impl.h"
#include "utils/option_utils.h"
#include "utils/subgraph_utils.h"

namespace neug {
namespace gds {

struct WCCInput : public function::CallFuncInputBase {
  ~WCCInput() = default;
  WCCInput() = default;
  bool parse_subgraph(const ::physical::Subgraph& subgraph,
                      const execution::ContextMeta& ctx_meta) {
    ParsedSubgraph parsed;
    if (!parse_subgraph_entries(subgraph, ctx_meta, parsed)) {
      return false;
    }
    if (!check_simple_graph_subgraph(parsed, "WCC")) {
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
  int32_t node_alias;
  int32_t comp_alias;
  int32_t concurrency;
};

std::unique_ptr<function::CallFuncInputBase> WCCFunction::bind(
    const Schema& schema, const execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr();
  const auto& subgraph = opr.gds_algo().sub_graph();
  const auto& options = opr.gds_algo().options();

  auto input = std::make_unique<WCCInput>();
  if (!input->parse_subgraph(subgraph, ctx_meta)) {
    LOG(ERROR) << "Failed to parse subgraph for WCC.";
    THROW_NOT_SUPPORTED_EXCEPTION("Invalid subgraph for WCC");
  }
  const auto& meta_data = plan.plan(op_idx);
  input->node_alias = -1;
  input->comp_alias = -1;
  for (int i = 0; i < meta_data.meta_data_size(); i++) {
    const auto& meta = meta_data.meta_data(i);
    auto type = parse_from_ir_data_type(meta.type());
    if (type.id() == common::DataTypeId::kVertex) {
      input->node_alias = meta.alias();
    } else if (type.id() == common::DataTypeId::kInt64) {
      input->comp_alias = meta.alias();
    }
  }
  input->concurrency = get_option_value<int32_t>(
      options, "concurrency", std::thread::hardware_concurrency());

  return input;
}

execution::Context WCCFunction::exec(
    const function::CallFuncInputBase& input_base, neug::IStorageInterface& g) {
  const auto& input = dynamic_cast<const WCCInput&>(input_base);
  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);

  execution::Context ret;
  // The plain WCC has no predicate support; dispatch to the predicate-aware
  // variant when a vertex or edge predicate is present.
  if (input.vertex_pred != nullptr || input.edge_pred != nullptr) {
    WCCPred wcc(graph, input.vertex_label, input.edge_label, input.concurrency,
                input.vertex_pred.get(), input.edge_pred.get());
    wcc.compute();
    wcc.sink(ret, input.node_alias, input.comp_alias);
  } else {
    WCC wcc(graph, input.vertex_label, input.edge_label, input.concurrency);
    wcc.compute();
    wcc.sink(ret, input.node_alias, input.comp_alias);
  }
  return ret;
}

function::function_set WCCFunction::getFunctionSet() {
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
      {"comp", common::DataTypeId::kInt64}};
  auto function = std::make_unique<function::GDSAlgoFunction>(name, inputTypes,
                                                              outputColumns);
  function->bindFunc = bind;
  function->execFunc = exec;

  funcSet.emplace_back(std::move(function));
  return funcSet;
}
}  // namespace gds
}  // namespace neug