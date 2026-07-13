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

#include "leiden.h"

#include "impl/leiden_impl.h"
#include "utils/option_utils.h"
#include "utils/subgraph_utils.h"

namespace neug {
namespace gds {

struct LeidenInput : public function::CallFuncInputBase {
  ~LeidenInput() = default;
  LeidenInput() = default;

  bool parse_subgraph(const ::physical::Subgraph& subgraph,
                      const execution::ContextMeta& ctx_meta) {
    ParsedSubgraph parsed;
    if (!parse_subgraph_entries(subgraph, ctx_meta, parsed)) {
      return false;
    }
    if (parsed.vertex_entries.empty()) {
      LOG(ERROR) << "leiden requires at least one vertex label.";
      return false;
    }
    if (parsed.edge_entries.empty()) {
      LOG(ERROR) << "leiden requires at least one edge label.";
      return false;
    }
    for (const auto& ve : parsed.vertex_entries) {
      if (ve.predicate != nullptr) {
        LOG(ERROR) << "Vertex predicates are not supported in leiden.";
        return false;
      }
    }
    for (const auto& ee : parsed.edge_entries) {
      if (ee.predicate != nullptr) {
        LOG(ERROR) << "Edge predicates are not supported in leiden.";
        return false;
      }
    }
    for (const auto& ve : parsed.vertex_entries) {
      vertex_labels.push_back(ve.label);
    }
    for (const auto& ee : parsed.edge_entries) {
      edge_triplets.push_back(ee.triplet);
    }
    return true;
  }

  std::vector<label_t> vertex_labels;
  std::vector<LabelTriplet> edge_triplets;
  double resolution = 1.0;
  bool directed = false;
  double threshold = 1e-7;
  int32_t concurrency;
  std::string initial_community_property;
  int32_t node_alias;
  int32_t community_alias;
  int32_t previous_community_alias = -1;
};

std::unique_ptr<function::CallFuncInputBase> LeidenFunction::bind(
    const Schema& schema, const execution::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.plan(op_idx).opr();
  const auto& subgraph = opr.gds_algo().sub_graph();
  const auto& options = opr.gds_algo().options();

  auto input = std::make_unique<LeidenInput>();
  if (!input->parse_subgraph(subgraph, ctx_meta)) {
    LOG(ERROR) << "Failed to parse subgraph for leiden.";
    THROW_NOT_SUPPORTED_EXCEPTION("Invalid subgraph for leiden");
  }

  input->resolution = get_option_value<double>(options, "resolution", 1.0);
  input->directed = get_option_value<bool>(options, "directed", false);
  input->threshold = get_option_value<double>(options, "threshold", 1e-7);
  input->concurrency = get_option_value<int32_t>(
      options, "concurrency", std::thread::hardware_concurrency());
  input->initial_community_property =
      get_option_value<std::string>(options, "initial_community_property", "");

  input->node_alias = -1;
  input->community_alias = -1;
  input->previous_community_alias = -1;
  int int64_count = 0;
  const auto& meta_data = plan.plan(op_idx);
  for (int i = 0; i < meta_data.meta_data_size(); i++) {
    const auto& meta = meta_data.meta_data(i);
    auto type = parse_from_ir_data_type(meta.type());
    if (type.id() == common::DataTypeId::kVertex) {
      input->node_alias = meta.alias();
    } else if (type.id() == common::DataTypeId::kInt64) {
      if (int64_count == 0)
        input->community_alias = meta.alias();
      else
        input->previous_community_alias = meta.alias();
      ++int64_count;
    }
  }

  return input;
}

execution::Context LeidenFunction::exec(
    const function::CallFuncInputBase& input_base, IStorageInterface& g) {
  const auto& input = dynamic_cast<const LeidenInput&>(input_base);
  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);

  // directed is accepted for interface compatibility but ignored (same as
  // the original leiden implementation).
  community::Leiden leiden(graph, input.vertex_labels, input.edge_triplets,
                           input.resolution, input.threshold, input.concurrency,
                           input.initial_community_property);
  leiden.compute();

  execution::Context ctx;
  leiden.sink(ctx, input.node_alias, input.community_alias,
              input.previous_community_alias);
  return ctx;
}

function::function_set LeidenFunction::getFunctionSet() {
  function::function_set funcSet;
  function::call_input_types inputTypes = {
      common::DataType(common::DataTypeId::kVarchar),
      common::DataType(common::DataTypeId::kUnknown)};
  function::call_output_columns outputColumns = {
      {"node", common::DataType(common::DataTypeId::kVertex)},
      {"community", common::DataType(common::DataTypeId::kInt64)},
      {"previous_community", common::DataType(common::DataTypeId::kInt64)}};
  auto function = std::make_unique<function::GDSAlgoFunction>(name, inputTypes,
                                                              outputColumns);
  function->bindFunc = bind;
  function->execFunc = exec;

  funcSet.emplace_back(std::move(function));
  return funcSet;
}

}  // namespace gds
}  // namespace neug
