/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/execution/execute/ops/retrieve/gds_algo.h"

#include "neug/compiler/main/metadata_registry.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace execution {
namespace ops {

GDSAlgoOpr::GDSAlgoOpr(physical::Subgraph subgraph, function::options_t options,
                       function::GDSAlgoFunction* algo_func)
    : subgraph_(std::move(subgraph)),
      options_(std::move(options)),
      algo_func_(algo_func) {}

neug::result<neug::execution::Context> GDSAlgoOpr::Eval(
    IStorageInterface& graph_interface, const ParamsMap& params,
    neug::execution::Context&& ctx, neug::execution::OprTimer* timer) {
  (void) params;
  (void) timer;
  if (algo_func_ == nullptr) {
    THROW_RUNTIME_ERROR("GDSAlgoOpr: GDSAlgoFunction pointer is null");
  }
  if (!algo_func_->algoExec) {
    THROW_RUNTIME_ERROR(
        "GDSAlgoOpr: algoExec not registered for GDS algorithm");
  }
  return algo_func_->algoExec(ctx, subgraph_, options_, graph_interface);
}

neug::result<OpBuildResultT> GDSAlgoOprBuilder::Build(
    const neug::Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  (void) schema;
  auto gCatalog = neug::main::MetadataRegistry::getCatalog();
  const auto& gds_pb = plan.plan(op_idx).opr().gds_algo();
  const std::string& algo_name = gds_pb.algo_name();
  auto* func = gCatalog->getFunctionWithSignature(algo_name);
  if (func == nullptr) {
    THROW_RUNTIME_ERROR("GDSAlgoOprBuilder: GDS function not found: " +
                        algo_name);
  }
  auto* gds_func = dynamic_cast<function::GDSAlgoFunction*>(func);
  if (gds_func == nullptr) {
    THROW_RUNTIME_ERROR("GDSAlgoOprBuilder: function is not GDSAlgoFunction: " +
                        algo_name);
  }

  function::options_t options;
  for (const auto& kv : gds_pb.options()) {
    options[kv.first] = kv.second;
  }
  // copy subgraph from protobuf
  physical::Subgraph subgraph = gds_pb.sub_graph();

  ContextMeta ret_meta = ctx_meta;
  return std::make_pair(std::make_unique<GDSAlgoOpr>(
                            std::move(subgraph), std::move(options), gds_func),
                        ret_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
