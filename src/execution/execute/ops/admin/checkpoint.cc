/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/execution/execute/ops/admin/checkpoint.h"

namespace neug {

namespace execution {

namespace ops {

class CheckpointOpr : public IOperator {
 public:
  CheckpointOpr() = default;
  ~CheckpointOpr() override = default;
  std::string get_operator_name() const override { return "CheckpointOpr"; }
  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override;
};

neug::result<Context> CheckpointOpr::Eval(IStorageInterface& graph_interface,
                                          const ParamsMap& params,
                                          Context&& ctx, OprTimer* timer) {
  auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
  graph.CreateCheckpoint();
  return neug::result<Context>(std::move(ctx));
}

neug::result<OpBuildResultT> CheckpointOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_id) {
  return std::make_pair(std::make_unique<CheckpointOpr>(), ctx_meta);
}

}  // namespace ops

}  // namespace execution

}  // namespace neug