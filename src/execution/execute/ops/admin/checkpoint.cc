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

namespace gs {

namespace runtime {
namespace ops {

gs::result<Context> CheckpointOpr::Eval(
    GraphUpdateInterface& graph,
    const std::map<std::string, std::string>& params, Context&& ctx,
    OprTimer* timer) {
  graph.CreateCheckpoint();
  return gs::result<Context>(std::move(ctx));
}

std::unique_ptr<IAdminOperator> CheckpointOprBuilder::Build(
    const Schema& schema, const physical::AdminPlan& plan, int op_idx) {
  return std::make_unique<CheckpointOpr>();
}

}  // namespace ops

}  // namespace runtime

}  // namespace gs
