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

#ifndef INCLUDE_NEUG_EXECUTION_EXECUTE_OPS_ADMIN_CHECKPOINT_H_
#define INCLUDE_NEUG_EXECUTION_EXECUTE_OPS_ADMIN_CHECKPOINT_H_

#include <map>
#include <memory>
#include <string>

#include "neug/execution/execute/operator.h"

namespace gs {
namespace runtime {
namespace ops {

class CheckpointOpr : public IOperator {
 public:
  CheckpointOpr() = default;
  ~CheckpointOpr() override = default;
  std::string get_operator_name() const override { return "CheckpointOpr"; }
  gs::result<Context> Eval(IStorageInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;
};

class CheckpointOprBuilder : public IAdminOperatorBuilder {
 public:
  CheckpointOprBuilder() = default;
  ~CheckpointOprBuilder() override = default;

  gs::result<OpBuildResultT> Build(const Schema& schema,
                                   const ContextMeta& ctx_meta,
                                   const physical::AdminPlan& plan,
                                   int op_id) override;

  physical::AdminPlan_Operator::KindCase GetOpKind() const override {
    return physical::AdminPlan_Operator::KindCase::kCheckpoint;
  }
};
}  // namespace ops
}  // namespace runtime

}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_EXECUTE_OPS_ADMIN_CHECKPOINT_H_