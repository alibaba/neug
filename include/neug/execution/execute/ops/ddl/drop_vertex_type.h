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
#pragma once

#include "neug/execution/execute/operator.h"

namespace neug {
namespace execution {
namespace ops {
class DropVertexTypeOprBuilder : public IOperatorBuilder {
 public:
  DropVertexTypeOprBuilder() = default;
  ~DropVertexTypeOprBuilder() = default;
  neug::result<OpBuildResultT> Build(const Schema& schema,
                                     const ContextMeta& ctx_meta,
                                     const physical::PhysicalPlan& plan,
                                     int op_id) override;
  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kDropVertexSchema};
  }
};
}  // namespace ops
}  // namespace execution

}  // namespace neug