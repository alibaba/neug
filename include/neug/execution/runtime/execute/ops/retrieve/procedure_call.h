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

#include <vector>

#include "neug/execution/runtime/execute/operator.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/generated/proto/plan/stored_procedure.pb.h"
#else
#include "neug/utils/proto/plan/physical.pb.h"
#include "neug/utils/proto/plan/stored_procedure.pb.h"
#endif

namespace gs {
class Schema;

namespace runtime {
class ContextMeta;

namespace ops {
class ProcedureCallOprBuilder : public IReadOperatorBuilder {
 public:
  ProcedureCallOprBuilder() = default;
  ~ProcedureCallOprBuilder() = default;

  gs::result<ReadOpBuildResultT> Build(const gs::Schema& schema,
                                       const ContextMeta& ctx_meta,
                                       const physical::PhysicalPlan& plan,
                                       int op_idx) override;

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kProcedureCall};
  }
};
}  // namespace ops
}  // namespace runtime
}  // namespace gs