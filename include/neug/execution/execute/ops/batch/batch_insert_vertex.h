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

#ifndef INCLUDE_NEUG_EXECUTION_EXECUTE_OPS_BATCH_BATCH_INSERT_VERTEX_H_
#define INCLUDE_NEUG_EXECUTION_EXECUTE_OPS_BATCH_BATCH_INSERT_VERTEX_H_

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/execution/execute/operator.h"
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/cypher_dml.pb.h"
#include "neug/generated/proto/plan/physical.pb.h"
#include "neug/utils/property/types.h"

namespace gs {
class Schema;

namespace runtime {
class Context;
class OprTimer;

namespace ops {

class BatchInsertVertexOprBuilder : public IOperatorBuilder {
 public:
  BatchInsertVertexOprBuilder() = default;
  ~BatchInsertVertexOprBuilder() = default;

  gs::result<OpBuildResultT> Build(const Schema& schema,
                                   const ContextMeta& ctx_meta,
                                   const physical::PhysicalPlan& plan,
                                   int op_idx) override;

  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kLoadVertex};
  }
};

class InsertVertexOprBuilder : public IOperatorBuilder {
 public:
  InsertVertexOprBuilder() = default;
  ~InsertVertexOprBuilder() = default;

  gs::result<OpBuildResultT> Build(const Schema& schema,
                                   const ContextMeta& ctx_meta,
                                   const physical::PhysicalPlan& plan,
                                   int op_idx) override;
  std::vector<physical::PhysicalOpr_Operator::OpKindCase> GetOpKinds()
      const override {
    return {physical::PhysicalOpr_Operator::OpKindCase::kCreateVertex};
  }
};

}  // namespace ops
}  // namespace runtime
}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_EXECUTE_OPS_BATCH_BATCH_INSERT_EDGE_H_