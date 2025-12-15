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
#include "neug/generated/proto/plan/cypher_ddl.pb.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/property/types.h"

namespace gs {

namespace runtime {
namespace ops {

class RenameVertexPropertyOprBuilder : public ISchemaOperatorBuilder {
 public:
  RenameVertexPropertyOprBuilder() = default;
  ~RenameVertexPropertyOprBuilder() = default;
  gs::result<OpBuildResultT> Build(const Schema& schema,
                                   const ContextMeta& ctx_meta,
                                   const physical::DDLPlan& plan,
                                   int op_id) override;
  physical::DDLPlan::PlanCase GetOpKind() const override {
    return physical::DDLPlan::PlanCase::kRenameVertexPropertySchema;
  }
};

}  // namespace ops

}  // namespace runtime

}  // namespace gs
