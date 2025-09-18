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
#ifndef EXECUTION_EXECUTE_OPS_BATCH_BATCH_DELETE_VERTEX_H_
#define EXECUTION_EXECUTE_OPS_BATCH_BATCH_DELETE_VERTEX_H_

#include "neug/execution/execute/operator.h"
#include "neug/execution/execute/ops/batch/batch_update_utils.h"

namespace gs {
namespace runtime {
namespace ops {
class BatchDeleteVertexOpr : public IUpdateOperator {
 public:
  BatchDeleteVertexOpr(const std::vector<std::vector<label_t>>& vertex_labels,
                       const std::vector<int32_t> vertex_bindings)
      : vertex_labels_(vertex_labels), vertex_bindings_(vertex_bindings) {}

  std::string get_operator_name() const override {
    return "BatchDeleteVertexOpr";
  }

  gs::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;

 private:
  std::vector<std::vector<label_t>> vertex_labels_;
  std::vector<int32_t> vertex_bindings_;
};

class BatchDeleteVertexOprBuilder : public IUpdateOperatorBuilder {
 public:
  BatchDeleteVertexOprBuilder() = default;
  ~BatchDeleteVertexOprBuilder() = default;

  std::unique_ptr<IUpdateOperator> Build(const Schema& schema,
                                         const physical::PhysicalPlan& plan,
                                         int op_idx) override;

  physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const override {
    return physical::PhysicalOpr_Operator::OpKindCase::kDeleteVertex;
  }
};
}  // namespace ops
}  // namespace runtime
}  // namespace gs

#endif  // EXECUTION_EXECUTE_OPS_BATCH_BATCH_DELETE_VERTEX_H_