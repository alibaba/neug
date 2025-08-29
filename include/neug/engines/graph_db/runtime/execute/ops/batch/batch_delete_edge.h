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

#ifndef RUNTIME_EXECUTE_OPS_BATCH_BATCH_DELETE_EDGE_H_
#define RUNTIME_EXECUTE_OPS_BATCH_BATCH_DELETE_EDGE_H_

#include "neug/engines/graph_db/runtime/execute/operator.h"
#include "neug/engines/graph_db/runtime/execute/ops/batch/batch_update_utils.h"

namespace gs {
namespace runtime {
namespace ops {
class BatchDeleteEdgeOpr : public IUpdateOperator {
 public:
  BatchDeleteEdgeOpr(
      const std::vector<std::vector<std::tuple<label_t, label_t, label_t>>>&
          edge_triplets,
      const std::vector<int32_t> edge_bindings)
      : edge_triplets_(edge_triplets), edge_bindings_(edge_bindings) {}

  std::string get_operator_name() const override {
    return "BatchDeleteEdgeOpr";
  }

  gs::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;

 private:
  std::vector<std::vector<std::tuple<label_t, label_t, label_t>>>
      edge_triplets_;
  std::vector<int32_t> edge_bindings_;
};

class BatchDeleteEdgeOprBuilder : public IUpdateOperatorBuilder {
 public:
  BatchDeleteEdgeOprBuilder() = default;
  ~BatchDeleteEdgeOprBuilder() = default;

  std::unique_ptr<IUpdateOperator> Build(const Schema& schema,
                                         const physical::PhysicalPlan& plan,
                                         int op_idx) override;

  physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const override {
    return physical::PhysicalOpr_Operator::OpKindCase::kDeleteEdge;
  }
};

}  // namespace ops
}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_EXECUTE_OPS_BATCH_BATCH_DELETE_EDGE_H_