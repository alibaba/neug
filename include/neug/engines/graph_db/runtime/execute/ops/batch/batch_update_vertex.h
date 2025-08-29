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

#ifndef SRC_ENGINES_GRAPH_DB_RUNTIME_EXECUTE_OPS_BATCH_BATCH_UPDATE_VERTEX_H_
#define SRC_ENGINES_GRAPH_DB_RUNTIME_EXECUTE_OPS_BATCH_BATCH_UPDATE_VERTEX_H_

#include "neug/engines/graph_db/runtime/execute/operator.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/physical.pb.h"
#else
#include "neug/utils/proto/plan/physical.pb.h"
#endif

namespace gs {

namespace runtime {
namespace ops {

/**
 * @brief UpdateVertexOpr is used to update vertex properties in batch.
 */
class UpdateVertexOpr : public IUpdateOperator {
 public:
  using vertex_prop_vec_t = std::vector<std::tuple<int32_t, std::string, Any>>;
  UpdateVertexOpr(vertex_prop_vec_t&& vertex_data)
      : vertex_data_(std::move(vertex_data)) {}

  std::string get_operator_name() const override { return "UpdateVertexOpr"; }

  template <typename GraphInterface>
  gs::result<Context> eval_impl(
      GraphInterface& graph, const std::map<std::string, std::string>& params,
      Context&& ctx, OprTimer* timer);

  gs::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;

 private:
  // No alias is produced in this operator.
  vertex_prop_vec_t vertex_data_;
};

class UpdateVertexOprBuilder : public IUpdateOperatorBuilder {
 public:
  UpdateVertexOprBuilder() = default;
  ~UpdateVertexOprBuilder() = default;

  std::unique_ptr<IUpdateOperator> Build(const Schema& schema,
                                         const physical::PhysicalPlan& plan,
                                         int op_idx) override;

  physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const override {
    return physical::PhysicalOpr_Operator::OpKindCase::kSetVertex;
  }
};

}  // namespace ops
}  // namespace runtime
}  // namespace gs

#endif  // SRC_ENGINES_GRAPH_DB_RUNTIME_EXECUTE_OPS_BATCH_BATCH_UPDATE_VERTEX_H_