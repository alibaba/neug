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

#ifndef SRC_ENGINES_GRAPH_DB_RUNTIME_EXECUTE_OPS_BATCH_BATCH_UPDATE_EDGE_H_
#define SRC_ENGINES_GRAPH_DB_RUNTIME_EXECUTE_OPS_BATCH_BATCH_UPDATE_EDGE_H_

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
 * @brief UpdateEdgeOpr is used to update edge properties in batch.
 */
class UpdateEdgeOpr : public IUpdateOperator {
 public:
  using edge_data_t = std::tuple<int32_t, std::string,
                                 gs::Any>;  // tag_id, property_name, value
  using edge_data_vec_t = std::vector<edge_data_t>;

  UpdateEdgeOpr(edge_data_vec_t&& edge_data)
      : edge_data_(std::move(edge_data)) {}

  std::string get_operator_name() const override { return "UpdateEdgeOpr"; }

  gs::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;

 private:
  edge_data_vec_t edge_data_;
};

class UpdateEdgeOprBuilder : public IUpdateOperatorBuilder {
 public:
  UpdateEdgeOprBuilder() = default;
  ~UpdateEdgeOprBuilder() = default;

  std::unique_ptr<IUpdateOperator> Build(const Schema& schema,
                                         const physical::PhysicalPlan& plan,
                                         int op_idx) override;

  physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const override {
    return physical::PhysicalOpr_Operator::OpKindCase::kSetEdge;
  }
};

}  // namespace ops

}  // namespace runtime

}  // namespace gs

#endif  // SRC_ENGINES_GRAPH_DB_RUNTIME_EXECUTE_OPS_BATCH_BATCH_UPDATE_EDGE_H_