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

#ifndef RUNTIME_EXECUTE_OPS_BATCH_BATCH_INSERT_VERTEX_H_
#define RUNTIME_EXECUTE_OPS_BATCH_BATCH_INSERT_VERTEX_H_

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/engines/graph_db/runtime/execute/operator.h"
#include "neug/storages/rt_mutable_graph/types.h"
#include "neug/utils/property/types.h"
#ifdef USE_SYSTEM_PROTOBUF
#include "neug/generated/proto/plan/common.pb.h"
#include "neug/generated/proto/plan/cypher_dml.pb.h"
#include "neug/generated/proto/plan/physical.pb.h"
#else
#include "neug/utils/proto/plan/common.pb.h"
#include "neug/utils/proto/plan/cypher_dml.pb.h"
#include "neug/utils/proto/plan/physical.pb.h"
#endif

namespace gs {
class Schema;

namespace runtime {
class Context;
class GraphUpdateInterface;
class OprTimer;

namespace ops {

class BatchInsertVertexOpr : public IUpdateOperator {
 public:
  BatchInsertVertexOpr(
      const label_t& vertex_label_id, const PropertyType& pk_type,
      const std::vector<std::pair<int32_t, std::string>>& prop_mappings)
      : vertex_label_id_(vertex_label_id),
        pk_type_(pk_type),
        prop_mappings_(prop_mappings) {}

  std::string get_operator_name() const override {
    return "BatchInsertVertexOpr";
  }

  gs::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;

 private:
  label_t vertex_label_id_;
  PropertyType pk_type_;
  std::vector<std::pair<int32_t, std::string>> prop_mappings_;
};

class BatchInsertVertexOprBuilder : public IUpdateOperatorBuilder {
 public:
  BatchInsertVertexOprBuilder() = default;
  ~BatchInsertVertexOprBuilder() = default;

  std::unique_ptr<IUpdateOperator> Build(const Schema& schema,
                                         const physical::PhysicalPlan& plan,
                                         int op_idx) override;

  physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const override {
    return physical::PhysicalOpr_Operator::OpKindCase::kLoadVertex;
  }
};

class InsertVertexOpr : public IUpdateOperator {
 public:
  using vertex_prop_vec_t = std::vector<std::pair<std::string, Any>>;
  InsertVertexOpr(std::vector<std::tuple<label_t, vertex_prop_vec_t, int32_t>>&&
                      vertex_data)
      : vertex_data_(std::move(vertex_data)) {}

  std::string get_operator_name() const override { return "InsertVertexOpr"; }

  template <typename GraphInterface>
  gs::result<Context> eval_impl(
      GraphInterface& graph, const std::map<std::string, std::string>& params,
      Context&& ctx, OprTimer* timer);

  gs::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;

 private:
  std::vector<std::tuple<label_t, vertex_prop_vec_t, int32_t>> vertex_data_;
};

class InsertVertexOprBuilder : public IUpdateOperatorBuilder {
 public:
  InsertVertexOprBuilder() = default;
  ~InsertVertexOprBuilder() = default;

  std::unique_ptr<IUpdateOperator> Build(const Schema& schema,
                                         const physical::PhysicalPlan& plan,
                                         int op_idx) override;

  physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const override {
    return physical::PhysicalOpr_Operator::OpKindCase::kCreateVertex;
  }
};

}  // namespace ops

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_EXECUTE_OPS_BATCH_BATCH_INSERT_VERTEX_H_
