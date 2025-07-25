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

#ifndef RUNTIME_EXECUTE_OPS_BATCH_BATCH_INSERT_EDGE_H_
#define RUNTIME_EXECUTE_OPS_BATCH_BATCH_INSERT_EDGE_H_

#include <boost/leaf.hpp>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "neug/engines/graph_db/runtime/execute/operator.h"
#include "neug/engines/graph_db/runtime/execute/ops/batch/batch_update_utils.h"
#include "neug/proto_generated_gie/physical.pb.h"
#include "neug/storages/rt_mutable_graph/types.h"
#include "neug/utils/property/types.h"
namespace gs {
class Schema;

namespace runtime {
class Context;
class GraphUpdateInterface;
class OprTimer;

namespace ops {

class BatchInsertEdgeOpr : public IUpdateOperator {
 public:
  BatchInsertEdgeOpr(
      const label_t& edge_label_id, const label_t& src_label_id,
      const label_t& dst_label_id, const PropertyType& e_prop,
      const PropertyType& src_pk_prop, const PropertyType& dst_pk_prop,
      const std::vector<std::pair<int32_t, std::string>>& prop_mappings,
      const std::vector<std::pair<int32_t, std::string>>& src_vertex_bindings,
      const std::vector<std::pair<int32_t, std::string>>& dst_vertex_bindings)
      : edge_label_id_(edge_label_id),
        src_label_id_(src_label_id),
        dst_label_id_(dst_label_id),
        e_prop_(e_prop),
        src_pk_prop_(src_pk_prop),
        dst_pk_prop_(dst_pk_prop),
        prop_mappings_(prop_mappings),
        src_vertex_bindings_(src_vertex_bindings),
        dst_vertex_bindings_(dst_vertex_bindings) {}

  std::string get_operator_name() const override {
    return "BatchInsertEdgeOpr";
  }

  bl::result<Context> Eval(GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer& timer) override;

 private:
  label_t edge_label_id_, src_label_id_, dst_label_id_;
  PropertyType e_prop_, src_pk_prop_, dst_pk_prop_;
  std::vector<std::pair<int32_t, std::string>> prop_mappings_,
      src_vertex_bindings_, dst_vertex_bindings_;
};

class BatchInsertEdgeOprBuilder : public IUpdateOperatorBuilder {
 public:
  BatchInsertEdgeOprBuilder() = default;
  ~BatchInsertEdgeOprBuilder() = default;

  std::unique_ptr<IUpdateOperator> Build(const Schema& schema,
                                         const physical::PhysicalPlan& plan,
                                         int op_idx) override;

  physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const override {
    return physical::PhysicalOpr_Operator::OpKindCase::kLoadEdge;
  }
};

class InsertEdgeOpr : public IUpdateOperator {
 public:
  using edge_prop_vec_t = std::vector<std::pair<std::string, Any>>;
  using edge_triplet_t =
      std::tuple<label_t, label_t, label_t>;  // edge, src, dst
  using edge_data_t =
      std::tuple<edge_triplet_t, int32_t, int32_t, edge_prop_vec_t>;
  // type_triplet, src_vertex_bindings, dst_vertex_bindings, edge_properties
  InsertEdgeOpr(std::vector<edge_data_t>&& edge_data)
      : edge_data_(std::move(edge_data)) {}

  std::string get_operator_name() const override { return "InsertEdgeOpr"; }

  template <typename GraphInterface>
  bl::result<Context> eval_impl(
      GraphInterface& graph, const std::map<std::string, std::string>& params,
      Context&& ctx, gs::runtime::OprTimer& timer);

  bl::result<Context> Eval(gs::runtime::GraphUpdateInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx,
                           gs::runtime::OprTimer& timer) override {
    return eval_impl(graph, params, std::move(ctx), timer);
  }

 private:
  std::vector<edge_data_t> edge_data_;
};

class InsertEdgeOprBuilder : public IUpdateOperatorBuilder {
 public:
  InsertEdgeOprBuilder() = default;
  ~InsertEdgeOprBuilder() = default;

  std::unique_ptr<IUpdateOperator> Build(const Schema& schema,
                                         const physical::PhysicalPlan& plan,
                                         int op_idx) override;

  physical::PhysicalOpr_Operator::OpKindCase GetOpKind() const override {
    return physical::PhysicalOpr_Operator::OpKindCase::kCreateEdge;
  }
};

}  // namespace ops
}  // namespace runtime
}  // namespace gs

#endif  // RUNTIME_EXECUTE_OPS_BATCH_BATCH_INSERT_EDGE_H_