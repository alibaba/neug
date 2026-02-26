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

#include "neug/execution/execute/ops/batch/batch_delete_edge.h"
#include "neug/execution/common/columns/edge_columns.h"
#include "neug/storages/csr/generic_view_utils.h"

#include <string_view>

namespace neug {
namespace execution {
namespace ops {
class BatchDeleteEdgeOpr : public IOperator {
 public:
  BatchDeleteEdgeOpr(
      const std::vector<std::vector<std::tuple<label_t, label_t, label_t>>>&
          edge_triplets,
      const std::vector<int32_t> edge_bindings)
      : edge_triplets_(edge_triplets), edge_bindings_(edge_bindings) {}

  std::string get_operator_name() const override {
    return "BatchDeleteEdgeOpr";
  }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override;

 private:
  std::vector<std::vector<std::tuple<label_t, label_t, label_t>>>
      edge_triplets_;
  std::vector<int32_t> edge_bindings_;
};

neug::result<Context> BatchDeleteEdgeOpr::Eval(
    IStorageInterface& graph_interface, const ParamsMap& params, Context&& ctx,
    OprTimer* timer) {
  auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
  size_t binding_size = edge_bindings_.size();
  for (size_t i = 0; i < binding_size; i++) {
    int32_t alias = edge_bindings_[i];
    auto& edge_triplets = edge_triplets_[i];
    auto edge_column = std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(alias));
    if (edge_triplets.size() == 1) {
      label_t src_v_label = std::get<0>(edge_triplets[0]);
      label_t dst_v_label = std::get<1>(edge_triplets[0]);
      label_t edge_label = std::get<2>(edge_triplets[0]);
      LabelTriplet request_triplet =
          LabelTriplet(src_v_label, dst_v_label, edge_label);
      size_t edge_size = edge_column->size();
      auto oe_view = graph.GetGenericOutgoingGraphView(src_v_label, dst_v_label,
                                                       edge_label);
      auto ie_view = graph.GetGenericIncomingGraphView(dst_v_label, src_v_label,
                                                       edge_label);
      auto edge_prop_types = graph.schema().get_edge_properties(
          src_v_label, dst_v_label, edge_label);
      std::vector<std::pair<vid_t, int32_t>> oe_to_delete, ie_to_delete;
      oe_to_delete.reserve(edge_size);
      ie_to_delete.reserve(edge_size);
      for (size_t j = 0; j < edge_size; j++) {
        auto record = edge_column->get_edge(j);
        if (record.label == request_triplet) {
          auto offset_pair = record_to_csr_offset_pair(oe_view, ie_view, record,
                                                       edge_prop_types);
          oe_to_delete.emplace_back(record.src, offset_pair.first);
          ie_to_delete.emplace_back(record.dst, offset_pair.second);
        }
      }
      RETURN_STATUS_ERROR_IF_NOT_OK(graph.BatchDeleteEdges(
          src_v_label, dst_v_label, edge_label, oe_to_delete, ie_to_delete));
    } else {
      std::unordered_map<uint32_t, std::vector<EdgeRecord>> edges_map;
      for (size_t j = 0; j < edge_column->size(); j++) {
        auto edge = edge_column->get_edge(j);
        uint32_t index = graph.schema().generate_edge_label(
            edge.label.src_label, edge.label.dst_label, edge.label.edge_label);
        if (edges_map.find(index) != edges_map.end()) {
          edges_map[index].emplace_back(edge);
        } else {
          edges_map[index] = {edge};
        }
      }

      for (auto& [index, edges] : edges_map) {
        auto [src_v_label, dst_v_label, edge_label] =
            graph.schema().parse_edge_label(index);
        auto oe_view = graph.GetGenericOutgoingGraphView(
            src_v_label, dst_v_label, edge_label);
        auto ie_view = graph.GetGenericIncomingGraphView(
            dst_v_label, src_v_label, edge_label);
        std::vector<std::pair<vid_t, int32_t>> oe_to_delete, ie_to_delete;
        oe_to_delete.reserve(edges.size());
        ie_to_delete.reserve(edges.size());
        auto edge_prop_types = graph.schema().get_edge_properties(
            src_v_label, dst_v_label, edge_label);
        for (auto& record : edges) {
          auto offset_pair = record_to_csr_offset_pair(oe_view, ie_view, record,
                                                       edge_prop_types);
          oe_to_delete.emplace_back(record.src, offset_pair.first);
          ie_to_delete.emplace_back(record.dst, offset_pair.second);
        }
        RETURN_STATUS_ERROR_IF_NOT_OK(graph.BatchDeleteEdges(
            src_v_label, dst_v_label, edge_label, oe_to_delete, ie_to_delete));
      }
    }
    std::vector<size_t> offsets;
    ctx.reshuffle(offsets);  // reshuffle the context with empty offsets, to
                             // remove all data.
  }

  return neug::result<Context>(std::move(ctx));
}

neug::result<OpBuildResultT> BatchDeleteEdgeOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta meta = ctx_meta;
  const auto& opr = plan.plan(op_idx).opr().delete_edge();
  std::vector<std::vector<std::tuple<label_t, label_t, label_t>>> edge_types;
  std::vector<int32_t> edge_bindings;
  for (auto& edge_binding : opr.edge_binding()) {
    std::vector<std::tuple<label_t, label_t, label_t>> edge_type;
    edge_bindings.emplace_back(edge_binding.tag().id());
    for (auto& graph_data_type :
         edge_binding.node_type().graph_type().graph_data_type()) {
      edge_type.emplace_back(std::make_tuple(
          static_cast<label_t>(graph_data_type.label().src_label().value()),
          static_cast<label_t>(graph_data_type.label().dst_label().value()),
          static_cast<label_t>(graph_data_type.label().label())));
    }
    edge_types.emplace_back(edge_type);
  }

  CHECK(edge_types.size() == edge_bindings.size());
  return std::make_pair(
      std::make_unique<BatchDeleteEdgeOpr>(edge_types, edge_bindings), meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug