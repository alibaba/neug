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

#include <string_view>

namespace gs {
namespace runtime {
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

  gs::result<Context> Eval(IStorageInterface& graph,
                           const std::map<std::string, std::string>& params,
                           Context&& ctx, OprTimer* timer) override;

 private:
  std::vector<std::vector<std::tuple<label_t, label_t, label_t>>>
      edge_triplets_;
  std::vector<int32_t> edge_bindings_;
};

gs::result<Context> BatchDeleteEdgeOpr::Eval(
    IStorageInterface& graph_interface,
    const std::map<std::string, std::string>& params, Context&& ctx,
    OprTimer* timer) {
  auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
  size_t binding_size = edge_bindings_.size();
  for (size_t i = 0; i < binding_size; i++) {
    int32_t alias = edge_bindings_[i];
    auto& edge_triplets = edge_triplets_[i];
    auto edge_column = std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(alias));
    if (edge_triplets.size() == 1) {
      std::vector<std::tuple<vid_t, vid_t>> edges;
      label_t src_v_label = std::get<0>(edge_triplets[0]);
      label_t dst_v_label = std::get<1>(edge_triplets[0]);
      label_t edge_label = std::get<2>(edge_triplets[0]);
      if (edge_column->edge_column_type() == EdgeColumnType::kSDSL) {
        size_t edge_size = edge_column->size();
        if (edge_column->dir() == Direction::kOut) {
          for (size_t j = 0; j < edge_size; j++) {
            auto edge = edge_column->get_edge(j);
            edges.emplace_back(std::make_tuple(edge.src, edge.dst));
          }
        } else {
          for (size_t j = 0; j < edge_size; j++) {
            auto edge = edge_column->get_edge(j);
            edges.emplace_back(std::make_tuple(edge.dst, edge.src));
          }
        }
      } else if (edge_column->edge_column_type() == EdgeColumnType::kBDSL) {
        size_t edge_size = edge_column->size();
        for (size_t j = 0; j < edge_size; j++) {
          auto edge = edge_column->get_edge(j);
          if (edge.dir == Direction::kOut) {
            edges.emplace_back(std::make_tuple(edge.src, edge.dst));
          } else {
            edges.emplace_back(std::make_tuple(edge.dst, edge.src));
          }
        }
      } else if (edge_column->edge_column_type() == EdgeColumnType::kSDML) {
        size_t edge_size = edge_column->size();
        if (edge_column->dir() == Direction::kOut) {
          for (size_t j = 0; j < edge_size; j++) {
            auto edge = edge_column->get_edge(j);
            edges.emplace_back(std::make_tuple(edge.src, edge.dst));
          }
        } else {
          for (size_t j = 0; j < edge_size; j++) {
            auto edge = edge_column->get_edge(j);
            edges.emplace_back(std::make_tuple(edge.dst, edge.src));
          }
        }
      } else if (edge_column->edge_column_type() == EdgeColumnType::kBDML) {
        size_t edge_size = edge_column->size();
        for (size_t j = 0; j < edge_size; j++) {
          auto edge = edge_column->get_edge(j);
          if (edge.dir == Direction::kOut) {
            edges.emplace_back(std::make_tuple(edge.src, edge.dst));
          } else {
            edges.emplace_back(std::make_tuple(edge.dst, edge.src));
          }
        }
      } else {
        LOG(FATAL) << "Unknown edge column type.";
      }
      graph.BatchDeleteEdges(src_v_label, dst_v_label, edge_label, edges);
    } else {
      std::unordered_map<uint32_t, std::vector<std::tuple<vid_t, vid_t>>>
          edges_map;
      for (auto edge_triplet : edge_triplets) {
        label_t src_v_label = std::get<0>(edge_triplet);
        label_t dst_v_label = std::get<1>(edge_triplet);
        label_t edge_label = std::get<2>(edge_triplet);
        std::vector<std::tuple<vid_t, vid_t>> edges;
        uint32_t index = graph.schema().generate_edge_label(
            src_v_label, dst_v_label, edge_label);
        edges_map.insert({index, edges});
      }
      if (edge_column->edge_column_type() == EdgeColumnType::kSDML) {
        if (edge_column->dir() == Direction::kOut) {
          for (size_t j = 0; j < edge_column->size(); j++) {
            auto edge = edge_column->get_edge(j);
            label_t src_v_label = edge.label.src_label;
            label_t dst_v_label = edge.label.dst_label;
            label_t edge_label = edge.label.edge_label;
            uint32_t index = graph.schema().generate_edge_label(
                src_v_label, dst_v_label, edge_label);
            edges_map.at(index).emplace_back(
                std::make_tuple(edge.src, edge.dst));
          }
        } else {
          for (size_t j = 0; j < edge_column->size(); j++) {
            auto edge = edge_column->get_edge(j);
            label_t src_v_label = edge.label.src_label;
            label_t dst_v_label = edge.label.dst_label;
            label_t edge_label = edge.label.edge_label;
            uint32_t index = graph.schema().generate_edge_label(
                src_v_label, dst_v_label, edge_label);
            edges_map.at(index).emplace_back(
                std::make_tuple(edge.dst, edge.src));
          }
        }
      } else if (edge_column->edge_column_type() == EdgeColumnType::kBDML) {
        size_t edge_size = edge_column->size();
        for (size_t j = 0; j < edge_size; j++) {
          auto edge = edge_column->get_edge(j);
          label_t src_v_label = edge.label.src_label;
          label_t dst_v_label = edge.label.dst_label;
          label_t edge_label = edge.label.edge_label;
          uint32_t index = graph.schema().generate_edge_label(
              src_v_label, dst_v_label, edge_label);
          if (edge.dir == Direction::kOut) {
            edges_map.at(index).emplace_back(
                std::make_tuple(edge.src, edge.dst));
          } else {
            edges_map.at(index).emplace_back(
                std::make_tuple(edge.dst, edge.src));
          }
        }
      } else {
        LOG(FATAL)
            << "Size of edge triplet is not consistent with edge column type.";
      }
    }
    std::vector<size_t> offsets;
    ctx.reshuffle(offsets);  // reshuffle the context with empty offsets, to
                             // remove all data.
  }

  return gs::result<Context>(std::move(ctx));
}

gs::result<OpBuildResultT> BatchDeleteEdgeOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta meta = ctx_meta;
  const auto& opr = plan.query_plan().plan(op_idx).opr().delete_edge();
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
}  // namespace runtime
}  // namespace gs