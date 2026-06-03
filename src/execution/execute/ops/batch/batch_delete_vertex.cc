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

#include "neug/execution/execute/ops/batch/batch_delete_vertex.h"
#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/utils/mi_allocator.h"

namespace neug {
namespace execution {
namespace ops {

class BatchDeleteVertexOpr : public IOperator {
 public:
  BatchDeleteVertexOpr(const vector_t<vector_t<label_t>>& vertex_labels,
                       const vector_t<int32_t> vertex_bindings)
      : vertex_labels_(vertex_labels), vertex_bindings_(vertex_bindings) {}

  std::string get_operator_name() const override {
    return "BatchDeleteVertexOpr";
  }

  neug::result<Context> Eval(IStorageInterface& graph, const ParamsMap& params,
                             Context&& ctx, OprTimer* timer) override;

 private:
  vector_t<vector_t<label_t>> vertex_labels_;
  vector_t<int32_t> vertex_bindings_;
};

neug::result<Context> BatchDeleteVertexOpr::Eval(
    IStorageInterface& graph_interface, const ParamsMap& params, Context&& ctx,
    OprTimer* timer) {
  auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
  size_t binding_size = vertex_bindings_.size();
  for (size_t i = 0; i < binding_size; i++) {
    int32_t alias = vertex_bindings_[i];
    auto vertex_column =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(alias));
    if (vertex_column->vertex_column_type() == VertexColumnType::kSingle) {
      auto sl_vertex_column =
          std::dynamic_pointer_cast<SLVertexColumn>(vertex_column);
      // Storage interface takes std::vector, so we build std::vector here
      // rather than vector_t to avoid an allocator-mismatched copy at the
      // call boundary.
      std::vector<vid_t> vids;
      vids.reserve(sl_vertex_column->size());
      for (auto vid : sl_vertex_column->vertices()) {
        vids.emplace_back(vid);
      }
      RETURN_STATUS_ERROR_IF_NOT_OK(
          graph.BatchDeleteVertices(sl_vertex_column->label(), vids));
    } else if (vertex_column->vertex_column_type() ==
                   VertexColumnType::kMultiple ||
               vertex_column->vertex_column_type() ==
                   VertexColumnType::kMultiSegment) {
      // Same boundary reason: keep std::vector for what we hand to the
      // storage layer's BatchDeleteVertices.
      flat_hash_map_t<label_t, std::vector<vid_t>> vids_map;
      for (auto label : vertex_column->get_labels_set()) {
        std::vector<vid_t> vids;
        vids_map.insert({label, vids});
      }
      size_t vertex_size = vertex_column->size();
      for (size_t j = 0; j < vertex_size; j++) {
        auto vertex = vertex_column->get_vertex(j);
        vids_map.at(vertex.label_).emplace_back(vertex.vid_);
      }
      for (auto& vids_pair : vids_map) {
        RETURN_STATUS_ERROR_IF_NOT_OK(
            graph.BatchDeleteVertices(vids_pair.first, vids_pair.second));
      }
    } else {
      THROW_RUNTIME_ERROR(
          "Unsupported vertex column type for batch delete vertex operation.");
    }
    ctx.reshuffle(sel_vec_t());  // reshuffle the context with empty offsets, to
                                 // remove all data.
  }

  return neug::result<Context>(std::move(ctx));
}

neug::result<OpBuildResultT> BatchDeleteVertexOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta = ctx_meta;
  const auto& opr = plan.plan(op_idx).opr().delete_vertex();
  vector_t<vector_t<label_t>> vertex_types;
  vector_t<int32_t> vertex_bindings;
  for (auto& entry : opr.entries()) {
    auto& vertex_binding = entry.vertex_binding();
    vector_t<label_t> vertex_type;
    vertex_bindings.emplace_back(vertex_binding.tag().id());
    for (auto& graph_data_type :
         vertex_binding.node_type().graph_type().graph_data_type()) {
      vertex_type.emplace_back(graph_data_type.label().label());
    }
    vertex_types.emplace_back(vertex_type);
  }

  CHECK(vertex_types.size() == vertex_bindings.size());
  return std::make_pair(
      std::make_unique<BatchDeleteVertexOpr>(vertex_types, vertex_bindings),
      ret_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug