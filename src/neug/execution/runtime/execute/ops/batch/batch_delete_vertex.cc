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

#include "neug/execution/runtime/execute/ops/batch/batch_delete_vertex.h"
#include "neug/execution/runtime/common/columns/edge_columns.h"

#include <string_view>

namespace gs {
namespace runtime {
namespace ops {
gs::result<Context> BatchDeleteVertexOpr::Eval(
    GraphUpdateInterface& graph,
    const std::map<std::string, std::string>& params, Context&& ctx,
    OprTimer* timer) {
  auto& frag = graph.GetTransaction().GetGraph();
  size_t binding_size = vertex_bindings_.size();
  for (size_t i = 0; i < binding_size; i++) {
    int32_t alias = vertex_bindings_[i];
    auto vertex_column =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(alias));
    if (vertex_column->vertex_column_type() == VertexColumnType::kSingle) {
      if (vertex_column->is_optional()) {
        auto sl_vertex_column =
            std::dynamic_pointer_cast<OptionalSLVertexColumn>(vertex_column);
        frag.batch_delete_vertices(sl_vertex_column->label(),
                                   sl_vertex_column->vertices());
      } else {
        auto sl_vertex_column =
            std::dynamic_pointer_cast<SLVertexColumn>(vertex_column);
        frag.batch_delete_vertices(sl_vertex_column->label(),
                                   sl_vertex_column->vertices());
      }
    } else if (vertex_column->vertex_column_type() ==
               VertexColumnType::kMultiple) {
      std::unordered_map<label_t, std::vector<vid_t>> vids_map;
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
        frag.batch_delete_vertices(vids_pair.first, vids_pair.second);
      }
    } else {
      THROW_RUNTIME_ERROR(
          "Unsupported vertex column type for batch delete vertex operation.");
    }
    std::vector<size_t> offsets;
    ctx.reshuffle(offsets);  // reshuffle the context with empty offsets, to
                             // remove all data.
  }

  return gs::result<Context>(std::move(ctx));
}

std::unique_ptr<IUpdateOperator> BatchDeleteVertexOprBuilder::Build(
    const Schema& schema, const physical::PhysicalPlan& plan, int op_idx) {
  const auto& opr = plan.query_plan().plan(op_idx).opr().delete_vertex();
  std::vector<std::vector<label_t>> vertex_types;
  std::vector<int32_t> vertex_bindings;
  for (auto& entry : opr.entries()) {
    auto& vertex_binding = entry.vertex_binding();
    std::vector<label_t> vertex_type;
    vertex_bindings.emplace_back(vertex_binding.tag().id());
    for (auto& graph_data_type :
         vertex_binding.node_type().graph_type().graph_data_type()) {
      vertex_type.emplace_back(graph_data_type.label().label());
    }
    vertex_types.emplace_back(vertex_type);
  }

  CHECK(vertex_types.size() == vertex_bindings.size());
  return std::make_unique<BatchDeleteVertexOpr>(vertex_types, vertex_bindings);
}

}  // namespace ops
}  // namespace runtime
}  // namespace gs