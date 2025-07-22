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

#include "src/engines/graph_db/runtime/common/operators/retrieve/intersect.h"

#include <glog/logging.h>
#include <parallel_hashmap/phmap_base.h>
#include <stddef.h>

#include <limits>
#include <memory>
#include <ostream>
#include <utility>

#include "parallel_hashmap/phmap.h"
#include "src/engines/graph_db/runtime/common/columns/i_context_column.h"
#include "src/engines/graph_db/runtime/common/columns/value_columns.h"
#include "src/engines/graph_db/runtime/common/columns/vertex_columns.h"
#include "src/engines/graph_db/runtime/common/context.h"
#include "src/engines/graph_db/runtime/common/leaf_utils.h"
#include "src/engines/graph_db/runtime/common/rt_any.h"

namespace gs {

namespace runtime {
bl::result<gs::runtime::Context> Intersect::Multiple_Intersect(
    const gs::runtime::GraphReadInterface& graph,
    const std::map<std::string, std::string>& params,
    gs::runtime::Context&& ctx,
    const std::vector<std::function<bool(label_t, vid_t, size_t)>>& preds,
    const std::vector<EdgeExpandParams>& eeps, int alias) {
  std::vector<IVertexColumn*> vertex_cols;
  for (const auto& eep : eeps) {
    auto col = ctx.get(eep.v_tag);
    vertex_cols.push_back(dynamic_cast<IVertexColumn*>(col.get()));
  }
  size_t row_num = ctx.row_num();
  auto builder = MLVertexColumnBuilder::builder();
  std::vector<size_t> offsets;

  for (size_t i = 0; i < row_num; ++i) {
    phmap::flat_hash_set<VertexRecord, VertexRecordHash> vertex_set;
    auto v = vertex_cols[0]->get_vertex(i);
    if (eeps[0].dir == Direction::kOut || eeps[0].dir == Direction::kBoth) {
      for (const auto& label_triplet : eeps[0].labels) {
        if (label_triplet.src_label != v.label_) {
          continue;
        }
        auto iter =
            graph.GetOutEdgeIterator(v.label_, v.vid_, label_triplet.dst_label,
                                     label_triplet.edge_label);
        while (iter.IsValid()) {
          label_t label = iter.GetNeighborLabel();
          vid_t vid = iter.GetNeighbor();
          if (preds[0](label, vid, i)) {
            vertex_set.emplace(VertexRecord{label, vid});
          }
          iter.Next();
        }
      }
    }
    if (eeps[0].dir == Direction::kIn || eeps[0].dir == Direction::kBoth) {
      for (const auto& label_triplet : eeps[0].labels) {
        if (label_triplet.dst_label != v.label_) {
          continue;
        }
        auto iter =
            graph.GetInEdgeIterator(v.label_, v.vid_, label_triplet.src_label,
                                    label_triplet.edge_label);
        while (iter.IsValid()) {
          label_t label = iter.GetNeighborLabel();
          vid_t vid = iter.GetNeighbor();
          if (preds[0](label, vid, i)) {
            vertex_set.emplace(VertexRecord{label, vid});
          }
          iter.Next();
        }
      }
    }

    for (size_t j = 1; j < eeps.size(); ++j) {
      phmap::flat_hash_set<VertexRecord, VertexRecordHash> tmp_set;
      v = vertex_cols[j]->get_vertex(i);
      if (eeps[j].dir == Direction::kOut || eeps[j].dir == Direction::kBoth) {
        for (const auto& label_triplet : eeps[j].labels) {
          if (label_triplet.src_label != v.label_) {
            continue;
          }
          auto iter = graph.GetOutEdgeIterator(v.label_, v.vid_,
                                               label_triplet.dst_label,
                                               label_triplet.edge_label);
          while (iter.IsValid()) {
            VertexRecord v_record{iter.GetNeighborLabel(), iter.GetNeighbor()};
            if (vertex_set.find(v_record) != vertex_set.end() &&
                preds[j](v_record.label_, v_record.vid_, i)) {
              tmp_set.emplace(v_record);
            }
            iter.Next();
          }
        }
      }
      if (eeps[j].dir == Direction::kIn || eeps[j].dir == Direction::kBoth) {
        for (const auto& label_triplet : eeps[j].labels) {
          if (label_triplet.dst_label != v.label_) {
            continue;
          }
          auto iter =
              graph.GetInEdgeIterator(v.label_, v.vid_, label_triplet.src_label,
                                      label_triplet.edge_label);
          while (iter.IsValid()) {
            VertexRecord v_record{iter.GetNeighborLabel(), iter.GetNeighbor()};
            if (vertex_set.find(v_record) != vertex_set.end() &&
                preds[j](v_record.label_, v_record.vid_, i)) {
              tmp_set.emplace(v_record);
            }
            iter.Next();
          }
        }
      }
      std::swap(vertex_set, tmp_set);
      if (vertex_set.empty()) {
        break;  // No intersection found, skip to next row
      }
    }
    if (!vertex_set.empty()) {
      for (const auto& v_record : vertex_set) {
        builder.push_back_opt(v_record);
        offsets.emplace_back(i);
      }
    }
  }
  ctx.reshuffle(offsets);
  auto col = builder.finish(nullptr);
  ctx.set(alias, std::move(col));
  return std::move(ctx);
}
}  // namespace runtime

}  // namespace gs
