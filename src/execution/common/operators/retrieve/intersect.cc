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

#include "neug/execution/common/operators/retrieve/intersect.h"

#include <glog/logging.h>
#include <parallel_hashmap/phmap_base.h>
#include <stddef.h>

#include <limits>
#include <memory>
#include <ostream>
#include <utility>

#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/rt_any.h"
#include "neug/execution/utils/params.h"
#include "neug/utils/leaf_utils.h"
#include "parallel_hashmap/phmap.h"

namespace gs {

namespace runtime {

void get_labels(
    const EdgeExpandParams& eep, const GraphReadInterface& graph,
    std::vector<std::vector<std::pair<LabelTriplet, PropertyType>>>& labels) {
  std::vector<std::pair<LabelTriplet, PropertyType>> labels_i;
  for (const auto& label_triplet : eep.labels) {
    std::vector<PropertyType> props;
    props = graph.schema().get_edge_properties(label_triplet.src_label,
                                               label_triplet.dst_label,
                                               label_triplet.edge_label);
    if (props.empty()) {
      labels_i.emplace_back(label_triplet, PropertyType::kEmpty);
    } else if (props.size() == 1) {
      labels_i.emplace_back(label_triplet, props[0]);
    } else {
      labels_i.emplace_back(label_triplet, PropertyType::kRecordView);
    }
  }
  labels.push_back(std::move(labels_i));
}

gs::result<gs::runtime::Context> Intersect::Multiple_Intersect(
    const gs::runtime::GraphReadInterface& graph,
    const std::map<std::string, std::string>& params,
    gs::runtime::Context&& ctx,
    const std::vector<std::function<bool(label_t, vid_t, size_t)>>& preds,
    const std::vector<std::function<bool(const LabelTriplet&, vid_t, vid_t,
                                         const Any&, Direction, size_t)>>&
        e_preds,
    const std::vector<EdgeExpandParams>& eeps, int vertex_alias,
    std::vector<int> edge_aliases) {
  std::vector<IVertexColumn*> vertex_cols;
  for (const auto& eep : eeps) {
    auto col = ctx.get(eep.v_tag);
    vertex_cols.push_back(dynamic_cast<IVertexColumn*>(col.get()));
  }
  size_t row_num = ctx.row_num();
  MLVertexColumnBuilder builder;
  std::vector<std::vector<std::pair<LabelTriplet, PropertyType>>> labels;
  std::vector<BDMLEdgeColumnBuilder> edge_builders;
  labels.reserve(eeps.size());
  edge_builders.reserve(eeps.size());

  for (size_t i = 0; i < eeps.size(); ++i) {
    get_labels(eeps[i], graph, labels);
    edge_builders.emplace_back(BDMLEdgeColumnBuilder::builder(labels[i]));
  }

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
          if (e_preds[0](label_triplet, v.vid_, vid, iter.GetData(),
                         Direction::kOut, i)) {
            if (preds[0](label, vid, i)) {
              edge_builders[0].push_back_opt(label_triplet, v.vid_, vid,
                                             iter.GetData(), Direction::kOut);
              vertex_set.emplace(VertexRecord{label, vid});
            }
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
          if (e_preds[0](label_triplet, vid, v.vid_, iter.GetData(),
                         Direction::kIn, i)) {
            if (preds[0](label, vid, i)) {
              edge_builders[0].push_back_opt(label_triplet, vid, v.vid_,
                                             iter.GetData(), Direction::kIn);
              vertex_set.emplace(VertexRecord{label, vid});
            }
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
            if (e_preds[j](label_triplet, v.vid_, iter.GetNeighbor(),
                           iter.GetData(), Direction::kOut, i)) {
              VertexRecord v_record{iter.GetNeighborLabel(),
                                    iter.GetNeighbor()};
              if (vertex_set.find(v_record) != vertex_set.end() &&
                  preds[j](v_record.label_, v_record.vid_, i)) {
                edge_builders[j].push_back_opt(label_triplet, v.vid_,
                                               v_record.vid_, iter.GetData(),
                                               Direction::kOut);
                tmp_set.emplace(v_record);
              }
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
            if (e_preds[j](label_triplet, iter.GetNeighbor(), v.vid_,
                           iter.GetData(), Direction::kIn, i)) {
              VertexRecord v_record{iter.GetNeighborLabel(),
                                    iter.GetNeighbor()};
              if (vertex_set.find(v_record) != vertex_set.end() &&
                  preds[j](v_record.label_, v_record.vid_, i)) {
                tmp_set.emplace(v_record);
                edge_builders[j].push_back_opt(label_triplet, v_record.vid_,
                                               v.vid_, iter.GetData(),
                                               Direction::kIn);
              }
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
  auto col = builder.finish();
  ctx.set(vertex_alias, std::move(col));
  for (size_t i = 0; i < edge_builders.size(); ++i) {
    auto e_col = edge_builders[i].finish();
    ctx.set(edge_aliases[i], std::move(e_col));
  }
  return std::move(ctx);
}
}  // namespace runtime

}  // namespace gs
