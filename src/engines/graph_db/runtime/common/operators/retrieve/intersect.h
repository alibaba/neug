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

#ifndef RUNTIME_COMMON_OPERATORS_RETRIEVE_INTERSECT_H_
#define RUNTIME_COMMON_OPERATORS_RETRIEVE_INTERSECT_H_

#include <boost/leaf.hpp>
#include <tuple>
#include <vector>

#include "parallel_hashmap/phmap.h"
#include "src/engines/graph_db/runtime/common/context.h"
#include "src/engines/graph_db/runtime/common/leaf_utils.h"
#include "src/engines/graph_db/runtime/utils/params.h"

namespace gs {

namespace runtime {
class Context;

class Intersect {
 public:
  template <typename PRED_LEFT, typename PRED_RIGHT>
  static bl::result<gs::runtime::Context> Binary_Intersect_SL_Impl(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, const PRED_LEFT& left_pred,
      const PRED_RIGHT& right_pred, const EdgeExpandParams& eep0,
      const EdgeExpandParams& eep1, int alias) {
    const auto& vertex_col0 =
        dynamic_cast<const IVertexColumn*>(ctx.get(eep0.v_tag).get());
    const auto& vertex_col1 =
        dynamic_cast<const IVertexColumn*>(ctx.get(eep1.v_tag).get());
    CHECK(eep0.labels.size() == 1 && eep1.labels.size() == 1)
        << "IntersectOprBeta only supports single label edge expand";
    CHECK(!eep0.is_optional && !eep1.is_optional)
        << "IntersectOprBeta does not support optional edge expand";
    size_t row_num = ctx.row_num();

    auto label = (eep0.dir == Direction::kOut ? eep0.labels[0].dst_label
                                              : eep0.labels[0].src_label);
    auto builder = SLVertexColumnBuilder::builder(label);
    std::vector<size_t> offsets;
    for (size_t i = 0; i < row_num; ++i) {
      phmap::flat_hash_set<vid_t> vertex_set;

      auto v0 = vertex_col0->get_vertex(i);
      if (eep0.dir == Direction::kOut || eep0.dir == Direction::kBoth) {
        auto iter = graph.GetOutEdgeIterator(v0.label_, v0.vid_,
                                             eep0.labels[0].dst_label,
                                             eep0.labels[0].edge_label);
        while (iter.IsValid()) {
          if (left_pred(iter.GetNeighborLabel(), iter.GetNeighbor(), i)) {
            vertex_set.emplace(iter.GetNeighbor());
          }
          iter.Next();
        }
      }
      if (eep0.dir == Direction::kIn || eep0.dir == Direction::kBoth) {
        auto iter = graph.GetInEdgeIterator(v0.label_, v0.vid_,
                                            eep0.labels[0].src_label,
                                            eep0.labels[0].edge_label);
        while (iter.IsValid()) {
          if (left_pred(iter.GetNeighborLabel(), iter.GetNeighbor(), i)) {
            vertex_set.emplace(iter.GetNeighbor());
          }
          iter.Next();
        }
      }
      auto v1 = vertex_col1->get_vertex(i);

      if (eep1.dir == Direction::kOut || eep1.dir == Direction::kBoth) {
        auto iter = graph.GetOutEdgeIterator(v1.label_, v1.vid_,
                                             eep1.labels[0].dst_label,
                                             eep1.labels[0].edge_label);
        while (iter.IsValid()) {
          if (vertex_set.find(iter.GetNeighbor()) != vertex_set.end() &&
              right_pred(iter.GetNeighborLabel(), iter.GetNeighbor(), i)) {
            builder.push_back_opt(iter.GetNeighbor());
            offsets.emplace_back(i);
          }
          iter.Next();
        }
      }
      if (eep1.dir == Direction::kIn || eep1.dir == Direction::kBoth) {
        auto iter = graph.GetInEdgeIterator(v1.label_, v1.vid_,
                                            eep1.labels[0].src_label,
                                            eep1.labels[0].edge_label);
        while (iter.IsValid()) {
          if (vertex_set.find(iter.GetNeighbor()) != vertex_set.end() &&
              right_pred(iter.GetNeighborLabel(), iter.GetNeighbor(), i)) {
            builder.push_back_opt(iter.GetNeighbor());
            offsets.emplace_back(i);
          }
          iter.Next();
        }
      }
    }
    ctx.reshuffle(offsets);
    auto col = builder.finish(nullptr);
    ctx.set(alias, std::move(col));
    return std::move(ctx);
  }

  template <typename PRED_LEFT, typename PRED_RIGHT>
  static bl::result<gs::runtime::Context> Binary_Intersect_ML_Impl(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, const PRED_LEFT& left_pred,
      const PRED_RIGHT& right_pred, const EdgeExpandParams& eep0,
      const EdgeExpandParams& eep1, int alias) {
    const auto& vertex_col0 =
        dynamic_cast<const IVertexColumn*>(ctx.get(eep0.v_tag).get());
    const auto& vertex_col1 =
        dynamic_cast<const IVertexColumn*>(ctx.get(eep1.v_tag).get());

    CHECK(!eep0.is_optional && !eep1.is_optional)
        << "IntersectOprBeta does not support optional edge expand";
    size_t row_num = ctx.row_num();

    auto builder = MLVertexColumnBuilder::builder();
    std::vector<size_t> offsets;
    for (size_t i = 0; i < row_num; ++i) {
      phmap::flat_hash_set<VertexRecord, VertexRecordHash> vertex_set;

      auto v0 = vertex_col0->get_vertex(i);
      if (eep0.dir == Direction::kOut || eep0.dir == Direction::kBoth) {
        for (const auto& label_triplet : eep0.labels) {
          if (label_triplet.src_label != v0.label_) {
            continue;
          }
          auto iter = graph.GetOutEdgeIterator(v0.label_, v0.vid_,
                                               label_triplet.dst_label,
                                               label_triplet.edge_label);
          while (iter.IsValid()) {
            if (left_pred(iter.GetNeighborLabel(), iter.GetNeighbor(), i)) {
              vertex_set.emplace(
                  VertexRecord{iter.GetNeighborLabel(), iter.GetNeighbor()});
            }
            iter.Next();
          }
        }
      }
      if (eep0.dir == Direction::kIn || eep0.dir == Direction::kBoth) {
        for (const auto& label_triplet : eep0.labels) {
          if (label_triplet.dst_label != v0.label_) {
            continue;
          }
          auto iter = graph.GetInEdgeIterator(v0.label_, v0.vid_,
                                              label_triplet.src_label,
                                              label_triplet.edge_label);
          while (iter.IsValid()) {
            if (left_pred(iter.GetNeighborLabel(), iter.GetNeighbor(), i)) {
              vertex_set.emplace(
                  VertexRecord{iter.GetNeighborLabel(), iter.GetNeighbor()});
            }
            iter.Next();
          }
        }
      }
      auto v1 = vertex_col1->get_vertex(i);

      if (eep1.dir == Direction::kOut || eep1.dir == Direction::kBoth) {
        for (const auto& label_triplet : eep1.labels) {
          if (label_triplet.src_label != v1.label_) {
            continue;
          }
          auto iter = graph.GetOutEdgeIterator(v1.label_, v1.vid_,
                                               label_triplet.dst_label,
                                               label_triplet.edge_label);
          while (iter.IsValid()) {
            VertexRecord v_record{iter.GetNeighborLabel(), iter.GetNeighbor()};
            if (vertex_set.find(v_record) != vertex_set.end() &&
                right_pred(v_record.label_, v_record.vid_, i)) {
              builder.push_back_opt(v_record);
              offsets.emplace_back(i);
            }
            iter.Next();
          }
        }
      }
      if (eep1.dir == Direction::kIn || eep1.dir == Direction::kBoth) {
        for (const auto& label_triplet : eep1.labels) {
          if (label_triplet.dst_label != v1.label_) {
            continue;
          }
          auto iter = graph.GetInEdgeIterator(v1.label_, v1.vid_,
                                              label_triplet.src_label,
                                              label_triplet.edge_label);
          while (iter.IsValid()) {
            VertexRecord v_record{iter.GetNeighborLabel(), iter.GetNeighbor()};
            if (vertex_set.find(v_record) != vertex_set.end() &&
                right_pred(v_record.label_, v_record.vid_, i)) {
              builder.push_back_opt(v_record);
              offsets.emplace_back(i);
            }
            iter.Next();
          }
        }
      }
    }
    ctx.reshuffle(offsets);
    auto col = builder.finish(nullptr);
    ctx.set(alias, std::move(col));
    return std::move(ctx);
  }

  template <typename PRED_LEFT, typename PRED_RIGHT>
  static bl::result<gs::runtime::Context> Binary_Intersect(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, const PRED_LEFT& left_pred,
      const PRED_RIGHT& right_pred, const EdgeExpandParams& eep0,
      const EdgeExpandParams& eep1, int alias) {
    if (eep0.labels.size() == 1 && eep1.labels.size() == 1) {
      return Binary_Intersect_SL_Impl(graph, params, std::move(ctx), left_pred,
                                      right_pred, eep0, eep1, alias);
    } else {
      return Binary_Intersect_ML_Impl(graph, params, std::move(ctx), left_pred,
                                      right_pred, eep0, eep1, alias);
    }
  }

  static bl::result<gs::runtime::Context> Multiple_Intersect(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx,
      const std::vector<std::function<bool(label_t, vid_t, size_t)>>& preds,
      const std::vector<EdgeExpandParams>& eeps, int alias);
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_RETRIEVE_INTERSECT_H_