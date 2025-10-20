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

#ifndef INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_INTERSECT_H_
#define INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_INTERSECT_H_

#include <map>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/utils/params.h"
#include "neug/utils/result.h"
#include "parallel_hashmap/phmap.h"

namespace gs {

namespace runtime {
class Context;

void get_labels(
    const EdgeExpandParams& eep, const GraphReadInterface& graph,
    std::vector<std::vector<std::pair<LabelTriplet, PropertyType>>>& labels);

class Intersect {
 public:
  template <typename PRED_LEFT, typename PRED_RIGHT, typename PRED_E_LEFT,
            typename PRED_E_RIGHT>
  static gs::result<gs::runtime::Context> Binary_Intersect_SL_Impl(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, const PRED_LEFT& left_pred,
      const PRED_RIGHT& right_pred, const PRED_E_LEFT& left_e_pred,
      const PRED_E_RIGHT& right_e_pred, const EdgeExpandParams& eep0,
      const EdgeExpandParams& eep1, int alias,
      const std::vector<int>& edge_alias) {
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
    MSVertexColumnBuilder builder(label);
    std::vector<size_t> offsets;

    bool keep_edges = !edge_alias.empty();
    std::vector<std::vector<std::pair<LabelTriplet, PropertyType>>> labels;
    std::vector<BDSLEdgeColumnBuilder> edge_builders;

    if (keep_edges) {
      labels.reserve(2);
      edge_builders.reserve(2);
      get_labels(eep0, graph, labels);
      CHECK(labels.back().size() == 1) << "Expect only one label triplet";
      edge_builders.emplace_back(BDSLEdgeColumnBuilder(labels.back()[0].first));
      get_labels(eep1, graph, labels);
      CHECK(labels.back().size() == 1) << "Expect only one label triplet";
      edge_builders.emplace_back(BDSLEdgeColumnBuilder(labels.back()[0].first));
    }

    for (size_t i = 0; i < row_num; ++i) {
      using value_t = std::tuple<vid_t, vid_t, const void*, Direction>;
      phmap::flat_hash_map<vid_t, std::vector<value_t>> vertex_set;

      auto v0 = vertex_col0->get_vertex(i);
      if (eep0.dir == Direction::kOut || eep0.dir == Direction::kBoth) {
        auto oview = graph.GetGenericOutgoingGraphView(
            v0.label_, eep0.labels[0].dst_label, eep0.labels[0].edge_label);
        auto oes = oview.get_edges(v0.vid_);
        for (auto iter = oes.begin(); iter != oes.end(); ++iter) {
          vid_t vid = iter.get_vertex();
          if (left_e_pred(v0.label_, v0.vid_, label, vid,
                          eep0.labels[0].edge_label, Direction::kOut,
                          iter.get_data_ptr(), i)) {
            if (left_pred(label, vid, i)) {
              if (vertex_set.find(vid) == vertex_set.end()) {
                vertex_set.emplace(vid, std::vector<value_t>());
              }
              vertex_set[vid].emplace_back(v0.vid_, vid, iter.get_data_ptr(),
                                           Direction::kOut);
            }
          }
        }
      }
      if (eep0.dir == Direction::kIn || eep0.dir == Direction::kBoth) {
        auto iview = graph.GetGenericIncomingGraphView(
            v0.label_, eep0.labels[0].src_label, eep0.labels[0].edge_label);
        auto ies = iview.get_edges(v0.vid_);
        for (auto iter = ies.begin(); iter != ies.end(); ++iter) {
          vid_t vid = iter.get_vertex();
          if (left_e_pred(v0.label_, v0.vid_, label, vid,
                          eep0.labels[0].edge_label, Direction::kIn,
                          iter.get_data_ptr(), i)) {
            if (left_pred(label, vid, i)) {
              if (vertex_set.find(vid) == vertex_set.end()) {
                vertex_set.emplace(vid, std::vector<value_t>());
              }
              vertex_set[vid].emplace_back(vid, v0.vid_, iter.get_data_ptr(),
                                           Direction::kIn);
            }
          }
        }
      }
      auto v1 = vertex_col1->get_vertex(i);

      if (eep1.dir == Direction::kOut || eep1.dir == Direction::kBoth) {
        auto oview = graph.GetGenericOutgoingGraphView(
            v1.label_, eep1.labels[0].dst_label, eep1.labels[0].edge_label);
        auto oes = oview.get_edges(v1.vid_);
        for (auto iter = oes.begin(); iter != oes.end(); ++iter) {
          vid_t vid = iter.get_vertex();
          if (right_e_pred(v1.label_, v1.vid_, label, vid,
                           eep1.labels[0].edge_label, Direction::kOut,
                           iter.get_data_ptr(), i)) {
            if (vertex_set.find(vid) != vertex_set.end() &&
                right_pred(label, vid, i)) {
              const auto& edges = vertex_set[vid];
              for (const auto& edge : edges) {
                builder.push_back_opt(vid);
                if (keep_edges) {
                  std::apply(
                      [&](const auto&... args) {
                        edge_builders[0].push_back_opt(args...);
                      },
                      edge);
                  edge_builders[1].push_back_opt(
                      v1.vid_, vid, iter.get_data_ptr(), Direction::kOut);
                }
                offsets.emplace_back(i);
              }
            }
          }
        }
      }
      if (eep1.dir == Direction::kIn || eep1.dir == Direction::kBoth) {
        auto iview = graph.GetGenericIncomingGraphView(
            v1.label_, eep1.labels[0].src_label, eep1.labels[0].edge_label);
        auto ies = iview.get_edges(v1.vid_);
        for (auto iter = ies.begin(); iter != ies.end(); ++iter) {
          vid_t vid = iter.get_vertex();
          if (right_e_pred(v1.label_, v1.vid_, label, vid,
                           eep1.labels[0].edge_label, Direction::kIn,
                           iter.get_data_ptr(), i)) {
            if (vertex_set.find(vid) != vertex_set.end() &&
                right_pred(label, vid, i)) {
              const auto& edges = vertex_set[vid];
              for (const auto& edge : edges) {
                builder.push_back_opt(vid);
                if (keep_edges) {
                  std::apply(
                      [&](const auto&... args) {
                        edge_builders[0].push_back_opt(args...);
                      },
                      edge);
                  edge_builders[1].push_back_opt(
                      vid, v1.vid_, iter.get_data_ptr(), Direction::kIn);
                }
                offsets.emplace_back(i);
              }
            }
          }
        }
      }
    }
    ctx.reshuffle(offsets);
    auto col = builder.finish();
    ctx.set(alias, std::move(col));
    if (keep_edges) {
      auto left_e_col = edge_builders[0].finish();
      ctx.set(edge_alias[0], std::move(left_e_col));
      auto right_e_col = edge_builders[1].finish();
      ctx.set(edge_alias[1], std::move(right_e_col));
    }
    return std::move(ctx);
  }

  template <typename PRED_LEFT, typename PRED_RIGHT, typename E_PRED_LEFT,
            typename E_PRED_RIGHT>
  static gs::result<gs::runtime::Context> Binary_Intersect_ML_Impl(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, const PRED_LEFT& left_pred,
      const PRED_RIGHT& right_pred, const E_PRED_LEFT& left_e_pred,
      const E_PRED_RIGHT& right_e_pred, const EdgeExpandParams& eep0,
      const EdgeExpandParams& eep1, int alias,
      const std::vector<int>& edge_alias) {
    const auto& vertex_col0 =
        dynamic_cast<const IVertexColumn*>(ctx.get(eep0.v_tag).get());
    const auto& vertex_col1 =
        dynamic_cast<const IVertexColumn*>(ctx.get(eep1.v_tag).get());

    CHECK(!eep0.is_optional && !eep1.is_optional)
        << "IntersectOprBeta does not support optional edge expand";
    size_t row_num = ctx.row_num();

    // TODO(luoxiaojian): use MLVertexColumnBuilderOpt
    MLVertexColumnBuilder builder;
    std::vector<size_t> offsets;

    std::vector<std::vector<std::pair<LabelTriplet, PropertyType>>> labels;
    std::vector<BDMLEdgeColumnBuilder> edge_builders;

    bool keep_edges = !edge_alias.empty();
    if (keep_edges) {
      labels.reserve(2);
      edge_builders.reserve(2);
      get_labels(eep0, graph, labels);
      std::vector<LabelTriplet> label_triplets;
      for (const auto& p : labels.back()) {
        label_triplets.push_back(p.first);
      }
      edge_builders.emplace_back(BDMLEdgeColumnBuilder(label_triplets));
      get_labels(eep1, graph, labels);
      label_triplets.clear();
      for (const auto& p : labels.back()) {
        label_triplets.push_back(p.first);
      }
      edge_builders.emplace_back(BDMLEdgeColumnBuilder(label_triplets));
    }

    for (size_t i = 0; i < row_num; ++i) {
      using value_t =
          std::tuple<LabelTriplet, vid_t, vid_t, const void*, Direction>;
      phmap::flat_hash_map<VertexRecord, std::vector<value_t>, VertexRecordHash>
          vertex_set;

      auto v0 = vertex_col0->get_vertex(i);
      if (eep0.dir == Direction::kOut || eep0.dir == Direction::kBoth) {
        for (const auto& label_triplet : eep0.labels) {
          if (label_triplet.src_label != v0.label_) {
            continue;
          }
          auto oview = graph.GetGenericOutgoingGraphView(
              v0.label_, label_triplet.dst_label, label_triplet.edge_label);
          auto oes = oview.get_edges(v0.vid_);
          for (auto iter = oes.begin(); iter != oes.end(); ++iter) {
            vid_t vid = iter.get_vertex();
            if (left_e_pred(v0.label_, v0.vid_, label_triplet.dst_label, vid,
                            label_triplet.edge_label, Direction::kOut,
                            iter.get_data_ptr(), i) &&
                left_pred(label_triplet.dst_label, vid, i)) {
              auto rcd = VertexRecord{label_triplet.dst_label, vid};
              if (vertex_set.find(rcd) == vertex_set.end()) {
                vertex_set.emplace(rcd, std::vector<value_t>());
              }
              vertex_set[rcd].emplace_back(label_triplet, v0.vid_, vid,
                                           iter.get_data_ptr(),
                                           Direction::kOut);
            }
          }
        }
      }
      if (eep0.dir == Direction::kIn || eep0.dir == Direction::kBoth) {
        for (const auto& label_triplet : eep0.labels) {
          if (label_triplet.dst_label != v0.label_) {
            continue;
          }
          auto iview = graph.GetGenericIncomingGraphView(
              v0.label_, label_triplet.src_label, label_triplet.edge_label);
          auto ies = iview.get_edges(v0.vid_);
          for (auto iter = ies.begin(); iter != ies.end(); ++iter) {
            vid_t vid = iter.get_vertex();
            if (left_e_pred(v0.label_, v0.vid_, label_triplet.src_label, vid,
                            label_triplet.edge_label, Direction::kIn,
                            iter.get_data_ptr(), i) &&
                left_pred(label_triplet.src_label, vid, i)) {
              auto rcd = VertexRecord{label_triplet.src_label, vid};
              if (vertex_set.find(rcd) == vertex_set.end()) {
                vertex_set.emplace(rcd, std::vector<value_t>());
              }
              vertex_set[rcd].emplace_back(label_triplet, vid, v0.vid_,
                                           iter.get_data_ptr(), Direction::kIn);
            }
          }
        }
      }
      auto v1 = vertex_col1->get_vertex(i);

      if (eep1.dir == Direction::kOut || eep1.dir == Direction::kBoth) {
        for (const auto& label_triplet : eep1.labels) {
          if (label_triplet.src_label != v1.label_) {
            continue;
          }
          auto oview = graph.GetGenericOutgoingGraphView(
              v1.label_, label_triplet.dst_label, label_triplet.edge_label);
          auto oes = oview.get_edges(v1.vid_);
          for (auto iter = oes.begin(); iter != oes.end(); ++iter) {
            vid_t vid = iter.get_vertex();
            if (right_e_pred(v1.label_, v1.vid_, label_triplet.dst_label, vid,
                             label_triplet.edge_label, Direction::kOut,
                             iter.get_data_ptr(), i) &&
                right_pred(label_triplet.dst_label, vid, i)) {
              auto rcd = VertexRecord{label_triplet.dst_label, vid};
              if (vertex_set.find(rcd) != vertex_set.end()) {
                const auto& values = vertex_set[rcd];
                for (const auto& value : values) {
                  builder.push_back_opt(rcd);
                  if (keep_edges) {
                    std::apply(
                        [&](const auto&... args) {
                          edge_builders[0].push_back_opt(args...);
                        },
                        value);
                    edge_builders[1].push_back_opt(label_triplet, v1.vid_, vid,
                                                   iter.get_data_ptr(),
                                                   Direction::kOut);
                  }
                  offsets.emplace_back(i);
                }
              }
            }
          }
        }
      }
      if (eep1.dir == Direction::kIn || eep1.dir == Direction::kBoth) {
        for (const auto& label_triplet : eep1.labels) {
          if (label_triplet.dst_label != v1.label_) {
            continue;
          }
          auto iview = graph.GetGenericIncomingGraphView(
              v1.label_, label_triplet.src_label, label_triplet.edge_label);
          auto ies = iview.get_edges(v1.vid_);
          for (auto iter = ies.begin(); iter != ies.end(); ++iter) {
            vid_t vid = iter.get_vertex();
            VertexRecord v_record{label_triplet.src_label, vid};
            if (right_e_pred(v1.label_, v1.vid_, label_triplet.src_label, vid,
                             label_triplet.edge_label, Direction::kIn,
                             iter.get_data_ptr(), i) &&
                vertex_set.find(v_record) != vertex_set.end() &&
                right_pred(label_triplet.src_label, vid, i)) {
              const auto& values = vertex_set[v_record];
              for (const auto& value : values) {
                builder.push_back_opt(v_record);
                if (keep_edges) {
                  std::apply(
                      [&](const auto&... args) {
                        edge_builders[0].push_back_opt(args...);
                      },
                      value);
                  edge_builders[1].push_back_opt(label_triplet, vid, v1.vid_,
                                                 iter.get_data_ptr(),
                                                 Direction::kIn);
                }
                offsets.emplace_back(i);
              }
            }
          }
        }
      }
    }
    ctx.reshuffle(offsets);
    auto col = builder.finish();
    ctx.set(alias, std::move(col));
    if (keep_edges) {
      auto left_e_col = edge_builders[0].finish();
      ctx.set(edge_alias[0], std::move(left_e_col));
      auto right_e_col = edge_builders[1].finish();
      ctx.set(edge_alias[1], std::move(right_e_col));
    }
    return std::move(ctx);
  }

  template <typename PRED_V_LEFT, typename PRED_V_RIGHT, typename PRED_E_LEFT,
            typename PRED_E_RIGHT>
  static gs::result<gs::runtime::Context> Binary_Intersect(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx, const PRED_V_LEFT& left_v_pred,
      const PRED_V_RIGHT& right_v_pred, const PRED_E_LEFT& left_e_pred,
      const PRED_E_RIGHT& right_e_pred, const EdgeExpandParams& eep0,
      const EdgeExpandParams& eep1, int alias,
      const std::vector<int>& edge_alias) {
    if (eep0.labels.size() == 1 && eep1.labels.size() == 1) {
      return Binary_Intersect_SL_Impl(
          graph, params, std::move(ctx), left_v_pred, right_v_pred, left_e_pred,
          right_e_pred, eep0, eep1, alias, edge_alias);
    } else {
      return Binary_Intersect_ML_Impl(
          graph, params, std::move(ctx), left_v_pred, right_v_pred, left_e_pred,
          right_e_pred, eep0, eep1, alias, edge_alias);
    }
  }

  static gs::result<gs::runtime::Context> Multiple_Intersect(
      const gs::runtime::GraphReadInterface& graph,
      const std::map<std::string, std::string>& params,
      gs::runtime::Context&& ctx,
      const std::vector<std::function<bool(label_t, vid_t, size_t)>>& preds,
      const std::vector<
          std::function<bool(label_t, vid_t, label_t, vid_t, label_t, Direction,
                             const void*, size_t)>>& e_preds,
      const std::vector<EdgeExpandParams>& eeps, int vertex_alias,
      std::vector<int> edge_aliases);
};

}  // namespace runtime

}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_INTERSECT_H_
