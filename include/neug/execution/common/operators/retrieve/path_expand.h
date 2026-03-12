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
#pragma once

#include "neug/execution/common/context.h"
#include "neug/execution/common/operators/retrieve/path_expand_impl.h"
#include "neug/execution/common/params_map.h"
#include "neug/execution/expression/special_predicates.h"
#include "neug/execution/utils/params.h"
#include "neug/utils/result.h"

namespace neug {

namespace execution {

class PathExpand {
 public:
  // PathExpand(expandOpt == Vertex && alias == -1 && resultOpt == END_V) +
  // GetV(opt == END)
  static neug::result<Context> edge_expand_v(const StorageReadInterface& graph, Context&& ctx,
                                             const PathExpandParams& params);
  static neug::result<Context> edge_expand_p(const StorageReadInterface& graph, Context&& ctx,
                                             const PathExpandParams& params);

  static neug::result<Context> all_shortest_paths_with_given_source_and_dest(
      const StorageReadInterface& graph, Context&& ctx, const ShortestPathParams& params,
      const std::pair<label_t, vid_t>& dst);
  // single dst
  static neug::result<Context> single_source_single_dest_shortest_path(
      const StorageReadInterface& graph, Context&& ctx, const ShortestPathParams& params,
      std::pair<label_t, vid_t>& dest);

  template <typename PRED_T>
  static neug::result<Context> single_source_shortest_path_with_order_by_length_limit(
      const StorageReadInterface& graph, Context&& ctx, const ShortestPathParams& params,
      const PRED_T& pred, int limit_upper) {
    std::vector<size_t> shuffle_offset;
    auto input_vertex_col = std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
    if (params.labels.size() == 1 && params.labels[0].src_label == params.labels[0].dst_label &&
        params.dir == Direction::kBoth && input_vertex_col->get_labels_set().size() == 1) {
      auto tup = single_source_shortest_path_with_order_by_length_limit_impl<PRED_T>(
          graph, *input_vertex_col, params.labels[0].edge_label, params.dir, params.hop_lower,
          params.hop_upper, pred, limit_upper);
      ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup), std::get<2>(tup));
      ctx.set(params.alias, std::get<1>(tup));
      return ctx;
    }

    LOG(ERROR) << "not support edge property type ";
    RETURN_UNSUPPORTED_ERROR("not support edge property type ");
  }

  template <typename PRED_T>
  static neug::result<Context> single_source_shortest_path(const StorageReadInterface& graph,
                                                           Context&& ctx,
                                                           const ShortestPathParams& params,
                                                           const PRED_T& pred) {
    std::vector<size_t> shuffle_offset;
    auto input_vertex_col = std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
    if (params.labels.size() == 1 && params.labels[0].src_label == params.labels[0].dst_label &&
        params.dir == Direction::kBoth && input_vertex_col->get_labels_set().size() == 1) {
      auto tup = single_source_shortest_path_impl<PRED_T>(graph, *input_vertex_col,
                                                          params.labels[0].edge_label, params.dir,
                                                          params.hop_lower, params.hop_upper, pred);
      ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup), std::get<2>(tup));
      ctx.set(params.alias, std::get<1>(tup));
      return ctx;
    }
    auto tup = default_single_source_shortest_path_impl<PRED_T>(
        graph, *input_vertex_col, params.labels, params.dir, params.hop_lower, params.hop_upper,
        pred);
    ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup), std::get<2>(tup));
    ctx.set(params.alias, std::get<1>(tup));
    return ctx;
  }

  static neug::result<Context> single_source_shortest_path_with_special_vertex_predicate(
      const StorageReadInterface& graph, Context&& ctx, const ShortestPathParams& params,
      const SpecialPredicateConfig& config, const ParamsMap& query_params);

  template <typename PRED_T>
  static neug::result<Context> edge_expand_p_with_pred(const StorageReadInterface& graph,
                                                       Context&& ctx,
                                                       const PathExpandParams& params,
                                                       const PRED_T& pred) {
    if (params.opt != PathOpt::kArbitrary) {
      LOG(ERROR) << "only support arbitrary path expand with predicate";
      RETURN_UNSUPPORTED_ERROR("only support arbitrary path expand with predicate");
    }
    std::vector<size_t> shuffle_offset;
    auto& input_vertex_list = *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
    auto label_sets = input_vertex_list.get_labels_set();
    auto labels = params.labels;
    std::vector<std::vector<LabelTriplet>> out_labels_map(graph.schema().vertex_label_frontier()),
        in_labels_map(graph.schema().vertex_label_frontier());
    for (const auto& triplet : labels) {
      out_labels_map[triplet.src_label].emplace_back(triplet);
      in_labels_map[triplet.dst_label].emplace_back(triplet);
    }
    auto dir = params.dir;
    std::vector<std::pair<Path, size_t>> input;
    std::vector<std::pair<Path, size_t>> output;

    PathColumnBuilder builder;

    if (dir == Direction::kOut) {
      foreach_vertex(input_vertex_list, [&](size_t index, label_t label, vid_t v) {
        auto p = Path(label, v);
        input.emplace_back(std::move(p), index);
      });
      int depth = 0;
      while (depth < params.hop_upper) {
        output.clear();
        if (depth + 1 < params.hop_upper) {
          for (auto& [path, index] : input) {
            auto end = path.end_node();
            for (const auto& label_triplet : out_labels_map[end.label_]) {
              auto oview = graph.GetGenericOutgoingGraphView(end.label_, label_triplet.dst_label,
                                                             label_triplet.edge_label);
              auto oes = oview.get_edges(end.vid_);
              for (auto it = oes.begin(); it != oes.end(); ++it) {
                if (pred(label_triplet, end.vid_, it.get_vertex(), it.get_data_ptr())) {
                  Path new_path = path.expand(label_triplet.edge_label, label_triplet.dst_label,
                                              it.get_vertex(), Direction::kOut, it.get_data_ptr());
                  output.emplace_back(std::move(new_path), index);
                }
              }
            }
          }
        }

        if (depth >= params.hop_lower) {
          for (auto& [path, index] : input) {
            builder.push_back_opt(Path(path));
            shuffle_offset.push_back(index);
          }
        }
        if (depth + 1 >= params.hop_upper) {
          break;
        }

        input.clear();
        std::swap(input, output);
        ++depth;
      }
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);

      return ctx;
    } else if (dir == Direction::kIn) {
      foreach_vertex(input_vertex_list, [&](size_t index, label_t label, vid_t v) {
        auto p = Path(label, v);
        input.emplace_back(std::move(p), index);
      });
      int depth = 0;
      while (depth < params.hop_upper) {
        output.clear();

        if (depth + 1 < params.hop_upper) {
          for (const auto& [path, index] : input) {
            auto end = path.end_node();
            for (const auto& label_triplet : in_labels_map[end.label_]) {
              auto iview = graph.GetGenericIncomingGraphView(end.label_, label_triplet.src_label,
                                                             label_triplet.edge_label);
              auto ies = iview.get_edges(end.vid_);
              for (auto it = ies.begin(); it != ies.end(); ++it) {
                if (pred(label_triplet, it.get_vertex(), end.vid_, it.get_data_ptr())) {
                  Path new_path = path.expand(label_triplet.edge_label, label_triplet.src_label,
                                              it.get_vertex(), Direction::kIn, it.get_data_ptr());
                  output.emplace_back(std::move(new_path), index);
                }
              }
            }
          }
        }

        if (depth >= params.hop_lower) {
          for (auto& [path, index] : input) {
            builder.push_back_opt(Path(path));
            shuffle_offset.push_back(index);
          }
        }
        if (depth + 1 >= params.hop_upper) {
          break;
        }

        input.clear();
        std::swap(input, output);
        ++depth;
      }
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);

      return ctx;

    } else if (dir == Direction::kBoth) {
      foreach_vertex(input_vertex_list, [&](size_t index, label_t label, vid_t v) {
        auto p = Path(label, v);
        input.emplace_back(std::move(p), index);
      });
      int depth = 0;
      while (depth < params.hop_upper) {
        output.clear();
        if (depth + 1 < params.hop_upper) {
          for (auto& [path, index] : input) {
            auto end = path.end_node();
            for (const auto& label_triplet : out_labels_map[end.label_]) {
              auto oview = graph.GetGenericOutgoingGraphView(end.label_, label_triplet.dst_label,
                                                             label_triplet.edge_label);
              auto oes = oview.get_edges(end.vid_);
              for (auto it = oes.begin(); it != oes.end(); ++it) {
                if (pred(label_triplet, end.vid_, it.get_vertex(), it.get_data_ptr())) {
                  Path new_path = path.expand(label_triplet.edge_label, label_triplet.dst_label,
                                              it.get_vertex(), Direction::kOut, it.get_data_ptr());
                  output.emplace_back(std::move(new_path), index);
                }
              }
            }

            for (const auto& label_triplet : in_labels_map[end.label_]) {
              auto iview = graph.GetGenericIncomingGraphView(end.label_, label_triplet.src_label,
                                                             label_triplet.edge_label);
              auto ies = iview.get_edges(end.vid_);
              for (auto it = ies.begin(); it != ies.end(); ++it) {
                if (pred(label_triplet, it.get_vertex(), end.vid_, it.get_data_ptr())) {
                  Path new_path = path.expand(label_triplet.edge_label, label_triplet.src_label,
                                              it.get_vertex(), Direction::kIn, it.get_data_ptr());
                  output.emplace_back(std::move(new_path), index);
                }
              }
            }
          }
        }

        if (depth >= params.hop_lower) {
          for (auto& [path, index] : input) {
            builder.push_back_opt(Path(path));
            shuffle_offset.push_back(index);
          }
        }
        if (depth + 1 >= params.hop_upper) {
          break;
        }

        input.clear();
        std::swap(input, output);
        ++depth;
      }
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      return ctx;
    }
    LOG(ERROR) << "not support path expand options";
    RETURN_UNSUPPORTED_ERROR("not support path expand options");
  }

  template <typename FUNC_T>
  static neug::result<Context> any_weighted_shortest_path(const StorageReadInterface& graph,
                                                          Context&& ctx,
                                                          const PathExpandParams& params,
                                                          const FUNC_T& weight_func) {
    auto col = ctx.get(params.start_tag);
    auto& input_vertex_list = *std::dynamic_pointer_cast<IVertexColumn>(col);
    PathColumnBuilder path_builder;
    std::vector<size_t> shuffle_offset;
    foreach_vertex(input_vertex_list, [&](size_t index, label_t label, vid_t v) {
      std::unordered_map<VertexRecord, double> dist;
      std::unordered_set<VertexRecord> visited;
      VertexRecord start_vr(label, v);
      dist[start_vr] = 0;
      auto cmp = [](const Path& a, const Path& b) {
        double wa = a.get_weight();
        double wb = b.get_weight();
        return wa > wb;
      };
      std::priority_queue<Path, std::vector<Path>, decltype(cmp)> pq(cmp);
      Path root = Path(label, v);
      root.set_weight(0.0);
      pq.push(root);
      while (!pq.empty()) {
        Path path = pq.top();
        label_t label = path.end_node().label_;
        vid_t cur = path.end_node().vid_;
        VertexRecord cur_vr(label, cur);
        if (visited.count(cur_vr) > 0) {
          pq.pop();
          continue;
        }
        visited.insert(cur_vr);
        path_builder.push_back_opt(path);
        shuffle_offset.push_back(index);
        pq.pop();
        for (LabelTriplet label_triplet : params.labels) {
          if (label_triplet.src_label != label && label_triplet.dst_label != label) {
            continue;
          }

          if (label_triplet.src_label == label &&
              (params.dir == Direction::kOut || params.dir == Direction::kBoth)) {
            auto oe_view = graph.GetGenericOutgoingGraphView(
                label_triplet.src_label, label_triplet.dst_label, label_triplet.edge_label);
            auto oes = oe_view.get_edges(cur);
            for (auto it = oes.begin(); it != oes.end(); ++it) {
              vid_t nbr = it.get_vertex();
              VertexRecord nbr_vr(label_triplet.dst_label, nbr);
              double weight = weight_func(label_triplet, cur, nbr, it.get_data_ptr());
              if (dist.count(nbr_vr) == 0 || dist[nbr_vr] > path.get_weight() + weight) {
                auto new_path = path.expand(label_triplet.edge_label, label_triplet.dst_label, nbr,
                                            Direction::kOut, it.get_data_ptr());
                new_path.set_weight(path.get_weight() + weight);
                dist[nbr_vr] = new_path.get_weight() + weight;

                pq.push(new_path);
              }
            }
          }
          if (label_triplet.dst_label == label &&
              (params.dir == Direction::kIn || params.dir == Direction::kBoth)) {
            auto ie_view = graph.GetGenericIncomingGraphView(
                label_triplet.dst_label, label_triplet.src_label, label_triplet.edge_label);
            auto ies = ie_view.get_edges(cur);
            for (auto it = ies.begin(); it != ies.end(); ++it) {
              vid_t nbr = it.get_vertex();
              VertexRecord nbr_vr(label_triplet.src_label, nbr);
              double weight = weight_func(label_triplet, cur, nbr, it.get_data_ptr());
              if (dist.count(nbr_vr) == 0 || dist[nbr_vr] > path.get_weight() + weight) {
                auto new_path = path.expand(label_triplet.edge_label, label_triplet.src_label, nbr,
                                            Direction::kOut, it.get_data_ptr());
                new_path.set_weight(path.get_weight() + weight);
                dist[nbr_vr] = new_path.get_weight() + weight;

                pq.push(new_path);
              }
            }
          }
        }
      }
    });
    ctx.set_with_reshuffle(params.alias, path_builder.finish(), shuffle_offset);
    return ctx;
  }
};

}  // namespace execution

}  // namespace neug
