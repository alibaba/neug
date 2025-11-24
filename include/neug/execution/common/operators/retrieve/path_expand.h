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

#ifndef INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_PATH_EXPAND_H_
#define INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_PATH_EXPAND_H_

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/path_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/common/operators/retrieve/path_expand_impl.h"
#include "neug/execution/common/types.h"
#include "neug/execution/utils/params.h"
#include "neug/execution/utils/special_predicates.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace grape {
struct EmptyType;
}  // namespace grape

namespace gs {

namespace runtime {

class PathExpand {
 public:
  // PathExpand(expandOpt == Vertex && alias == -1 && resultOpt == END_V) +
  // GetV(opt == END)
  static gs::result<Context> edge_expand_v(const StorageReadInterface& graph,
                                           Context&& ctx,
                                           const PathExpandParams& params);
  static gs::result<Context> edge_expand_p(const StorageReadInterface& graph,
                                           Context&& ctx,
                                           const PathExpandParams& params);

  static gs::result<Context> all_shortest_paths_with_given_source_and_dest(
      const StorageReadInterface& graph, Context&& ctx,
      const ShortestPathParams& params, const std::pair<label_t, vid_t>& dst);
  // single dst
  static gs::result<Context> single_source_single_dest_shortest_path(
      const StorageReadInterface& graph, Context&& ctx,
      const ShortestPathParams& params, std::pair<label_t, vid_t>& dest);

  template <typename PRED_T>
  static gs::result<Context>
  single_source_shortest_path_with_order_by_length_limit(
      const StorageReadInterface& graph, Context&& ctx,
      const ShortestPathParams& params, const PRED_T& pred, int limit_upper) {
    std::vector<size_t> shuffle_offset;
    auto input_vertex_col =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
    if (params.labels.size() == 1 &&
        params.labels[0].src_label == params.labels[0].dst_label &&
        params.dir == Direction::kBoth &&
        input_vertex_col->get_labels_set().size() == 1) {
      auto tup =
          single_source_shortest_path_with_order_by_length_limit_impl<PRED_T>(
              graph, *input_vertex_col, params.labels[0].edge_label, params.dir,
              params.hop_lower, params.hop_upper, pred, limit_upper);
      ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                             std::get<2>(tup));
      ctx.set(params.alias, std::get<1>(tup));
      return ctx;
    }

    LOG(ERROR) << "not support edge property type ";
    RETURN_UNSUPPORTED_ERROR("not support edge property type ");
  }

  template <typename PRED_T>
  static gs::result<Context> single_source_shortest_path(
      const StorageReadInterface& graph, Context&& ctx,
      const ShortestPathParams& params, const PRED_T& pred) {
    std::vector<size_t> shuffle_offset;
    auto input_vertex_col =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
    if (params.labels.size() == 1 &&
        params.labels[0].src_label == params.labels[0].dst_label &&
        params.dir == Direction::kBoth &&
        input_vertex_col->get_labels_set().size() == 1) {
      auto tup = single_source_shortest_path_impl<PRED_T>(
          graph, *input_vertex_col, params.labels[0].edge_label, params.dir,
          params.hop_lower, params.hop_upper, pred);
      ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup),
                             std::get<2>(tup));
      ctx.set(params.alias, std::get<1>(tup));
      return ctx;
    }
    auto tup = default_single_source_shortest_path_impl<PRED_T>(
        graph, *input_vertex_col, params.labels, params.dir, params.hop_lower,
        params.hop_upper, pred);
    ctx.set_with_reshuffle(params.v_alias, std::get<0>(tup), std::get<2>(tup));
    ctx.set(params.alias, std::get<1>(tup));
    return ctx;
  }

  static gs::result<Context>
  single_source_shortest_path_with_special_vertex_predicate(
      const StorageReadInterface& graph, Context&& ctx,
      const ShortestPathParams& params,
      const SpecialVertexPredicateConfig& config,
      const std::map<std::string, std::string>& query_params);

  template <typename PRED_T>
  static gs::result<Context> edge_expand_p_with_pred(
      const StorageReadInterface& graph, Context&& ctx,
      const PathExpandParams& params, const PRED_T& pred) {
    if (params.opt != PathOpt::kArbitrary) {
      LOG(ERROR) << "only support arbitrary path expand with predicate";
      RETURN_UNSUPPORTED_ERROR(
          "only support arbitrary path expand with predicate");
    }
    std::vector<size_t> shuffle_offset;
    auto& input_vertex_list =
        *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
    auto label_sets = input_vertex_list.get_labels_set();
    auto labels = params.labels;
    std::vector<std::vector<LabelTriplet>> out_labels_map(
        graph.schema().vertex_label_num()),
        in_labels_map(graph.schema().vertex_label_num());
    for (const auto& triplet : labels) {
      out_labels_map[triplet.src_label].emplace_back(triplet);
      in_labels_map[triplet.dst_label].emplace_back(triplet);
    }
    auto dir = params.dir;
    std::vector<std::pair<PathImpl*, size_t>> input;
    std::vector<std::pair<PathImpl*, size_t>> output;

    GeneralPathColumnBuilder builder;
    std::shared_ptr<Arena> arena = std::make_shared<Arena>();
    if (dir == Direction::kOut) {
      foreach_vertex(input_vertex_list,
                     [&](size_t index, label_t label, vid_t v) {
                       auto p = PathImpl::make_path_impl(label, v, *arena);
                       input.emplace_back(std::move(p), index);
                     });
      int depth = 0;
      while (depth < params.hop_upper) {
        output.clear();
        if (depth + 1 < params.hop_upper) {
          for (auto& [path, index] : input) {
            auto end = path->get_end();
            for (const auto& label_triplet : out_labels_map[end.label_]) {
              auto oview = graph.GetGenericOutgoingGraphView(
                  end.label_, label_triplet.dst_label,
                  label_triplet.edge_label);
              auto oes = oview.get_edges(end.vid_);
              for (auto it = oes.begin(); it != oes.end(); ++it) {
                if (pred(end.label_, end.vid_, label_triplet.dst_label,
                         it.get_vertex(), label_triplet.edge_label,
                         Direction::kOut, it.get_data_ptr(), index)) {
                  PathImpl* new_path =
                      path->expand(label_triplet.edge_label,
                                   label_triplet.dst_label, it.get_vertex(),
                                   Direction::kOut, it.get_data_ptr(), *arena);
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
      builder.set_arena(arena);
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);

      return ctx;
    } else if (dir == Direction::kIn) {
      foreach_vertex(input_vertex_list,
                     [&](size_t index, label_t label, vid_t v) {
                       auto p = PathImpl::make_path_impl(label, v, *arena);
                       input.emplace_back(std::move(p), index);
                     });
      int depth = 0;
      while (depth < params.hop_upper) {
        output.clear();

        if (depth + 1 < params.hop_upper) {
          for (const auto& [path, index] : input) {
            auto end = path->get_end();
            for (const auto& label_triplet : in_labels_map[end.label_]) {
              auto iview = graph.GetGenericIncomingGraphView(
                  end.label_, label_triplet.src_label,
                  label_triplet.edge_label);
              auto ies = iview.get_edges(end.vid_);
              for (auto it = ies.begin(); it != ies.end(); ++it) {
                if (pred(end.label_, end.vid_, label_triplet.src_label,
                         it.get_vertex(), label_triplet.edge_label,
                         Direction::kIn, it.get_data_ptr(), index)) {
                  PathImpl* new_path =
                      path->expand(label_triplet.edge_label,
                                   label_triplet.src_label, it.get_vertex(),
                                   Direction::kIn, it.get_data_ptr(), *arena);
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
      builder.set_arena(arena);
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);

      return ctx;

    } else if (dir == Direction::kBoth) {
      foreach_vertex(input_vertex_list,
                     [&](size_t index, label_t label, vid_t v) {
                       auto p = PathImpl::make_path_impl(label, v, *arena);
                       input.emplace_back(std::move(p), index);
                     });
      int depth = 0;
      while (depth < params.hop_upper) {
        output.clear();
        if (depth + 1 < params.hop_upper) {
          for (auto& [path, index] : input) {
            auto end = path->get_end();
            for (const auto& label_triplet : out_labels_map[end.label_]) {
              auto oview = graph.GetGenericOutgoingGraphView(
                  end.label_, label_triplet.dst_label,
                  label_triplet.edge_label);
              auto oes = oview.get_edges(end.vid_);
              for (auto it = oes.begin(); it != oes.end(); ++it) {
                if (pred(end.label_, end.vid_, label_triplet.dst_label,
                         it.get_vertex(), label_triplet.edge_label,
                         Direction::kOut, it.get_data_ptr(), index)) {
                  PathImpl* new_path =
                      path->expand(label_triplet.edge_label,
                                   label_triplet.dst_label, it.get_vertex(),
                                   Direction::kOut, it.get_data_ptr(), *arena);
                  output.emplace_back(std::move(new_path), index);
                }
              }
            }

            for (const auto& label_triplet : in_labels_map[end.label_]) {
              auto iview = graph.GetGenericIncomingGraphView(
                  end.label_, label_triplet.src_label,
                  label_triplet.edge_label);
              auto ies = iview.get_edges(end.vid_);
              for (auto it = ies.begin(); it != ies.end(); ++it) {
                if (pred(end.label_, end.vid_, label_triplet.src_label,
                         it.get_vertex(), label_triplet.edge_label,
                         Direction::kIn, it.get_data_ptr(), index)) {
                  PathImpl* new_path =
                      path->expand(label_triplet.edge_label,
                                   label_triplet.src_label, it.get_vertex(),
                                   Direction::kIn, it.get_data_ptr(), *arena);
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
      builder.set_arena(arena);
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      return ctx;
    }
    LOG(ERROR) << "not support path expand options";
    RETURN_UNSUPPORTED_ERROR("not support path expand options");
  }
};

}  // namespace runtime

}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_PATH_EXPAND_H_
