
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

#ifndef INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_PATH_EXPAND_IMPL_H_
#define INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_PATH_EXPAND_IMPL_H_

#include <glog/logging.h>
#include <stddef.h>
#include <algorithm>
#include <cstdint>

#include <limits>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/i_context_column.h"
#include "neug/execution/common/columns/path_columns.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/common/types.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/property/types.h"
#include "neug/utils/runtime/rt_any.h"

namespace gs {
namespace runtime {
class IContextColumn;

inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
iterative_expand_vertex_on_graph_view(const GenericView& view,
                                      const SLVertexColumn& input, int lower,
                                      int upper) {
  int input_label = input.label();
  MSVertexColumnBuilder builder(input_label);
  std::vector<size_t> offsets;
  if (upper == lower) {
    return std::make_pair(builder.finish(), std::move(offsets));
  }
  if (upper == 1) {
    CHECK_EQ(lower, 0);
    size_t idx = 0;
    for (auto v : input.vertices()) {
      builder.push_back_opt(v);
      offsets.push_back(idx++);
    }
    return std::make_pair(builder.finish(), std::move(offsets));
  }
  // upper >= 2
  std::vector<std::pair<vid_t, vid_t>> input_list;
  std::vector<std::pair<vid_t, vid_t>> output_list;

  {
    vid_t idx = 0;
    for (auto v : input.vertices()) {
      output_list.emplace_back(v, idx++);
    }
  }
  int depth = 0;
  while (!output_list.empty()) {
    input_list.clear();
    std::swap(input_list, output_list);
    if (depth >= lower && depth < upper) {
      if (depth == (upper - 1)) {
        for (auto& pair : input_list) {
          builder.push_back_opt(pair.first);
          offsets.push_back(pair.second);
        }
      } else {
        for (auto& pair : input_list) {
          builder.push_back_opt(pair.first);
          offsets.push_back(pair.second);

          auto es = view.get_edges(pair.first);
          for (auto it = es.begin(); it != es.end(); ++it) {
            output_list.emplace_back(it.get_vertex(), pair.second);
          }
        }
      }
    } else if (depth < lower) {
      for (auto& pair : input_list) {
        auto es = view.get_edges(pair.first);
        for (auto it = es.begin(); it != es.end(); ++it) {
          output_list.emplace_back(it.get_vertex(), pair.second);
        }
      }
    }
    ++depth;
  }

  return std::make_pair(builder.finish(), std::move(offsets));
}

inline std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
iterative_expand_vertex_on_dual_graph_view(const GenericView& iview,
                                           const GenericView& oview,
                                           const SLVertexColumn& input,
                                           int lower, int upper) {
  int input_label = input.label();
  MSVertexColumnBuilder builder(input_label);
  std::vector<size_t> offsets;
  if (upper == lower) {
    return std::make_pair(builder.finish(), std::move(offsets));
  }
  if (upper == 1) {
    CHECK_EQ(lower, 0);
    size_t idx = 0;
    for (auto v : input.vertices()) {
      builder.push_back_opt(v);
      offsets.push_back(idx++);
    }
    return std::make_pair(builder.finish(), std::move(offsets));
  }
  // upper >= 2
  std::vector<std::pair<vid_t, vid_t>> input_list;
  std::vector<std::pair<vid_t, vid_t>> output_list;

  {
    vid_t idx = 0;
    for (auto v : input.vertices()) {
      output_list.emplace_back(v, idx++);
    }
  }
  int depth = 0;
  while (!output_list.empty()) {
    input_list.clear();
    std::swap(input_list, output_list);
    if (depth >= lower && depth < upper) {
      if (depth == (upper - 1)) {
        for (auto& pair : input_list) {
          builder.push_back_opt(pair.first);
          offsets.push_back(pair.second);
        }
      } else {
        for (auto& pair : input_list) {
          builder.push_back_opt(pair.first);
          offsets.push_back(pair.second);

          auto ies = iview.get_edges(pair.first);
          for (auto it = ies.begin(); it != ies.end(); ++it) {
            output_list.emplace_back(it.get_vertex(), pair.second);
          }
          auto oes = oview.get_edges(pair.first);
          for (auto it = oes.begin(); it != oes.end(); ++it) {
            output_list.emplace_back(it.get_vertex(), pair.second);
          }
        }
      }
    } else if (depth < lower) {
      for (auto& pair : input_list) {
        auto ies = iview.get_edges(pair.first);
        for (auto it = ies.begin(); it != ies.end(); ++it) {
          output_list.emplace_back(it.get_vertex(), pair.second);
        }
        auto oes = oview.get_edges(pair.first);
        for (auto it = oes.begin(); it != oes.end(); ++it) {
          output_list.emplace_back(it.get_vertex(), pair.second);
        }
      }
    }
    ++depth;
  }

  return std::make_pair(builder.finish(), std::move(offsets));
}

std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
path_expand_vertex_without_predicate_impl(
    const GraphReadInterface& graph, const SLVertexColumn& input,
    const std::vector<LabelTriplet>& labels, Direction dir, int lower,
    int upper);

template <typename PRED_T>
void sssp_dir(const GenericView& view, Direction dir, label_t v_label, vid_t v,
              label_t e_label, const GraphReadInterface::vertex_set_t& vertices,
              size_t idx, int lower, int upper,
              MSVertexColumnBuilder& dest_col_builder,
              GeneralPathColumnBuilder& path_col_builder, Arena& path_impls,
              std::vector<size_t>& offsets, const PRED_T& pred) {
  std::vector<vid_t> cur;
  std::vector<vid_t> next;
  cur.push_back(v);
  int depth = 0;
  GraphReadInterface::vertex_array_t<vid_t> parent(
      vertices, GraphReadInterface::kInvalidVid);

  while (depth < upper && !cur.empty()) {
    if (depth >= lower) {
      if (depth == upper - 1) {
        for (auto u : cur) {
          if (pred(v_label, u, idx)) {
            std::vector<vid_t> path(depth + 1);
            vid_t x = u;
            for (int i = 0; i <= depth; ++i) {
              path[depth - i] = x;
              x = parent[x];
            }
            std::vector<std::pair<Direction, const void*>> edge_datas;
            for (int i = 0; i < depth; ++i) {
              auto oes = view.get_edges(path[i]);
              for (auto it = oes.begin(); it != oes.end(); ++it) {
                auto nbr = it.get_vertex();
                if (nbr == path[i + 1]) {
                  edge_datas.emplace_back(dir, it.get_data_ptr());
                  break;
                }
              }
            }

            dest_col_builder.push_back_opt(u);
            auto impl = PathImpl::make_path_impl(v_label, e_label, path,
                                                 edge_datas, path_impls);
            path_col_builder.push_back_opt(Path(impl));
            offsets.push_back(idx);
          }
        }
      } else {
        for (auto u : cur) {
          if (pred(v_label, u, idx)) {
            std::vector<vid_t> path(depth + 1);
            vid_t x = u;
            for (int i = 0; i <= depth; ++i) {
              path[depth - i] = x;
              x = parent[x];
            }

            std::vector<std::pair<Direction, const void*>> edge_datas;
            for (int i = 0; i < depth; ++i) {
              auto oes = view.get_edges(path[i]);
              for (auto it = oes.begin(); it != oes.end(); ++it) {
                auto nbr = it.get_vertex();
                if (nbr == path[i + 1]) {
                  edge_datas.emplace_back(dir, it.get_data_ptr());
                  break;
                }
              }
            }

            dest_col_builder.push_back_opt(u);
            auto impl = PathImpl::make_path_impl(v_label, e_label, path,
                                                 edge_datas, path_impls);
            path_col_builder.push_back_opt(Path(impl));
            offsets.push_back(idx);
          }
          auto es = view.get_edges(u);
          for (auto it = es.begin(); it != es.end(); ++it) {
            auto nbr = it.get_vertex();
            if (parent[nbr] == GraphReadInterface::kInvalidVid) {
              parent[nbr] = u;
              next.push_back(nbr);
            }
          }
        }
      }
    } else {
      for (auto u : cur) {
        auto es = view.get_edges(u);
        for (auto it = es.begin(); it != es.end(); ++it) {
          auto nbr = it.get_vertex();
          if (parent[nbr] == GraphReadInterface::kInvalidVid) {
            parent[nbr] = u;
            next.push_back(nbr);
          }
        }
      }
    }
    ++depth;
    cur.clear();
    std::swap(cur, next);
  }
}

template <typename PRED_T>
void sssp_both_dir(const GenericView& view0, const GenericView& view1,
                   label_t v_label, vid_t v, label_t e_label,
                   const GraphReadInterface::vertex_set_t& vertices, size_t idx,
                   int lower, int upper,
                   MSVertexColumnBuilder& dest_col_builder,
                   GeneralPathColumnBuilder& path_col_builder,
                   Arena& path_impls, std::vector<size_t>& offsets,
                   const PRED_T& pred) {
  std::vector<vid_t> cur;
  std::vector<vid_t> next;
  cur.push_back(v);
  int depth = 0;
  GraphReadInterface::vertex_array_t<vid_t> parent(
      vertices, GraphReadInterface::kInvalidVid);

  while (depth < upper && !cur.empty()) {
    if (depth >= lower) {
      if (depth == upper - 1) {
        for (auto u : cur) {
          if (pred(v_label, u, idx)) {
            std::vector<vid_t> path(depth + 1);
            vid_t x = u;
            for (int i = 0; i <= depth; ++i) {
              path[depth - i] = x;
              x = parent[x];
            }
            std::vector<std::pair<Direction, const void*>> edge_datas;
            for (int i = 0; i < depth; ++i) {
              auto oes0 = view0.get_edges(path[i]);
              for (auto it = oes0.begin(); it != oes0.end(); ++it) {
                auto nbr = it.get_vertex();
                if (nbr == path[i + 1]) {
                  edge_datas.emplace_back(Direction::kOut, it.get_data_ptr());
                  break;
                }
              }
              if (edge_datas.size() == static_cast<size_t>(i)) {
                auto oes1 = view1.get_edges(path[i]);
                for (auto it = oes1.begin(); it != oes1.end(); ++it) {
                  auto nbr = it.get_vertex();
                  if (nbr == path[i + 1]) {
                    edge_datas.emplace_back(Direction::kIn, it.get_data_ptr());
                    break;
                  }
                }
              }
            }

            dest_col_builder.push_back_opt(u);
            auto impl = PathImpl::make_path_impl(v_label, e_label, path,
                                                 edge_datas, path_impls);
            path_col_builder.push_back_opt(Path(impl));
            offsets.push_back(idx);
          }
        }
      } else {
        for (auto u : cur) {
          if (pred(v_label, u, idx)) {
            std::vector<vid_t> path(depth + 1);
            vid_t x = u;
            for (int i = 0; i <= depth; ++i) {
              path[depth - i] = x;
              x = parent[x];
            }

            std::vector<std::pair<Direction, const void*>> edge_datas;
            for (int i = 0; i < depth; ++i) {
              auto oes0 = view0.get_edges(path[i]);
              for (auto it = oes0.begin(); it != oes0.end(); ++it) {
                auto nbr = it.get_vertex();
                if (nbr == path[i + 1]) {
                  edge_datas.emplace_back(Direction::kOut, it.get_data_ptr());
                  break;
                }
              }
              if (edge_datas.size() == static_cast<size_t>(i)) {
                auto oes1 = view1.get_edges(path[i]);
                for (auto it = oes1.begin(); it != oes1.end(); ++it) {
                  auto nbr = it.get_vertex();
                  if (nbr == path[i + 1]) {
                    edge_datas.emplace_back(Direction::kIn, it.get_data_ptr());
                    break;
                  }
                }
              }
            }

            dest_col_builder.push_back_opt(u);
            auto impl = PathImpl::make_path_impl(v_label, e_label, path,
                                                 edge_datas, path_impls);
            path_col_builder.push_back_opt(Path(impl));
            offsets.push_back(idx);
          }
          auto es0 = view0.get_edges(u);
          for (auto it = es0.begin(); it != es0.end(); ++it) {
            auto nbr = it.get_vertex();
            if (parent[nbr] == GraphReadInterface::kInvalidVid) {
              parent[nbr] = u;
              next.push_back(nbr);
            }
          }
          auto es1 = view1.get_edges(u);
          for (auto it = es1.begin(); it != es1.end(); ++it) {
            auto nbr = it.get_vertex();
            if (parent[nbr] == GraphReadInterface::kInvalidVid) {
              parent[nbr] = u;
              next.push_back(nbr);
            }
          }
        }
      }
    } else {
      for (auto u : cur) {
        auto es0 = view0.get_edges(u);
        for (auto it = es0.begin(); it != es0.end(); ++it) {
          auto nbr = it.get_vertex();
          if (parent[nbr] == GraphReadInterface::kInvalidVid) {
            parent[nbr] = u;
            next.push_back(nbr);
          }
        }
        auto es1 = view1.get_edges(u);
        for (auto it = es1.begin(); it != es1.end(); ++it) {
          auto nbr = it.get_vertex();
          if (parent[nbr] == GraphReadInterface::kInvalidVid) {
            parent[nbr] = u;
            next.push_back(nbr);
          }
        }
      }
    }
    ++depth;
    cur.clear();
    std::swap(cur, next);
  }
}

template <typename PRED_T>
void sssp_both_dir_with_order_by_length_limit(
    const GenericView& view0, const GenericView& view1, label_t v_label,
    vid_t v, const GraphReadInterface::vertex_set_t& vertices, size_t idx,
    int lower, int upper, MSVertexColumnBuilder& dest_col_builder,
    ValueColumnBuilder<int>& path_len_builder, std::vector<size_t>& offsets,
    const PRED_T& pred, int limit_upper) {
  std::vector<vid_t> cur;
  std::vector<vid_t> next;
  cur.push_back(v);
  int depth = 0;
  GraphReadInterface::vertex_array_t<bool> vis(vertices, false);
  vis[v] = true;

  while (depth < upper && !cur.empty()) {
    if (offsets.size() >= static_cast<size_t>(limit_upper)) {
      break;
    }
    if (depth >= lower) {
      if (depth == upper - 1) {
        for (auto u : cur) {
          if (pred(v_label, u, idx)) {
            dest_col_builder.push_back_opt(u);

            path_len_builder.push_back_opt(depth);
            offsets.push_back(idx);
          }
        }
      } else {
        for (auto u : cur) {
          if (pred(v_label, u, idx)) {
            dest_col_builder.push_back_opt(u);

            path_len_builder.push_back_opt(depth);
            offsets.push_back(idx);
          }
          auto es0 = view0.get_edges(u);
          for (auto it = es0.begin(); it != es0.end(); ++it) {
            auto nbr = it.get_vertex();
            if (!vis[nbr]) {
              vis[nbr] = true;
              next.push_back(nbr);
            }
          }
          auto es1 = view1.get_edges(u);
          for (auto it = es1.begin(); it != es1.end(); ++it) {
            auto nbr = it.get_vertex();
            if (!vis[nbr]) {
              vis[nbr] = true;
              next.push_back(nbr);
            }
          }
        }
      }
    } else {
      for (auto u : cur) {
        auto es0 = view0.get_edges(u);
        for (auto it = es0.begin(); it != es0.end(); ++it) {
          auto nbr = it.get_vertex();
          if (!vis[nbr]) {
            vis[nbr] = true;
            next.push_back(nbr);
          }
        }
        auto es1 = view1.get_edges(u);
        for (auto it = es1.begin(); it != es1.end(); ++it) {
          auto nbr = it.get_vertex();
          if (!vis[nbr]) {
            vis[nbr] = true;
            next.push_back(nbr);
          }
        }
      }
    }
    ++depth;
    cur.clear();
    std::swap(cur, next);
  }
}
template <typename PRED_T>
std::tuple<std::shared_ptr<IContextColumn>, std::shared_ptr<IContextColumn>,
           std::vector<size_t>>
single_source_shortest_path_with_order_by_length_limit_impl(
    const GraphReadInterface& graph, const IVertexColumn& input,
    label_t e_label, Direction dir, int lower, int upper, const PRED_T& pred,
    int limit_upper) {
  label_t v_label = *input.get_labels_set().begin();
  auto vertices = graph.GetVertexSet(v_label);
  MSVertexColumnBuilder dest_col_builder(v_label);
  ValueColumnBuilder<int32_t> path_len_builder;

  std::vector<size_t> offsets;
  {
    CHECK(dir == Direction::kBoth);
    auto oe_view = graph.GetGenericOutgoingGraphView(v_label, v_label, e_label);
    auto ie_view = graph.GetGenericIncomingGraphView(v_label, v_label, e_label);
    foreach_vertex(input, [&](size_t idx, label_t label, vid_t v) {
      sssp_both_dir_with_order_by_length_limit(
          oe_view, ie_view, v_label, v, vertices, idx, lower, upper,
          dest_col_builder, path_len_builder, offsets, pred, limit_upper);
    });
  }

  return std::make_tuple(dest_col_builder.finish(), path_len_builder.finish(),
                         std::move(offsets));
}

template <typename PRED_T>
std::tuple<std::shared_ptr<IContextColumn>, std::shared_ptr<IContextColumn>,
           std::vector<size_t>>
single_source_shortest_path_impl(const GraphReadInterface& graph,
                                 const IVertexColumn& input, label_t e_label,
                                 Direction dir, int lower, int upper,
                                 const PRED_T& pred) {
  std::shared_ptr<Arena> path_impls = std::make_shared<Arena>();
  label_t v_label = *input.get_labels_set().begin();
  auto vertices = graph.GetVertexSet(v_label);
  MSVertexColumnBuilder dest_col_builder(v_label);
  GeneralPathColumnBuilder path_col_builder;
  std::vector<size_t> offsets;
  if (dir == Direction::kIn || dir == Direction::kOut) {
    auto view =
        (dir == Direction::kIn)
            ? graph.GetGenericIncomingGraphView(v_label, v_label, e_label)
            : graph.GetGenericOutgoingGraphView(v_label, v_label, e_label);
    foreach_vertex(input, [&](size_t idx, label_t label, vid_t v) {
      sssp_dir(view, dir, label, v, e_label, vertices, idx, lower, upper,
               dest_col_builder, path_col_builder, *path_impls, offsets, pred);
    });
  } else {
    CHECK(dir == Direction::kBoth);
    auto oe_view = graph.GetGenericOutgoingGraphView(v_label, v_label, e_label);
    auto ie_view = graph.GetGenericIncomingGraphView(v_label, v_label, e_label);
    foreach_vertex(input, [&](size_t idx, label_t label, vid_t v) {
      sssp_both_dir(oe_view, ie_view, v_label, v, e_label, vertices, idx, lower,
                    upper, dest_col_builder, path_col_builder, *path_impls,
                    offsets, pred);
    });
  }
  path_col_builder.set_arena(path_impls);
  return std::make_tuple(dest_col_builder.finish(), path_col_builder.finish(),
                         std::move(offsets));
}

template <typename PRED_T>
std::tuple<std::shared_ptr<IContextColumn>, std::shared_ptr<IContextColumn>,
           std::vector<size_t>>
default_single_source_shortest_path_impl(
    const GraphReadInterface& graph, const IVertexColumn& input,
    const std::vector<LabelTriplet>& labels, Direction dir, int lower,
    int upper, const PRED_T& pred) {
  label_t label_num = graph.schema().vertex_label_num();
  std::vector<std::vector<std::tuple<label_t, label_t, Direction>>> labels_map(
      label_num);
  const auto& input_labels_set = input.get_labels_set();
  std::set<label_t> dest_labels;
  std::shared_ptr<Arena> path_impls = std::make_shared<Arena>();
  for (auto& triplet : labels) {
    if (!graph.schema().exist(triplet.src_label, triplet.dst_label,
                              triplet.edge_label)) {
      continue;
    }
    if (dir == Direction::kOut || dir == Direction::kBoth) {
      if (input_labels_set.find(triplet.src_label) != input_labels_set.end()) {
        labels_map[triplet.src_label].emplace_back(
            triplet.dst_label, triplet.edge_label, Direction::kOut);
        dest_labels.insert(triplet.dst_label);
      }
    }
    if (dir == Direction::kIn || dir == Direction::kBoth) {
      if (input_labels_set.find(triplet.dst_label) != input_labels_set.end()) {
        labels_map[triplet.dst_label].emplace_back(
            triplet.src_label, triplet.edge_label, Direction::kIn);
        dest_labels.insert(triplet.src_label);
      }
    }
  }
  GeneralPathColumnBuilder path_col_builder;
  std::vector<size_t> offsets;

  std::shared_ptr<IContextColumn> dest_col(nullptr);
  if (dest_labels.size() == 1) {
    MSVertexColumnBuilder dest_col_builder(*dest_labels.begin());

    foreach_vertex(input, [&](size_t idx, label_t label, vid_t v) {
      std::vector<std::pair<label_t, vid_t>> cur;
      std::vector<std::pair<label_t, vid_t>> next;
      cur.emplace_back(label, v);
      std::map<std::pair<label_t, vid_t>,
               std::tuple<label_t, vid_t, label_t, Direction, const void*>>
          parent;
      std::set<std::pair<label_t, vid_t>> visited;
      visited.emplace(std::make_pair(label, v));
      int depth = 0;
      while (depth < upper && !cur.empty()) {
        for (auto [v_label, vid] : cur) {
          if (depth >= lower && pred(v_label, vid, idx)) {
            std::vector<VertexRecord> path;
            std::vector<std::tuple<label_t, Direction, const void*>>
                edge_labels;
            auto x = std::make_pair(label, vid);
            while (!(v_label == label && vid == v)) {
              path.push_back(VertexRecord{x.first, x.second});
              auto [p_v_label, p_vid, p_edge_label, p_dir, p_payload] =
                  parent[x];
              edge_labels.emplace_back(p_edge_label, p_dir, p_payload);
              x = std::make_pair(p_v_label, p_vid);
            }
            path.emplace_back(VertexRecord{label, v});
            std::reverse(edge_labels.begin(), edge_labels.end());
            std::reverse(path.begin(), path.end());

            if (path.size() > 1) {
              auto impl =
                  PathImpl::make_path_impl(edge_labels, path, *path_impls);

              path_col_builder.push_back_opt(Path(impl));

              dest_col_builder.push_back_opt(vid);
              offsets.push_back(idx);
            }
          }

          for (auto& l : labels_map[v_label]) {
            label_t nbr_label = std::get<0>(l);
            auto view = (std::get<2>(l) == Direction::kOut)
                            ? graph.GetGenericOutgoingGraphView(
                                  v_label, nbr_label, std::get<1>(l))
                            : graph.GetGenericIncomingGraphView(
                                  v_label, nbr_label, std::get<1>(l));
            auto es = view.get_edges(vid);
            for (auto it = es.begin(); it != es.end(); ++it) {
              auto nbr = std::make_pair(nbr_label, it.get_vertex());
              auto vertex = std::make_pair(nbr_label, it.get_vertex());
              if (visited.find(vertex) == visited.end()) {
                visited.emplace(vertex);
                auto data_ptr = it.get_data_ptr();
                parent[nbr] = std::make_tuple(v_label, vid, std::get<1>(l),
                                              std::get<2>(l), data_ptr);
                next.push_back(nbr);
              }
            }
          }

          ++depth;
          cur.clear();
          std::swap(cur, next);
        }
      }
    });

    dest_col = dest_col_builder.finish();
  } else {
    // TODO(luoxiaojian): opt with MLVertexColumnBuilderOpt
    MLVertexColumnBuilder dest_col_builder;

    foreach_vertex(input, [&](size_t idx, label_t label, vid_t v) {
      std::vector<std::pair<label_t, vid_t>> cur;
      std::vector<std::pair<label_t, vid_t>> next;
      cur.emplace_back(label, v);
      std::map<std::pair<label_t, vid_t>,
               std::tuple<label_t, vid_t, label_t, Direction, const void*>>
          parent;
      std::set<std::pair<label_t, vid_t>> visited;
      visited.insert(std::make_pair(label, v));
      int depth = 0;
      while (depth < upper && !cur.empty()) {
        for (auto [v_label, vid] : cur) {
          if (depth >= lower && pred(v_label, vid, idx)) {
            std::vector<VertexRecord> path;
            std::vector<std::tuple<label_t, Direction, const void*>>
                edge_labels;
            auto x = std::make_pair(v_label, vid);
            while (!(v_label == label && vid == v)) {
              path.emplace_back(x.first, x.second);
              auto [p_v_label, p_vid, p_edge_label, p_dir, p_payload] =
                  parent[x];
              edge_labels.emplace_back(p_edge_label, p_dir, p_payload);
              x = std::make_pair(p_v_label, p_vid);
            }
            path.emplace_back(VertexRecord{label, v});
            std::reverse(edge_labels.begin(), edge_labels.end());
            std::reverse(path.begin(), path.end());

            if (path.size() > 1) {
              auto impl =
                  PathImpl::make_path_impl(edge_labels, path, *path_impls);
              path_col_builder.push_back_opt(Path(impl));

              dest_col_builder.push_back_vertex({v_label, vid});
              offsets.push_back(idx);
            }
          }

          for (auto& l : labels_map[v_label]) {
            label_t nbr_label = std::get<0>(l);
            auto view = (std::get<2>(l) == Direction::kOut)
                            ? graph.GetGenericOutgoingGraphView(
                                  v_label, nbr_label, std::get<1>(l))
                            : graph.GetGenericIncomingGraphView(
                                  v_label, nbr_label, std::get<1>(l));
            auto es = view.get_edges(vid);
            for (auto it = es.begin(); it != es.end(); ++it) {
              auto nbr = std::make_pair(nbr_label, it.get_vertex());
              auto vertex = std::make_pair(nbr_label, it.get_vertex());
              if (visited.find(vertex) == visited.end()) {
                visited.insert(vertex);
                auto data_ptr = it.get_data_ptr();
                parent[nbr] = std::tie(v_label, vid, std::get<1>(l),
                                       std::get<2>(l), data_ptr);
                next.push_back(nbr);
              }
            }
          }

          ++depth;
          cur.clear();
          std::swap(cur, next);
        }
      }
    });

    dest_col = dest_col_builder.finish();
  }
  path_col_builder.set_arena(path_impls);
  return std::make_tuple(dest_col, path_col_builder.finish(),
                         std::move(offsets));
}

}  // namespace runtime

}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_RETRIEVE_PATH_EXPAND_IMPL_H_
