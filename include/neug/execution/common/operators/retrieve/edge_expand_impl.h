
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

#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace execution {

#define expand_sv_np_ms(v, v_idx, view, builder, offsets) \
  {                                                       \
    auto es = view.get_edges(v);                          \
    for (auto it = es.begin(); it != es.end(); ++it) {    \
      builder.push_back_opt(it.get_vertex());             \
      offsets.push_back(v_idx);                           \
    }                                                     \
  }

#define expand_sv_np_ml(v, v_idx, view, nbr_label, builder, offsets) \
  {                                                                  \
    auto es = view.get_edges(v);                                     \
    for (auto it = es.begin(); it != es.end(); ++it) {               \
      builder.push_back_opt({nbr_label, it.get_vertex()});           \
      offsets.push_back(v_idx);                                      \
    }                                                                \
  }

#define expand_sv_p_ms(input_label, v, v_idx, nbr_label, edge_label, dir, \
                       view, pred, builder, offsets)                      \
  {                                                                       \
    auto es = view.get_edges(v);                                          \
    for (auto it = es.begin(); it != es.end(); ++it) {                    \
      auto nbr = it.get_vertex();                                         \
      if (pred(input_label, v, nbr_label, nbr, edge_label, dir,           \
               it.get_data_ptr())) {                                      \
        builder.push_back_opt(nbr);                                       \
        offsets.push_back(v_idx);                                         \
      }                                                                   \
    }                                                                     \
  }

#define expand_sv_p_ml(input_label, v, v_idx, nbr_label, edge_label, dir, \
                       view, pred, builder, offsets)                      \
  {                                                                       \
    auto es = view.get_edges(v);                                          \
    for (auto it = es.begin(); it != es.end(); ++it) {                    \
      auto nbr = it.get_vertex();                                         \
      if (pred(input_label, v, nbr_label, nbr, edge_label, dir,           \
               it.get_data_ptr())) {                                      \
        builder.push_back_opt({nbr_label, nbr});                          \
        offsets.push_back(v_idx);                                         \
      }                                                                   \
    }                                                                     \
  }

static inline std::vector<std::tuple<label_t, label_t, Direction>>
get_label_dirs(label_t input_label, const Schema& schema,
               const std::vector<LabelTriplet>& labels, Direction dir) {
  std::vector<std::tuple<label_t, label_t, Direction>> label_dirs;
  for (auto& triplet : labels) {
    if (!schema.exist(triplet.src_label, triplet.dst_label,
                      triplet.edge_label)) {
      continue;
    }
    if (triplet.src_label == input_label &&
        ((dir == Direction::kOut) || (dir == Direction::kBoth))) {
      label_dirs.emplace_back(triplet.dst_label, triplet.edge_label,
                              Direction::kOut);
    }
    if (triplet.dst_label == input_label &&
        ((dir == Direction::kIn) || (dir == Direction::kBoth))) {
      label_dirs.emplace_back(triplet.src_label, triplet.edge_label,
                              Direction::kIn);
    }
  }
  {
    std::sort(label_dirs.begin(), label_dirs.end());
    label_dirs.erase(std::unique(label_dirs.begin(), label_dirs.end()),
                     label_dirs.end());
  }
  return label_dirs;
}

static inline std::vector<std::vector<std::tuple<label_t, label_t, Direction>>>
get_label_dirs_list(const std::set<label_t>& input_labels, const Schema& schema,
                    const std::vector<LabelTriplet>& labels, Direction dir) {
  int label_num = schema.vertex_label_frontier();
  std::vector<std::vector<std::tuple<label_t, label_t, Direction>>> label_dirs(
      label_num);
  for (auto& triplet : labels) {
    if (!schema.exist(triplet.src_label, triplet.dst_label,
                      triplet.edge_label)) {
      continue;
    }
    if ((input_labels.find(triplet.src_label) != input_labels.end()) &&
        ((dir == Direction::kOut) || (dir == Direction::kBoth))) {
      label_dirs[triplet.src_label].emplace_back(
          triplet.dst_label, triplet.edge_label, Direction::kOut);
    }
    if ((input_labels.find(triplet.dst_label) != input_labels.end()) &&
        ((dir == Direction::kIn) || (dir == Direction::kBoth))) {
      label_dirs[triplet.dst_label].emplace_back(
          triplet.src_label, triplet.edge_label, Direction::kIn);
    }
  }
  for (auto& label_dir : label_dirs) {
    {
      std::sort(label_dir.begin(), label_dir.end());
      label_dir.erase(std::unique(label_dir.begin(), label_dir.end()),
                      label_dir.end());
    }
  }
  return label_dirs;
}

template <typename GPRED_T, bool is_optional = false>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_impl(const StorageReadInterface& graph,
                   const SLVertexColumn& input,
                   const std::vector<LabelTriplet>& labels, Direction dir,
                   const GPRED_T& gpred) {
  label_t input_label = input.label();
  std::vector<std::tuple<label_t, label_t, Direction>> label_dirs =
      get_label_dirs(input_label, graph.schema(), labels, dir);
  if (label_dirs.empty()) {
    MLVertexColumnBuilder builder;
    return std::make_pair(builder.finish(), std::vector<size_t>());
  }
  MSVertexColumnBuilder builder(std::get<0>(label_dirs[0]));
  std::vector<size_t> offsets;
  auto& vertices = input.vertices();
  std::vector<bool> matched;
  if constexpr (is_optional) {
    matched.resize(vertices.size(), false);
  }
  for (auto& label_dir : label_dirs) {
    label_t nbr_label = std::get<0>(label_dir);
    label_t edge_label = std::get<1>(label_dir);
    Direction dir = std::get<2>(label_dir);

    auto view = (dir == Direction::kOut)
                    ? (graph.GetGenericOutgoingGraphView(input_label, nbr_label,
                                                         edge_label))
                    : (graph.GetGenericIncomingGraphView(input_label, nbr_label,
                                                         edge_label));
    builder.start_label(nbr_label);
    if constexpr (GPRED_T::is_dummy) {
      for (size_t idx = 0; idx < vertices.size(); ++idx) {
        auto v = vertices[idx];
        if constexpr (is_optional) {
          if (v != std::numeric_limits<vid_t>::max()) {
            size_t old_size = builder.cur_size();
            expand_sv_np_ms(v, idx, view, builder, offsets);
            if (builder.cur_size() != old_size) {
              matched[idx] = true;
            }
          } else {
            if (!matched[idx]) {
              builder.push_back_null();
              offsets.push_back(idx);
              matched[idx] = true;
            }
          }
        } else {
          expand_sv_np_ms(v, idx, view, builder, offsets);
        }
      }
    } else {
      for (size_t idx = 0; idx < vertices.size(); ++idx) {
        auto v = vertices[idx];
        if constexpr (is_optional) {
          if (v != std::numeric_limits<vid_t>::max()) {
            size_t old_size = builder.cur_size();
            expand_sv_p_ms(input_label, v, idx, nbr_label, edge_label, dir,
                           view, gpred, builder, offsets);
            if (builder.cur_size() != old_size) {
              matched[idx] = true;
            }
          } else {
            if (!matched[idx]) {
              builder.push_back_null();
              offsets.push_back(idx);
              matched[idx] = true;
            }
          }
        } else {
          expand_sv_p_ms(input_label, v, idx, nbr_label, edge_label, dir, view,
                         gpred, builder, offsets);
        }
      }
    }
  }
  if constexpr (is_optional) {
    for (size_t idx = 0; idx < vertices.size(); ++idx) {
      if (!matched[idx]) {
        builder.push_back_null();
        offsets.push_back(idx);
      }
    }
  }
  return std::make_pair(builder.finish(), std::move(offsets));
}

template <typename GPRED_T, bool is_optional = false>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_impl(const StorageReadInterface& graph,
                   const MLVertexColumn& input,
                   const std::vector<LabelTriplet>& labels, Direction dir,
                   const GPRED_T& gpred) {
  const std::set<label_t>& input_labels = input.get_labels_set();
  int label_num = graph.schema().vertex_label_frontier();
  std::vector<std::vector<std::tuple<label_t, label_t, Direction>>> label_dirs =
      get_label_dirs_list(input_labels, graph.schema(), labels, dir);
  std::set<label_t> nbr_labels;
  bool single_view_per_label = true;
  for (label_t v_label = 0; v_label < label_num; ++v_label) {
    if (!graph.schema().vertex_label_valid(v_label)) {
      continue;
    }
    for (auto& t : label_dirs[v_label]) {
      label_t nbr_label = std::get<0>(t);
      nbr_labels.insert(nbr_label);
    }
    if (label_dirs[v_label].size() > 1) {
      single_view_per_label = false;
    }
  }
  if (nbr_labels.size() == 0) {
    MLVertexColumnBuilder builder;
    return std::make_pair(builder.finish(), std::vector<size_t>());
  }
  if (input_labels.size() == 1) {
    std::vector<bool> matched;
    if constexpr (is_optional) {
      matched.resize(input.size(), false);
    }
    label_t input_label = *input_labels.begin();
    MSVertexColumnBuilder builder(std::get<0>(label_dirs[input_label][0]));
    std::vector<size_t> offsets;
    const auto& label_dirs_input = label_dirs[input_label];
    for (auto& label_dir : label_dirs_input) {
      label_t nbr_label = std::get<0>(label_dir);
      label_t edge_label = std::get<1>(label_dir);
      Direction dir = std::get<2>(label_dir);

      auto view = (dir == Direction::kOut)
                      ? (graph.GetGenericOutgoingGraphView(
                            input_label, nbr_label, edge_label))
                      : (graph.GetGenericIncomingGraphView(
                            input_label, nbr_label, edge_label));
      builder.start_label(nbr_label);
      if constexpr (GPRED_T::is_dummy) {
        input.foreach_vertex([&](size_t idx, label_t l, vid_t v) {
          if constexpr (is_optional) {
            if (v != std::numeric_limits<vid_t>::max()) {
              size_t old_size = builder.cur_size();
              expand_sv_np_ms(v, idx, view, builder, offsets);
              if (builder.cur_size() != old_size) {
                matched[idx] = true;
              }
            } else {
              if (!matched[idx]) {
                builder.push_back_null();
                offsets.push_back(idx);
                matched[idx] = true;
              }
            }
          } else {
            expand_sv_np_ms(v, idx, view, builder, offsets);
          }
        });
      } else {
        input.foreach_vertex([&](size_t idx, label_t l, vid_t v) {
          if constexpr (is_optional) {
            if (v != std::numeric_limits<vid_t>::max()) {
              size_t old_size = builder.cur_size();
              expand_sv_p_ms(input_label, v, idx, nbr_label, edge_label, dir,
                             view, gpred, builder, offsets);
              if (builder.cur_size() != old_size) {
                matched[idx] = true;
              }
            } else {
              if (!matched[idx]) {
                builder.push_back_null();
                offsets.push_back(idx);
                matched[idx] = true;
              }
            }
          } else {
            expand_sv_p_ms(input_label, v, idx, nbr_label, edge_label, dir,
                           view, gpred, builder, offsets);
          }
        });
      }
    }
    if constexpr (is_optional) {
      for (size_t idx = 0; idx < matched.size(); ++idx) {
        if (!matched[idx]) {
          builder.push_back_null();
          offsets.push_back(idx);
        }
      }
    }
    return std::make_pair(builder.finish(), std::move(offsets));
  } else if (nbr_labels.size() == 1) {
    label_t nbr_label = *nbr_labels.begin();
    MSVertexColumnBuilder builder(nbr_label);
    std::vector<size_t> offsets;
    if (single_view_per_label) {
      std::vector<GenericView> single_views(label_num);
      std::vector<label_t> single_edge_labels(
          label_num, std::numeric_limits<label_t>::max());
      std::vector<Direction> single_dirs(label_num);
      for (auto input_label : input_labels) {
        if (!label_dirs[input_label].empty()) {
          auto& t = label_dirs[input_label][0];
          label_t edge_label = std::get<1>(t);
          Direction dir = std::get<2>(t);
          if (dir == Direction::kOut) {
            single_views[input_label] = graph.GetGenericOutgoingGraphView(
                input_label, nbr_label, edge_label);
          } else {
            CHECK(dir == Direction::kIn);
            single_views[input_label] = graph.GetGenericIncomingGraphView(
                input_label, nbr_label, edge_label);
          }
          single_edge_labels[input_label] = edge_label;
          single_dirs[input_label] = dir;
        }
      }
      if constexpr (GPRED_T::is_dummy) {
        input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
          auto& view = single_views[l];
          if constexpr (is_optional) {
            if (vid != std::numeric_limits<vid_t>::max()) {
              size_t old_size = builder.cur_size();
              expand_sv_np_ms(vid, idx, view, builder, offsets);
              if (builder.cur_size() == old_size) {
                builder.push_back_null();
                offsets.push_back(idx);
              }
            } else {
              builder.push_back_null();
              offsets.push_back(idx);
            }
          } else {
            expand_sv_np_ms(vid, idx, view, builder, offsets);
          }
        });
      } else {
        input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
          auto& view = single_views[l];
          label_t edge_label = single_edge_labels[l];
          Direction dir = single_dirs[l];
          if constexpr (is_optional) {
            if (vid != std::numeric_limits<vid_t>::max()) {
              size_t old_size = builder.cur_size();
              expand_sv_p_ms(l, vid, idx, nbr_label, edge_label, dir, view,
                             gpred, builder, offsets);
              if (builder.cur_size() == old_size) {
                builder.push_back_null();
                offsets.push_back(idx);
              }
            } else {
              builder.push_back_null();
              offsets.push_back(idx);
            }
          } else {
            expand_sv_p_ms(l, vid, idx, nbr_label, edge_label, dir, view, gpred,
                           builder, offsets);
          }
        });
      }
    } else {
      std::vector<std::vector<GenericView>> views(label_num);
      for (label_t v_label = 0; v_label < label_num; ++v_label) {
        for (auto& t : label_dirs[v_label]) {
          label_t edge_label = std::get<1>(t);
          Direction dir = std::get<2>(t);
          if (dir == Direction::kOut) {
            views[v_label].emplace_back(graph.GetGenericOutgoingGraphView(
                v_label, nbr_label, edge_label));
          } else {
            CHECK(dir == Direction::kIn);
            views[v_label].emplace_back(graph.GetGenericIncomingGraphView(
                v_label, nbr_label, edge_label));
          }
        }
      }
      if constexpr (GPRED_T::is_dummy) {
        input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
          if constexpr (is_optional) {
            if (vid == std::numeric_limits<vid_t>::max()) {
              builder.push_back_null();
              offsets.push_back(idx);
            } else {
              bool has_nbr = false;
              for (auto& view : views[l]) {
                auto es = view.get_edges(vid);
                if (!es.empty()) {
                  has_nbr = true;
                  for (auto it = es.begin(); it != es.end(); ++it) {
                    builder.push_back_opt(it.get_vertex());
                    offsets.push_back(idx);
                  }
                }
              }
              if (!has_nbr) {
                builder.push_back_null();
                offsets.push_back(idx);
              }
            }
          } else {
            for (auto& view : views[l]) {
              expand_sv_np_ms(vid, idx, view, builder, offsets);
            }
          }
        });
      } else {
        input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
          if constexpr (is_optional) {
            if (vid != std::numeric_limits<vid_t>::max()) {
              size_t old_size = builder.cur_size();
              size_t csr_idx = 0;
              for (auto& view : views[l]) {
                label_t edge_label = std::get<1>(label_dirs[l][csr_idx]);
                Direction dir = std::get<2>(label_dirs[l][csr_idx]);
                expand_sv_p_ms(l, vid, idx, nbr_label, edge_label, dir, view,
                               gpred, builder, offsets);
                ++csr_idx;
              }
              if (builder.cur_size() == old_size) {
                builder.push_back_null();
                offsets.push_back(idx);
              }
            } else {
              builder.push_back_null();
              offsets.push_back(idx);
            }
          } else {
            size_t csr_idx = 0;
            for (auto& view : views[l]) {
              label_t edge_label = std::get<1>(label_dirs[l][csr_idx]);
              Direction dir = std::get<2>(label_dirs[l][csr_idx]);
              expand_sv_p_ms(l, vid, idx, nbr_label, edge_label, dir, view,
                             gpred, builder, offsets);
              ++csr_idx;
            }
          }
        });
      }
    }
    return std::make_pair(builder.finish(), std::move(offsets));
  } else {
    MLVertexColumnBuilderOpt builder(nbr_labels);
    std::vector<size_t> offsets;
    if (single_view_per_label) {
      std::vector<GenericView> single_views(label_num);
      std::vector<label_t> single_nbr_labels(
          label_num, std::numeric_limits<label_t>::max());
      std::vector<label_t> single_edge_labels(
          label_num, std::numeric_limits<label_t>::max());
      std::vector<Direction> single_dirs(label_num);
      for (auto input_label : input_labels) {
        if (!label_dirs[input_label].empty()) {
          auto& t = label_dirs[input_label][0];
          label_t nbr_label = std::get<0>(t);
          label_t edge_label = std::get<1>(t);
          Direction dir = std::get<2>(t);
          if (dir == Direction::kOut) {
            single_views[input_label] = graph.GetGenericOutgoingGraphView(
                input_label, nbr_label, edge_label);
          } else {
            CHECK(dir == Direction::kIn);
            single_views[input_label] = graph.GetGenericIncomingGraphView(
                input_label, nbr_label, edge_label);
          }
          single_nbr_labels[input_label] = nbr_label;
          single_edge_labels[input_label] = edge_label;
          single_dirs[input_label] = dir;
        }
      }
      if constexpr (GPRED_T::is_dummy) {
        input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
          auto& view = single_views[l];
          label_t nbr_label = single_nbr_labels[l];
          if constexpr (is_optional) {
            if (vid == std::numeric_limits<vid_t>::max()) {
              builder.push_back_null();
              offsets.push_back(idx);
            } else {
              auto es = view.get_edges(vid);
              if (!es.empty()) {
                for (auto it = es.begin(); it != es.end(); ++it) {
                  builder.push_back_opt({nbr_label, it.get_vertex()});
                  offsets.push_back(idx);
                }
              } else {
                builder.push_back_null();
                offsets.push_back(idx);
              }
            }
          } else {
            expand_sv_np_ml(vid, idx, view, nbr_label, builder, offsets);
          }
        });
      } else {
        input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
          auto& view = single_views[l];
          label_t nbr_label = single_nbr_labels[l];
          label_t edge_label = single_edge_labels[l];
          Direction dir = single_dirs[l];
          if constexpr (is_optional) {
            if (vid != std::numeric_limits<vid_t>::max()) {
              size_t old_size = builder.cur_size();
              expand_sv_p_ml(l, vid, idx, nbr_label, edge_label, dir, view,
                             gpred, builder, offsets);
              if (builder.cur_size() == old_size) {
                builder.push_back_null();
                offsets.push_back(idx);
              }
            } else {
              builder.push_back_null();
              offsets.push_back(idx);
            }
          } else {
            expand_sv_p_ml(l, vid, idx, nbr_label, edge_label, dir, view, gpred,
                           builder, offsets);
          }
        });
      }
    } else {
      std::vector<std::vector<GenericView>> views(label_num);
      for (label_t v_label = 0; v_label < label_num; ++v_label) {
        for (auto& t : label_dirs[v_label]) {
          label_t nbr_label = std::get<0>(t);
          label_t edge_label = std::get<1>(t);
          Direction dir = std::get<2>(t);
          if (dir == Direction::kOut) {
            views[v_label].emplace_back(graph.GetGenericOutgoingGraphView(
                v_label, nbr_label, edge_label));
          } else {
            CHECK(dir == Direction::kIn);
            views[v_label].emplace_back(graph.GetGenericIncomingGraphView(
                v_label, nbr_label, edge_label));
          }
        }
      }
      if constexpr (GPRED_T::is_dummy) {
        input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
          if constexpr (is_optional) {
            if (vid != std::numeric_limits<vid_t>::max()) {
              size_t old_size = builder.cur_size();
              for (size_t i = 0; i < views[l].size(); ++i) {
                expand_sv_np_ml(vid, idx, views[l][i],
                                std::get<0>(label_dirs[l][i]), builder,
                                offsets);
              }
              if (builder.cur_size() == old_size) {
                builder.push_back_null();
                offsets.push_back(idx);
              }
            } else {
              builder.push_back_null();
              offsets.push_back(idx);
            }
          } else {
            for (size_t i = 0; i < views[l].size(); ++i) {
              expand_sv_np_ml(vid, idx, views[l][i],
                              std::get<0>(label_dirs[l][i]), builder, offsets);
            }
          }
        });
      } else {
        input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
          if constexpr (is_optional) {
            if (vid != std::numeric_limits<vid_t>::max()) {
              size_t old_size = builder.cur_size();
              for (size_t i = 0; i < views[l].size(); ++i) {
                auto& view = views[l][i];
                label_t nbr_label = std::get<0>(label_dirs[l][i]);
                label_t edge_label = std::get<1>(label_dirs[l][i]);
                Direction dir = std::get<2>(label_dirs[l][i]);
                expand_sv_p_ml(l, vid, idx, nbr_label, edge_label, dir, view,
                               gpred, builder, offsets);
              }
              if (builder.cur_size() == old_size) {
                builder.push_back_null();
                offsets.push_back(idx);
              }
            } else {
              builder.push_back_null();
              offsets.push_back(idx);
            }
          } else {
            for (size_t i = 0; i < views[l].size(); ++i) {
              auto& view = views[l][i];
              label_t nbr_label = std::get<0>(label_dirs[l][i]);
              label_t edge_label = std::get<1>(label_dirs[l][i]);
              Direction dir = std::get<2>(label_dirs[l][i]);
              expand_sv_p_ml(l, vid, idx, nbr_label, edge_label, dir, view,
                             gpred, builder, offsets);
            }
          }
        });
      }
    }
    return std::make_pair(builder.finish(), std::move(offsets));
  }
}

template <typename GPRED_T, bool is_optional = false>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_impl(const StorageReadInterface& graph,
                   const MSVertexColumn& input,
                   const std::vector<LabelTriplet>& labels, Direction dir,
                   const GPRED_T& gpred) {
  const std::set<label_t>& input_labels = input.get_labels_set();
  int label_num = graph.schema().vertex_label_frontier();
  std::vector<std::vector<std::tuple<label_t, label_t, Direction>>> label_dirs =
      get_label_dirs_list(input_labels, graph.schema(), labels, dir);
  std::set<label_t> nbr_labels;
  for (label_t v_label = 0; v_label < label_num; ++v_label) {
    if (!graph.schema().vertex_label_valid(v_label)) {
      continue;
    }
    for (auto& t : label_dirs[v_label]) {
      label_t nbr_label = std::get<0>(t);
      nbr_labels.insert(nbr_label);
    }
  }
  if (nbr_labels.empty()) {
    MLVertexColumnBuilder builder;
    return std::make_pair(builder.finish(), std::vector<size_t>());
  }
  std::vector<std::vector<GenericView>> views(label_num);
  for (auto v_label : input_labels) {
    for (auto& t : label_dirs[v_label]) {
      label_t nbr_label = std::get<0>(t);
      label_t edge_label = std::get<1>(t);
      Direction dir = std::get<2>(t);
      if (dir == Direction::kOut) {
        views[v_label].emplace_back(
            graph.GetGenericOutgoingGraphView(v_label, nbr_label, edge_label));
      } else {
        CHECK(dir == Direction::kIn);
        views[v_label].emplace_back(
            graph.GetGenericIncomingGraphView(v_label, nbr_label, edge_label));
      }
    }
  }
  MSVertexColumnBuilder builder(*nbr_labels.begin());
  std::vector<size_t> offsets;

  size_t input_seg_num = input.seg_num();
  size_t seg_start_idx = 0;
  std::vector<bool> edges_found;
  if constexpr (is_optional) {
    edges_found.resize(input.size(), false);
  }
  for (size_t k = 0; k < input_seg_num; ++k) {
    label_t input_label = input.seg_label(k);
    auto& vertices = input.seg_vertices(k);
    size_t csr_num = label_dirs[input_label].size();
    for (size_t csr_idx = 0; csr_idx < csr_num; ++csr_idx) {
      auto& view = views[input_label][csr_idx];
      label_t nbr_label = std::get<0>(label_dirs[input_label][csr_idx]);
      builder.start_label(nbr_label);
      size_t vertex_idx = seg_start_idx;
      if constexpr (GPRED_T::is_dummy) {
        for (auto vid : vertices) {
          size_t old_size = builder.cur_size();
          if constexpr (is_optional) {
            if (vid != std::numeric_limits<vid_t>::max()) {
              expand_sv_np_ms(vid, vertex_idx, view, builder, offsets);
            }
            if (builder.cur_size() > old_size) {
              edges_found[vertex_idx] = true;
            }
          } else {
            expand_sv_np_ms(vid, vertex_idx, view, builder, offsets);
          }
          ++vertex_idx;
        }
      } else {
        label_t edge_label = std::get<1>(label_dirs[input_label][csr_idx]);
        Direction dir = std::get<2>(label_dirs[input_label][csr_idx]);

        for (auto vid : vertices) {
          size_t old_size = builder.cur_size();
          if constexpr (is_optional) {
            if (vid != std::numeric_limits<vid_t>::max()) {
              expand_sv_p_ms(input_label, vid, vertex_idx, nbr_label,
                             edge_label, dir, view, gpred, builder, offsets);
            }
            if (builder.cur_size() > old_size) {
              edges_found[vertex_idx] = true;
            }
          } else {
            expand_sv_p_ms(input_label, vid, vertex_idx, nbr_label, edge_label,
                           dir, view, gpred, builder, offsets);
          }
          ++vertex_idx;
        }
      }
    }
    seg_start_idx += vertices.size();
  }
  if constexpr (is_optional) {
    for (size_t i = 0; i < edges_found.size(); ++i) {
      if (!edges_found[i]) {
        builder.push_back_null();
        offsets.push_back(i);
      }
    }
  }

  return std::make_pair(builder.finish(), std::move(offsets));
}

#undef expand_sv_np_ms
#undef expand_sv_p_ms
#undef expand_sv_np_ml
#undef expand_sv_p_ml

template <typename PRED_T>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_vertex_optional_impl(const StorageReadInterface& graph,
                            const IVertexColumn& input,
                            const std::vector<LabelTriplet>& labels,
                            Direction dir, const PRED_T& pred) {
  auto vertex_column_type = input.vertex_column_type();
  if (vertex_column_type == VertexColumnType::kSingle) {
    const SLVertexColumn& sl_col = dynamic_cast<const SLVertexColumn&>(input);
    return expand_vertex_impl<PRED_T, true>(graph, sl_col, labels, dir, pred);
  } else if (vertex_column_type == VertexColumnType::kMultiple) {
    const MLVertexColumn& ml_col = dynamic_cast<const MLVertexColumn&>(input);
    return expand_vertex_impl<PRED_T, true>(graph, ml_col, labels, dir, pred);
  } else {
    const MSVertexColumn& ms_col = dynamic_cast<const MSVertexColumn&>(input);
    return expand_vertex_impl<PRED_T, true>(graph, ms_col, labels, dir, pred);
  }
}

template <typename PRED_T, bool is_optional = false>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_edge_impl(const StorageReadInterface& graph, const SLVertexColumn& input,
                 const std::vector<LabelTriplet>& labels, Direction dir,
                 const PRED_T& pred) {
  label_t input_label = input.label();
  auto& vertices = input.vertices();
  std::vector<std::tuple<label_t, label_t, Direction>> label_dirs =
      get_label_dirs(input_label, graph.schema(), labels, dir);
  if (label_dirs.empty()) {
    MSEdgeColumnBuilder builder;
    return std::make_pair(builder.finish(), std::vector<size_t>());
  }
  MSEdgeColumnBuilder builder;
  std::vector<size_t> offsets;
  std::vector<bool> matched;
  if constexpr (is_optional) {
    matched.resize(vertices.size(), false);
  }
  for (auto& label_dir : label_dirs) {
    label_t nbr_label = std::get<0>(label_dir);
    label_t edge_label = std::get<1>(label_dir);
    Direction dir = std::get<2>(label_dir);

    auto view = (dir == Direction::kOut)
                    ? (graph.GetGenericOutgoingGraphView(input_label, nbr_label,
                                                         edge_label))
                    : (graph.GetGenericIncomingGraphView(input_label, nbr_label,
                                                         edge_label));
    LabelTriplet triplet(dir == Direction::kOut ? input_label : nbr_label,
                         dir == Direction::kOut ? nbr_label : input_label,
                         edge_label);
    builder.start_label_dir(triplet, dir);
    if constexpr (PRED_T::is_dummy) {
      for (size_t idx = 0; idx < vertices.size(); ++idx) {
        auto v = vertices[idx];
        if constexpr (is_optional) {
          if (v == std::numeric_limits<vid_t>::max()) {
            if (!matched[idx]) {
              builder.push_back_null();
              offsets.push_back(idx);
              matched[idx] = true;
            }

            continue;
          }
          size_t old_size = offsets.size();
          auto es = view.get_edges(v);
          for (auto it = es.begin(); it != es.end(); ++it) {
            auto nbr = it.get_vertex();
            builder.push_back_opt(dir == Direction::kOut ? v : nbr,
                                  dir == Direction::kOut ? nbr : v,
                                  it.get_data_ptr());
            offsets.push_back(idx);
          }
          if (offsets.size() != old_size) {
            matched[idx] = true;
          }
        } else {
          auto es = view.get_edges(v);
          for (auto it = es.begin(); it != es.end(); ++it) {
            auto nbr = it.get_vertex();
            builder.push_back_opt(dir == Direction::kOut ? v : nbr,
                                  dir == Direction::kOut ? nbr : v,
                                  it.get_data_ptr());
            offsets.push_back(idx);
          }
        }
      }
    } else {
      for (size_t idx = 0; idx < vertices.size(); ++idx) {
        auto v = vertices[idx];
        if constexpr (is_optional) {
          if (v == std::numeric_limits<vid_t>::max()) {
            if (!matched[idx]) {
              builder.push_back_null();
              offsets.push_back(idx);
              matched[idx] = true;
            }
            continue;
          }
          size_t old_size = offsets.size();
          auto es = view.get_edges(v);
          for (auto it = es.begin(); it != es.end(); ++it) {
            auto nbr = it.get_vertex();
            if (pred(input_label, v, nbr_label, nbr, edge_label, dir,
                     it.get_data_ptr())) {
              builder.push_back_opt(dir == Direction::kOut ? v : nbr,
                                    dir == Direction::kOut ? nbr : v,
                                    it.get_data_ptr());
              offsets.push_back(idx);
            }
          }
          if (offsets.size() != old_size) {
            matched[idx] = true;
          }
        } else {
          auto es = view.get_edges(v);
          for (auto it = es.begin(); it != es.end(); ++it) {
            auto nbr = it.get_vertex();
            if (pred(input_label, v, nbr_label, nbr, edge_label, dir,
                     it.get_data_ptr())) {
              builder.push_back_opt(dir == Direction::kOut ? v : nbr,
                                    dir == Direction::kOut ? nbr : v,
                                    it.get_data_ptr());
              offsets.push_back(idx);
            }
          }
        }
      }
    }
  }
  if constexpr (is_optional) {
    for (size_t i = 0; i < matched.size(); ++i) {
      if (!matched[i]) {
        builder.push_back_null();
        offsets.push_back(i);
      }
    }
  }
  return std::make_pair(builder.finish(), std::move(offsets));
}

template <typename PRED_T, bool is_optional = false>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_edge_impl(const StorageReadInterface& graph, const MSVertexColumn& input,
                 const std::vector<LabelTriplet>& labels, Direction dir,
                 const PRED_T& pred) {
  auto input_labels = input.get_labels_set();
  std::vector<std::vector<std::tuple<label_t, label_t, Direction>>> label_dirs =
      get_label_dirs_list(input_labels, graph.schema(), labels, dir);
  MSEdgeColumnBuilder builder;
  std::vector<size_t> offsets;
  std::vector<bool> matched;
  if constexpr (is_optional) {
    matched.resize(input.seg_num(), false);
  }
  size_t input_seg_num = input.seg_num();
  size_t seg_start_idx = 0;
  for (size_t k = 0; k < input_seg_num; ++k) {
    label_t input_label = input.seg_label(k);
    auto& vertices = input.seg_vertices(k);
    for (auto& label_dir : label_dirs[input_label]) {
      label_t nbr_label = std::get<0>(label_dir);
      label_t edge_label = std::get<1>(label_dir);
      Direction dir = std::get<2>(label_dir);

      auto view = (dir == Direction::kOut)
                      ? (graph.GetGenericOutgoingGraphView(
                            input_label, nbr_label, edge_label))
                      : (graph.GetGenericIncomingGraphView(
                            input_label, nbr_label, edge_label));
      LabelTriplet triplet(dir == Direction::kOut ? input_label : nbr_label,
                           dir == Direction::kOut ? nbr_label : input_label,
                           edge_label);
      builder.start_label_dir(triplet, dir);
      size_t vertex_idx = seg_start_idx;
      if constexpr (PRED_T::is_dummy) {
        for (auto v : vertices) {
          if constexpr (is_optional) {
            if (v == std::numeric_limits<vid_t>::max()) {
              if (!matched[vertex_idx]) {
                builder.push_back_null();
                offsets.push_back(vertex_idx);
                matched[vertex_idx] = true;
              }
              ++vertex_idx;
              continue;
            }
            size_t old_size = offsets.size();
            auto es = view.get_edges(v);
            for (auto it = es.begin(); it != es.end(); ++it) {
              auto nbr = it.get_vertex();
              builder.push_back_opt(dir == Direction::kOut ? v : nbr,
                                    dir == Direction::kOut ? nbr : v,
                                    it.get_data_ptr());
              offsets.push_back(vertex_idx);
            }
            if (offsets.size() != old_size) {
              matched[vertex_idx] = true;
            }
          } else {
            auto es = view.get_edges(v);
            for (auto it = es.begin(); it != es.end(); ++it) {
              auto nbr = it.get_vertex();
              builder.push_back_opt(dir == Direction::kOut ? v : nbr,
                                    dir == Direction::kOut ? nbr : v,
                                    it.get_data_ptr());
              offsets.push_back(vertex_idx);
            }
          }
          ++vertex_idx;
        }
      } else {
        for (auto v : vertices) {
          if constexpr (is_optional) {
            if (v == std::numeric_limits<vid_t>::max()) {
              if (!matched[vertex_idx]) {
                builder.push_back_null();
                offsets.push_back(vertex_idx);
                matched[vertex_idx] = true;
              }
              ++vertex_idx;
              continue;
            }
            size_t old_size = offsets.size();
            auto es = view.get_edges(v);
            for (auto it = es.begin(); it != es.end(); ++it) {
              auto nbr = it.get_vertex();
              if (pred(input_label, v, nbr_label, nbr, edge_label, dir,
                       it.get_data_ptr())) {
                builder.push_back_opt(dir == Direction::kOut ? v : nbr,
                                      dir == Direction::kOut ? nbr : v,
                                      it.get_data_ptr());
                offsets.push_back(vertex_idx);
              }
            }
            if (offsets.size() != old_size) {
              matched[vertex_idx] = true;
            }
          } else {
            auto es = view.get_edges(v);
            for (auto it = es.begin(); it != es.end(); ++it) {
              auto nbr = it.get_vertex();
              if (pred(input_label, v, nbr_label, nbr, edge_label, dir,
                       it.get_data_ptr())) {
                builder.push_back_opt(dir == Direction::kOut ? v : nbr,
                                      dir == Direction::kOut ? nbr : v,
                                      it.get_data_ptr());
                offsets.push_back(vertex_idx);
              }
            }
          }
          ++vertex_idx;
        }
      }
    }
    seg_start_idx += vertices.size();
  }
  for (size_t i = 0; i < matched.size(); ++i) {
    if (!matched[i]) {
      builder.push_back_null();
      offsets.push_back(i);
    }
  }
  return std::make_pair(builder.finish(), std::move(offsets));
}

template <typename PRED_T, bool is_optional = false>
std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
expand_edge_impl(const StorageReadInterface& graph, const MLVertexColumn& input,
                 const std::vector<LabelTriplet>& labels, Direction dir,
                 const PRED_T& pred) {
  auto input_labels = input.get_labels_set();
  label_t label_num = graph.schema().vertex_label_frontier();
  std::vector<std::vector<std::tuple<label_t, label_t, Direction>>> label_dirs =
      get_label_dirs_list(input_labels, graph.schema(), labels, dir);
  std::vector<std::vector<GenericView>> views(label_num);
  std::vector<LabelTriplet> all_triplets;
  for (label_t v_label = 0; v_label < label_num; ++v_label) {
    if (!graph.schema().vertex_label_valid(v_label)) {
      continue;
    }
    for (auto& t : label_dirs[v_label]) {
      label_t nbr_label = std::get<0>(t);
      label_t edge_label = std::get<1>(t);
      Direction dir = std::get<2>(t);
      if (dir == Direction::kOut || dir == Direction::kBoth) {
        all_triplets.emplace_back(v_label, nbr_label, edge_label);
        views[v_label].emplace_back(
            graph.GetGenericOutgoingGraphView(v_label, nbr_label, edge_label));
      }
      if (dir == Direction::kIn || dir == Direction::kBoth) {
        all_triplets.emplace_back(nbr_label, v_label, edge_label);
        views[v_label].emplace_back(
            graph.GetGenericIncomingGraphView(v_label, nbr_label, edge_label));
      }
    }
  }
  {
    std::sort(all_triplets.begin(), all_triplets.end());
    all_triplets.erase(std::unique(all_triplets.begin(), all_triplets.end()),
                       all_triplets.end());
  }
  BDMLEdgeColumnBuilder builder(all_triplets);
  std::vector<size_t> offsets;
  std::vector<std::vector<int>> triplet_idx_map(label_num);
  for (label_t v_label = 0; v_label < label_num; ++v_label) {
    for (auto& t : label_dirs[v_label]) {
      label_t nbr_label = std::get<0>(t);
      label_t edge_label = std::get<1>(t);
      Direction dir = std::get<2>(t);
      LabelTriplet triplet(dir == Direction::kOut ? v_label : nbr_label,
                           dir == Direction::kOut ? nbr_label : v_label,
                           edge_label);
      triplet_idx_map[v_label].push_back(builder.get_label_index(triplet));
    }
  }
  size_t old_size;
  input.foreach_vertex([&](size_t idx, label_t l, vid_t vid) {
    if constexpr (is_optional) {
      if (vid == std::numeric_limits<vid_t>::max()) {
        builder.push_back_null();
        offsets.push_back(idx);
        return;
      }
      old_size = offsets.size();
    }
    size_t csr_num = label_dirs[l].size();
    for (size_t csr_idx = 0; csr_idx < csr_num; ++csr_idx) {
      auto& view = views[l][csr_idx];
      int triplet_idx = triplet_idx_map[l][csr_idx];
      Direction dir = std::get<2>(label_dirs[l][csr_idx]);
      if constexpr (PRED_T::is_dummy) {
        auto es = view.get_edges(vid);
        for (auto it = es.begin(); it != es.end(); ++it) {
          auto nbr = it.get_vertex();
          builder.push_back_opt(triplet_idx, dir == Direction::kOut ? vid : nbr,
                                dir == Direction::kOut ? nbr : vid,
                                it.get_data_ptr(), dir);
          offsets.push_back(idx);
        }
      } else {
        label_t nbr_label = std::get<0>(label_dirs[l][csr_idx]);
        label_t edge_label = std::get<1>(label_dirs[l][csr_idx]);
        auto es = view.get_edges(vid);
        for (auto it = es.begin(); it != es.end(); ++it) {
          auto nbr = it.get_vertex();
          if (pred(l, vid, nbr_label, nbr, edge_label, dir,
                   it.get_data_ptr())) {
            builder.push_back_opt(
                triplet_idx, dir == Direction::kOut ? vid : nbr,
                dir == Direction::kOut ? nbr : vid, it.get_data_ptr(), dir);
            offsets.push_back(idx);
          }
        }
      }
    }
    if constexpr (is_optional) {
      if (offsets.size() == old_size) {
        builder.push_back_null();
        offsets.push_back(idx);
      }
    }
  });
  return std::make_pair(builder.finish(), std::move(offsets));
}

}  // namespace execution
}  // namespace neug
