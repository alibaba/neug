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

#include "neug/execution/common/operators/retrieve/path_expand.h"

#include "neug/execution/common/columns/path_columns.h"
#include "neug/execution/common/operators/retrieve/path_expand_impl.h"
#include "neug/execution/expression/special_predicates.h"

namespace neug {

namespace execution {

neug::result<Context> PathExpand::edge_expand_v(
    const StorageReadInterface& graph, Context&& ctx,
    const PathExpandParams& params) {
  std::vector<size_t> shuffle_offset;

  if (params.labels.size() == 1 &&
      params.labels[0].src_label == params.labels[0].dst_label &&
      ctx.get(params.start_tag)->column_type() == ContextColumnType::kVertex) {
    auto vertex_col =
        dynamic_cast<const IVertexColumn*>(ctx.get(params.start_tag).get());
    if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
      const auto& input_vertex_list =
          dynamic_cast<const SLVertexColumn&>(*ctx.get(params.start_tag));
      if (input_vertex_list.label() == params.labels[0].src_label) {
        auto pair = path_expand_vertex_without_predicate_impl(
            graph, input_vertex_list, params.labels, params.dir,
            params.hop_lower, params.hop_upper);
        ctx.set_with_reshuffle(params.alias, pair.first, pair.second);
        return ctx;
      }
    }
  }

  if (params.dir == Direction::kOut) {
    auto& input_vertex_list =
        *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
    std::set<label_t> labels;
    std::vector<std::vector<LabelTriplet>> out_labels_map(
        graph.schema().vertex_label_frontier());
    for (const auto& label : params.labels) {
      labels.emplace(label.dst_label);
      if (params.hop_lower == 0) {
        labels.emplace(label.src_label);
      }
      out_labels_map[label.src_label].emplace_back(label);
    }

    MLVertexColumnBuilderOpt builder(labels);
    std::vector<std::tuple<label_t, vid_t, size_t>> input;
    std::vector<std::tuple<label_t, vid_t, size_t>> output;
    foreach_vertex(input_vertex_list,
                   [&](size_t index, label_t label, vid_t v) {
                     output.emplace_back(label, v, index);
                   });
    int depth = 0;
    while (depth < params.hop_upper && (!output.empty())) {
      input.clear();
      std::swap(input, output);
      if (depth >= params.hop_lower) {
        for (auto& tuple : input) {
          builder.push_back_vertex({std::get<0>(tuple), std::get<1>(tuple)});
          shuffle_offset.push_back(std::get<2>(tuple));
        }
      }

      if (depth + 1 >= params.hop_upper) {
        break;
      }

      for (auto& tuple : input) {
        auto label = std::get<0>(tuple);
        auto v = std::get<1>(tuple);
        auto index = std::get<2>(tuple);
        for (const auto& label_triplet : out_labels_map[label]) {
          auto oe_view = graph.GetGenericOutgoingGraphView(
              label_triplet.src_label, label_triplet.dst_label,
              label_triplet.edge_label);
          auto oes = oe_view.get_edges(v);
          for (auto it = oes.begin(); it != oes.end(); ++it) {
            output.emplace_back(label_triplet.dst_label, it.get_vertex(),
                                index);
          }
        }
      }
      ++depth;
    }
    ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
    return ctx;
  } else if (params.dir == Direction::kIn) {
    auto& input_vertex_list =
        *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
    std::set<label_t> labels;
    std::vector<std::vector<LabelTriplet>> in_labels_map(
        graph.schema().vertex_label_frontier());
    for (auto& label : params.labels) {
      labels.emplace(label.src_label);
      if (params.hop_lower == 0) {
        labels.emplace(label.dst_label);
      }
      in_labels_map[label.dst_label].emplace_back(label);
    }

    MLVertexColumnBuilderOpt builder(labels);
    std::vector<std::tuple<label_t, vid_t, size_t>> input;
    std::vector<std::tuple<label_t, vid_t, size_t>> output;
    foreach_vertex(input_vertex_list,
                   [&](size_t index, label_t label, vid_t v) {
                     output.emplace_back(label, v, index);
                   });
    int depth = 0;
    while (depth < params.hop_upper && (!output.empty())) {
      input.clear();
      std::swap(input, output);
      if (depth >= params.hop_lower) {
        for (const auto& tuple : input) {
          builder.push_back_vertex({std::get<0>(tuple), std::get<1>(tuple)});
          shuffle_offset.push_back(std::get<2>(tuple));
        }
      }

      if (depth + 1 >= params.hop_upper) {
        break;
      }

      for (const auto& tuple : input) {
        auto label = std::get<0>(tuple);
        auto v = std::get<1>(tuple);
        auto index = std::get<2>(tuple);
        for (const auto& label_triplet : in_labels_map[label]) {
          auto iview = graph.GetGenericIncomingGraphView(
              label_triplet.dst_label, label_triplet.src_label,
              label_triplet.edge_label);
          auto ies = iview.get_edges(v);
          for (auto it = ies.begin(); it != ies.end(); ++it) {
            output.emplace_back(label_triplet.src_label, it.get_vertex(),
                                index);
          }
        }
      }
      ++depth;
    }
    ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
    return ctx;
  } else {
    std::set<label_t> labels;
    std::vector<std::vector<LabelTriplet>> in_labels_map(
        graph.schema().vertex_label_frontier()),
        out_labels_map(graph.schema().vertex_label_frontier());
    for (const auto& label : params.labels) {
      labels.emplace(label.dst_label);
      labels.emplace(label.src_label);
      in_labels_map[label.dst_label].emplace_back(label);
      out_labels_map[label.src_label].emplace_back(label);
    }

    MLVertexColumnBuilderOpt builder(labels);
    std::vector<std::tuple<label_t, vid_t, size_t>> input;
    std::vector<std::tuple<label_t, vid_t, size_t>> output;
    auto input_vertex_list =
        std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
    if (input_vertex_list->vertex_column_type() ==
        VertexColumnType::kMultiple) {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<MLVertexColumn>(ctx.get(params.start_tag));

      input_vertex_list.foreach_vertex(
          [&](size_t index, label_t label, vid_t v) {
            output.emplace_back(label, v, index);
          });
    } else {
      foreach_vertex(*input_vertex_list,
                     [&](size_t index, label_t label, vid_t v) {
                       output.emplace_back(label, v, index);
                     });
    }
    int depth = 0;
    while (depth < params.hop_upper && (!output.empty())) {
      input.clear();
      std::swap(input, output);
      if (depth >= params.hop_lower) {
        for (auto& tuple : input) {
          builder.push_back_vertex({std::get<0>(tuple), std::get<1>(tuple)});
          shuffle_offset.push_back(std::get<2>(tuple));
        }
      }

      if (depth + 1 >= params.hop_upper) {
        break;
      }

      for (auto& tuple : input) {
        auto label = std::get<0>(tuple);
        auto v = std::get<1>(tuple);
        auto index = std::get<2>(tuple);
        for (const auto& label_triplet : out_labels_map[label]) {
          auto oview = graph.GetGenericOutgoingGraphView(
              label_triplet.src_label, label_triplet.dst_label,
              label_triplet.edge_label);
          auto oes = oview.get_edges(v);
          for (auto it = oes.begin(); it != oes.end(); ++it) {
            output.emplace_back(label_triplet.dst_label, it.get_vertex(),
                                index);
          }
        }
        for (const auto& label_triplet : in_labels_map[label]) {
          auto iview = graph.GetGenericIncomingGraphView(
              label_triplet.dst_label, label_triplet.src_label,
              label_triplet.edge_label);
          auto ies = iview.get_edges(v);
          for (auto it = ies.begin(); it != ies.end(); ++it) {
            output.emplace_back(label_triplet.src_label, it.get_vertex(),
                                index);
          }
        }
      }
      depth++;
    }
    ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
    return ctx;
  }

  LOG(ERROR) << "not support path expand options";
  RETURN_UNSUPPORTED_ERROR("not support path expand options");
}

neug::result<Context> path_expand_p_arbitrary(const StorageReadInterface& graph,
                                              Context&& ctx,
                                              const PathExpandParams& params) {
  std::vector<size_t> shuffle_offset;
  auto& input_vertex_list =
      *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
  auto label_sets = input_vertex_list.get_labels_set();
  auto labels = params.labels;
  std::vector<std::vector<LabelTriplet>> out_labels_map(
      graph.schema().vertex_label_frontier()),
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
    foreach_vertex(input_vertex_list,
                   [&](size_t index, label_t label, vid_t v) {
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
            auto oview = graph.GetGenericOutgoingGraphView(
                end.label_, label_triplet.dst_label, label_triplet.edge_label);
            auto oes = oview.get_edges(end.vid_);
            for (auto it = oes.begin(); it != oes.end(); ++it) {
              Path new_path = path.expand(
                  label_triplet.edge_label, label_triplet.dst_label,
                  it.get_vertex(), Direction::kOut, it.get_data_ptr());
              output.emplace_back(std::move(new_path), index);
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
    foreach_vertex(input_vertex_list,
                   [&](size_t index, label_t label, vid_t v) {
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
            auto iview = graph.GetGenericIncomingGraphView(
                end.label_, label_triplet.src_label, label_triplet.edge_label);
            auto ies = iview.get_edges(end.vid_);
            for (auto it = ies.begin(); it != ies.end(); ++it) {
              Path new_path = path.expand(
                  label_triplet.edge_label, label_triplet.src_label,
                  it.get_vertex(), Direction::kIn, it.get_data_ptr());
              output.emplace_back(std::move(new_path), index);
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
    foreach_vertex(input_vertex_list,
                   [&](size_t index, label_t label, vid_t v) {
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
            auto oview = graph.GetGenericOutgoingGraphView(
                end.label_, label_triplet.dst_label, label_triplet.edge_label);
            auto oes = oview.get_edges(end.vid_);
            for (auto it = oes.begin(); it != oes.end(); ++it) {
              auto new_path = path.expand(
                  label_triplet.edge_label, label_triplet.dst_label,
                  it.get_vertex(), Direction::kOut, it.get_data_ptr());
              output.emplace_back(std::move(new_path), index);
            }
          }

          for (const auto& label_triplet : in_labels_map[end.label_]) {
            auto iview = graph.GetGenericIncomingGraphView(
                end.label_, label_triplet.src_label, label_triplet.edge_label);
            auto ies = iview.get_edges(end.vid_);
            for (auto it = ies.begin(); it != ies.end(); ++it) {
              auto new_path = path.expand(
                  label_triplet.edge_label, label_triplet.src_label,
                  it.get_vertex(), Direction::kIn, it.get_data_ptr());
              output.emplace_back(std::move(new_path), index);
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

neug::result<Context> path_expand_p_simple(const StorageReadInterface& graph,
                                           Context&& ctx,
                                           const PathExpandParams& params) {
  std::vector<size_t> shuffle_offset;
  auto& input_vertex_list =
      *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
  auto label_sets = input_vertex_list.get_labels_set();
  auto labels = params.labels;
  std::vector<std::vector<LabelTriplet>> out_labels_map(
      graph.schema().vertex_label_frontier()),
      in_labels_map(graph.schema().vertex_label_frontier());
  for (const auto& triplet : labels) {
    out_labels_map[triplet.src_label].emplace_back(triplet);
    in_labels_map[triplet.dst_label].emplace_back(triplet);
  }
  auto dir = params.dir;

  PathColumnBuilder builder;
  std::function<void(std::vector<VertexRecord>&,
                     std::vector<std::tuple<label_t, Direction, const void*>>&,
                     size_t)>
      dfs = [&](std::vector<VertexRecord>& path,
                std::vector<std::tuple<label_t, Direction, const void*>>&
                    edge_labels,
                size_t index) {
        if (path.size() >= static_cast<size_t>(params.hop_lower)) {
          auto ptr = Path(edge_labels, path);
          builder.push_back_opt(Path(ptr));

          shuffle_offset.push_back(index);
        }
        if (path.size() == static_cast<size_t>(params.hop_upper)) {
          return;
        }
        auto end = path.back();
        if (dir == Direction::kOut || dir == Direction::kBoth) {
          for (const auto& label_triplet : out_labels_map[end.label_]) {
            auto oview = graph.GetGenericOutgoingGraphView(
                end.label_, label_triplet.dst_label, label_triplet.edge_label);
            auto oes = oview.get_edges(end.vid_);
            for (auto it = oes.begin(); it != oes.end(); ++it) {
              VertexRecord nbr{label_triplet.dst_label, it.get_vertex()};
              if (std::find(path.begin(), path.end(), nbr) == path.end()) {
                path.emplace_back(nbr);
                edge_labels.emplace_back(label_triplet.edge_label,
                                         Direction::kOut, it.get_data_ptr());
                dfs(path, edge_labels, index);
                path.pop_back();
                edge_labels.pop_back();
              }
            }
          }
          if (dir == Direction::kIn || dir == Direction::kBoth) {
            for (const auto& label_triplet : in_labels_map[end.label_]) {
              auto iview = graph.GetGenericIncomingGraphView(
                  end.label_, label_triplet.src_label,
                  label_triplet.edge_label);
              auto ies = iview.get_edges(end.vid_);
              for (auto it = ies.begin(); it != ies.end(); ++it) {
                VertexRecord nbr{label_triplet.src_label, it.get_vertex()};
                if (std::find(path.begin(), path.end(), nbr) == path.end()) {
                  path.emplace_back(nbr);
                  edge_labels.emplace_back(label_triplet.edge_label,
                                           Direction::kIn, it.get_data_ptr());
                  dfs(path, edge_labels, index);
                  path.pop_back();
                  edge_labels.pop_back();
                }
              }
            }
          }
        }
      };
  foreach_vertex(input_vertex_list, [&](size_t index, label_t label, vid_t v) {
    std::vector<VertexRecord> path = {VertexRecord{label, v}};
    std::vector<std::tuple<label_t, Direction, const void*>> edge_labels;
    dfs(path, edge_labels, index);
  });
  ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
  return ctx;
}

neug::result<Context> path_expand_p_trail(const StorageReadInterface& graph,
                                          Context&& ctx,
                                          const PathExpandParams& params) {
  std::vector<size_t> shuffle_offset;
  auto& input_vertex_list =
      *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
  auto label_sets = input_vertex_list.get_labels_set();
  auto labels = params.labels;
  std::vector<std::vector<LabelTriplet>> out_labels_map(
      graph.schema().vertex_label_frontier()),
      in_labels_map(graph.schema().vertex_label_frontier());
  for (const auto& triplet : labels) {
    out_labels_map[triplet.src_label].emplace_back(triplet);
    in_labels_map[triplet.dst_label].emplace_back(triplet);
  }
  auto dir = params.dir;

  PathColumnBuilder builder;
  std::function<void(std::vector<VertexRecord>&,
                     std::vector<std::tuple<label_t, Direction, const void*>>&,
                     size_t)>
      dfs = [&](std::vector<VertexRecord>& path,
                std::vector<std::tuple<label_t, Direction, const void*>>&
                    edge_labels,
                size_t index) {
        if (path.size() >= static_cast<size_t>(params.hop_lower)) {
          auto ptr = Path(edge_labels, path);
          builder.push_back_opt(ptr);
          shuffle_offset.push_back(index);
        }
        if (path.size() == static_cast<size_t>(params.hop_upper)) {
          return;
        }
        auto end = path.back();
        if (dir == Direction::kOut || dir == Direction::kBoth) {
          for (const auto& label_triplet : out_labels_map[end.label_]) {
            auto oview = graph.GetGenericOutgoingGraphView(
                end.label_, label_triplet.dst_label, label_triplet.edge_label);
            auto oes = oview.get_edges(end.vid_);
            for (auto it = oes.begin(); it != oes.end(); ++it) {
              VertexRecord nbr{label_triplet.dst_label, it.get_vertex()};
              bool skip = false;
              if (path.size() > 1) {
                for (size_t i = 0; i + 1 < path.size(); ++i) {
                  if (path[i] == end && path[i + 1] == nbr &&
                      std::get<0>(edge_labels[i]) == label_triplet.edge_label) {
                    skip = true;
                    break;
                  }
                }
              }
              if (!skip) {
                path.emplace_back(nbr);
                edge_labels.emplace_back(label_triplet.edge_label,
                                         Direction::kOut, it.get_data_ptr());
                dfs(path, edge_labels, index);
                path.pop_back();
                edge_labels.pop_back();
              }
            }
          }
          if (dir == Direction::kIn || dir == Direction::kBoth) {
            for (const auto& label_triplet : in_labels_map[end.label_]) {
              auto iview = graph.GetGenericIncomingGraphView(
                  end.label_, label_triplet.src_label,
                  label_triplet.edge_label);
              auto es = iview.get_edges(end.vid_);
              for (auto it = es.begin(); it != es.end(); ++it) {
                VertexRecord nbr{label_triplet.src_label, it.get_vertex()};
                bool skip = false;
                if (path.size() > 1) {
                  for (size_t i = 0; i + 1 < path.size(); ++i) {
                    if (path[i] == end && path[i + 1] == nbr &&
                        std::get<0>(edge_labels[i]) ==
                            label_triplet.edge_label) {
                      skip = true;
                      break;
                    }
                  }
                }
                if (!skip) {
                  path.emplace_back(nbr);
                  edge_labels.emplace_back(label_triplet.edge_label,
                                           Direction::kIn, it.get_data_ptr());
                  dfs(path, edge_labels, index);
                  path.pop_back();
                  edge_labels.pop_back();
                }
              }
            }
          }
        }
      };
  foreach_vertex(input_vertex_list, [&](size_t index, label_t label, vid_t v) {
    std::vector<VertexRecord> path = {VertexRecord{label, v}};
    std::vector<std::tuple<label_t, Direction, const void*>> edge_labels;
    dfs(path, edge_labels, index);
  });
  ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
  return ctx;
}

neug::result<Context> path_expand_p_any_shortest(
    const StorageReadInterface& graph, Context&& ctx,
    const PathExpandParams& params) {
  std::vector<size_t> shuffle_offset;
  auto& input_vertex_list =
      *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
  auto label_sets = input_vertex_list.get_labels_set();
  auto labels = params.labels;
  std::vector<std::vector<LabelTriplet>> out_labels_map(
      graph.schema().vertex_label_frontier()),
      in_labels_map(graph.schema().vertex_label_frontier());
  for (const auto& triplet : labels) {
    out_labels_map[triplet.src_label].emplace_back(triplet);
    in_labels_map[triplet.dst_label].emplace_back(triplet);
  }
  auto dir = params.dir;

  PathColumnBuilder builder;
  std::function<void(const VertexRecord&, size_t)> bfs = [&](const VertexRecord&
                                                                 root,
                                                             size_t index) {
    std::unordered_map<
        VertexRecord, std::tuple<VertexRecord, label_t, Direction, const void*>>
        visited;
    std::tuple<VertexRecord, label_t, Direction, const void*> dummy;
    visited.emplace(root, dummy);
    std::vector<VertexRecord> current_level;
    std::vector<VertexRecord> next_level;
    current_level.emplace_back(root);
    int depth = 0;
    while (depth + 1 < params.hop_upper && !current_level.empty()) {
      next_level.clear();
      for (const auto& node : current_level) {
        if (dir == Direction::kOut || dir == Direction::kBoth) {
          for (const auto& label_triplet : out_labels_map[node.label_]) {
            auto oview = graph.GetGenericOutgoingGraphView(
                node.label_, label_triplet.dst_label, label_triplet.edge_label);
            auto oes = oview.get_edges(node.vid_);
            for (auto it = oes.begin(); it != oes.end(); ++it) {
              VertexRecord nbr{label_triplet.dst_label, it.get_vertex()};
              if (visited.find(nbr) == visited.end()) {
                visited.emplace(
                    nbr, std::make_tuple(node, label_triplet.edge_label,
                                         Direction::kOut, it.get_data_ptr()));
                next_level.emplace_back(nbr);
              }
            }
          }
        }
        if (dir == Direction::kIn || dir == Direction::kBoth) {
          for (const auto& label_triplet : in_labels_map[node.label_]) {
            auto iview = graph.GetGenericIncomingGraphView(
                node.label_, label_triplet.src_label, label_triplet.edge_label);
            auto es = iview.get_edges(node.vid_);
            for (auto it = es.begin(); it != es.end(); ++it) {
              VertexRecord nbr{label_triplet.src_label, it.get_vertex()};
              if (visited.find(nbr) == visited.end()) {
                visited.emplace(
                    nbr, std::make_tuple(node, label_triplet.edge_label,
                                         Direction::kIn, it.get_data_ptr()));
                next_level.emplace_back(nbr);
              }
            }
          }
        }
      }
      std::swap(current_level, next_level);
      ++depth;
    }
    if (depth >= params.hop_lower) {
      for (const auto& pair : visited) {
        const auto& node = pair.first;
        if (node == root) {
          continue;
        }
        std::vector<VertexRecord> path;
        std::vector<std::tuple<label_t, Direction, const void*>> edge_labels;
        VertexRecord cur = node;
        while (!(cur == root)) {
          path.emplace_back(cur);
          auto info = visited.find(cur);
          edge_labels.emplace_back(std::get<1>(info->second),
                                   std::get<2>(info->second),
                                   std::get<3>(info->second));
          cur = std::get<0>(info->second);
        }
        path.emplace_back(root);
        std::reverse(path.begin(), path.end());
        std::reverse(edge_labels.begin(), edge_labels.end());
        auto ptr = Path(edge_labels, path);
        builder.push_back_opt(ptr);
        shuffle_offset.push_back(index);
      }
    }
  };
  foreach_vertex(input_vertex_list, [&](size_t index, label_t label, vid_t v) {
    VertexRecord root{label, v};
    bfs(root, index);
  });
  ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
  return ctx;
}

neug::result<Context> PathExpand::edge_expand_p(
    const StorageReadInterface& graph, Context&& ctx,
    const PathExpandParams& params) {
  if (params.opt == PathOpt::kArbitrary) {
    return path_expand_p_arbitrary(graph, std::move(ctx), params);
  } else if (params.opt == PathOpt::kAnyShortest) {
    return path_expand_p_any_shortest(graph, std::move(ctx), params);
  } else if (params.opt == PathOpt::kTrail) {
    return path_expand_p_trail(graph, std::move(ctx), params);
  } else if (params.opt == PathOpt::kSimple) {
    return path_expand_p_simple(graph, std::move(ctx), params);
  } else {
    LOG(ERROR) << "not support path expand options"
               << static_cast<int>(params.opt);
    RETURN_UNSUPPORTED_ERROR("not support path expand options");
  }
  RETURN_UNSUPPORTED_ERROR("not support path expand options");
}

static bool single_source_single_dest_shortest_path_impl(
    const StorageReadInterface& graph, const ShortestPathParams& params,
    vid_t src, vid_t dst, std::vector<vid_t>& path,
    std::vector<std::pair<Direction, const void*>>& edge_datas) {
  std::queue<vid_t> q1;
  std::queue<vid_t> q2;
  std::queue<vid_t> tmp;

  label_t v_label = params.labels[0].src_label;
  label_t e_label = params.labels[0].edge_label;
  auto vertices = graph.GetVertexSet(v_label);
  StorageReadInterface::vertex_array_t<int> pre(vertices, -1);
  StorageReadInterface::vertex_array_t<int> dis(vertices, 0);
  q1.push(src);
  dis[src] = 1;
  q2.push(dst);
  dis[dst] = -1;

  auto oview = graph.GetGenericOutgoingGraphView(v_label, v_label, e_label);
  auto iview = graph.GetGenericIncomingGraphView(v_label, v_label, e_label);

  auto get_edge_datas = [&]() {
    edge_datas.clear();
    for (size_t i = 0; i + 1 < path.size(); ++i) {
      vid_t u = path[i];
      vid_t v = path[i + 1];
      bool found = false;
      auto oes = oview.get_edges(u);
      for (auto it = oes.begin(); it != oes.end(); ++it) {
        if (it.get_vertex() == v) {
          edge_datas.emplace_back(Direction::kOut, it.get_data_ptr());
          found = true;
          break;
        }
      }
      if (found) {
        continue;
      }
      auto ies = iview.get_edges(u);
      for (auto it = ies.begin(); it != ies.end(); ++it) {
        if (it.get_vertex() == v) {
          edge_datas.emplace_back(Direction::kIn, it.get_data_ptr());
          break;
        }
      }
    }
  };

  while (true) {
    if (q1.size() <= q2.size()) {
      if (q1.empty()) {
        break;
      }
      while (!q1.empty()) {
        int x = q1.front();
        if (dis[x] >= params.hop_upper + 1) {
          return false;
        }
        q1.pop();
        auto oes = oview.get_edges(x);
        for (auto it = oes.begin(); it != oes.end(); ++it) {
          int y = it.get_vertex();
          if (dis[y] == 0) {
            dis[y] = dis[x] + 1;
            tmp.push(y);
            pre[y] = x;
          } else if (dis[y] < 0) {
            while (x != -1) {
              path.emplace_back(x);
              x = pre[x];
            }
            std::reverse(path.begin(), path.end());
            while (y != -1) {
              path.emplace_back(y);
              y = pre[y];
            }
            int len = path.size() - 1;
            get_edge_datas();
            return len >= params.hop_lower && len < params.hop_upper;
          }
        }
        auto ies = iview.get_edges(x);
        for (auto it = ies.begin(); it != ies.end(); ++it) {
          int y = it.get_vertex();
          if (dis[y] == 0) {
            dis[y] = dis[x] + 1;
            tmp.push(y);
            pre[y] = x;
          } else if (dis[y] < 0) {
            while (x != -1) {
              path.emplace_back(x);
              x = pre[x];
            }
            std::reverse(path.begin(), path.end());
            while (y != -1) {
              path.emplace_back(y);
              y = pre[y];
            }
            int len = path.size() - 1;
            get_edge_datas();
            return len >= params.hop_lower && len < params.hop_upper;
          }
        }
      }
      std::swap(q1, tmp);
    } else {
      if (q2.empty()) {
        break;
      }
      while (!q2.empty()) {
        int x = q2.front();
        if (dis[x] <= -params.hop_upper - 1) {
          return false;
        }
        q2.pop();
        auto oes = oview.get_edges(x);
        for (auto it = oes.begin(); it != oes.end(); ++it) {
          int y = it.get_vertex();
          if (dis[y] == 0) {
            dis[y] = dis[x] - 1;
            tmp.push(y);
            pre[y] = x;
          } else if (dis[y] > 0) {
            while (y != -1) {
              path.emplace_back(y);
              y = pre[y];
            }
            std::reverse(path.begin(), path.end());
            while (x != -1) {
              path.emplace_back(x);
              x = pre[x];
            }
            int len = path.size() - 1;
            get_edge_datas();
            return len >= params.hop_lower && len < params.hop_upper;
          }
        }
        auto ies = iview.get_edges(x);
        for (auto it = ies.begin(); it != ies.end(); ++it) {
          int y = it.get_vertex();
          if (dis[y] == 0) {
            dis[y] = dis[x] - 1;
            tmp.push(y);
            pre[y] = x;
          } else if (dis[y] > 0) {
            while (y != -1) {
              path.emplace_back(y);
              y = pre[y];
            }
            std::reverse(path.begin(), path.end());
            while (x != -1) {
              path.emplace_back(x);
              x = pre[x];
            }
            int len = path.size() - 1;
            get_edge_datas();
            return len >= params.hop_lower && len < params.hop_upper;
          }
        }
      }
      std::swap(q2, tmp);
    }
  }
  return false;
}

neug::result<Context> PathExpand::single_source_single_dest_shortest_path(
    const StorageReadInterface& graph, Context&& ctx,
    const ShortestPathParams& params, std::pair<label_t, vid_t>& dest) {
  std::vector<size_t> shuffle_offset;
  auto& input_vertex_list =
      *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
  auto label_sets = input_vertex_list.get_labels_set();
  auto labels = params.labels;
  if (labels.size() != 1 || label_sets.size() != 1) {
    LOG(ERROR) << "only support one label triplet";
    RETURN_UNSUPPORTED_ERROR("only support one label triplet");
  }
  auto label_triplet = labels[0];
  if (label_triplet.src_label != label_triplet.dst_label ||
      params.dir != Direction::kBoth) {
    LOG(ERROR) << "only support same src and dst label and both direction";
    RETURN_UNSUPPORTED_ERROR(
        "only support same src and dst label and both "
        "direction");
  }
  MSVertexColumnBuilder builder(label_triplet.dst_label);
  PathColumnBuilder path_builder;
  foreach_vertex(input_vertex_list, [&](size_t index, label_t label, vid_t v) {
    std::vector<vid_t> path;
    std::vector<std::pair<Direction, const void*>> edge_datas;
    if (single_source_single_dest_shortest_path_impl(
            graph, params, v, dest.second, path, edge_datas)) {
      builder.push_back_opt(dest.second);
      shuffle_offset.push_back(index);
      auto impl = Path(label_triplet.src_label, label_triplet.edge_label, path,
                       edge_datas);
      path_builder.push_back_opt(impl);
    }
  });

  ctx.set_with_reshuffle(params.v_alias, builder.finish(), shuffle_offset);
  ctx.set(params.alias, path_builder.finish());
  return ctx;
}

static void dfs(
    const GenericView& oview, const GenericView& iview, vid_t src, vid_t dst,
    const StorageReadInterface::vertex_array_t<bool>& visited,
    const StorageReadInterface::vertex_array_t<int8_t>& dist,
    const ShortestPathParams& params, std::vector<std::vector<vid_t>>& paths,
    std::vector<vid_t>& cur_path,
    std::vector<std::vector<std::pair<Direction, const void*>>>& edge_datas,
    std::vector<std::pair<Direction, const void*>>& cur_edge_data) {
  cur_path.push_back(src);
  if (src == dst) {
    paths.emplace_back(cur_path);
    edge_datas.emplace_back(cur_edge_data);
    cur_path.pop_back();
    return;
  }
  auto oes = oview.get_edges(src);
  for (auto it = oes.begin(); it != oes.end(); ++it) {
    vid_t nbr = it.get_vertex();
    if (visited[nbr] && dist[nbr] == dist[src] + 1) {
      cur_edge_data.emplace_back(Direction::kOut, it.get_data_ptr());
      dfs(oview, iview, nbr, dst, visited, dist, params, paths, cur_path,
          edge_datas, cur_edge_data);
      cur_edge_data.pop_back();
    }
  }
  auto ies = iview.get_edges(src);
  for (auto it = ies.begin(); it != ies.end(); ++it) {
    vid_t nbr = it.get_vertex();
    if (visited[nbr] && dist[nbr] == dist[src] + 1) {
      cur_edge_data.emplace_back(Direction::kIn, it.get_data_ptr());
      dfs(oview, iview, nbr, dst, visited, dist, params, paths, cur_path,
          edge_datas, cur_edge_data);
      cur_edge_data.pop_back();
    }
  }
  cur_path.pop_back();
}

static void all_shortest_path_with_given_source_and_dest_impl(
    const StorageReadInterface& graph, const ShortestPathParams& params,
    vid_t src, vid_t dst, std::vector<std::vector<vid_t>>& paths,
    std::vector<std::vector<std::pair<Direction, const void*>>>& edge_datas) {
  StorageReadInterface::vertex_array_t<int8_t> dist_from_src(
      graph.GetVertexSet(params.labels[0].src_label), -1);
  StorageReadInterface::vertex_array_t<int8_t> dist_from_dst(
      graph.GetVertexSet(params.labels[0].dst_label), -1);
  dist_from_src[src] = 0;
  dist_from_dst[dst] = 0;
  std::queue<vid_t> q1, q2, tmp;
  q1.push(src);
  q2.push(dst);
  std::vector<vid_t> vec;
  int8_t src_dep = 0, dst_dep = 0;

  auto oview1 = graph.GetGenericOutgoingGraphView(params.labels[0].src_label,
                                                  params.labels[0].dst_label,
                                                  params.labels[0].edge_label);
  auto iview1 = graph.GetGenericIncomingGraphView(params.labels[0].dst_label,
                                                  params.labels[0].src_label,
                                                  params.labels[0].edge_label);
  auto oview2 = graph.GetGenericOutgoingGraphView(params.labels[0].dst_label,
                                                  params.labels[0].src_label,
                                                  params.labels[0].edge_label);
  auto iview2 = graph.GetGenericIncomingGraphView(params.labels[0].src_label,
                                                  params.labels[0].dst_label,
                                                  params.labels[0].edge_label);

  while (true) {
    if (src_dep >= params.hop_upper || dst_dep >= params.hop_upper ||
        !vec.empty()) {
      break;
    }
    if (q1.size() <= q2.size()) {
      if (q1.empty()) {
        break;
      }
      while (!q1.empty()) {
        vid_t v = q1.front();
        q1.pop();
        auto oes = oview1.get_edges(v);
        for (auto it = oes.begin(); it != oes.end(); ++it) {
          vid_t nbr = it.get_vertex();
          if (dist_from_src[nbr] == -1) {
            dist_from_src[nbr] = src_dep + 1;
            tmp.push(nbr);
            if (dist_from_dst[nbr] != -1) {
              vec.push_back(nbr);
            }
          }
        }
        auto ies = iview1.get_edges(v);
        for (auto it = ies.begin(); it != ies.end(); ++it) {
          vid_t nbr = it.get_vertex();
          if (dist_from_src[nbr] == -1) {
            dist_from_src[nbr] = src_dep + 1;
            tmp.push(nbr);
            if (dist_from_dst[nbr] != -1) {
              vec.push_back(nbr);
            }
          }
        }
      }
      std::swap(q1, tmp);
      ++src_dep;
    } else {
      if (q2.empty()) {
        break;
      }
      while (!q2.empty()) {
        vid_t v = q2.front();
        q2.pop();
        auto oes = oview2.get_edges(v);
        for (auto it = oes.begin(); it != oes.end(); ++it) {
          vid_t nbr = it.get_vertex();
          if (dist_from_dst[nbr] == -1) {
            dist_from_dst[nbr] = dst_dep + 1;
            tmp.push(nbr);
            if (dist_from_src[nbr] != -1) {
              vec.push_back(nbr);
            }
          }
        }
        auto ies = iview2.get_edges(v);
        for (auto it = ies.begin(); it != ies.end(); ++it) {
          vid_t nbr = it.get_vertex();
          if (dist_from_dst[nbr] == -1) {
            dist_from_dst[nbr] = dst_dep + 1;
            tmp.push(nbr);
            if (dist_from_src[nbr] != -1) {
              vec.push_back(nbr);
            }
          }
        }
      }
      std::swap(q2, tmp);
      ++dst_dep;
    }
  }

  while (!q1.empty()) {
    q1.pop();
  }
  if (vec.empty()) {
    return;
  }
  if (src_dep + dst_dep >= params.hop_upper) {
    return;
  }
  StorageReadInterface::vertex_array_t<bool> visited(
      graph.GetVertexSet(params.labels[0].src_label), false);
  for (auto v : vec) {
    q1.push(v);
    visited[v] = true;
  }
  while (!q1.empty()) {
    auto v = q1.front();
    q1.pop();
    auto oes = oview1.get_edges(v);
    for (auto it = oes.begin(); it != oes.end(); ++it) {
      vid_t nbr = it.get_vertex();
      if (visited[nbr]) {
        continue;
      }
      if (dist_from_src[nbr] != -1 &&
          dist_from_src[nbr] + 1 == dist_from_src[v]) {
        q1.push(nbr);
        visited[nbr] = true;
      }
      if (dist_from_dst[nbr] != -1 &&
          dist_from_dst[nbr] + 1 == dist_from_dst[v]) {
        q1.push(nbr);
        visited[nbr] = true;
        dist_from_src[nbr] = dist_from_src[v] + 1;
      }
    }

    auto ies = iview1.get_edges(v);
    for (auto it = ies.begin(); it != ies.end(); ++it) {
      vid_t nbr = it.get_vertex();
      if (visited[nbr]) {
        continue;
      }
      if (dist_from_src[nbr] != -1 &&
          dist_from_src[nbr] + 1 == dist_from_src[v]) {
        q1.push(nbr);
        visited[nbr] = true;
      }
      if (dist_from_dst[nbr] != -1 &&
          dist_from_dst[nbr] + 1 == dist_from_dst[v]) {
        q1.push(nbr);
        visited[nbr] = true;
        dist_from_src[nbr] = dist_from_src[v] + 1;
      }
    }
  }
  std::vector<vid_t> cur_path;
  std::vector<std::pair<Direction, const void*>> cur_edge_data;
  dfs(oview1, iview1, src, dst, visited, dist_from_src, params, paths, cur_path,
      edge_datas, cur_edge_data);
}

neug::result<Context> PathExpand::all_shortest_paths_with_given_source_and_dest(
    const StorageReadInterface& graph, Context&& ctx,
    const ShortestPathParams& params, const std::pair<label_t, vid_t>& dest) {
  auto& input_vertex_list =
      *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
  auto label_sets = input_vertex_list.get_labels_set();
  auto labels = params.labels;
  if (labels.size() != 1 || label_sets.size() != 1) {
    LOG(ERROR) << "only support one label triplet";
    RETURN_UNSUPPORTED_ERROR("only support one label triplet");
  }
  auto label_triplet = labels[0];
  if (label_triplet.src_label != label_triplet.dst_label) {
    LOG(ERROR) << "only support same src and dst label";
    RETURN_UNSUPPORTED_ERROR("only support same src and dst label");
  }
  auto dir = params.dir;
  if (dir != Direction::kBoth) {
    LOG(ERROR) << "only support both direction";
    RETURN_UNSUPPORTED_ERROR("only support both direction");
  }

  if (dest.first != label_triplet.dst_label) {
    LOG(ERROR) << "only support same src and dst label";
    RETURN_UNSUPPORTED_ERROR("only support same src and dst label");
  }
  MSVertexColumnBuilder builder(label_triplet.dst_label);
  PathColumnBuilder path_builder;
  std::vector<size_t> shuffle_offset;
  foreach_vertex(input_vertex_list, [&](size_t index, label_t label, vid_t v) {
    std::vector<std::vector<vid_t>> paths;
    std::vector<std::vector<std::pair<Direction, const void*>>> edge_datas;
    all_shortest_path_with_given_source_and_dest_impl(
        graph, params, v, dest.second, paths, edge_datas);
    for (size_t i = 0; i < paths.size(); ++i) {
      const auto& path = paths[i];
      const auto& cur_edge_datas = edge_datas[i];
      auto ptr = Path(label_triplet.src_label, label_triplet.edge_label, path,
                      cur_edge_datas);
      builder.push_back_opt(dest.second);
      path_builder.push_back_opt(ptr);
      shuffle_offset.push_back(index);
    }
  });
  ctx.set_with_reshuffle(params.v_alias, builder.finish(), shuffle_offset);
  ctx.set(params.alias, path_builder.finish());
  return ctx;
}

struct SSSPSPOp {
  template <typename PRED_T>
  static neug::result<Context> eval_with_predicate(
      const PRED_T& pred, const StorageReadInterface& graph, Context&& ctx,
      const ShortestPathParams& params) {
    return PathExpand::single_source_shortest_path<PRED_T>(
        graph, std::move(ctx), params, pred);
  }
};

neug::result<Context>
PathExpand::single_source_shortest_path_with_special_vertex_predicate(
    const StorageReadInterface& graph, Context&& ctx,
    const ShortestPathParams& params, const SpecialPredicateConfig& config,
    const ParamsMap& query_params) {
  std::set<label_t> expected_labels;
  for (const auto& label_triplet : params.labels) {
    expected_labels.insert(label_triplet.dst_label);
    expected_labels.insert(label_triplet.src_label);
  }
  return dispatch_vertex_predicate<SSSPSPOp>(graph, expected_labels, config,
                                             query_params, graph,
                                             std::move(ctx), params);
}

}  // namespace execution

}  // namespace neug
