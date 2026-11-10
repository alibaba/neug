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

#include "neug/common/columns/path_columns.h"
#include "neug/execution/common/operators/retrieve/path_expand_impl.h"
#include "neug/execution/expression/special_predicates.h"

namespace neug {

namespace execution {

bool PathExpand::same_relationship(const StorageReadInterface& graph,
                                   const EdgeRecord& left,
                                   const EdgeRecord& right) {
  if (!(left == right)) {
    return false;
  }
  if (left.prop == right.prop) {
    return true;
  }
  auto outgoing = graph.GetGenericOutgoingGraphView(
      left.label.src_label, left.label.dst_label, left.label.edge_label);
  auto incoming = graph.GetGenericIncomingGraphView(
      left.label.dst_label, left.label.src_label, left.label.edge_label);
  const auto schema = graph.schema().get_edge_schema(
      left.label.src_label, left.label.dst_label, left.label.edge_label);
  auto same_payload = [&](const void* a, const void* b) {
    if (!schema->is_bundled()) {
      // Both CSR directions store the same stable property-row identity.
      return *static_cast<const uint64_t*>(a) ==
             *static_cast<const uint64_t*>(b);
    }
    if (schema->properties.empty() ||
        schema->properties[0].id() == DataTypeId::kEmpty) {
      return true;
    }
    auto accessor = graph.GetEdgeDataAccessor(
        left.label.src_label, left.label.dst_label, left.label.edge_label, 0);
    return accessor.get_data_from_ptr(a) == accessor.get_data_from_ptr(b);
  };
  if (!same_payload(left.prop, right.prop)) {
    return false;
  }
  // Bundled edges have separate incoming/outgoing storage. Pair equal visible
  // entries by occurrence within their endpoint/payload/timestamp group rather
  // than collapsing parallel edges (including identical or propertyless ones).
  auto occurrence = [&](const EdgeRecord& edge) {
    // Named-path construction can reverse traversal orientation without
    // relocating the payload. Determine the owning CSR from the pointer.
    auto edges = outgoing.get_edges(edge.src);
    bool owns_outgoing = false;
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      if (it.get_data_ptr() == edge.prop) {
        owns_outgoing = true;
        break;
      }
    }
    if (!owns_outgoing) {
      edges = incoming.get_edges(edge.dst);
    }
    auto timestamp = [&](const void* data) -> timestamp_t {
      return edges.cfg.ts_offset == 0
                 ? 0
                 : *reinterpret_cast<const timestamp_t*>(
                       static_cast<const char*>(data) - edges.cfg.data_offset +
                       edges.cfg.ts_offset);
    };
    const auto ts = timestamp(edge.prop);
    size_t ordinal = 0;
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      if (it.get_data_ptr() == edge.prop) {
        return std::make_pair(ts, ordinal);
      }
      if (it.get_vertex() == (owns_outgoing ? edge.dst : edge.src) &&
          timestamp(it.get_data_ptr()) == ts &&
          same_payload(it.get_data_ptr(), edge.prop)) {
        ++ordinal;
      }
    }
    THROW_INTERNAL_EXCEPTION("Path relationship is absent from its CSR view");
  };
  return occurrence(left) == occurrence(right);
}

neug::result<ContextChunk> PathExpand::edge_expand_v(
    const StorageReadInterface& graph, ContextChunk&& chunk,
    const PathExpandParams& params) {
  sel_vec_t shuffle_offset;

  if (params.labels.size() == 1 &&
      params.labels[0].src_label == params.labels[0].dst_label &&
      chunk.get(params.start_tag)->column_type() ==
          ContextColumnType::kVertex) {
    auto vertex_col =
        dynamic_cast<const IVertexColumn*>(chunk.get(params.start_tag).get());
    if (vertex_col->vertex_column_type() == VertexColumnType::kSingle) {
      const auto& input_vertex_list =
          dynamic_cast<const SLVertexColumn&>(*chunk.get(params.start_tag));
      if (input_vertex_list.label() == params.labels[0].src_label) {
        auto pair = path_expand_vertex_without_predicate_impl(
            graph, input_vertex_list, params.labels, params.dir,
            params.hop_lower, params.hop_upper);
        chunk.set_with_reshuffle(params.alias, pair.first, pair.second);
        return chunk;
      }
    }
  }

  if (params.dir == Direction::kOut) {
    auto& input_vertex_list =
        *std::dynamic_pointer_cast<IVertexColumn>(chunk.get(params.start_tag));
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
    vector_t<std::tuple<label_t, vid_t, size_t>> input;
    vector_t<std::tuple<label_t, vid_t, size_t>> output;
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
    chunk.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
    return chunk;
  } else if (params.dir == Direction::kIn) {
    auto& input_vertex_list =
        *std::dynamic_pointer_cast<IVertexColumn>(chunk.get(params.start_tag));
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
    vector_t<std::tuple<label_t, vid_t, size_t>> input;
    vector_t<std::tuple<label_t, vid_t, size_t>> output;
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
    chunk.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
    return chunk;
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
    vector_t<std::tuple<label_t, vid_t, size_t>> input;
    vector_t<std::tuple<label_t, vid_t, size_t>> output;
    auto input_vertex_list =
        std::dynamic_pointer_cast<IVertexColumn>(chunk.get(params.start_tag));
    if (input_vertex_list->vertex_column_type() ==
        VertexColumnType::kMultiple) {
      auto& input_vertex_list = *std::dynamic_pointer_cast<MLVertexColumn>(
          chunk.get(params.start_tag));

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
    chunk.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
    return chunk;
  }

  LOG(ERROR) << "not support path expand options";
  RETURN_UNSUPPORTED_ERROR("not support path expand options");
}

neug::result<ContextChunk> path_expand_p_arbitrary(
    const StorageReadInterface& graph, ContextChunk&& chunk,
    const PathExpandParams& params) {
  sel_vec_t shuffle_offset;
  auto& input_vertex_list =
      *std::dynamic_pointer_cast<IVertexColumn>(chunk.get(params.start_tag));
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
  vector_t<std::pair<Path, size_t>> input;
  vector_t<std::pair<Path, size_t>> output;

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
    chunk.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);

    return chunk;
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
    chunk.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);

    return chunk;

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
    chunk.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
    return chunk;
  }
  LOG(ERROR) << "not support path expand options";
  RETURN_UNSUPPORTED_ERROR("not support path expand options");
}

neug::result<ContextChunk> path_expand_p_simple(
    const StorageReadInterface& graph, ContextChunk&& chunk,
    const PathExpandParams& params) {
  sel_vec_t shuffle_offset;
  auto& input_vertex_list =
      *std::dynamic_pointer_cast<IVertexColumn>(chunk.get(params.start_tag));
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
  chunk.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
  return chunk;
}

neug::result<ContextChunk> path_expand_p_trail(
    const StorageReadInterface& graph, ContextChunk&& chunk,
    const PathExpandParams& params) {
  sel_vec_t shuffle_offset;
  auto& input_vertex_list =
      *std::dynamic_pointer_cast<IVertexColumn>(chunk.get(params.start_tag));
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
  chunk.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
  return chunk;
}

neug::result<ContextChunk> PathExpand::path_expand_p_shortest(
    const StorageReadInterface& graph, ContextChunk&& chunk,
    const PathExpandParams& params, const HopPredicate& pred) {
  auto& vertices =
      *std::dynamic_pointer_cast<IVertexColumn>(chunk.get(params.start_tag));
  using Predecessor = std::tuple<VertexRecord, label_t, Direction, const void*>;
  std::vector<std::vector<std::pair<LabelTriplet, Direction>>> adjacency(
      graph.schema().vertex_label_frontier());
  for (const auto& triplet : params.labels) {
    if (params.dir != Direction::kIn) {
      adjacency[triplet.src_label].emplace_back(triplet, Direction::kOut);
    }
    if (params.dir != Direction::kOut) {
      adjacency[triplet.dst_label].emplace_back(triplet, Direction::kIn);
    }
  }
  const bool all = params.opt == PathOpt::kAllShortest;
  PathColumnBuilder builder;
  sel_vec_t offsets;
  foreach_vertex(vertices, [&](size_t index, label_t label, vid_t vertex) {
    const VertexRecord root{label, vertex};
    flat_hash_map<VertexRecord, int> distances;
    flat_hash_map<VertexRecord, std::vector<Predecessor>> predecessors;
    distances.emplace(root, 0);
    vector_t<VertexRecord> frontier{root};
    for (int depth = 0; depth + 1 < params.hop_upper && !frontier.empty();
         ++depth) {
      vector_t<VertexRecord> next;
      for (const auto& node : frontier) {
        for (const auto& [triplet, direction] : adjacency[node.label_]) {
          const auto neighbor_label = direction == Direction::kOut
                                          ? triplet.dst_label
                                          : triplet.src_label;
          auto view =
              direction == Direction::kOut
                  ? graph.GetGenericOutgoingGraphView(
                        node.label_, neighbor_label, triplet.edge_label)
                  : graph.GetGenericIncomingGraphView(
                        node.label_, neighbor_label, triplet.edge_label);
          auto edges = view.get_edges(node.vid_);
          for (auto it = edges.begin(); it != edges.end(); ++it) {
            const VertexRecord neighbor{neighbor_label, it.get_vertex()};
            // Reject a hop before recording reachability. Endpoint predicates
            // remain in the following GetV and do not prune intermediate nodes.
            if (pred &&
                !pred(node.label_, node.vid_, neighbor_label, neighbor.vid_,
                      triplet.edge_label, direction, it.get_data_ptr())) {
              continue;
            }
            auto [distance, inserted] = distances.emplace(neighbor, depth + 1);
            if (inserted) {
              next.push_back(neighbor);
            }
            if (inserted || (all && distance->second == depth + 1)) {
              // Keep each relationship, including parallel edges, in the DAG.
              predecessors[neighbor].emplace_back(node, triplet.edge_label,
                                                  direction, it.get_data_ptr());
            }
          }
        }
      }
      frontier.swap(next);
    }
    std::vector<VertexRecord> nodes;
    std::vector<std::tuple<label_t, Direction, const void*>> edges;
    std::function<void(const VertexRecord&)> emit =
        [&](const VertexRecord& node) {
          nodes.push_back(node);
          if (node == root) {
            auto ordered_nodes = nodes;
            auto ordered_edges = edges;
            std::reverse(ordered_nodes.begin(), ordered_nodes.end());
            std::reverse(ordered_edges.begin(), ordered_edges.end());
            builder.push_back_opt(Path(ordered_edges, ordered_nodes));
            offsets.push_back(index);
          } else {
            for (const auto& [previous, edge_label, direction, data] :
                 predecessors.at(node)) {
              edges.emplace_back(edge_label, direction, data);
              emit(previous);
              edges.pop_back();
            }
          }
          nodes.pop_back();
        };
    for (const auto& [node, distance] : distances) {
      if (distance >= params.hop_lower && distance < params.hop_upper) {
        emit(node);
      }
    }
  });
  chunk.set_with_reshuffle(params.alias, builder.finish(), offsets);
  return chunk;
}

neug::result<ContextChunk> PathExpand::edge_expand_p(
    const StorageReadInterface& graph, ContextChunk&& chunk,
    const PathExpandParams& params) {
  if (params.opt == PathOpt::kArbitrary) {
    return path_expand_p_arbitrary(graph, std::move(chunk), params);
  } else if (params.opt == PathOpt::kAnyShortest ||
             params.opt == PathOpt::kAllShortest) {
    return path_expand_p_shortest(graph, std::move(chunk), params);
  } else if (params.opt == PathOpt::kTrail) {
    return path_expand_p_trail(graph, std::move(chunk), params);
  } else if (params.opt == PathOpt::kSimple) {
    return path_expand_p_simple(graph, std::move(chunk), params);
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

neug::result<ContextChunk> PathExpand::single_source_single_dest_shortest_path(
    const StorageReadInterface& graph, ContextChunk&& chunk,
    const ShortestPathParams& params, std::pair<label_t, vid_t>& dest) {
  sel_vec_t shuffle_offset;
  auto& input_vertex_list =
      *std::dynamic_pointer_cast<IVertexColumn>(chunk.get(params.start_tag));
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

  chunk.set_with_reshuffle(params.v_alias, builder.finish(), shuffle_offset);
  chunk.set(params.alias, path_builder.finish());
  return chunk;
}

struct SSSPSPOp {
  template <typename PRED_T>
  static neug::result<ContextChunk> eval_with_predicate(
      const PRED_T& pred, const StorageReadInterface& graph,
      ContextChunk&& chunk, const ShortestPathParams& params) {
    return PathExpand::single_source_shortest_path<PRED_T>(
        graph, std::move(chunk), params, pred);
  }
};

neug::result<ContextChunk>
PathExpand::single_source_shortest_path_with_special_vertex_predicate(
    const StorageReadInterface& graph, ContextChunk&& chunk,
    const ShortestPathParams& params, const SpecialPredicateConfig& config,
    const ParamsMap& query_params) {
  std::set<label_t> expected_labels;
  for (const auto& label_triplet : params.labels) {
    expected_labels.insert(label_triplet.dst_label);
    expected_labels.insert(label_triplet.src_label);
  }
  return dispatch_vertex_predicate<SSSPSPOp>(graph, expected_labels, config,
                                             query_params, graph,
                                             std::move(chunk), params);
}

}  // namespace execution

}  // namespace neug
