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
#include "neug/execution/common/operators/update/path_expand.h"

namespace gs {
namespace runtime {
gs::result<Context> UPathExpand::path_expand_v(
    const GraphUpdateInterface& graph, Context&& ctx,
    const PathExpandParams& params) {
  std::vector<size_t> shuffle_offset;
  if (params.dir == Direction::kOut) {
    auto& input_vertex_list =
        *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.start_tag));
    std::set<label_t> labels;
    std::vector<std::vector<LabelTriplet>> out_labels_map(
        graph.schema().vertex_label_num());
    for (const auto& label : params.labels) {
      labels.emplace(label.dst_label);
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
          auto oe_iter = graph.GetOutEdgeIterator(label_triplet.src_label, v,
                                                  label_triplet.dst_label,
                                                  label_triplet.edge_label, 0);

          while (oe_iter.IsValid()) {
            auto nbr = oe_iter.GetNeighbor();
            output.emplace_back(label_triplet.dst_label, nbr, index);
            oe_iter.Next();
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
        graph.schema().vertex_label_num());
    for (auto& label : params.labels) {
      labels.emplace(label.src_label);
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
          auto oe_iter = graph.GetInEdgeIterator(label_triplet.dst_label, v,
                                                 label_triplet.src_label,
                                                 label_triplet.edge_label, 0);

          while (oe_iter.IsValid()) {
            auto nbr = oe_iter.GetNeighbor();
            output.emplace_back(label_triplet.src_label, nbr, index);
            oe_iter.Next();
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
        graph.schema().vertex_label_num()),
        out_labels_map(graph.schema().vertex_label_num());
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
          auto oe_iter = graph.GetOutEdgeIterator(label_triplet.src_label, v,
                                                  label_triplet.dst_label,
                                                  label_triplet.edge_label, 0);

          while (oe_iter.IsValid()) {
            auto nbr = oe_iter.GetNeighbor();
            output.emplace_back(label_triplet.dst_label, nbr, index);
            oe_iter.Next();
          }
        }
        for (const auto& label_triplet : in_labels_map[label]) {
          auto ie_iter = graph.GetInEdgeIterator(label_triplet.dst_label, v,
                                                 label_triplet.src_label,
                                                 label_triplet.edge_label, 0);
          while (ie_iter.IsValid()) {
            auto nbr = ie_iter.GetNeighbor();
            output.emplace_back(label_triplet.src_label, nbr, index);
            ie_iter.Next();
          }
        }
      }
      depth++;
    }
    ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
    return ctx;
  }
}

}  // namespace runtime
}  // namespace gs
