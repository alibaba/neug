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

#ifndef INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_UPDATE_EDGE_EXPAND_H_
#define INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_UPDATE_EDGE_EXPAND_H_

#include <utility>
#include <vector>

#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/graph_interface.h"
#include "neug/execution/utils/params.h"
#include "neug/utils/result.h"

namespace gs {
namespace runtime {
class Context;
struct EdgeExpandParams;

class UEdgeExpand {
 public:
  static gs::result<Context> edge_expand_v_without_pred(
      const GraphUpdateInterface& graph, Context&& ctx,
      const EdgeExpandParams& params);

  static gs::result<Context> edge_expand_e_without_pred(
      const GraphUpdateInterface& graph, Context&& ctx,
      const EdgeExpandParams& params);

  template <typename PRED_T>
  static gs::result<Context> edge_expand_e(const GraphUpdateInterface& graph,
                                           Context&& ctx,
                                           const EdgeExpandParams& params,
                                           const PRED_T& pred) {
    if (params.is_optional) {
      LOG(ERROR) << "not support optional edge expand";
      RETURN_UNSUPPORTED_ERROR("not support optional edge expand");
    }
    std::vector<size_t> shuffle_offset;

    if (params.dir == Direction::kBoth) {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
      std::vector<std::pair<LabelTriplet, PropertyType>> label_props;
      std::vector<LabelTriplet> label_triplets;
      for (auto& triplet : params.labels) {
        auto& props = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        PropertyType pt = PropertyType::kEmpty;
        if (!props.empty()) {
          pt = props[0];
        }
        label_props.emplace_back(triplet, pt);
        label_triplets.push_back(triplet);
      }
      BDMLEdgeColumnBuilder builder(label_triplets);

      foreach_vertex(
          input_vertex_list, [&](size_t index, label_t label, vid_t v) {
            for (auto& label_prop : label_props) {
              auto& triplet = label_prop.first;
              if (label == triplet.src_label) {
                auto view = graph.GetGenericOutgoingGraphView(
                    label, triplet.dst_label, triplet.edge_label);
                auto oes = view.get_edges(v);
                for (auto it = oes.begin(); it != oes.end(); ++it) {
                  auto nbr = it.get_vertex();
                  if (pred(label, v, triplet.dst_label, nbr, triplet.edge_label,
                           Direction::kOut, it.get_data_ptr(), index)) {
                    builder.push_back_opt(triplet, v, nbr, it.get_data_ptr(),
                                          Direction::kOut);
                    shuffle_offset.push_back(index);
                  }
                }
              }
              if (label == triplet.dst_label) {
                auto view = graph.GetGenericIncomingGraphView(
                    label, triplet.src_label, triplet.edge_label);
                auto ies = view.get_edges(v);
                for (auto it = ies.begin(); it != ies.end(); ++it) {
                  auto nbr = it.get_vertex();
                  if (pred(label, v, triplet.src_label, nbr, triplet.edge_label,
                           Direction::kIn, it.get_data_ptr(), index)) {
                    builder.push_back_opt(triplet, nbr, v, it.get_data_ptr(),
                                          Direction::kIn);
                    shuffle_offset.push_back(index);
                  }
                }
              }
            }
          });
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      return ctx;
    } else if (params.dir == Direction::kOut) {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
      std::vector<std::pair<LabelTriplet, PropertyType>> label_props;
      std::vector<LabelTriplet> label_triplets;
      for (auto& triplet : params.labels) {
        auto& props = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        PropertyType pt = PropertyType::kEmpty;
        if (!props.empty()) {
          pt = props[0];
        }
        label_props.emplace_back(triplet, pt);
        label_triplets.push_back(triplet);
      }
      SDMLEdgeColumnBuilder builder(Direction::kOut, label_triplets);

      foreach_vertex(
          input_vertex_list, [&](size_t index, label_t label, vid_t v) {
            for (auto& label_prop : label_props) {
              auto& triplet = label_prop.first;
              if (label != triplet.src_label)
                continue;
              auto view = graph.GetGenericOutgoingGraphView(
                  label, triplet.dst_label, triplet.edge_label);
              auto oes = view.get_edges(v);
              for (auto it = oes.begin(); it != oes.end(); ++it) {
                auto nbr = it.get_vertex();
                if (pred(label, v, triplet.dst_label, nbr, triplet.edge_label,
                         Direction::kOut, it.get_data_ptr(), index)) {
                  builder.push_back_opt(triplet, v, nbr, it.get_data_ptr());
                  shuffle_offset.push_back(index);
                }
              }
            }
          });
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      return ctx;
    } else {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
      std::vector<std::pair<LabelTriplet, PropertyType>> label_props;
      std::vector<LabelTriplet> label_triplets;
      for (auto& triplet : params.labels) {
        auto& props = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        PropertyType pt = PropertyType::kEmpty;
        if (!props.empty()) {
          pt = props[0];
        }
        label_props.emplace_back(triplet, pt);
        label_triplets.push_back(triplet);
      }
      SDMLEdgeColumnBuilder builder(Direction::kIn, label_triplets);

      foreach_vertex(
          input_vertex_list, [&](size_t index, label_t label, vid_t v) {
            for (auto& label_prop : label_props) {
              auto& triplet = label_prop.first;
              if (label != triplet.dst_label)
                continue;
              auto view = graph.GetGenericIncomingGraphView(
                  label, triplet.src_label, triplet.edge_label);
              auto ies = view.get_edges(v);
              for (auto it = ies.begin(); it != ies.end(); ++it) {
                auto nbr = it.get_vertex();
                if (pred(label, v, triplet.src_label, nbr, triplet.edge_label,
                         Direction::kIn, it.get_data_ptr(), index)) {
                  builder.push_back_opt(triplet, nbr, v, it.get_data_ptr());
                  shuffle_offset.push_back(index);
                }
              }
            }
          });
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      return ctx;
    }
  }
};
}  // namespace runtime
}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_COMMON_OPERATORS_UPDATE_EDGE_EXPAND_H_
