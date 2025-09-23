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

#ifndef EXECUTION_COMMON_OPERATORS_UPDATE_EDGE_H_
#define EXECUTION_COMMON_OPERATORS_UPDATE_EDGE_H_

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
      for (auto& triplet : params.labels) {
        auto& props = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        PropertyType pt = PropertyType::kEmpty;
        if (!props.empty()) {
          pt = props[0];
        }
        label_props.emplace_back(triplet, pt);
      }
      auto builder = BDMLEdgeColumnBuilder::builder(label_props);

      foreach_vertex(
          input_vertex_list, [&](size_t index, label_t label, vid_t v) {
            for (auto& label_prop : label_props) {
              auto& triplet = label_prop.first;
              if (label == triplet.src_label) {
                auto oe_iter = graph.GetOutEdgeIterator(
                    label, v, triplet.dst_label, triplet.edge_label);
                while (oe_iter.IsValid()) {
                  auto nbr = oe_iter.GetNeighbor();
                  if (pred(triplet, v, nbr, oe_iter.GetData(), Direction::kOut,
                           index)) {
                    builder.push_back_opt(triplet, v, nbr, oe_iter.GetData(),
                                          Direction::kOut);
                    shuffle_offset.push_back(index);
                  }
                  oe_iter.Next();
                }
              }
              if (label == triplet.dst_label) {
                auto ie_iter = graph.GetInEdgeIterator(
                    label, v, triplet.src_label, triplet.edge_label);
                while (ie_iter.IsValid()) {
                  auto nbr = ie_iter.GetNeighbor();
                  if (pred(triplet, nbr, v, ie_iter.GetData(), Direction::kIn,
                           index)) {
                    builder.push_back_opt(triplet, nbr, v, ie_iter.GetData(),
                                          Direction::kIn);
                    shuffle_offset.push_back(index);
                  }
                  ie_iter.Next();
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
      for (auto& triplet : params.labels) {
        auto& props = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        PropertyType pt = PropertyType::kEmpty;
        if (!props.empty()) {
          pt = props[0];
        }
        label_props.emplace_back(triplet, pt);
      }
      auto builder =
          SDMLEdgeColumnBuilder::builder(Direction::kOut, label_props);

      foreach_vertex(
          input_vertex_list, [&](size_t index, label_t label, vid_t v) {
            for (auto& label_prop : label_props) {
              auto& triplet = label_prop.first;
              if (label != triplet.src_label)
                continue;
              auto oe_iter = graph.GetOutEdgeIterator(
                  label, v, triplet.dst_label, triplet.edge_label);
              while (oe_iter.IsValid()) {
                auto nbr = oe_iter.GetNeighbor();
                if (pred(triplet, v, nbr, oe_iter.GetData(), Direction::kOut,
                         index)) {
                  // assert(oe_iter.GetData().type == label_prop.second);
                  builder.push_back_opt(triplet, v, nbr, oe_iter.GetData());
                  shuffle_offset.push_back(index);
                }
                oe_iter.Next();
              }
            }
          });
      ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
      return ctx;
    } else {
      auto& input_vertex_list =
          *std::dynamic_pointer_cast<IVertexColumn>(ctx.get(params.v_tag));
      std::vector<std::pair<LabelTriplet, PropertyType>> label_props;
      for (auto& triplet : params.labels) {
        auto& props = graph.schema().get_edge_properties(
            triplet.src_label, triplet.dst_label, triplet.edge_label);
        PropertyType pt = PropertyType::kEmpty;
        if (!props.empty()) {
          pt = props[0];
        }
        label_props.emplace_back(triplet, pt);
      }
      auto builder =
          SDMLEdgeColumnBuilder::builder(Direction::kIn, label_props);

      foreach_vertex(
          input_vertex_list, [&](size_t index, label_t label, vid_t v) {
            for (auto& label_prop : label_props) {
              auto& triplet = label_prop.first;
              if (label != triplet.dst_label)
                continue;
              auto ie_iter = graph.GetInEdgeIterator(
                  label, v, triplet.src_label, triplet.edge_label);
              while (ie_iter.IsValid()) {
                auto nbr = ie_iter.GetNeighbor();
                if (pred(triplet, nbr, v, ie_iter.GetData(), Direction::kIn,
                         index)) {
                  builder.push_back_opt(triplet, nbr, v, ie_iter.GetData());
                  shuffle_offset.push_back(index);
                }
                ie_iter.Next();
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
#endif  // EXECUTION_COMMON_OPERATORS_UPDATE_EDGE_H_