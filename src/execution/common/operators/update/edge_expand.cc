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
#include "neug/execution/common/operators/update/edge_expand.h"

#include <glog/logging.h>
#include <stddef.h>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/types.h"
#include "neug/execution/utils/params.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/update_transaction.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace gs {
namespace runtime {
gs::result<Context> UEdgeExpand::edge_expand_v_without_pred(
    const GraphUpdateInterface& graph, Context&& ctx,
    const EdgeExpandParams& params) {
  const auto& input_vertex_list =
      dynamic_cast<const IVertexColumn&>(*ctx.get(params.v_tag).get());
  std::vector<size_t> shuffle_offset;
  const std::set<label_t> v_labels = input_vertex_list.get_labels_set();
  std::set<label_t> nbr_labels_set;
  for (const auto& triplet : params.labels) {
    if (params.dir == Direction::kIn || params.dir == Direction::kBoth) {
      if (v_labels.find(triplet.dst_label) != v_labels.end()) {
        nbr_labels_set.insert(triplet.src_label);
      }
    }
    if (params.dir == Direction::kOut || params.dir == Direction::kBoth) {
      if (v_labels.find(triplet.src_label) != v_labels.end()) {
        nbr_labels_set.insert(triplet.dst_label);
      }
    }
  }
  MLVertexColumnBuilderOpt builder(nbr_labels_set);
  if (params.dir == Direction::kIn || params.dir == Direction::kBoth) {
    foreach_vertex(
        input_vertex_list, [&](size_t index, label_t label, vid_t v) {
          for (const auto& triplet : params.labels) {
            if (label == triplet.dst_label) {
              auto ie_iter = graph.GetInEdgeIterator(
                  label, v, triplet.src_label, triplet.edge_label, 0);
              for (; ie_iter.IsValid(); ie_iter.Next()) {
                builder.push_back_vertex(VertexRecord{
                    ie_iter.GetNeighborLabel(), ie_iter.GetNeighbor()});
                shuffle_offset.push_back(index);
              }
            }
          }
        });
  }

  if (params.dir == Direction::kOut || params.dir == Direction::kBoth) {
    foreach_vertex(
        input_vertex_list, [&](size_t index, label_t label, vid_t v) {
          for (const auto& triplet : params.labels) {
            if (label == triplet.src_label) {
              auto oe_iter = graph.GetOutEdgeIterator(
                  label, v, triplet.dst_label, triplet.edge_label, 0);
              for (; oe_iter.IsValid(); oe_iter.Next()) {
                builder.push_back_vertex(VertexRecord{
                    oe_iter.GetNeighborLabel(), oe_iter.GetNeighbor()});
                shuffle_offset.push_back(index);
              }
            }
          }
        });
  }
  ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
  return ctx;
}

gs::result<Context> UEdgeExpand::edge_expand_e_without_pred(
    const GraphUpdateInterface& graph, Context&& ctx,
    const EdgeExpandParams& params) {
  const auto& input_vertex_list =
      dynamic_cast<const IVertexColumn&>(*ctx.get(params.v_tag).get());
  std::vector<size_t> shuffle_offset;
  const auto& v_labels = input_vertex_list.get_labels_set();
  std::vector<LabelTriplet> labels;
  for (auto& label : params.labels) {
    if (params.dir == Direction::kIn || params.dir == Direction::kBoth) {
      if (v_labels.find(label.dst_label) != v_labels.end()) {
        labels.push_back(label);
      }
    }
    if (params.dir == Direction::kOut || params.dir == Direction::kBoth) {
      if (v_labels.find(label.src_label) != v_labels.end()) {
        labels.push_back(label);
      }
    }
  }
  BDMLEdgeColumnBuilder builder(labels);

  if (params.dir == Direction::kBoth) {
    foreach_vertex(
        input_vertex_list, [&](size_t index, label_t label, vid_t v) {
          for (const auto& triplet : params.labels) {
            int label_idx = builder.get_label_index(triplet);
            if (triplet.src_label == label) {
              auto oe_view = graph.GetGenericOutgoingGraphView(
                  triplet.src_label, triplet.dst_label, triplet.edge_label);
              auto es = oe_view.get_edges(v);
              for (auto it = es.begin(); it != es.end(); ++it) {
                builder.push_back_opt(label_idx, v, it.get_vertex(),
                                      it.get_data_ptr(), Direction::kOut);
                shuffle_offset.push_back(index);
              }
            }
            if (triplet.dst_label == label) {
              auto ie_view = graph.GetGenericIncomingGraphView(
                  triplet.dst_label, triplet.src_label, triplet.edge_label);
              auto es = ie_view.get_edges(v);
              for (auto it = es.begin(); it != es.end(); ++it) {
                builder.push_back_opt(label_idx, it.get_vertex(), v,
                                      it.get_data_ptr(), Direction::kIn);
                shuffle_offset.push_back(index);
              }
            }
          }
        });
    ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
    return ctx;
  } else if (params.dir == Direction::kIn) {
    foreach_vertex(
        input_vertex_list, [&](size_t index, label_t label, vid_t v) {
          for (const auto& triplet : params.labels) {
            int label_idx = builder.get_label_index(triplet);
            auto ie_view = graph.GetGenericIncomingGraphView(
                triplet.dst_label, triplet.src_label, triplet.edge_label);
            auto es = ie_view.get_edges(v);
            for (auto it = es.begin(); it != es.end(); ++it) {
              builder.push_back_opt(label_idx, it.get_vertex(), v,
                                    it.get_data_ptr(), Direction::kIn);
              shuffle_offset.push_back(index);
            }
          }
        });
    ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
    return ctx;
  } else if (params.dir == Direction::kOut) {
    foreach_vertex(
        input_vertex_list, [&](size_t index, label_t label, vid_t v) {
          for (const auto& triplet : params.labels) {
            if (triplet.src_label == label) {
              int label_idx = builder.get_label_index(triplet);
              auto ie_view = graph.GetGenericOutgoingGraphView(
                  triplet.src_label, triplet.dst_label, triplet.edge_label);
              auto es = ie_view.get_edges(v);
              for (auto it = es.begin(); it != es.end(); ++it) {
                builder.push_back_opt(label_idx, v, it.get_vertex(),
                                      it.get_data_ptr(), Direction::kOut);
                shuffle_offset.push_back(index);
              }
            }
          }
        });
    ctx.set_with_reshuffle(params.alias, builder.finish(), shuffle_offset);
    return ctx;
  }
  LOG(ERROR) << "should not reach here: " << static_cast<int>(params.dir);
  RETURN_UNSUPPORTED_ERROR("should not reach here " +
                           std::to_string(static_cast<int>(params.dir)));
}

}  // namespace runtime
}  // namespace gs
