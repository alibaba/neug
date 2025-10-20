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

#include "neug/execution/common/operators/retrieve/path_expand_impl.h"

#include "neug/storages/csr/mutable_csr.h"
#include "neug/utils/property/types.h"

namespace gs {

namespace runtime {
class IContextColumn;

std::pair<std::shared_ptr<IContextColumn>, std::vector<size_t>>
path_expand_vertex_without_predicate_impl(
    const GraphReadInterface& graph, const SLVertexColumn& input,
    const std::vector<LabelTriplet>& labels, Direction dir, int lower,
    int upper) {
  if (labels.size() == 1) {
    if (labels[0].src_label == labels[0].dst_label &&
        labels[0].src_label == input.label()) {
      if (dir == Direction::kBoth) {
        auto iview = graph.GetGenericIncomingGraphView(
            labels[0].src_label, labels[0].dst_label, labels[0].edge_label);
        auto oview = graph.GetGenericOutgoingGraphView(
            labels[0].src_label, labels[0].dst_label, labels[0].edge_label);
        return iterative_expand_vertex_on_dual_graph_view(iview, oview, input,
                                                          lower, upper);
      } else if (dir == Direction::kIn) {
        auto iview = graph.GetGenericIncomingGraphView(
            labels[0].src_label, labels[0].dst_label, labels[0].edge_label);
        return iterative_expand_vertex_on_graph_view(iview, input, lower,
                                                     upper);
      } else {
        CHECK(dir == Direction::kOut);
        auto oview = graph.GetGenericOutgoingGraphView(
            labels[0].src_label, labels[0].dst_label, labels[0].edge_label);
        return iterative_expand_vertex_on_graph_view(oview, input, lower,
                                                     upper);
      }
    }
  }
  LOG(FATAL) << "not implemented...";
  std::shared_ptr<IContextColumn> ret(nullptr);
  return std::make_pair(ret, std::vector<size_t>());
}

}  // namespace runtime

}  // namespace gs
