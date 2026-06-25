/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "neug/common/extra_type_info.h"
#include "neug/common/types.h"
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/common/constants.h"
#include "neug/compiler/function/gds/gds_algo_function.h"
#include "neug/compiler/function/table/table_function.h"
#include "neug/execution/common/columns/path_columns.h"
#include "neug/execution/common/operators/retrieve/sink.h"
#include "neug/execution/common/types/graph_types.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace gds {

// Build a Path object from a predecessor chain, looking up real edge data
// pointers from the CSR graph view.  The caller provides the vertex chain in
// source-to-target order.
inline execution::Path build_path_from_chain(
    const std::vector<vid_t>& chain, label_t vertex_label, label_t edge_label,
    bool directed, const StorageReadInterface& graph) {
  if (chain.size() <= 1) {
    return execution::Path(vertex_label, chain[0]);
  }

  auto oe_view =
      graph.GetGenericOutgoingGraphView(vertex_label, vertex_label, edge_label);
  auto ie_view =
      graph.GetGenericIncomingGraphView(vertex_label, vertex_label, edge_label);

  std::vector<std::pair<execution::Direction, const void*>> edge_datas;
  edge_datas.reserve(chain.size() - 1);

  for (size_t i = 0; i + 1 < chain.size(); ++i) {
    vid_t from = chain[i];
    vid_t to = chain[i + 1];
    const void* prop = nullptr;
    execution::Direction dir = execution::Direction::kOut;

    // Try outgoing edges first
    auto oe_edges = oe_view.get_edges(from);
    for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
      if (*it == to) {
        prop = it.get_data_ptr();
        break;
      }
    }

    // For undirected graphs, try incoming edges if not found in outgoing
    if (prop == nullptr && !directed) {
      auto ie_edges = ie_view.get_edges(from);
      for (auto it = ie_edges.begin(); it != ie_edges.end(); ++it) {
        if (*it == to) {
          prop = it.get_data_ptr();
          dir = execution::Direction::kIn;
          break;
        }
      }
    }

    edge_datas.push_back({dir, prop});
  }

  return execution::Path(vertex_label, edge_label, chain, edge_datas);
}

// Reconstruct a path by walking backward from `target` to `source` using
// a callable that finds the predecessor of a given vertex.  The callable
// is invoked as `find_pred(vid_t v)` and must return the predecessor's
// vertex ID.  This enables post-hoc path reconstruction from the distance
// array without storing predecessors during computation.
template <typename PredFinder>
inline execution::Path reconstruct_path(vid_t target, vid_t source,
                                        const PredFinder& find_pred,
                                        label_t vertex_label,
                                        label_t edge_label, bool directed,
                                        const StorageReadInterface& graph) {
  std::vector<vid_t> chain;
  vid_t cur = target;
  while (cur != source) {
    chain.push_back(cur);
    cur = find_pred(cur);
  }
  chain.push_back(source);
  std::reverse(chain.begin(), chain.end());
  return build_path_from_chain(chain, vertex_label, edge_label, directed,
                               graph);
}

}  // namespace gds
}  // namespace neug
