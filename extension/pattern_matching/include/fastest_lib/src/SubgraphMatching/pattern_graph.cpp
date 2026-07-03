/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

/**
 * This file is originally from the FaSTest project
 * (https://github.com/SNUCSE-CTA/FaSTest) Licensed under the MIT License.
 * Modified by Yunkai Lou and Shunyang Li in 2025 to support Neug-specific
 * features.
 */

#include "SubgraphMatching/pattern_graph.h"
#include <algorithm>

namespace neug {
namespace pattern_matching {
namespace graphlib {
namespace SubgraphMatching {

void PatternGraph::ProcessPattern(
    DataGraphMeta& data_meta,
    std::shared_ptr<std::unordered_map<
        label_t, std::unordered_map<label_t, std::vector<label_t>>>>
        schema_graph) {
  // Construct adj list and label frequency
  // Labels are already consecutive integers starting from 0, no transfer needed
  // For directed graph: update max_out_degree, max_in_degree, and max_degree
  max_degree = 0;
  max_out_degree = 0;
  max_in_degree = 0;
  for (int v = 0; v < GetNumVertices(); v++) {
    // vertex_label[v] is already set correctly, no need to transfer
    // For directed graph: track both out and in degrees
    max_out_degree = std::max(max_out_degree, (int) (out_adj_list[v].size()));
    max_in_degree = std::max(max_in_degree, (int) (in_adj_list[v].size()));
    max_degree = std::max(max_degree, (int) (adj_list[v].size()));
  }
  num_vertex_labels = data_meta.GetNumLabels();
  num_edge_labels = data_meta.GetNumEdgeLabels();
  BuildIncidenceList(true, schema_graph);
  ComputeCoreNum();
  // For directed graph: build separate indices for out-neighbors and
  // in-neighbors out_candidate_neighbors[u][v_idx][out_adj_idx] stores out-edge
  // candidates in_candidate_neighbors[u][v_idx][in_adj_idx] stores in-edge
  // candidates
  out_adj_idx.resize(GetNumVertices(), std::vector<int>(GetNumVertices(), -1));
  in_adj_idx.resize(GetNumVertices(), std::vector<int>(GetNumVertices(), -1));
  for (int u = 0; u < GetNumVertices(); u++) {
    // out_adj_idx: index in out_candidate_neighbors
    for (size_t i = 0; i < out_adj_list[u].size(); i++) {
      out_adj_idx[u][out_adj_list[u][i]] = i;
    }
    // in_adj_idx: index in in_candidate_neighbors (no offset needed)
    for (size_t i = 0; i < in_adj_list[u].size(); i++) {
      in_adj_idx[u][in_adj_list[u][i]] = i;
    }
  }

  // Legacy adj_idx (for backward compatibility, may have issues with
  // bidirectional edges)
  adj_idx.resize(GetNumVertices(), std::vector<int>(GetNumVertices(), -1));
  for (int u = 0; u < GetNumVertices(); u++) {
    for (size_t i = 0; i < adj_list[u].size(); i++) {
      adj_idx[u][adj_list[u][i]] = i;
    }
  }
}

}  // namespace SubgraphMatching
}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug
