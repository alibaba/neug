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

#include "SubgraphMatching/data_graph.h"
#include <algorithm>

namespace neug {
namespace pattern_matching {
namespace graphlib {
namespace SubgraphMatching {

std::vector<int>& DataGraph::GetVerticesByLabel(int label) {
  return vertex_by_labels[label];
}

void DataGraph::TransformLabel() {
  int cur_transferred_label = 0;
  for (int v = 0; v < GetNumVertices(); v++) {
    int l = vertex_label[v];
    if (transferred_label_map.find(l) == transferred_label_map.end()) {
      transferred_label_map[l] = cur_transferred_label;
      cur_transferred_label += 1;
    }
    vertex_label[v] = transferred_label_map[l];
    num_vertex_labels = std::max(num_vertex_labels, vertex_label[v] + 1);
  }

  for (int e = 0; e < GetNumEdges(); e++) {
    num_edge_labels = std::max(num_edge_labels, edge_label[e] + 1);
  }
}

void DataGraph::ComputeLabelStatistics() {
  label_statistics.vertex_label_probability.resize(GetNumLabels(), 1e-4);
  for (int i = 0; i < GetNumVertices(); i++) {
    label_statistics.vertex_label_probability[GetVertexLabel(i)] += 1.0;
  }
  for (int i = 0; i < GetNumLabels(); i++) {
    label_statistics.vertex_label_probability[i] /= (1.0 * GetNumVertices());
  }
  for (auto x : label_statistics.vertex_label_probability) {
    label_statistics.vertex_label_entropy -= x * log2(x);
  }
}

void DataGraph::Preprocess() {
  // Compute max degrees for directed graph support
  for (int i = 0; i < GetNumVertices(); i++) {
    max_degree = std::max(max_degree, (int) adj_list[i].size());
    max_out_degree = std::max(max_out_degree, (int) out_adj_list[i].size());
    max_in_degree = std::max(max_in_degree, (int) in_adj_list[i].size());
  }
  TransformLabel();
  BuildIncidenceList();
  ComputeCoreNum();
  ComputeLabelStatistics();
  vertex_by_labels.resize(GetNumLabels());
  for (int i = 0; i < GetNumVertices(); i++) {
    vertex_by_labels[GetVertexLabel(i)].push_back(i);
  }
  for (int i = 0; i < GetNumLabels(); i++) {
    if (vertex_by_labels[i].empty())
      continue;
    std::stable_sort(
        vertex_by_labels[i].begin(), vertex_by_labels[i].end(),
        [this](int a, int b) { return GetDegree(a) > GetDegree(b); });
  }
}

}  // namespace SubgraphMatching
}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug
