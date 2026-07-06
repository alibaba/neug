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

#pragma once
/**
 * @brief Class for data graph in pattern matching related problems
 * @type Supports directed, connected, labeled graphs with vertex/edge
 * properties
 */
#include <algorithm>
#include <fstream>
#include <unordered_map>
#include "DataStructure/graph.h"

namespace neug {
namespace pattern_matching {
namespace graphlib {
namespace SubgraphMatching {
struct LabelStatistics {
  std::vector<double> vertex_label_probability, edge_label_probability;
  double vertex_label_entropy = 0.0, edge_label_entropy = 0.0;
};
class DataGraph : public neug::pattern_matching::graphlib::Graph {
 protected:
  // array of vertices, grouped by label, ordered by decreasing order of degree
  std::vector<std::vector<int>> vertex_by_labels;
  // num_vertex_by_label_degree;
  std::unordered_map<int, int> transferred_label_map;
  LabelStatistics label_statistics;

 public:
  DataGraph(const Graph& g) : Graph(g){};
  DataGraph(){};
  std::vector<int>& GetVerticesByLabel(int label);
  inline int GetTransferredLabel(int l) { return transferred_label_map[l]; }
  void Preprocess();
  void TransformLabel();
  void ComputeLabelStatistics();
  bool FourCycleEnumerated() { return !local_four_cycles.empty(); }
};

}  // namespace SubgraphMatching
}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug
