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
 * @brief Class for subgraph pattern
 */
#include "DataStructure/graph.h"
#include "pattern_matching_data_graph_meta.h"

namespace neug {
namespace pattern_matching {
namespace graphlib {
namespace SubgraphMatching {
class PatternGraph : public Graph {
 public:
  PatternGraph(){};
  PatternGraph(const Graph& g) : Graph(g){};
  ~PatternGraph(){};

  PatternGraph& operator=(const PatternGraph&) = delete;
  PatternGraph(const PatternGraph&) = delete;

  void ProcessPattern(
      DataGraphMeta& data_meta,
      std::shared_ptr<std::unordered_map<
          label_t, std::unordered_map<label_t, std::vector<label_t>>>>
          schema_graph = nullptr);

  // For directed graph: separate indices for out-neighbors and in-neighbors
  std::vector<std::vector<int>>
      out_adj_idx;  // out_adj_idx[u][uc] = index of uc in out_adj_list[u]
  std::vector<std::vector<int>>
      in_adj_idx;  // in_adj_idx[u][uc] = index of uc in in_adj_list[u]

  // GetOutAdjIdx: for query edge u -> uc, get index in out_candidate_neighbors
  inline int GetOutAdjIdx(int u, int uc) const { return out_adj_idx[u][uc]; }
  // GetInAdjIdx: for query edge uc -> u, get index in in_candidate_neighbors
  inline int GetInAdjIdx(int u, int uc) const { return in_adj_idx[u][uc]; }

  // Legacy interface (deprecated, use GetOutAdjIdx/GetInAdjIdx instead)
  std::vector<std::vector<int>> adj_idx;
  inline int GetAdjIdx(int u, int uc) const { return adj_idx[u][uc]; }

  std::vector<std::vector<PropCons>> vertex_property_constraints;
  std::vector<std::vector<PropCons>> edge_property_constraints;
};

}  // namespace SubgraphMatching
}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug
