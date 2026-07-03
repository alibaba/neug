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
#include "base.h"

namespace neug {
namespace pattern_matching {
namespace graphlib {

struct UnionFind {
  std::vector<int> par, sz;

  UnionFind(int n = 0);

  void init();

  int find(int x) { return x == par[x] ? x : (par[x] = find(par[x])); }

  bool unite(int x, int y);
};

struct BipartiteMaximumMatching {
  int *left, *right;
  int left_len, right_len, arr_len;
  bool* used;
  int **adj, *adj_size;
  int** adj_index;

  bool** matchable;
  bool* bfs_visited;
  int *right_order, *inverse_right_order;
  int **lower_graph, *lower_graph_size;
  int **upper_graph, *upper_graph_size;

  int *Q, *S;
  int qright = 0, qleft = 0;
  int stkright = 0;
  int *dfsn, *scch, *scc_idx, ord, found_scc;

  /* SCC */
  int FindSCC(int v);

  bool FindUnmatchableEdges(int required);

  ~BipartiteMaximumMatching();

  void Initialize(int max_left, int max_right, int max_query_vertex);

  void Reset(bool reset_edges = true);

  void AddEdge(int u, int v);

  int RemoveEdge(int u, int v);

  void Revert(int* tmp_left);

  // Time: O( V(V+E) )
  int Solve(int ignore = -1);

  bool dfs(int r);

  bool FindAugmentingPath(int r);
};

void MultiWayIntersection(
    std::vector<std::pair<std::vector<int>::iterator,
                          std::vector<int>::iterator>>& iterators,
    int* results, int& results_size);

void VectorIntersection(std::vector<int>& A, std::vector<int>& B,
                        std::vector<int>& results);

}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug
