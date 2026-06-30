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
 * This file is originally from the DAF project
 * (https://github.com/SNUCSE-CTA/DAF) Licensed under the MIT License.
 * Modified by Yunkai Lou and Shunyang Li in 2025 to support Neug-specific
 * features.
 */

#ifndef BACKTRACK_H_
#define BACKTRACK_H_

#include <iostream>
#include <vector>

#include "../global/global.h"
#include "../include/backtrack_helper.h"
#include "../include/candidate_space.h"
#include "../include/data_graph.h"
#include "../include/match_leaves.h"
#include "../include/ordering.h"
#include "../include/query_graph.h"

namespace daf {
struct SearchTreeNode;

class Backtrack {
 public:
  Backtrack(const DataGraph& data, const QueryGraph& query,
            const CandidateSpace& cs);
  ~Backtrack();

  Backtrack& operator=(const Backtrack&) = delete;
  Backtrack(const Backtrack&) = delete;

  uint64_t FindMatches(uint64_t limit);
  inline uint64_t GetNumBacktrackCalls() const;

  std::vector<std::vector<Vertex>> match_results_;
  MatchLeaves* match_leaves_;

 private:
  const DataGraph& data_;
  const QueryGraph& query_;
  const CandidateSpace& cs_;

  Ordering* extendable_queue_;
  BacktrackHelper* helpers_;

  Vertex* mapped_query_vtx_;
  SearchTreeNode* node_stack_;
  SearchTreeNode** mapped_nodes_;

  uint64_t num_embeddings_;
  uint64_t num_backtrack_calls_;
  Size backtrack_depth_;

  Vertex GetRootVertex();
  void InitializeNodeStack();
  void ComputeExtendable(Vertex u, Vertex u_nbr, Size u_nbr_idx, Size cs_v_idx);
  void ComputeDynamicAncestor(Vertex u, Vertex u_nbr);
  bool ComputeExtendableForAllNeighbors(SearchTreeNode* cur_node,
                                        Size cs_v_idx);
  void ReleaseNeighbors(SearchTreeNode* cur_node);
};

inline uint64_t Backtrack::GetNumBacktrackCalls() const {
  return num_backtrack_calls_;
}

}  // namespace daf

#endif  // BACKTRACK_H_
