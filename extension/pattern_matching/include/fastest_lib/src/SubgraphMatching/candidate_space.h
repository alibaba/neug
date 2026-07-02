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
#include <algorithm>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>
#include "Base/base.h"
#include "Base/basic_algorithms.h"
#include "DataStructure/graph.h"
#include "SubgraphMatching/data_graph.h"
#include "SubgraphMatching/pattern_graph.h"
#include "pattern_matching_data_graph_meta.h"
#include "pattern_matching_value.h"  // Value with comparison operators (>, <, >=, <=)

#include <glog/logging.h>

/**
 * @brief The Candidate Space structure
 * @date 2023-05
 */

namespace neug {
namespace pattern_matching {
namespace graphlib {
namespace SubgraphMatching {

// Alias for the core neug Value type used by candidate constraints.
using Value = neug::Value;

// Dedupes "constraint references unknown property" warnings so a typo'd
// property name doesn't spam the log on every candidate vertex/edge.
// Keyed by "v:<label>:<prop>" (vertex) or "e:<src>:<dst>:<label>:<prop>"
// (edge). Survives across SAMPLED_PATTERN_MATCH invocations on purpose — the
// same typo is only worth flagging once per process.
void WarnUnknownConstraintPropertyOnce(const std::string& key,
                                       const std::string& message);

enum STRUCTURE_FILTER {
  NO_STRUCTURE_FILTER,
  TRIANGLE_SAFETY,
  FOURCYCLE_SAFETY
};
enum NEIGHBOR_FILTER {
  NEIGHBOR_SAFETY,
  NEIGHBOR_BIPARTITE_SAFETY,
  EDGE_BIPARTITE_SAFETY
};
class SubgraphMatchingOption {
 public:
  // For directed graphs: disable cycle filters by default
  STRUCTURE_FILTER structure_filter = NO_STRUCTURE_FILTER;
  NEIGHBOR_FILTER neighborhood_filter = NEIGHBOR_SAFETY;
  int MAX_QUERY_VERTEX = 50, MAX_QUERY_EDGE = 250;
  long long max_num_matches = -1;
};

class CandidateSpace {
 public:
  SubgraphMatchingOption opt;
  CandidateSpace(const neug::StorageReadInterface& graph,
                 DataGraphMeta& data_meta,
                 SubgraphMatchingOption filter_option);

  ~CandidateSpace();

  CandidateSpace& operator=(const CandidateSpace&) = delete;

  CandidateSpace(const CandidateSpace&) = delete;

  inline int GetCandidateSetSize(const int u) const {
    return candidate_set_[u].size();
  };

  inline int GetCandidate(const int u, const int v_idx) const {
    return candidate_set_[u][v_idx];
  };

  bool BuildCS(PatternGraph* query);

  std::vector<int>& GetCandidates(int u) { return candidate_set_[u]; }

  // For directed graph: separate out/in candidate neighbors
  // GetOutCandidateNeighbors: for query edge cur -> nxt (out-edge from cur)
  std::vector<int>& GetOutCandidateNeighbors(int cur, int cand_idx, int nxt) {
    return out_candidate_neighbors[cur][cand_idx]
                                  [query_->GetOutAdjIdx(cur, nxt)];
  }
  // GetInCandidateNeighbors: for query edge nxt -> cur (in-edge to cur)
  std::vector<int>& GetInCandidateNeighbors(int cur, int cand_idx, int nxt) {
    return in_candidate_neighbors[cur][cand_idx][query_->GetInAdjIdx(cur, nxt)];
  }

  int GetOutCandidateNeighbor(int cur, int cand_idx, int nxt, int nxt_idx) {
    return out_candidate_neighbors[cur][cand_idx]
                                  [query_->GetOutAdjIdx(cur, nxt)][nxt_idx];
  }
  int GetInCandidateNeighbor(int cur, int cand_idx, int nxt, int nxt_idx) {
    return in_candidate_neighbors[cur][cand_idx][query_->GetInAdjIdx(cur, nxt)]
                                 [nxt_idx];
  }

  // Auto-detect direction version (for compatibility, handles single direction
  // only)
  std::vector<int>& GetCandidateNeighbors(int cur, int cand_idx, int nxt);
  int GetCandidateNeighbor(int cur, int cand_idx, int nxt, int nxt_idx);

  dict GetCSInfo() { return CSInfo; };

  int GetNumCSVertex() { return num_candidate_vertex; };
  int GetNumCSEdge() { return num_candidate_edge; };

 private:
  // Per-instance bipartite matcher: previously a single inline global,
  // which (a) raced when concurrent queries each constructed their own
  // CandidateSpace and (b) leaked because each ctor called Initialize()
  // again, overwriting the previous run's buffers without freeing.
  // Owning it here gives each query its own solver and lets ~CandidateSpace
  // free the buffers via BipartiteMaximumMatching's destructor.
  BipartiteMaximumMatching BPSolver;

  dict CSInfo;
  const neug::StorageReadInterface& graph_;
  DataGraphMeta& data_meta_;
  PatternGraph* query_;
  // Separate storage for out/in candidate neighbors
  // out_candidate_neighbors[u][cand_idx][out_neighbor_idx] = list of uc's
  // candidate indices
  std::vector<std::vector<std::vector<std::vector<int>>>>
      out_candidate_neighbors;
  // in_candidate_neighbors[u][cand_idx][in_neighbor_idx] = list of uc's
  // candidate indices
  std::vector<std::vector<std::vector<std::vector<int>>>>
      in_candidate_neighbors;

  std::vector<std::vector<int>> candidate_set_;
  // For directed graph: separate label frequencies for out/in neighbors
  std::vector<int> out_neighbor_label_frequency;
  std::vector<int> in_neighbor_label_frequency;
  int num_candidate_vertex = 0, num_candidate_edge = 0;
  bool* out_neighbor_cs;
  bool* in_neighbor_cs;
  bool** BitsetCS;
  // Edge candidate set: BitsetEdgeCS[query_edge_idx] contains integer keys of
  // candidate data edges
  std::vector<std::unordered_set<EdgeKey, EdgeKeyHash>> BitsetEdgeCS;
  int* num_visit_cs_;

  bool BuildInitialCS();

  void ConstructCS();

  bool InitRootCandidates(int root);

  bool RefineCS();

  void PrepareNeighborSafety(int cur);

  bool CheckNeighborSafety(int cur, int cand);

  bool NeighborBipartiteSafety(int cur, int cand);

  bool EdgeBipartiteSafety(int cur, int cand);

  bool EdgeCandidacy(int query_edge_id, const EdgeKey& data_edge_key);

  bool TriangleSafety(int query_edge_id, int data_edge_id);

  bool FourCycleSafety(int query_edge_id, int data_edge_id);

  bool CheckSubStructures(int cur, int cand);

  bool CheckVertexPropertyConstraints(int query_vertex, int data_vertex);

  bool CheckEdgePropertyConstraints(int query_edge_id,
                                    const DataGraphMeta::Edge& data_edge);

  bool CheckValueConstraint(const Value& data_value, CompType comp_type,
                            const Value& constraint_value);

  bool NeighborFilter(int cur, int cand);

  bool StructureFilter(int query_edge_id, int data_edge_id);
};

}  // namespace SubgraphMatching
}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug
