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

#ifndef CANDIDATE_SPACE_H_
#define CANDIDATE_SPACE_H_

#include <utility>

#include "../global/global.h"
#include "../include/dag.h"
#include "../include/data_graph.h"
#include "../include/query_graph.h"

namespace daf {
class CandidateSpace {
 public:
  CandidateSpace(const DataGraph& data, const QueryGraph& query,
                 const DAG& dag);
  ~CandidateSpace();

  CandidateSpace& operator=(const CandidateSpace&) = delete;
  CandidateSpace(const CandidateSpace&) = delete;

  bool BuildCS();

  inline Size GetCandidateSetSize(Vertex u) const;
  inline Vertex GetCandidate(Vertex u, Size v_idx) const;

  inline Size GetCandidateStartOffset(Vertex u, Size u_adj_idx,
                                      Size v_idx) const;
  inline Size GetCandidateEndOffset(Vertex u, Size u_adj_idx, Size v_idx) const;
  inline Size GetCandidateIndex(Size idx) const;

 private:
  const DataGraph& data_;
  const QueryGraph& query_;
  const DAG& dag_;

  Size* candidate_set_size_;
  Vertex** candidate_set_;
  Size*** candidate_offsets_;
  Vertex* linear_cs_adj_list_;

  QueryDegree* num_visit_cs_;
  Vertex* visited_candidates_;
  Size* cand_to_cs_idx_;
  Size num_visitied_candidates_;

  Size num_cs_edges_;

  bool FilterByTopDownWithInit();
  bool FilterByBottomUp();
  bool FilterByTopDown();

  void ConstructCS();
  bool InitRootCandidates();

  void ComputeNbrInformation(Vertex u, Size* max_nbr_degree,
                             uint64_t* label_set);
  bool CheckCandidateVertexAttributes(Vertex query_u, Vertex data_v) const;
  bool CheckCandidateEdgeAttributes(Vertex query_parent, Vertex query_child,
                                    Vertex data_parent,
                                    Vertex data_child) const;
};

inline Size CandidateSpace::GetCandidateSetSize(Vertex u) const {
  return candidate_set_size_[u];
}

inline Vertex CandidateSpace::GetCandidate(Vertex u, Size v_idx) const {
  return candidate_set_[u][v_idx];
}

inline Size CandidateSpace::GetCandidateStartOffset(Vertex u, Size u_adj_idx,
                                                    Size v_idx) const {
  return candidate_offsets_[u][u_adj_idx][v_idx];
}

inline Size CandidateSpace::GetCandidateEndOffset(Vertex u, Size u_adj_idx,
                                                  Size v_idx) const {
  return candidate_offsets_[u][u_adj_idx][v_idx + 1];
}

inline Size CandidateSpace::GetCandidateIndex(Size idx) const {
  return linear_cs_adj_list_[idx];
}

}  // namespace daf

#endif  // CANDIDATE_SPACE_H_
