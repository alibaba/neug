#ifndef BACKTRACK_H_
#define BACKTRACK_H_

#include <boost/dynamic_bitset.hpp>
#include <vector>
#include <iostream>

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
  Backtrack(const DataGraph &data, const QueryGraph &query,
            const CandidateSpace &cs);
  ~Backtrack();

  Backtrack &operator=(const Backtrack &) = delete;
  Backtrack(const Backtrack &) = delete;

  uint64_t FindMatches(uint64_t limit);
  inline uint64_t GetNumBacktrackCalls() const;

  std::vector<std::vector<Vertex>> match_results_;
  MatchLeaves *match_leaves_;

 private:
  const DataGraph &data_;
  const QueryGraph &query_;
  const CandidateSpace &cs_;

  Ordering *extendable_queue_;
  BacktrackHelper *helpers_;

  Vertex *mapped_query_vtx_;
  SearchTreeNode *node_stack_;
  SearchTreeNode **mapped_nodes_;

  uint64_t num_embeddings_;
  uint64_t num_backtrack_calls_;
  Size backtrack_depth_;

  Vertex GetRootVertex();
  void InitializeNodeStack();
  void ComputeExtendable(Vertex u, Vertex u_nbr, Size u_nbr_idx, Size cs_v_idx);
  void ComputeDynamicAncestor(Vertex u, Vertex u_nbr);
  bool ComputeExtendableForAllNeighbors(SearchTreeNode *cur_node,
                                        Size cs_v_idx);
  void ReleaseNeighbors(SearchTreeNode *cur_node);
};

inline uint64_t Backtrack::GetNumBacktrackCalls() const {
  return num_backtrack_calls_;
}

}  // namespace daf

#endif  // BACKTRACK_H_
