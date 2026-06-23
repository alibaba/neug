#ifndef MATCH_LEAVES_H_
#define MATCH_LEAVES_H_

#include <vector>

#include "../global/global.h"
#include "../include/backtrack_helper.h"
#include "../include/candidate_space.h"
#include "../include/data_graph.h"
#include "../include/maximum_matching.h"
#include "../include/ordering.h"
#include "../include/query_graph.h"

namespace gbi {
class GraphStorage;
class PatternGraph;
}  // namespace gbi

namespace daf {

class MatchLeaves {
 public:
  MatchLeaves(const DataGraph &data, const QueryGraph &query,
              const CandidateSpace &cs, Vertex *data_to_query_vtx,
              BacktrackHelper *helpers, SearchTreeNode **mapped_nodes);
  ~MatchLeaves();

  MatchLeaves &operator=(const MatchLeaves &) = delete;
  MatchLeaves(const MatchLeaves &) = delete;

  uint64_t Match(uint64_t limit, std::vector<Vertex> &current_mapping, std::vector<std::vector<Vertex>> &match_results);
  void EnumerateAllLeafMatches(
      const std::vector<Size> &nec_labels, Size idx,
      std::vector<std::pair<Vertex, Vertex>> &current_leaf,
      const std::vector<Vertex> &current_mapping,
      std::vector<std::vector<Vertex>> &match_results,
      uint64_t &result, uint64_t limit);

  void EnumerateAssignment(
      Size ridx,
      const std::vector<Vertex> &represents,
      const std::vector<Size> &sizes,
      const std::vector<std::vector<Vertex>> &candidate_lists,
      std::vector<bool> &used,
      std::vector<std::pair<Vertex, Vertex>> &local_match,
      std::vector<std::pair<Vertex, Vertex>> &current_leaf,
      const std::vector<Size> &nec_labels, Size nec_idx,
      const std::vector<Vertex> &current_mapping,
      std::vector<std::vector<Vertex>> &match_results,
      uint64_t &result, uint64_t limit);

 public: 
  std::shared_ptr<gbi::PatternGraph> ori_pattern_graph;
  gbi::GraphStorage* ori_data_graph;

 private:
  const DataGraph &data_;
  const QueryGraph &query_;
  const CandidateSpace &cs_;

  Vertex *backtrack_mapped_query_vtx;
  BacktrackHelper *backtrack_helpers_;
  SearchTreeNode **backtrack_mapped_nodes_;

  MaximumMatching *maximum_matching_;

  Vertex *nec_distinct_cands_;
  Size num_nec_distinct_cands_;
  std::vector<Size> *cand_to_nec_idx_;
  std::vector<Vertex> reserved_data_vtx_;
  std::vector<Vertex> reserved_query_vtx_;

  Size *num_remaining_nec_vertices_;
  Size *num_remaining_cands_;

  Size *sum_nec_cands_size_;
  Size *sum_nec_size_;

  Size *nec_ranking_;

  Size cur_label_idx;

  uint64_t Combine(uint64_t limit);
  uint64_t EnumerateLeafMappings(Size leaf_idx, uint64_t limit, std::vector<Vertex> &current_mapping, std::vector<std::vector<Vertex>> &match_results);

  void ReserveVertex(Vertex represent, BacktrackHelper *repr_helper);

  void ClearMemoryForBacktrack();
};
}  // namespace daf

#endif  // MATCH_LEAVES_H_
