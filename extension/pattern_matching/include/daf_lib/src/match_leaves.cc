#include "include/match_leaves.h"
#include "../../../storage/graph_storage.hpp"
#include <iostream>

namespace daf {
namespace {
constexpr uint64_t Factorial(Size n) {
  return n == 1 ? 1 : n * Factorial(n - 1);
}
}  // namespace

MatchLeaves::MatchLeaves(const DataGraph &data, const QueryGraph &query,
                         const CandidateSpace &cs, Vertex *mapped_query_vtx,
                         BacktrackHelper *helpers,
                         SearchTreeNode **mapped_nodes)
    : data_(data),
      query_(query),
      cs_(cs),
      backtrack_mapped_query_vtx(mapped_query_vtx),
      backtrack_helpers_(helpers),
      backtrack_mapped_nodes_(mapped_nodes) {
  nec_distinct_cands_ = new Vertex[data_.GetNumVertices()];
  cand_to_nec_idx_ = new std::vector<Size>[data_.GetNumVertices()];
  nec_ranking_ = new Size[query_.GetNumNECLabel()];

  sum_nec_cands_size_ = new Size[query_.GetNumNECLabel()];
  sum_nec_size_ = new Size[query_.GetNumNECLabel()];

  num_remaining_cands_ = new Size[query_.GetNumVertices()];
  num_remaining_nec_vertices_ = new Size[query_.GetNumVertices()];

  num_nec_distinct_cands_ = 0;

  maximum_matching_ =
      new MaximumMatching(data_, query_, cs_, backtrack_helpers_);
}

MatchLeaves::~MatchLeaves() {
  delete[] cand_to_nec_idx_;

  delete[] nec_distinct_cands_;
  delete[] nec_ranking_;
  delete[] sum_nec_cands_size_;
  delete[] sum_nec_size_;

  delete[] num_remaining_cands_;
  delete[] num_remaining_nec_vertices_;

  delete maximum_matching_;
}

uint64_t MatchLeaves::Match(uint64_t limit, std::vector<Vertex> &current_mapping, std::vector<std::vector<Vertex>> &match_results) {
  // Ensure current_mapping has enough space
  if (current_mapping.size() < query_.GetNumVertices()) {
    current_mapping.resize(query_.GetNumVertices(), INVALID_VTX);
  }

  // Enumerate all possible mappings for leaf vertices
  uint64_t result = EnumerateLeafMappings(0, limit, current_mapping, match_results);

  return result;
}

// Helper method to enumerate leaf vertex mappings (now directly using current_mapping)
uint64_t MatchLeaves::EnumerateLeafMappings(Size leaf_idx, uint64_t limit, std::vector<Vertex> &current_mapping, std::vector<std::vector<Vertex>> &match_results) {
  uint64_t count = 0;
  
  if (match_results.size() >= limit) {
    return count;
  }

  // Find next unmapped leaf vertex (process all leaf vertices)
  Vertex next_leaf = INVALID_VTX;
  for (Vertex u = 0; u < query_.GetNumVertices(); ++u) {
    if (query_.IsLeafVertex(u) && current_mapping[u] == INVALID_VTX) {
      next_leaf = u;
      break;
    }
  }
  
  // If all leaf vertices are mapped, we found a complete match
  if (next_leaf == INVALID_VTX) {
    // Add a copy of current mapping to results
    // Validate that all directed edges in pattern exist in the data graph
    match_results.push_back(current_mapping);
    return 1;
  }

  // Try all possible candidates for this leaf vertex
  // For NEC non-representative nodes, use the representative's candidate space
  Vertex candidate_source = next_leaf;
  if (query_.IsInNEC(next_leaf) && !query_.IsNECRepresentation(next_leaf)) {
    candidate_source = query_.GetNECRepresentative(next_leaf);
  }
  
  BacktrackHelper *leaf_helper = backtrack_helpers_ + candidate_source;
  for (Size k = 0; k < leaf_helper->GetNumExtendable(); ++k) {
    Vertex cand = cs_.GetCandidate(candidate_source, leaf_helper->GetExtendableIndex(k));
    
    // Check if candidate is already used
    bool candidate_used = false;
    for (Vertex v = 0; v < query_.GetNumVertices(); ++v) {
      if (current_mapping[v] == cand) {
        candidate_used = true;
        break;
      }
    }
    
    if (candidate_used || backtrack_mapped_query_vtx[cand] != INVALID_VTX) {
      continue;
    }

    bool has_forbidden_edge = false;
    // 检查是否存在schema中定义但query中不存在的边
    for (size_t i = 0; i < query_.no_edge_pairs[next_leaf].size(); i++) {
      Vertex u_nbr = query_.no_edge_pairs[next_leaf][i].first;
      Label edge_label = query_.no_edge_pairs[next_leaf][i].second;
      if (current_mapping[u_nbr] != INVALID_VTX) {
        Size data_edge_idx = data_.GetEdgeIndex(current_mapping[u_nbr], cand, edge_label);
        if (data_edge_idx != INVALID_SZ) {
          has_forbidden_edge = true;
          break;
        }
      }
    }
    if (has_forbidden_edge) {
      continue;
    }

    bool is_valid = true;
    for (size_t i = 0; i < query_.has_out_edge_pairs[next_leaf].size(); ++i) {
      Vertex u_nbr = query_.has_out_edge_pairs[next_leaf][i].first;
      Label edge_label = query_.has_out_edge_pairs[next_leaf][i].second;
      if (current_mapping[u_nbr] != INVALID_VTX && ori_data_graph) {
        if (!ori_data_graph->HasEdge(cand, current_mapping[u_nbr], edge_label)) {
          is_valid = false;
          break;
        }
      }
    }
    if (!is_valid)
      continue;
    for (size_t i = 0; i < query_.has_in_edge_pairs[next_leaf].size(); ++i) {
      Vertex u_nbr = query_.has_in_edge_pairs[next_leaf][i].first;
      Label edge_label = query_.has_in_edge_pairs[next_leaf][i].second;
      if (current_mapping[u_nbr] != INVALID_VTX && ori_data_graph) {
        if (!ori_data_graph->HasEdge(current_mapping[u_nbr], cand, edge_label)) {
          is_valid = false;
          break;
        }
      }
    }
    if (!is_valid)
      continue;

    // Map this leaf vertex to the candidate
    current_mapping[next_leaf] = cand;
    backtrack_mapped_query_vtx[cand] = next_leaf;
    
    // Recursively map remaining leaf vertices
    uint64_t sub_count = EnumerateLeafMappings(leaf_idx + 1, limit, current_mapping, match_results);
    count += sub_count;
    
    // Backtrack: restore the original state
    current_mapping[next_leaf] = INVALID_VTX;
    backtrack_mapped_query_vtx[cand] = INVALID_VTX;
    
    if (match_results.size() >= limit) {
      break;
    }
  }

  return count;
}




uint64_t MatchLeaves::Combine(uint64_t limit) {
  uint64_t result = 0;

  Size max_cand_idx = INVALID_SZ;
  Size max_cand_size = 1;

  for (Size j = 0; j < num_nec_distinct_cands_; ++j) {
    Vertex cand = nec_distinct_cands_[j];

    if (max_cand_size < cand_to_nec_idx_[cand].size()) {
      max_cand_size = cand_to_nec_idx_[cand].size();
      max_cand_idx = j;
    }
  }

  if (max_cand_idx == INVALID_SZ) {
    result = 1;
    for (Size j = query_.GetNECStartOffset(cur_label_idx);
         j < query_.GetNECEndOffset(cur_label_idx); ++j) {
      for (Size k = 0; k < num_remaining_nec_vertices_[j]; ++k) {
        result *= num_remaining_cands_[j] - k;
      }
    }
    return result;
  }

  Vertex max_cand = nec_distinct_cands_[max_cand_idx];

  std::swap(nec_distinct_cands_[max_cand_idx],
            nec_distinct_cands_[num_nec_distinct_cands_ - 1]);

  num_nec_distinct_cands_ -= 1;

  backtrack_mapped_query_vtx[max_cand] = cur_label_idx;

  for (Size j : cand_to_nec_idx_[max_cand]) {
    num_remaining_cands_[j] -= 1;
  }

  std::sort(cand_to_nec_idx_[max_cand].begin(),
            cand_to_nec_idx_[max_cand].end(), [this](Size j1, Size j2) -> bool {
              return this->num_remaining_cands_[j1] <
                     this->num_remaining_cands_[j2];
            });

  for (Size k = 0; k < cand_to_nec_idx_[max_cand].size(); ++k) {
    Size j = cand_to_nec_idx_[max_cand][k];

    if (num_remaining_nec_vertices_[j] == 0) continue;

    std::swap(cand_to_nec_idx_[max_cand][k], cand_to_nec_idx_[max_cand].back());
    cand_to_nec_idx_[max_cand].pop_back();
    num_remaining_nec_vertices_[j] -= 1;

    if (num_remaining_nec_vertices_[j] == 0) {
      Vertex represent = query_.GetNECElement(j).represent;
      BacktrackHelper *repr_helper = backtrack_helpers_ + represent;

      for (Size k = 0; k < repr_helper->GetNumUnmappedExtendable(); ++k) {
        Vertex cand =
            cs_.GetCandidate(represent, repr_helper->GetExtendableIndex(k));

        if (backtrack_mapped_query_vtx[cand] == INVALID_VTX) {
          for (auto &elem : cand_to_nec_idx_[cand]) {
            if (elem == j) {
              std::swap(elem, cand_to_nec_idx_[cand].back());
              break;
            }
          }

          cand_to_nec_idx_[cand].pop_back();
        }
      }
    }

    Size cartesian = num_remaining_nec_vertices_[j] + 1;
    uint64_t res = Combine((limit - result) / cartesian +
                           ((limit - result) % cartesian == 0 ? 0 : 1));
    result += cartesian * res;
    if (result >= limit) {
      return result;
    }

    if (num_remaining_nec_vertices_[j] == 0) {
      Vertex represent = query_.GetNECElement(j).represent;
      BacktrackHelper *repr_helper = backtrack_helpers_ + represent;

      for (Size k = 0; k < repr_helper->GetNumUnmappedExtendable(); ++k) {
        Vertex cand =
            cs_.GetCandidate(represent, repr_helper->GetExtendableIndex(k));

        if (backtrack_mapped_query_vtx[cand] == INVALID_VTX) {
          cand_to_nec_idx_[cand].push_back(j);
        }
      }
    }

    cand_to_nec_idx_[max_cand].push_back(j);
    std::swap(cand_to_nec_idx_[max_cand][k], cand_to_nec_idx_[max_cand].back());
    num_remaining_nec_vertices_[j] += 1;
  }

  result += Combine(limit - result);
  if (result >= limit) {
    return result;
  }

  for (Size j : cand_to_nec_idx_[max_cand]) {
    num_remaining_cands_[j] += 1;
  }

  backtrack_mapped_query_vtx[max_cand] = INVALID_VTX;

  num_nec_distinct_cands_ += 1;

  return result;
}

void MatchLeaves::ReserveVertex(Vertex represent,
                                BacktrackHelper *repr_helper) {
  repr_helper->GetMappingState() = RESERVED;
  reserved_query_vtx_.push_back(represent);
  for (Size k = 0; k < repr_helper->GetNumUnmappedExtendable(); ++k) {
    Vertex cand =
        cs_.GetCandidate(represent, repr_helper->GetExtendableIndex(k));

    backtrack_mapped_query_vtx[cand] = represent;
    reserved_data_vtx_.push_back(cand);
  }
}

void MatchLeaves::ClearMemoryForBacktrack() {
  while (!reserved_data_vtx_.empty()) {
    Vertex cand = reserved_data_vtx_.back();
    reserved_data_vtx_.pop_back();
    backtrack_mapped_query_vtx[cand] = INVALID_VTX;
  }

  while (!reserved_query_vtx_.empty()) {
    Vertex represent = reserved_query_vtx_.back();
    reserved_query_vtx_.pop_back();
    BacktrackHelper *repr_helper = backtrack_helpers_ + represent;
    repr_helper->GetMappingState() = UNMAPPED;
  }
  // No longer need to clear current_leaf_mapping_ and full_embedding_ 
  // since we now operate directly on the passed current_mapping parameter
}
}  // namespace daf
