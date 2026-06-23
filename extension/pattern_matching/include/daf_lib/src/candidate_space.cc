#include "include/candidate_space.h"

namespace daf {
CandidateSpace::CandidateSpace(const DataGraph &data, const QueryGraph &query,
                               const DAG &dag)
    : data_(data), query_(query), dag_(dag) {
  candidate_set_size_ = new Size[query_.GetNumVertices()];
  candidate_set_ = new Vertex *[query_.GetNumVertices()];
  candidate_offsets_ = new Size **[query_.GetNumVertices()];

  for (Vertex u = 0; u < query_.GetNumVertices(); ++u) {
    if (query_.IsInNEC(u) && !query_.IsNECRepresentation(u)) {
      candidate_set_[u] = nullptr;
      candidate_offsets_[u] = nullptr;
    } else {
      candidate_set_[u] = new Vertex[dag_.GetInitCandSize(u)];
      candidate_offsets_[u] = new Size *[query_.GetDegree(u)];
      // 初始化所有指针为nullptr，避免析构时释放垃圾指针
      std::fill(candidate_offsets_[u], candidate_offsets_[u] + query_.GetDegree(u), nullptr);
    }
  }

  linear_cs_adj_list_ = nullptr;

  num_visit_cs_ = new QueryDegree[data_.GetNumVertices()];
  visited_candidates_ =
      new Vertex[data_.GetNumVertices() * query_.GetNumVertices()];
  cand_to_cs_idx_ = new Size[data_.GetNumVertices()];

  num_visitied_candidates_ = 0;

  std::fill(num_visit_cs_, num_visit_cs_ + data_.GetNumVertices(), 0);
  std::fill(candidate_set_size_, candidate_set_size_ + query_.GetNumVertices(),
            0);
  std::fill(cand_to_cs_idx_, cand_to_cs_idx_ + data_.GetNumVertices(),
            INVALID_SZ);
  num_cs_edges_ = 0;
}

CandidateSpace::~CandidateSpace() {
  delete[] candidate_set_size_;

  for (Vertex u = 0; u < query_.GetNumVertices(); ++u) {
    if (candidate_set_[u] != nullptr) {
      delete[] candidate_set_[u];
      for (Size i = query_.GetStartOffset(u); i < query_.GetEndOffset(u); ++i) {
        Vertex u_adj = query_.GetNeighbor(i);
        if (query_.IsInNEC(u_adj) && !query_.IsNECRepresentation(u_adj))
          continue;
        // 只释放非空指针，避免释放垃圾指针
        Size u_adj_idx = i - query_.GetStartOffset(u);
        if (candidate_offsets_[u][u_adj_idx] != nullptr) {
          delete[] candidate_offsets_[u][u_adj_idx];
        }
      }
      delete[] candidate_offsets_[u];
    }
  }
  delete[] candidate_set_;
  delete[] candidate_offsets_;

  if (linear_cs_adj_list_ != nullptr) delete[] linear_cs_adj_list_;

  delete[] num_visit_cs_;
  delete[] visited_candidates_;
  delete[] cand_to_cs_idx_;
}

bool CandidateSpace::BuildCS() {
  if (!FilterByTopDownWithInit()) return false;
  if (!FilterByBottomUp()) return false;
  if (!FilterByTopDown()) return false;

  ConstructCS();

  return true;
}

bool CandidateSpace::FilterByTopDownWithInit() {
  bool result = true;

  uint64_t *nbr_label_bitset = new uint64_t[data_.GetNbrBitsetSize()];
  Size max_nbr_degree;

  if (!InitRootCandidates()) {
    result = false;
  } else {
    for (Size i = 1; i < query_.GetNumVertices(); ++i) {
      Vertex cur = dag_.GetVertexOrderedByBFS(i);

      if (query_.IsInNEC(cur) && !query_.IsNECRepresentation(cur)) continue;

      Label cur_label = query_.GetLabel(cur);
      QueryDegree num_parent = 0;
      for (Size i = 0; i < dag_.GetNumParents(cur); ++i) {
        Vertex parent = dag_.GetParent(cur, i);

        for (Size i = 0; i < candidate_set_size_[parent]; ++i) {
          Vertex parent_cand = candidate_set_[parent][i];

          for (Size i = data_.GetStartOffset(parent_cand, cur_label);
               i < data_.GetEndOffset(parent_cand, cur_label); ++i) {
            Vertex cand = data_.GetNeighbor(i);

            if (data_.GetDegree(cand) < query_.GetDegree(cur)) break;
            
            // Check if edge labels match between query and data graph
            Size query_edge_idx = query_.GetEdgeIndex(parent, cur);
            Size data_edge_idx = data_.GetEdgeIndex(parent_cand, cand);
            if (query_edge_idx != INVALID_SZ && data_edge_idx != INVALID_SZ) {
              Label query_edge_label = query_.GetEdgeLabel(query_edge_idx);
              Label data_edge_label = data_.GetEdgeLabel(data_edge_idx);
              if (query_edge_label != data_edge_label) continue;
            }
            
            if (!CheckCandidateVertexAttributes(cur, cand)) continue;
            if (!CheckCandidateEdgeAttributes(parent, cur, parent_cand, cand)) continue;

            if (num_visit_cs_[cand] == num_parent) {
              num_visit_cs_[cand] += 1;
              if (num_parent == 0) {
                visited_candidates_[num_visitied_candidates_] = cand;
                num_visitied_candidates_ += 1;
              }
            }
          }
        }
        num_parent += 1;
      }

      ComputeNbrInformation(cur, &max_nbr_degree, nbr_label_bitset);

      for (Size i = 0; i < num_visitied_candidates_; ++i) {
        Vertex cand = visited_candidates_[i];
        if (num_visit_cs_[cand] == num_parent &&
            data_.GetCoreNum(cand) >= query_.GetCoreNum(cur) &&
            data_.GetMaxNbrDegree(cand) >= max_nbr_degree &&
            data_.CheckAllNbrLabelExist(cand, nbr_label_bitset)) {
          candidate_set_[cur][candidate_set_size_[cur]] = cand;
          candidate_set_size_[cur] += 1;
        }
      }

      if (candidate_set_size_[cur] == 0) {
        result = false;
        break;
      }

      while (num_visitied_candidates_ > 0) {
        num_visitied_candidates_ -= 1;
        num_visit_cs_[visited_candidates_[num_visitied_candidates_]] = 0;
      }
    }
  }

  delete[] nbr_label_bitset;
  return result;
}

bool CandidateSpace::CheckCandidateVertexAttributes(Vertex query_u, Vertex data_v) const {
  // Check vertex property constraints first
  const auto& vertex_constraints = query_.GetVertexPropertyConstraints(query_u);
  if (!data_.CheckVertexPropertyConstraints(vertex_constraints, data_v)) {
    return false;
  }

  return true;
}

bool CandidateSpace::CheckCandidateEdgeAttributes(Vertex query_parent, Vertex query_child, Vertex data_parent, Vertex data_child) const {  
  // Check if there's an edge between query_parent and query_child
  Size query_edge_idx = query_.GetEdgeIndex(query_parent, query_child);
  if (query_edge_idx == INVALID_SZ) {
    return true; // No edge in query, so no constraint to check
  }
  
  // Get edge constraints from query graph
  const auto& edge_constraints = query_.GetEdgePropertyConstraints(query_edge_idx);
  if (edge_constraints.empty()) {
    return true; // No edge constraints to check
  }

  
  // Get the data edge index between data_parent and data_child
  Size data_edge_idx = data_.GetEdgeIndex(data_parent, data_child);
  if (data_edge_idx == INVALID_SZ) {
    return false; // No edge in data graph but constraints exist
  }
  // Check if edge constraints are satisfied
  return data_.CheckEdgePropertyConstraints(edge_constraints, data_edge_idx);
}

bool CandidateSpace::FilterByBottomUp() {
  bool result = true;
  for (Size i = 0; i < query_.GetNumVertices(); ++i) {
    Vertex cur = dag_.GetVertexOrderedByBFS(query_.GetNumVertices() - i - 1);

    if (dag_.GetNumChildren(cur) == 0) continue;

    Label cur_label = query_.GetLabel(cur);

    QueryDegree num_child = 0;
    for (Size i = 0; i < dag_.GetNumChildren(cur); ++i) {
      Vertex child = dag_.GetChild(cur, i);

      if (query_.IsInNEC(child) && !query_.IsNECRepresentation(child)) continue;

      for (Size i = 0; i < candidate_set_size_[child]; ++i) {
        Vertex child_cand = candidate_set_[child][i];

        for (Size i = data_.GetStartOffset(child_cand, cur_label);
             i < data_.GetEndOffset(child_cand, cur_label); ++i) {
          Vertex cand = data_.GetNeighbor(i);
          if (data_.GetDegree(cand) < query_.GetDegree(cur)) break;
          
          // Check if edge labels match between query and data graph
          Size query_edge_idx = query_.GetEdgeIndex(child, cur);
          Size data_edge_idx = data_.GetEdgeIndex(child_cand, cand);
          if (query_edge_idx != INVALID_SZ && data_edge_idx != INVALID_SZ) {
            Label query_edge_label = query_.GetEdgeLabel(query_edge_idx);
            Label data_edge_label = data_.GetEdgeLabel(data_edge_idx);
            if (query_edge_label != data_edge_label) continue;
          }
          
          if (!CheckCandidateEdgeAttributes(child, cur, child_cand, cand)) continue;

          if (num_visit_cs_[cand] == num_child) {
            num_visit_cs_[cand] += 1;
            if (num_child == 0) {
              visited_candidates_[num_visitied_candidates_] = cand;
              num_visitied_candidates_ += 1;
            }
          }
        }
      }
      num_child += 1;
    }

    for (Size i = 0; i < candidate_set_size_[cur]; ++i) {
      Vertex cand = candidate_set_[cur][i];
      if (num_visit_cs_[cand] != num_child) {
        candidate_set_[cur][i] =
            candidate_set_[cur][candidate_set_size_[cur] - 1];
        candidate_set_size_[cur] -= 1;
        --i;
      }
    }

    if (candidate_set_size_[cur] == 0) {
      result = false;
      break;
    }

    while (num_visitied_candidates_ > 0) {
      num_visitied_candidates_ -= 1;
      num_visit_cs_[visited_candidates_[num_visitied_candidates_]] = 0;
    }
  }

  return result;
}

bool CandidateSpace::FilterByTopDown() {
  bool result = true;
  for (Size i = 1; i < query_.GetNumVertices(); ++i) {
    Vertex cur = dag_.GetVertexOrderedByBFS(i);
    if (query_.IsInNEC(cur) && !query_.IsNECRepresentation(cur)) continue;

    Label cur_label = query_.GetLabel(cur);

    QueryDegree num_parent = 0;
    for (Size i = 0; i < dag_.GetNumParents(cur); ++i) {
      Vertex parent = dag_.GetParent(cur, i);

      for (Size i = 0; i < candidate_set_size_[parent]; ++i) {
        Vertex parent_cand = candidate_set_[parent][i];

        for (Size i = data_.GetStartOffset(parent_cand, cur_label);
             i < data_.GetEndOffset(parent_cand, cur_label); ++i) {
          Vertex cand = data_.GetNeighbor(i);
          if (data_.GetDegree(cand) < query_.GetDegree(cur)) break;
          
          // Check if edge labels match between query and data graph
          Size query_edge_idx = query_.GetEdgeIndex(parent, cur);
          Size data_edge_idx = data_.GetEdgeIndex(parent_cand, cand);
          if (query_edge_idx != INVALID_SZ && data_edge_idx != INVALID_SZ) {
            Label query_edge_label = query_.GetEdgeLabel(query_edge_idx);
            Label data_edge_label = data_.GetEdgeLabel(data_edge_idx);
            if (query_edge_label != data_edge_label) continue;
          }
          
          if (!CheckCandidateEdgeAttributes(parent, cur, parent_cand, cand)) continue;

          if (num_visit_cs_[cand] == num_parent) {
            num_visit_cs_[cand] += 1;
            if (num_parent == 0) {
              visited_candidates_[num_visitied_candidates_] = cand;
              num_visitied_candidates_ += 1;
              cand_to_cs_idx_[cand] = 0;
            }
            cand_to_cs_idx_[cand] += 1;
          } else if (num_visit_cs_[cand] > num_parent) {
            cand_to_cs_idx_[cand] += 1;
          }
        }
      }
      num_parent += 1;
    }

    for (Size i = 0; i < candidate_set_size_[cur]; ++i) {
      Vertex cand = candidate_set_[cur][i];
      if (num_visit_cs_[cand] != num_parent) {
        candidate_set_[cur][i] =
            candidate_set_[cur][candidate_set_size_[cur] - 1];
        candidate_set_size_[cur] -= 1;
        --i;
      } else {
        num_cs_edges_ += cand_to_cs_idx_[cand];
      }
    }

    if (candidate_set_size_[cur] == 0) {
      result = false;
      break;
    }

    while (num_visitied_candidates_ > 0) {
      num_visitied_candidates_ -= 1;
      num_visit_cs_[visited_candidates_[num_visitied_candidates_]] = 0;
      cand_to_cs_idx_[visited_candidates_[num_visitied_candidates_]] =
          INVALID_SZ;
    }
  }

  return result;
}

void CandidateSpace::ConstructCS() {
  linear_cs_adj_list_ = new Vertex[num_cs_edges_ * 2];

  Size cur_cand_offset = 0;

  for (Size i = 0; i < query_.GetNumVertices(); ++i) {
    Vertex u = dag_.GetVertexOrderedByBFS(i);

    if (query_.IsInNEC(u) && !query_.IsNECRepresentation(u)) continue;

    Label u_label = query_.GetLabel(u);
    Size u_degree = query_.GetDegree(u);

    for (Size i = 0; i < candidate_set_size_[u]; ++i) {
      cand_to_cs_idx_[candidate_set_[u][i]] = i;
    }

    for (Size i = query_.GetStartOffset(u); i < query_.GetEndOffset(u); ++i) {
      Vertex u_adj = query_.GetNeighbor(i);

      if (query_.IsInNEC(u_adj) && !query_.IsNECRepresentation(u_adj)) continue;

      Size u_adj_idx = i - query_.GetStartOffset(u);

      candidate_offsets_[u][u_adj_idx] = new Size[GetCandidateSetSize(u) + 1];

      std::fill(candidate_offsets_[u][u_adj_idx],
                candidate_offsets_[u][u_adj_idx] + GetCandidateSetSize(u) + 1,
                cur_cand_offset);

      for (Size v_adj_idx = 0; v_adj_idx < candidate_set_size_[u_adj];
           ++v_adj_idx) {
        Vertex v_adj = candidate_set_[u_adj][v_adj_idx];

        for (Size i = data_.GetStartOffset(v_adj, u_label);
             i < data_.GetEndOffset(v_adj, u_label); ++i) {
          Vertex v = data_.GetNeighbor(i);

          if (data_.GetDegree(v) < u_degree) break;

          // Check if edge labels match between query and data graph
          Size query_edge_idx = query_.GetEdgeIndex(u_adj, u);
          Size data_edge_idx = data_.GetEdgeIndex(v_adj, v);
          bool edge_label_match = true;
          if (query_edge_idx != INVALID_SZ && data_edge_idx != INVALID_SZ) {
            Label query_edge_label = query_.GetEdgeLabel(query_edge_idx);
            Label data_edge_label = data_.GetEdgeLabel(data_edge_idx);
            edge_label_match = (query_edge_label == data_edge_label);
          }

          if (cand_to_cs_idx_[v] != INVALID_SZ && edge_label_match &&
              CheckCandidateEdgeAttributes(u_adj, u, v_adj, v)) {
            candidate_offsets_[u][u_adj_idx][cand_to_cs_idx_[v] + 1] += 1;
          }
        }
      }

      for (Size i = 2; i < GetCandidateSetSize(u) + 1; ++i) {
        candidate_offsets_[u][u_adj_idx][i] +=
            candidate_offsets_[u][u_adj_idx][i - 1] - cur_cand_offset;
      }

      for (Size v_adj_idx = 0; v_adj_idx < candidate_set_size_[u_adj];
           ++v_adj_idx) {
        Vertex v_adj = candidate_set_[u_adj][v_adj_idx];

        for (Size i = data_.GetStartOffset(v_adj, u_label);
             i < data_.GetEndOffset(v_adj, u_label); ++i) {
          Vertex v = data_.GetNeighbor(i);

          if (data_.GetDegree(v) < u_degree) break;

          // Check if edge labels match between query and data graph
          Size query_edge_idx = query_.GetEdgeIndex(u_adj, u);
          Size data_edge_idx = data_.GetEdgeIndex(v_adj, v);
          bool edge_label_match = true;
          if (query_edge_idx != INVALID_SZ && data_edge_idx != INVALID_SZ) {
            Label query_edge_label = query_.GetEdgeLabel(query_edge_idx);
            Label data_edge_label = data_.GetEdgeLabel(data_edge_idx);
            edge_label_match = (query_edge_label == data_edge_label);
          }

          Size v_idx = cand_to_cs_idx_[v];
          if (v_idx != INVALID_SZ && edge_label_match &&
              CheckCandidateEdgeAttributes(u_adj, u, v_adj, v)) {
            linear_cs_adj_list_[candidate_offsets_[u][u_adj_idx][v_idx]] =
                v_adj_idx;
            candidate_offsets_[u][u_adj_idx][v_idx] += 1;
          }
        }
      }

      for (Size i = GetCandidateSetSize(u) - 1; i--;) {
        candidate_offsets_[u][u_adj_idx][i + 1] =
            candidate_offsets_[u][u_adj_idx][i];
      }

      candidate_offsets_[u][u_adj_idx][0] = cur_cand_offset;

      cur_cand_offset =
          candidate_offsets_[u][u_adj_idx][GetCandidateSetSize(u)];
    }

    for (Size i = 0; i < candidate_set_size_[u]; ++i) {
      cand_to_cs_idx_[candidate_set_[u][i]] = INVALID_SZ;
    }
  }
}

bool CandidateSpace::InitRootCandidates() {
  Vertex root = dag_.GetRoot();
  Label root_label = query_.GetLabel(root);

  uint64_t *nbr_label_bitset = new uint64_t[data_.GetNbrBitsetSize()];
  Size max_nbr_degree;

  ComputeNbrInformation(root, &max_nbr_degree, nbr_label_bitset);

  for (Size i = data_.GetStartOffsetByLabel(root_label);
       i < data_.GetEndOffsetByLabel(root_label); ++i) {
    Vertex cand = data_.GetVertexBySortedLabelOffset(i);

    if (data_.GetDegree(cand) < query_.GetDegree(root)) break;

    if (data_.GetCoreNum(cand) >= query_.GetCoreNum(root) &&
        data_.CheckAllNbrLabelExist(cand, nbr_label_bitset) &&
        data_.GetMaxNbrDegree(cand) >= max_nbr_degree &&
        CheckCandidateVertexAttributes(root, cand)) {
      candidate_set_[root][candidate_set_size_[root]] = cand;
      candidate_set_size_[root] += 1;
    }
  }

  delete[] nbr_label_bitset;
  return candidate_set_size_[root] > 0;
}

void CandidateSpace::ComputeNbrInformation(Vertex u, Size *max_nbr_degree,
                                           uint64_t *nbr_label_bitset) {
  *max_nbr_degree = 0;
  std::fill(nbr_label_bitset, nbr_label_bitset + data_.GetNbrBitsetSize(),
            0ull);
  for (Size i = query_.GetStartOffset(u); i < query_.GetEndOffset(u); ++i) {
    Vertex adj = query_.GetNeighbor(i);

    nbr_label_bitset[query_.GetLabel(adj) / (sizeof(uint64_t) * CHAR_BIT)] |=
        1ull << (query_.GetLabel(adj) % (sizeof(uint64_t) * CHAR_BIT));

    if (query_.GetDegree(adj) > *max_nbr_degree)
      *max_nbr_degree = query_.GetDegree(adj);
  }
}
}  // namespace daf
