#include "include/backtrack.h"
#include "../../../storage/graph_storage.hpp"

namespace daf {
Backtrack::Backtrack(const DataGraph &data, const QueryGraph &query,
                     const CandidateSpace &cs)
    : data_(data), query_(query), cs_(cs) {
  num_embeddings_ = 0;
  num_backtrack_calls_ = 0;
  backtrack_depth_ = 1;

  mapped_query_vtx_ = new Vertex[data_.GetNumVertices()];
  node_stack_ = new SearchTreeNode[query_.GetNumNonLeafVertices() + 2];
  mapped_nodes_ = new SearchTreeNode *[query_.GetNumVertices()];
  helpers_ = new BacktrackHelper[query_.GetNumVertices()];

  extendable_queue_ = new Ordering(query_.GetNumVertices());

  if (query_.GetNumNonLeafVertices() < query_.GetNumVertices()) {
    match_leaves_ = new MatchLeaves(data_, query_, cs_, mapped_query_vtx_,
                                    helpers_, mapped_nodes_);
  } else {
    match_leaves_ = nullptr;
  }

  std::fill(mapped_query_vtx_, mapped_query_vtx_ + data_.GetNumVertices(),
            INVALID_VTX);

  for (Vertex u = 0; u < query_.GetNumVertices(); ++u) {
    helpers_[u].Initialize(query_.GetNumVertices(), query_.GetDegree(u),
                           cs_.GetCandidateSetSize(u), u);
  }
}

Backtrack::~Backtrack() {
  delete[] mapped_query_vtx_;
  delete[] node_stack_;
  delete[] mapped_nodes_;
  delete[] helpers_;

  delete extendable_queue_;
  if (match_leaves_ != nullptr) {
    delete match_leaves_;
  }
}

uint64_t Backtrack::FindMatches(uint64_t limit) {
  std::vector<Vertex> current_mapping(query_.GetNumVertices() + 1, INVALID_VTX);
  Vertex root_vertex = GetRootVertex();
  Size root_cs_size = cs_.GetCandidateSetSize(root_vertex);

  extendable_queue_->Insert(root_vertex, root_cs_size);

  InitializeNodeStack();

  while (backtrack_depth_ > 0) {
    if (match_results_.size() >= limit) {
      return match_results_.size();
    }

    SearchTreeNode *parent_node = node_stack_ + (backtrack_depth_ - 1);
    SearchTreeNode *cur_node = node_stack_ + backtrack_depth_;

    BacktrackHelper *u_helper;

    if (cur_node->initialized == false) {
      // newly expanded search tree node
      num_backtrack_calls_ += 1;

      cur_node->initialized = true;
      cur_node->u = extendable_queue_->PopMinWeight();
      cur_node->v_idx = 0;
      cur_node->embedding_founded = false;
      cur_node->failing_set.reset();

      u_helper = helpers_ + cur_node->u;
      u_helper->GetMappingState() = MAPPED;
    } else {
      // backtrack from child node
      ReleaseNeighbors(cur_node);

      u_helper = helpers_ + cur_node->u;

      // compute failing set of parent node (non-leaf node)
      if (cur_node->embedding_founded) {
        // case 1
        parent_node->embedding_founded = true;
        cur_node->v_idx += 1;
      } else {
        if (cur_node->failing_set.test(cur_node->u) == false) {
          // case 2.1
          parent_node->failing_set = cur_node->failing_set;
          cur_node->v_idx = std::numeric_limits<Size>::max();
        } else {
          // case 2.2
          parent_node->failing_set |= cur_node->failing_set;
          cur_node->v_idx += 1;
        }
      }
    }

    Size num_extendable = u_helper->GetNumExtendable();

    while (cur_node->v_idx < num_extendable) {
      Size cs_v_idx = u_helper->GetExtendableIndex(cur_node->v_idx);
      cur_node->v = cs_.GetCandidate(cur_node->u, cs_v_idx);

      if (mapped_query_vtx_[cur_node->v] == INVALID_VTX) {
        bool has_forbidden_edge = false;
        // 检查是否存在schema中定义但query中不存在的边
        for (size_t i = 0; i < query_.no_edge_pairs[cur_node->u].size(); i++) {
          Vertex u_nbr = query_.no_edge_pairs[cur_node->u][i].first;
          Label edge_label = query_.no_edge_pairs[cur_node->u][i].second;
          if (current_mapping[u_nbr] != INVALID_VTX) {
            Size data_edge_idx = data_.GetEdgeIndex(current_mapping[u_nbr], cur_node->v, edge_label);
            if (data_edge_idx != INVALID_SZ) {
              has_forbidden_edge = true;
              break;
            }
          }
        }
        if (has_forbidden_edge) {
          cur_node->v_idx += 1;
          continue;
        }

        gbi::GraphStorage* edge_checker =
            match_leaves_ ? match_leaves_->ori_data_graph
                          : data_.graph_storage_ptr;
        bool is_valid = true;
        for (size_t i = 0; i < query_.has_out_edge_pairs[cur_node->u].size(); ++i) {
          Vertex u_nbr = query_.has_out_edge_pairs[cur_node->u][i].first;
          Label edge_label = query_.has_out_edge_pairs[cur_node->u][i].second;
          if (current_mapping[u_nbr] != INVALID_VTX && edge_checker) {
            if (!edge_checker->HasEdge(cur_node->v, current_mapping[u_nbr], edge_label)) {
              is_valid = false;
              break;
            }
          }
        }
        if (!is_valid) {
          cur_node->v_idx += 1;
          continue;
        }
        for (size_t i = 0; i < query_.has_in_edge_pairs[cur_node->u].size(); ++i) {
          Vertex u_nbr = query_.has_in_edge_pairs[cur_node->u][i].first;
          Label edge_label = query_.has_in_edge_pairs[cur_node->u][i].second;
          if (current_mapping[u_nbr] != INVALID_VTX && edge_checker) {
            if (!edge_checker->HasEdge(current_mapping[u_nbr], cur_node->v, edge_label)) {
              is_valid = false;
              break;
            }
          }
        }
        if (!is_valid) {
          cur_node->v_idx += 1;
          continue;
        }
      }

      if (mapped_query_vtx_[cur_node->v] == INVALID_VTX) {
        bool success = ComputeExtendableForAllNeighbors(cur_node, cs_v_idx);

        if (success) {
          current_mapping[cur_node->u] = cur_node->v;
        }

        if (!success) {
          // go to sibling node (need to compute failing set of parent node)
          break;
        } else if (backtrack_depth_ >= query_.GetNumNonLeafVertices()) {
          // embedding class
          uint64_t num_cur_embeddings;

          if (query_.GetNumNonLeafVertices() == query_.GetNumVertices()) {
            match_results_.push_back(current_mapping);
            num_cur_embeddings = 1;
          } else {
            num_cur_embeddings = match_leaves_->Match(limit, current_mapping, match_results_);
          }

          cur_node->embedding_founded = true;
          num_embeddings_ += num_cur_embeddings;
          current_mapping[cur_node->u] = INVALID_VTX;
          break;
        } else {
          // expand to child node
          backtrack_depth_ += 1;
          break;
        }
      } else {
        // conflict class
        if (parent_node->embedding_founded == false) {
          Vertex u_conflict = mapped_query_vtx_[cur_node->v];
          BacktrackHelper *u_conflict_helper = helpers_ + u_conflict;

          parent_node->failing_set |=
              u_helper->GetAncestor() | u_conflict_helper->GetAncestor();
        }

        cur_node->v_idx += 1;
      }
    }
    if (cur_node->v_idx >= num_extendable) {
      // go to parent node
      extendable_queue_->Insert(cur_node->u, num_extendable);
      u_helper->GetMappingState() = UNMAPPED;
      cur_node->initialized = false;

      backtrack_depth_ -= 1;
      current_mapping[cur_node->u] = INVALID_VTX;
    }
  }

  return match_results_.size();
}

Vertex Backtrack::GetRootVertex() {
  Vertex root_vertex = 0;
  Size root_cs_size = std::numeric_limits<Size>::max();

  for (Vertex u = 0; u < query_.GetNumVertices(); ++u) {
    if (query_.IsInNEC(u)) continue;

    Size u_cs_size = cs_.GetCandidateSetSize(u);

    if (root_cs_size > u_cs_size) {
      root_vertex = u;
      root_cs_size = u_cs_size;
    }
  }

  return root_vertex;
}

void Backtrack::InitializeNodeStack() {
  for (Size d = 0; d <= query_.GetNumNonLeafVertices() + 1; ++d) {
    SearchTreeNode *node = node_stack_ + d;

    node->initialized = false;
    node->failing_set.resize(query_.GetNumVertices());
  }
}

void Backtrack::ComputeExtendable(Vertex u, Vertex u_nbr, Size u_nbr_idx,
                                  Size cs_v_idx) {
  BacktrackHelper *u_nbr_helper = helpers_ + u_nbr;

  Size *extendable_indices = u_nbr_helper->GetExtendableIndices();
  Size &num_extendable = u_nbr_helper->GetNumExtendable();
  Size &num_unmapped_extendable = u_nbr_helper->GetNumUnmappedExtendable();

  if (u_nbr_helper->GetNumMappedNeighbors() == 1) {
    for (Size i = cs_.GetCandidateStartOffset(u, u_nbr_idx, cs_v_idx);
         i < cs_.GetCandidateEndOffset(u, u_nbr_idx, cs_v_idx); ++i) {
      Size v_nbr_idx = cs_.GetCandidateIndex(i);
      Vertex v_nbr = cs_.GetCandidate(u_nbr, v_nbr_idx);

      extendable_indices[num_extendable] = v_nbr_idx;
      num_extendable += 1;
      if (mapped_query_vtx_[v_nbr] == INVALID_VTX) {
        num_unmapped_extendable += 1;
      }
    }
  } else {
    // intersection
    Size i = 0;
    Size j = cs_.GetCandidateStartOffset(u, u_nbr_idx, cs_v_idx);

    Size num_prev_extendable = u_nbr_helper->GetNumPrevExtendable();
    Size candidate_end_offset =
        cs_.GetCandidateEndOffset(u, u_nbr_idx, cs_v_idx);

    Size *prev_extendable_indices = u_nbr_helper->GetPrevExtendableIndices();

    while (i < num_prev_extendable && j < candidate_end_offset) {
      Size vi = prev_extendable_indices[i];
      Size vj = cs_.GetCandidateIndex(j);

      if (vi == vj) {
        Vertex v_nbr = cs_.GetCandidate(u_nbr, vi);

        extendable_indices[num_extendable] = vi;
        num_extendable += 1;

        if (mapped_query_vtx_[v_nbr] == INVALID_VTX) {
          num_unmapped_extendable += 1;
        }

        i += 1;
        j += 1;
      } else if (vi < vj) {
        i += 1;
      } else {
        j += 1;
      }
    }
  }
}

void Backtrack::ComputeDynamicAncestor(Vertex ancsetor, Vertex child) {
  BacktrackHelper *child_helper = helpers_ + child;
  BacktrackHelper *ancestor_helper = helpers_ + ancsetor;

  child_helper->GetAncestor() |= ancestor_helper->GetAncestor();
}

bool Backtrack::ComputeExtendableForAllNeighbors(SearchTreeNode *cur_node,
                                                 Size cs_v_idx) {
  Size start_offset = query_.GetStartOffset(cur_node->u);
  Size end_offset = query_.GetEndOffset(cur_node->u);

  mapped_query_vtx_[cur_node->v] = cur_node->u;
  mapped_nodes_[cur_node->u] = cur_node;

  for (Size u_nbr_idx = start_offset; u_nbr_idx < end_offset; ++u_nbr_idx) {
    Vertex u_nbr = query_.GetNeighbor(u_nbr_idx);

    BacktrackHelper *u_nbr_helper = helpers_ + u_nbr;

    if (u_nbr_helper->GetMappingState() == MAPPED ||
        (query_.IsInNEC(u_nbr) && !query_.IsNECRepresentation(u_nbr)))
      continue;

    u_nbr_helper->AddMapping(cur_node->u);

    ComputeExtendable(cur_node->u, u_nbr, u_nbr_idx - start_offset, cs_v_idx);
    ComputeDynamicAncestor(cur_node->u, u_nbr);

    Size num_extendable = u_nbr_helper->GetNumExtendable();
    Size num_unmapped_extendable = u_nbr_helper->GetNumUnmappedExtendable();

    if (!query_.IsInNEC(u_nbr)) {
      if (u_nbr_helper->GetNumMappedNeighbors() == 1) {
        extendable_queue_->Insert(u_nbr, num_extendable);
      } else {
        extendable_queue_->UpdateWeight(u_nbr, num_extendable);
      }
    }

    if (num_unmapped_extendable == 0) {
      // compute failing set (emptyset class)
      cur_node->failing_set = u_nbr_helper->GetAncestor();

      for (Size i = 0; i < num_extendable; ++i) {
        Vertex v_nbr =
            cs_.GetCandidate(u_nbr, u_nbr_helper->GetExtendableIndex(i));
        // conflict class
        Vertex u_nbr_conflict = mapped_query_vtx_[v_nbr];
        BacktrackHelper *u_nbr_conflict_helper = helpers_ + u_nbr_conflict;
        cur_node->failing_set |= u_nbr_conflict_helper->GetAncestor();
      }

      return false;
    }
  }
  return true;
}

void Backtrack::ReleaseNeighbors(SearchTreeNode *cur_node) {
  Size start_offset = query_.GetStartOffset(cur_node->u);
  Size end_offset = query_.GetEndOffset(cur_node->u);

  for (Size u_nbr_idx = start_offset; u_nbr_idx < end_offset; ++u_nbr_idx) {
    Vertex u_nbr = query_.GetNeighbor(u_nbr_idx);

    BacktrackHelper *u_nbr_helper = helpers_ + u_nbr;

    if (u_nbr_helper->GetMappingState() == MAPPED ||
        (query_.IsInNEC(u_nbr) && !query_.IsNECRepresentation(u_nbr)))
      continue;

    if (u_nbr_helper->GetLastMappedNeighbor() != cur_node->u) break;

    Size num_prev_extendable = u_nbr_helper->GetNumPrevExtendable();

    u_nbr_helper->RemoveMapping();

    if (!query_.IsInNEC(u_nbr)) {
      if (u_nbr_helper->GetNumMappedNeighbors() == 0) {
        extendable_queue_->Remove(u_nbr);
      } else {
        extendable_queue_->UpdateWeight(u_nbr, num_prev_extendable);
      }
    }
  }
  mapped_query_vtx_[cur_node->v] = INVALID_VTX;
}
}  // namespace daf
