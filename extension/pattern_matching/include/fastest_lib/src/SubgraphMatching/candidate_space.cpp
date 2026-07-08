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

#include "SubgraphMatching/candidate_space.h"

#include <chrono>
#include <cstring>
#include <sstream>

namespace neug {
namespace pattern_matching {
namespace graphlib {
namespace SubgraphMatching {

void WarnUnknownConstraintPropertyOnce(const std::string& key,
                                       const std::string& message) {
  static std::mutex mu;
  static std::unordered_set<std::string> seen;
  bool fresh = false;
  {
    std::lock_guard<std::mutex> lk(mu);
    fresh = seen.insert(key).second;
  }
  if (fresh) {
    LOG(WARNING) << message;
  }
}

std::vector<int>& CandidateSpace::GetCandidateNeighbors(int cur, int cand_idx,
                                                        int nxt) {
  if (query_->GetEdgeIndex(cur, nxt) != -1) {
    return GetOutCandidateNeighbors(cur, cand_idx, nxt);
  }
  return GetInCandidateNeighbors(cur, cand_idx, nxt);
}

int CandidateSpace::GetCandidateNeighbor(int cur, int cand_idx, int nxt,
                                         int nxt_idx) {
  if (query_->GetEdgeIndex(cur, nxt) != -1) {
    return GetOutCandidateNeighbor(cur, cand_idx, nxt, nxt_idx);
  }
  return GetInCandidateNeighbor(cur, cand_idx, nxt, nxt_idx);
}

bool CandidateSpace::NeighborFilter(int cur, int cand) {
  switch (opt.neighborhood_filter) {
  case NEIGHBOR_SAFETY:
    return CheckNeighborSafety(cur, cand);
  case NEIGHBOR_BIPARTITE_SAFETY:
    return NeighborBipartiteSafety(cur, cand);
  case EDGE_BIPARTITE_SAFETY:
    return EdgeBipartiteSafety(cur, cand);
  default:
    return true;
  }
}

bool CandidateSpace::StructureFilter(int query_edge_id, int data_edge_id) {
  switch (opt.structure_filter) {
  case NO_STRUCTURE_FILTER:
    return true;
  case TRIANGLE_SAFETY:
    return TriangleSafety(query_edge_id, data_edge_id);
  case FOURCYCLE_SAFETY:
    return TriangleSafety(query_edge_id, data_edge_id) and
           FourCycleSafety(query_edge_id, data_edge_id);
  }
  return true;
}

CandidateSpace::CandidateSpace(const neug::StorageReadInterface& graph,
                               DataGraphMeta& data_meta,
                               SubgraphMatchingOption filter_option)
    : graph_(graph), data_meta_(data_meta) {
  opt = filter_option;
  BitsetCS = new bool*[opt.MAX_QUERY_VERTEX];
  for (int i = 0; i < opt.MAX_QUERY_VERTEX; i++) {
    BitsetCS[i] = new bool[data_meta_.GetNumVertices()]();
  }
  // Use vector of unordered_set for edge candidacy (sparse storage)
  BitsetEdgeCS.resize(opt.MAX_QUERY_EDGE);
  out_neighbor_cs = new bool[data_meta_.GetNumVertices()]();
  in_neighbor_cs = new bool[data_meta_.GetNumVertices()]();
  out_neighbor_label_frequency.resize(data_meta_.GetNumLabels());
  in_neighbor_label_frequency.resize(data_meta_.GetNumLabels());
  num_visit_cs_ = new int[data_meta_.GetNumVertices()]();
  candidate_set_.resize(opt.MAX_QUERY_VERTEX);
  BPSolver.Initialize(50, data_meta_.GetMaxDegree(), 50);
  VLOG(1) << "[CSBuild] Constructing candidate space: max_query_vertex="
          << opt.MAX_QUERY_VERTEX << ", max_query_edge=" << opt.MAX_QUERY_EDGE;
}

CandidateSpace::~CandidateSpace() {
  for (int i = 0; i < opt.MAX_QUERY_VERTEX; i++) {
    delete[] BitsetCS[i];
  }
  delete[] BitsetCS;
  // BitsetEdgeCS is a vector, no manual delete needed
  delete[] num_visit_cs_;
  delete[] out_neighbor_cs;
  delete[] in_neighbor_cs;
}

bool CandidateSpace::BuildCS(PatternGraph* query) {
  CSInfo.clear();
  query_ = query;
  num_candidate_vertex = num_candidate_edge = 0;
  for (int i = 0; i < query_->GetNumVertices(); i++) {
    memset(BitsetCS[i], false, data_meta_.GetNumVertices());
  }
  // Clear edge candidate sets
  for (int i = 0; i < query_->GetNumEdges(); i++) {
    BitsetEdgeCS[i].clear();
  }
  memset(num_visit_cs_, 0, data_meta_.GetNumVertices());
  BPSolver.Reset();
  for (int i = 0; i < query_->GetNumVertices(); i++) {
    candidate_set_[i].clear();
  }
  using hrc = std::chrono::high_resolution_clock;
  auto cs_start = hrc::now();

  auto p_start = hrc::now();
  if (!BuildInitialCS())
    return false;
  auto p_end = hrc::now();
  double init_ms =
      std::chrono::duration<double, std::milli>(p_end - p_start).count();
  CSInfo["CS_InitialCS_ms"] = init_ms;
  VLOG(1) << "[CSBuild] BuildInitialCS: " << init_ms << " ms";

  p_start = hrc::now();
  if (!RefineCS())
    return false;
  p_end = hrc::now();
  double refine_ms =
      std::chrono::duration<double, std::milli>(p_end - p_start).count();
  CSInfo["CS_RefineCS_ms"] = refine_ms;
  VLOG(1) << "[CSBuild] RefineCS: " << refine_ms << " ms";

  p_start = hrc::now();
  ConstructCS();
  p_end = hrc::now();
  double construct_ms =
      std::chrono::duration<double, std::milli>(p_end - p_start).count();
  CSInfo["CS_ConstructCS_ms"] = construct_ms;
  VLOG(1) << "[CSBuild] ConstructCS: " << construct_ms << " ms";

  double total_ms =
      std::chrono::duration<double, std::milli>(hrc::now() - cs_start).count();
  CSInfo["CSBuildTime"] = total_ms;
  CSInfo["#CSVertex"] = num_candidate_vertex;
  CSInfo["#CSEdge"] = num_candidate_edge;
  return true;
}

void CandidateSpace::ConstructCS() {
  using hrc_cs = std::chrono::high_resolution_clock;
  auto cs_init_start = hrc_cs::now();

  out_candidate_neighbors.clear();
  out_candidate_neighbors.resize(query_->GetNumVertices());
  in_candidate_neighbors.clear();
  in_candidate_neighbors.resize(query_->GetNumVertices());

  std::vector<int> candidate_index(data_meta_.GetNumVertices(), -1);
  std::vector<int> candidate_index2(data_meta_.GetNumVertices(), -1);

  for (int i = 0; i < query_->GetNumVertices(); ++i) {
    out_candidate_neighbors[i].resize(GetCandidateSetSize(i));
    in_candidate_neighbors[i].resize(GetCandidateSetSize(i));
  }

  auto cs_init_end = hrc_cs::now();
  VLOG(1) << "[CSBuild] ConstructCS::Init: "
          << std::chrono::duration<double, std::milli>(cs_init_end -
                                                       cs_init_start)
                 .count()
          << " ms";

  double t_in_edges = 0, t_out_edges = 0;
  for (int u = 0; u < query_->GetNumVertices(); u++) {
    int u_label = query_->GetVertexLabel(u);
    int u_in_degree = query_->GetInDegree(u);
    int u_out_degree = query_->GetOutDegree(u);
    num_candidate_vertex += GetCandidateSetSize(u);

    for (int idx = 0; idx < GetCandidateSetSize(u); idx++) {
      candidate_index[candidate_set_[u][idx]] = idx;
      out_candidate_neighbors[u][idx].resize(u_out_degree);
      in_candidate_neighbors[u][idx].resize(u_in_degree);
    }

    auto cs_u_start = hrc_cs::now();
    // --- Process in-edges: query edge uc -> u ---
    for (int uc : query_->GetInNeighbors(u)) {
      int uc_label = query_->GetVertexLabel(uc);
      int query_edge_idx = query_->GetEdgeIndex(uc, u);
      // Adaptive direction: compare candidate density
      // Option A: iterate uc's candidates, GetOutIncidentEdges(vc, u_label)
      // Option B: iterate u's candidates, GetInIncidentEdges(v, uc_label)
      size_t V_uc = data_meta_.GetVerticesByLabel(uc_label).size();
      size_t V_u = data_meta_.GetVerticesByLabel(u_label).size();
      double cost_a =
          V_uc > 0 ? (double) candidate_set_[uc].size() / V_uc : 1e18;
      double cost_b = V_u > 0 ? (double) candidate_set_[u].size() / V_u : 1e18;

      if (cost_a <= cost_b) {
        // Direction A: iterate uc's candidates, use GetOutIncidentEdges
        // (src-side)
        for (size_t vc_idx = 0; vc_idx < candidate_set_[uc].size(); ++vc_idx) {
          int vc = candidate_set_[uc][vc_idx];
          for (const auto& data_edge :
               data_meta_.GetOutIncidentEdges(vc, u_label)) {
            int v = data_meta_.GetDestPoint(data_edge);
            if (data_meta_.GetInDegree(v) < u_in_degree ||
                data_meta_.GetOutDegree(v) < u_out_degree)
              continue;
            EdgeKey data_edge_key = data_meta_.EdgeToIntKey(data_edge);
            if (BitsetEdgeCS[query_edge_idx].find(data_edge_key) ==
                BitsetEdgeCS[query_edge_idx].end())
              continue;
            int v_idx = candidate_index[v];
            if (v_idx == -1)
              continue;
            num_candidate_edge++;
            in_candidate_neighbors[u][v_idx][query_->GetInAdjIdx(u, uc)]
                .emplace_back(vc_idx);
          }
        }
      } else {
        // Direction B: iterate u's candidates, use GetInIncidentEdges
        // (dst-side)
        for (size_t idx = 0; idx < candidate_set_[uc].size(); ++idx)
          candidate_index2[candidate_set_[uc][idx]] = idx;
        for (int v_idx = 0; v_idx < (int) candidate_set_[u].size(); ++v_idx) {
          int v = candidate_set_[u][v_idx];
          for (const auto& data_edge :
               data_meta_.GetInIncidentEdges(v, uc_label)) {
            int vc = data_meta_.GetSourcePoint(data_edge);
            if (data_meta_.GetInDegree(vc) < query_->GetInDegree(uc) ||
                data_meta_.GetOutDegree(vc) < query_->GetOutDegree(uc))
              continue;
            EdgeKey data_edge_key = data_meta_.EdgeToIntKey(data_edge);
            if (BitsetEdgeCS[query_edge_idx].find(data_edge_key) ==
                BitsetEdgeCS[query_edge_idx].end())
              continue;
            int vc_idx = candidate_index2[vc];
            if (vc_idx == -1)
              continue;
            num_candidate_edge++;
            in_candidate_neighbors[u][v_idx][query_->GetInAdjIdx(u, uc)]
                .emplace_back(vc_idx);
          }
        }
        for (size_t idx = 0; idx < candidate_set_[uc].size(); ++idx)
          candidate_index2[candidate_set_[uc][idx]] = -1;
      }
    }

    auto cs_u_mid = hrc_cs::now();
    t_in_edges +=
        std::chrono::duration<double, std::milli>(cs_u_mid - cs_u_start)
            .count();
    // --- Process out-edges: query edge u -> uc ---
    for (int uc : query_->GetOutNeighbors(u)) {
      int uc_label = query_->GetVertexLabel(uc);
      int query_edge_idx = query_->GetEdgeIndex(u, uc);
      // Adaptive direction: compare candidate density
      // Option A: iterate u's candidates, GetOutIncidentEdges(v, uc_label)
      // Option B: iterate uc's candidates, GetInIncidentEdges(vc, u_label)
      size_t V_u = data_meta_.GetVerticesByLabel(u_label).size();
      size_t V_uc = data_meta_.GetVerticesByLabel(uc_label).size();
      double cost_a = V_u > 0 ? (double) candidate_set_[u].size() / V_u : 1e18;
      double cost_b =
          V_uc > 0 ? (double) candidate_set_[uc].size() / V_uc : 1e18;

      if (cost_a <= cost_b) {
        // Direction A: iterate u's candidates, use GetOutIncidentEdges
        // (src-side)
        for (size_t idx = 0; idx < candidate_set_[uc].size(); ++idx)
          candidate_index2[candidate_set_[uc][idx]] = idx;
        for (int v_idx = 0; v_idx < (int) candidate_set_[u].size(); ++v_idx) {
          int v = candidate_set_[u][v_idx];
          for (const auto& data_edge :
               data_meta_.GetOutIncidentEdges(v, uc_label)) {
            int vc = data_meta_.GetDestPoint(data_edge);
            if (data_meta_.GetInDegree(vc) < query_->GetInDegree(uc) ||
                data_meta_.GetOutDegree(vc) < query_->GetOutDegree(uc))
              continue;
            EdgeKey data_edge_key = data_meta_.EdgeToIntKey(data_edge);
            if (BitsetEdgeCS[query_edge_idx].find(data_edge_key) ==
                BitsetEdgeCS[query_edge_idx].end())
              continue;
            int vc_idx = candidate_index2[vc];
            if (vc_idx == -1)
              continue;
            num_candidate_edge++;
            out_candidate_neighbors[u][v_idx][query_->GetOutAdjIdx(u, uc)]
                .emplace_back(vc_idx);
          }
        }
        for (size_t idx = 0; idx < candidate_set_[uc].size(); ++idx)
          candidate_index2[candidate_set_[uc][idx]] = -1;
      } else {
        // Direction B: iterate uc's candidates, use GetInIncidentEdges
        // (dst-side)
        for (size_t vc_idx = 0; vc_idx < candidate_set_[uc].size(); ++vc_idx) {
          int vc = candidate_set_[uc][vc_idx];
          for (const auto& data_edge :
               data_meta_.GetInIncidentEdges(vc, u_label)) {
            int v = data_meta_.GetSourcePoint(data_edge);
            if (data_meta_.GetInDegree(v) < u_in_degree ||
                data_meta_.GetOutDegree(v) < u_out_degree)
              continue;
            EdgeKey data_edge_key = data_meta_.EdgeToIntKey(data_edge);
            if (BitsetEdgeCS[query_edge_idx].find(data_edge_key) ==
                BitsetEdgeCS[query_edge_idx].end())
              continue;
            int v_idx = candidate_index[v];
            if (v_idx == -1)
              continue;
            num_candidate_edge++;
            out_candidate_neighbors[u][v_idx][query_->GetOutAdjIdx(u, uc)]
                .emplace_back(vc_idx);
          }
        }
      }
    }

    auto cs_u_end = hrc_cs::now();
    t_out_edges +=
        std::chrono::duration<double, std::milli>(cs_u_end - cs_u_mid).count();
    for (int idx = 0; idx < GetCandidateSetSize(u); idx++) {
      candidate_index[candidate_set_[u][idx]] = -1;
    }
  }
  VLOG(1) << "[CSBuild] ConstructCS::InEdges: " << t_in_edges
          << " ms, OutEdges: " << t_out_edges << " ms";
}

bool CandidateSpace::BuildInitialCS() {
  using hrc_init = std::chrono::high_resolution_clock;
  std::memset(num_visit_cs_, 0, data_meta_.GetNumVertices() * sizeof(int));
  std::vector<int> initial_cs_size(query_->GetNumVertices(), 0);
  std::vector<std::vector<int>> built_out_neighbors(query_->GetNumVertices());
  std::vector<std::vector<int>> built_in_neighbors(query_->GetNumVertices());
  int root = 0;
  for (int i = 0; i < query_->GetNumVertices(); i++) {
    int l = query_->GetVertexLabel(i);
    initial_cs_size[i] = data_meta_.GetVerticesByLabel(l).size();
    if (initial_cs_size[i] <= initial_cs_size[0]) {
      root = i;
    }
  }
  InitRootCandidates(root);
  auto bfs_start = hrc_init::now();
  // For directed graphs: separate out and in neighbors
  for (int uc : query_->GetOutNeighbors(root)) {
    // root -> uc, so uc has root as in-neighbor
    built_in_neighbors[uc].push_back(root);
  }
  for (int uc : query_->GetInNeighbors(root)) {
    // uc -> root, so uc has root as out-neighbor
    built_out_neighbors[uc].push_back(root);
  }
  for (int i = 1; i < query_->GetNumVertices(); i++) {
    int cur = -1;
    for (int j = 0; j < query_->GetNumVertices(); j++) {
      if (!candidate_set_[j].empty())
        continue;
      if (cur == -1) {
        cur = j;
        continue;
      }
      int j_total =
          built_out_neighbors[j].size() + built_in_neighbors[j].size();
      int cur_total =
          built_out_neighbors[cur].size() + built_in_neighbors[cur].size();
      if (j_total > cur_total) {
        cur = j;
        continue;
      }
      if (j_total == cur_total && initial_cs_size[j] < initial_cs_size[cur]) {
        cur = j;
        continue;
      }
    }
    int cur_label = query_->GetVertexLabel(cur);
    int num_parent = 0;

    // Process in-neighbors (parent -> cur edges)
    for (int parent : built_in_neighbors[cur]) {
      int query_edge_idx = query_->GetEdgeIndex(parent, cur);
      for (int parent_cand : candidate_set_[parent]) {
        // parent -> cur, so get out-edges from parent_cand
        for (const auto& data_edge :
             data_meta_.GetOutIncidentEdges(parent_cand, cur_label)) {
          int cand = data_meta_.GetDestPoint(data_edge);
          if (data_meta_.GetInDegree(cand) < query_->GetInDegree(cur) ||
              data_meta_.GetOutDegree(cand) < query_->GetOutDegree(cur))
            continue;
          if (data_meta_.GetEdgeLabel(data_edge) !=
              query_->GetEdgeLabel(query_edge_idx))
            continue;
          if (num_visit_cs_[cand] < num_parent)
            continue;
          if (data_meta_.GetCoreNum(cand) < query_->GetCoreNum(cur))
            continue;
          if (!CheckVertexPropertyConstraints(cur, cand))
            continue;
          if (!CheckEdgePropertyConstraints(query_edge_idx, data_edge))
            continue;
          if (num_visit_cs_[cand] == num_parent) {
            num_visit_cs_[cand] += 1;
            if (num_visit_cs_[cand] == 1) {
              candidate_set_[cur].emplace_back(cand);
              BitsetCS[cur][cand] = true;
            }
          }
        }
      }
      num_parent++;
    }

    // Process out-neighbors (cur -> parent edges)
    // Query edge: cur -> parent. Data: cand -> parent_cand.
    // Option A (from cand side): would need to iterate all vertices with
    // cur_label — not useful here. We use GetInIncidentEdges(parent_cand,
    // cur_label) to find candidates for cur. No adaptive direction needed here
    // since we're building candidate_set_[cur] from scratch and must iterate
    // parent's candidates.
    for (int parent : built_out_neighbors[cur]) {
      int query_edge_idx = query_->GetEdgeIndex(cur, parent);
      for (int parent_cand : candidate_set_[parent]) {
        for (const auto& data_edge :
             data_meta_.GetInIncidentEdges(parent_cand, cur_label)) {
          int cand = data_meta_.GetSourcePoint(data_edge);
          if (data_meta_.GetInDegree(cand) < query_->GetInDegree(cur) ||
              data_meta_.GetOutDegree(cand) < query_->GetOutDegree(cur))
            continue;
          if (data_meta_.GetEdgeLabel(data_edge) !=
              query_->GetEdgeLabel(query_edge_idx))
            continue;
          if (num_visit_cs_[cand] < num_parent)
            continue;
          if (data_meta_.GetCoreNum(cand) < query_->GetCoreNum(cur))
            continue;
          if (!CheckVertexPropertyConstraints(cur, cand))
            continue;
          if (!CheckEdgePropertyConstraints(query_edge_idx, data_edge))
            continue;
          if (num_visit_cs_[cand] == num_parent) {
            num_visit_cs_[cand] += 1;
            if (num_visit_cs_[cand] == 1) {
              candidate_set_[cur].emplace_back(cand);
              BitsetCS[cur][cand] = true;
            }
          }
        }
      }
      num_parent++;
    }

    for (size_t j = 0; j < candidate_set_[cur].size(); j++) {
      int cand = candidate_set_[cur][j];
      BitsetCS[cur][cand] = false;
      if (num_visit_cs_[cand] == num_parent) {
        BitsetCS[cur][cand] = true;
      } else {
        candidate_set_[cur][j] = candidate_set_[cur].back();
        candidate_set_[cur].pop_back();
        j--;
      }
      num_visit_cs_[cand] = 0;
    }
    if (candidate_set_[cur].empty()) {
      LOG(WARNING)
          << "[CSBuild] Empty candidate set during initial CS construction "
          << "(pos 1), query_vertex=" << cur;
      return false;
    }
    // Update built neighbors for unvisited neighbors
    for (int uc : query_->GetOutNeighbors(cur)) {
      if (candidate_set_[uc].empty()) {
        // cur -> uc, so uc has cur as in-neighbor
        built_in_neighbors[uc].push_back(cur);
      }
    }
    for (int uc : query_->GetInNeighbors(cur)) {
      if (candidate_set_[uc].empty()) {
        // uc -> cur, so uc has cur as out-neighbor
        built_out_neighbors[uc].push_back(cur);
      }
    }
  }

  auto bfs_end = hrc_init::now();
  VLOG(1)
      << "[CSBuild] InitialCS::BFS_Expansion: "
      << std::chrono::duration<double, std::milli>(bfs_end - bfs_start).count()
      << " ms";

  auto ec_start = hrc_init::now();
  int cs_edge = 0, cs_vertex = 0;
  for (int i = 0; i < query_->GetNumVertices(); i++) {
    // For directed graphs: use out-edges for edge candidacy
    for (int q_edge_idx : query_->GetAllOutIncidentEdges(i)) {
      int q_nxt = query_->GetOppositePoint(q_edge_idx);
      for (int j : candidate_set_[i]) {
        for (const auto& d_edge :
             data_meta_.GetOutIncidentEdges(j, query_->GetVertexLabel(q_nxt))) {
          if (data_meta_.GetEdgeLabel(d_edge) !=
              query_->GetEdgeLabel(q_edge_idx))
            continue;

          if (!CheckEdgePropertyConstraints(q_edge_idx, d_edge))
            continue;

          int d_nxt = data_meta_.GetDestPoint(d_edge);
          if (BitsetCS[q_nxt][d_nxt]) {
            BitsetEdgeCS[q_edge_idx].insert(data_meta_.EdgeToIntKey(d_edge));
            cs_edge++;
          }
        }
      }
    }
    cs_vertex += candidate_set_[i].size();
  }
  auto ec_end = hrc_init::now();
  VLOG(1)
      << "[CSBuild] InitialCS::EdgeCandidacy: "
      << std::chrono::duration<double, std::milli>(ec_end - ec_start).count()
      << " ms (cs_edge=" << cs_edge << ", cs_vertex=" << cs_vertex << ")";

  return true;
}

bool CandidateSpace::InitRootCandidates(int root) {
  int root_label = query_->GetVertexLabel(root);
  for (int cand : data_meta_.GetVerticesByLabel(root_label)) {
    // Directed graph: check both in-degree and out-degree
    if (data_meta_.GetInDegree(cand) < query_->GetInDegree(root) ||
        data_meta_.GetOutDegree(cand) < query_->GetOutDegree(root))
      continue;
    if (data_meta_.GetCoreNum(cand) < query_->GetCoreNum(root))
      continue;

    // 添加属性约束检查
    if (!CheckVertexPropertyConstraints(root, cand))
      continue;

    candidate_set_[root].emplace_back(cand);
    BitsetCS[root][cand] = true;
  }
  return !candidate_set_[root].empty();
}

bool CandidateSpace::RefineCS() {
  std::vector<int> local_stage(query_->GetNumVertices(), 0);
  std::vector<double> priority(query_->GetNumVertices(), 0.50);
  int queue_pop_count = 0;
  int maximum_queue_cnt = 5 * query_->GetNumEdges();
  int current_stage = 0;
  while (queue_pop_count < maximum_queue_cnt) {
    int cur = 0;
    double cur_priority = priority[cur];
    for (int i = 1; i < query_->GetNumVertices(); i++) {
      if (priority[i] > cur_priority) {
        cur_priority = priority[i];
        cur = i;
      } else if (priority[i] == cur_priority) {
        if (GetCandidateSetSize(i) > GetCandidateSetSize(cur)) {
          cur = i;
        }
      }
    }
    if (cur_priority < 0.1)
      break;
    current_stage++;
    queue_pop_count += query_->GetDegree(cur);
    int bef_cand_size = candidate_set_[cur].size();
    if (opt.neighborhood_filter == NEIGHBOR_SAFETY) {
      std::fill(out_neighbor_label_frequency.begin(),
                out_neighbor_label_frequency.end(), 0);
      std::fill(in_neighbor_label_frequency.begin(),
                in_neighbor_label_frequency.end(), 0);
      memset(out_neighbor_cs, false, data_meta_.GetNumVertices());
      memset(in_neighbor_cs, false, data_meta_.GetNumVertices());
      PrepareNeighborSafety(cur);
    }
    for (int i = 0; i < candidate_set_[cur].size(); i++) {
      int cand = candidate_set_[cur][i];
      bool valid = true;
      if (valid and opt.structure_filter > NO_STRUCTURE_FILTER)
        valid = CheckSubStructures(cur, cand);
      if (valid)
        valid = NeighborFilter(cur, cand);
      if (!valid) {
        int removed = candidate_set_[cur][i];
        for (int query_edge_idx : query_->GetAllOutIncidentEdges(cur)) {
          int nxt = query_->GetOppositePoint(query_edge_idx);
          int nxt_label = query_->GetVertexLabel(nxt);
          for (const auto& data_edge :
               data_meta_.GetOutIncidentEdges(removed, nxt_label)) {
            int nxt_cand = data_meta_.GetDestPoint(data_edge);
            if (data_meta_.GetInDegree(nxt_cand) < query_->GetInDegree(nxt) ||
                data_meta_.GetOutDegree(nxt_cand) < query_->GetOutDegree(nxt))
              continue;
            BitsetEdgeCS[query_edge_idx].erase(
                data_meta_.EdgeToIntKey(data_edge));
          }
        }
        candidate_set_[cur][i] = candidate_set_[cur].back();
        candidate_set_[cur].pop_back();
        --i;
        BitsetCS[cur][cand] = false;
      }
    }
    if (candidate_set_[cur].empty()) {
      LOG(WARNING)
          << "[CSBuild] Empty candidate set during initial CS construction "
          << "(pos 2), query_vertex=" << cur;
      return false;
    }
    int aft_cand_size = candidate_set_[cur].size();

    if (aft_cand_size == bef_cand_size) {
      priority[cur] = 0;
      continue;
    }
    double out_prob = 1 - aft_cand_size * 1.0 / bef_cand_size;
    priority[cur] = 0;
    for (int nxt : query_->GetNeighbors(cur)) {
      priority[nxt] = 1 - (1 - out_prob) * (1 - priority[nxt]);
      local_stage[nxt] = current_stage;
    }
  }
  return true;
}

void CandidateSpace::PrepareNeighborSafety(int cur) {
  // For directed graphs: separately count out-neighbors and in-neighbors
  // out_neighbor_label_frequency: labels of cur's out-neighbors (cur ->
  // neighbor) in_neighbor_label_frequency: labels of cur's in-neighbors
  // (neighbor -> cur)
  for (int q_neighbor : query_->GetOutNeighbors(cur)) {
    out_neighbor_label_frequency[query_->GetVertexLabel(q_neighbor)]++;
    for (int d_neighbor : candidate_set_[q_neighbor]) {
      out_neighbor_cs[d_neighbor] = true;
    }
  }
  for (int q_neighbor : query_->GetInNeighbors(cur)) {
    in_neighbor_label_frequency[query_->GetVertexLabel(q_neighbor)]++;
    for (int d_neighbor : candidate_set_[q_neighbor]) {
      in_neighbor_cs[d_neighbor] = true;
    }
  }
}

bool CandidateSpace::CheckNeighborSafety(int cur, int cand) {
  // For each label with a positive frequency requirement, directly count
  // masked neighbors of that label using
  // GetOutNeighborCountMasked/GetInNeighborCountMasked. This avoids iterating
  // ALL neighbors and the modify/restore pattern.
  for (int l = 0; l < data_meta_.GetNumLabels(); ++l) {
    if (out_neighbor_label_frequency[l] > 0) {
      int count =
          data_meta_.GetOutNeighborCountMasked(cand, l, out_neighbor_cs);
      if (count < out_neighbor_label_frequency[l])
        return false;
    }
    if (in_neighbor_label_frequency[l] > 0) {
      int count = data_meta_.GetInNeighborCountMasked(cand, l, in_neighbor_cs);
      if (count < in_neighbor_label_frequency[l])
        return false;
    }
  }
  return true;
}

bool CandidateSpace::EdgeCandidacy(int query_edge_id,
                                   const EdgeKey& data_edge_key) {
  if (query_edge_id == -1 || data_edge_key.invalid()) {
    return false;
  }
  return BitsetEdgeCS[query_edge_id].count(data_edge_key) > 0;
}

// Triangle and FourCycle safety checks - disabled as DataGraphMeta doesn't
// store local cycles
// TODO: Implement when DataGraphMeta supports cycle enumeration
bool CandidateSpace::TriangleSafety(int query_edge_id, int data_edge_id) {
  (void) query_edge_id;
  (void) data_edge_id;
  // Substructure filtering disabled - DataGraphMeta doesn't have
  // GetLocalTriangles
  return true;
}

bool CandidateSpace::FourCycleSafety(int query_edge_id, int data_edge_id) {
  (void) query_edge_id;
  (void) data_edge_id;
  // Substructure filtering disabled - DataGraphMeta doesn't have
  // GetLocalFourCycles
  return true;
}

bool CandidateSpace::CheckSubStructures(int cur, int cand) {
  // For directed graphs: check out-edges from cur
  for (int query_edge_idx : query_->GetAllOutIncidentEdges(cur)) {
    int nxt = query_->GetOppositePoint(query_edge_idx);
    int nxt_label = query_->GetVertexLabel(nxt);
    bool found = false;
    for (const auto& data_edge :
         data_meta_.GetOutIncidentEdges(cand, nxt_label)) {
      int nxt_cand = data_meta_.GetDestPoint(data_edge);
      EdgeKey data_edge_key = data_meta_.EdgeToIntKey(data_edge);
      // Directed graph: check both in-degree and out-degree for nxt
      if (data_meta_.GetInDegree(nxt_cand) < query_->GetInDegree(nxt) ||
          data_meta_.GetOutDegree(nxt_cand) < query_->GetOutDegree(nxt))
        continue;
      if (BitsetEdgeCS[query_edge_idx].find(data_edge_key) ==
          BitsetEdgeCS[query_edge_idx].end())
        continue;
      if (!StructureFilter(query_edge_idx,
                           0)) {  // data_edge_id not used in disabled filters
        BitsetEdgeCS[query_edge_idx].erase(data_edge_key);
        // No opposite edge in directed graph
        continue;
      }
      found = true;
    }
    if (!found) {
      return false;
    }
  }
  return true;
}

bool CandidateSpace::NeighborBipartiteSafety(int cur, int cand) {
  // For directed graphs: check out-edges
  if (query_->GetOutDegree(cur) == 1) {
    int uc = query_->GetOutNeighbors(cur)[0];
    int query_edge_index = query_->GetEdgeIndex(cur, uc);
    int label = query_->GetVertexLabel(uc);
    auto edges = data_meta_.GetOutIncidentEdges(cand, label);
    for (const auto& data_edge : edges) {
      if (BitsetEdgeCS[query_edge_index].count(
              data_meta_.EdgeToIntKey(data_edge)) > 0) {
        return true;
      }
    }
    return false;
  }
  BPSolver.Reset();
  int i = 0, j = 0;
  for (int query_edge_index : query_->GetAllOutIncidentEdges(cur)) {
    j = 0;
    for (const auto& data_edge : data_meta_.GetAllOutIncidentEdges(cand)) {
      if (BitsetEdgeCS[query_edge_index].count(
              data_meta_.EdgeToIntKey(data_edge)) > 0) {
        BPSolver.AddEdge(i, j);
      }
      j++;
    }
    i++;
  }
  return BPSolver.Solve() == query_->GetOutDegree(cur);
}

bool CandidateSpace::EdgeBipartiteSafety(int cur, int cand) {
  // For directed graphs: use out-edges
  auto query_edges = query_->GetAllOutIncidentEdges(cur);
  auto data_edges_vec = data_meta_.GetAllOutIncidentEdges(cand);
  // Convert data edges to integer keys for BitsetEdgeCS access
  std::vector<EdgeKey> data_edge_keys;
  data_edge_keys.reserve(data_edges_vec.size());
  for (const auto& e : data_edges_vec) {
    data_edge_keys.push_back(data_meta_.EdgeToIntKey(e));
  }

  if (query_edges.size() == 1) {
    int q_edge_id = query_edges[0];
    for (const auto& d_edge_key : data_edge_keys) {
      if (BitsetEdgeCS[q_edge_id].count(d_edge_key) > 0)
        return true;
    }
    return false;
  }
  std::vector<std::pair<int, int>> edge_pairs;
  int ii = 0, jj = 0;
  BPSolver.Reset();
  for (int query_edge_index : query_edges) {
    int uc = query_->GetOppositePoint(query_edge_index);
    jj = 0;
    for (size_t idx = 0; idx < data_edges_vec.size(); ++idx) {
      const auto& data_edge = data_edges_vec[idx];
      const EdgeKey& edge_key = data_edge_keys[idx];
      int vc = data_meta_.GetDestPoint(data_edge);
      // Directed graph: check both in-degree and out-degree for uc
      if (data_meta_.GetInDegree(vc) < query_->GetInDegree(uc) ||
          data_meta_.GetOutDegree(vc) < query_->GetOutDegree(uc)) {
        jj++;
        continue;
      }
      if (BitsetEdgeCS[query_edge_index].count(edge_key) > 0) {
        BPSolver.AddEdge(ii, jj);
        edge_pairs.emplace_back(ii, jj);
      }
      jj++;
    }
    ii++;
  }
  bool b = BPSolver.FindUnmatchableEdges(query_edges.size());
  if (!b) {
    return false;
  }
  for (auto& [i, j] : edge_pairs) {
    if (!BPSolver.matchable[i][j]) {
      int left_unmatch = query_edges[i];
      const EdgeKey& right_unmatch_key = data_edge_keys[j];
      BitsetEdgeCS[left_unmatch].erase(right_unmatch_key);
      // No opposite edge in directed graph
    }
  }
  return true;
}

// Property constraint checking using graph_ interface
bool CandidateSpace::CheckVertexPropertyConstraints(int query_vertex,
                                                    int data_vertex) {
  // If no property constraints, return true
  if (query_->vertex_property_constraints.empty()) {
    return true;
  }

  // Check if query vertex index is within constraint range
  if (query_vertex >= (int) query_->vertex_property_constraints.size()) {
    return true;  // No constraints for this vertex
  }

  const std::vector<PropCons>& constraints =
      query_->vertex_property_constraints[query_vertex];

  // If this vertex has no constraints, return true
  if (constraints.empty()) {
    return true;
  }

  // Get the (label, local_vid) from global vertex id
  auto [data_label, data_vid] = data_meta_.ToLocalId(data_vertex);

  // Get property names for this vertex label from schema
  const auto& schema = graph_.schema();
  std::vector<std::string> prop_names =
      schema.get_vertex_property_names(data_label);

  // Check all constraints - all must be satisfied
  for (const auto& constraint : constraints) {
    // Skip constraints with empty property name
    if (constraint._prop_name.empty()) {
      WarnUnknownConstraintPropertyOnce(
          "v:empty",
          "[SAMPLED_PATTERN_MATCH] Vertex constraint has empty property name; "
          "constraint silently skipped.");
      continue;
    }

    // Find property name index
    auto it =
        std::find(prop_names.begin(), prop_names.end(), constraint._prop_name);
    if (it == prop_names.end()) {
      // Property not found on this vertex label. Skip this constraint
      // and warn once per (label, property) pair so users notice typos
      // rather than getting silently-wrong (over-broad) candidate sets.
      std::ostringstream key;
      key << "v:" << static_cast<int>(data_label) << ":"
          << constraint._prop_name;
      std::ostringstream msg;
      msg << "[SAMPLED_PATTERN_MATCH] Vertex constraint property '"
          << constraint._prop_name << "' not found on vertex label id "
          << static_cast<int>(data_label) << "; constraint silently skipped.";
      WarnUnknownConstraintPropertyOnce(key.str(), msg.str());
      continue;
    }

    int prop_idx = std::distance(prop_names.begin(), it);

    // Get property value from graph
    Value data_value = graph_.GetVertexProperty(data_label, data_vid, prop_idx);

    // Check if value satisfies constraint
    if (!CheckValueConstraint(data_value, constraint._comp_type,
                              constraint._value)) {
      return false;  // Constraint not satisfied
    }
  }

  return true;  // All constraints satisfied
}

bool CandidateSpace::CheckEdgePropertyConstraints(
    int query_edge_id, const DataGraphMeta::Edge& data_edge) {
  // If no edge property constraints, return true
  if (query_->edge_property_constraints.empty()) {
    return true;
  }

  // Check if query edge index is within constraint range
  if (query_edge_id >= (int) query_->edge_property_constraints.size()) {
    return true;  // No constraints for this edge
  }

  const std::vector<PropCons>& constraints =
      query_->edge_property_constraints[query_edge_id];

  // If this edge has no constraints, return true
  if (constraints.empty()) {
    return true;
  }

  // Get edge info from tuple: (src_global, dst_global, edge_label)
  int src_global = std::get<0>(data_edge);
  int dst_global = std::get<1>(data_edge);
  neug::label_t edge_label = std::get<2>(data_edge);

  // Convert global IDs to (label, local_vid)
  auto [src_label, src_vid] = data_meta_.ToLocalId(src_global);
  auto [dst_label, dst_vid] = data_meta_.ToLocalId(dst_global);

  // Get property names for this edge type from schema
  const auto& schema = graph_.schema();
  std::vector<std::string> prop_names =
      schema.get_edge_property_names(src_label, dst_label, edge_label);

  // If no properties defined for this edge type, skip property checks.
  // Warn once per (src,dst,label,prop) so callers see they wrote a
  // constraint that can never match (e.g. constraint on an edge type
  // that has no properties at all).
  if (prop_names.empty()) {
    for (const auto& constraint : constraints) {
      std::ostringstream key;
      key << "e:no_props:" << static_cast<int>(src_label) << ":"
          << static_cast<int>(dst_label) << ":" << static_cast<int>(edge_label)
          << ":" << constraint._prop_name;
      std::ostringstream msg;
      msg << "[SAMPLED_PATTERN_MATCH] Edge type ("
          << static_cast<int>(src_label) << "->" << static_cast<int>(dst_label)
          << ", label " << static_cast<int>(edge_label)
          << ") has no properties, but constraint references '"
          << constraint._prop_name << "'; constraint silently skipped.";
      WarnUnknownConstraintPropertyOnce(key.str(), msg.str());
    }
    return true;
  }

  // Get EdgeDataAccessor for property lookup
  // We need to iterate through edge data to find the specific edge's properties
  try {
    // Check all constraints
    for (const auto& constraint : constraints) {
      // Skip constraints with empty property name
      if (constraint._prop_name.empty()) {
        WarnUnknownConstraintPropertyOnce(
            "e:empty",
            "[SAMPLED_PATTERN_MATCH] Edge constraint has empty property name; "
            "constraint silently skipped.");
        continue;
      }

      // Find property name index
      auto it = std::find(prop_names.begin(), prop_names.end(),
                          constraint._prop_name);
      if (it == prop_names.end()) {
        // Property not found on this edge type. Skip and warn once
        // per (src_label, dst_label, edge_label, prop) tuple.
        std::ostringstream key;
        key << "e:" << static_cast<int>(src_label) << ":"
            << static_cast<int>(dst_label) << ":"
            << static_cast<int>(edge_label) << ":" << constraint._prop_name;
        std::ostringstream msg;
        msg << "[SAMPLED_PATTERN_MATCH] Edge constraint property '"
            << constraint._prop_name << "' not found on edge type ("
            << static_cast<int>(src_label) << "->"
            << static_cast<int>(dst_label) << ", label "
            << static_cast<int>(edge_label)
            << "); constraint silently skipped.";
        WarnUnknownConstraintPropertyOnce(key.str(), msg.str());
        continue;
      }

      int prop_idx = std::distance(prop_names.begin(), it);

      // Get EdgeDataAccessor
      neug::EdgeDataAccessor accessor = graph_.GetEdgeDataAccessor(
          src_label, dst_label, edge_label, prop_idx);

      // Get the outgoing graph view to find the edge
      neug::CsrView view =
          graph_.GetGenericOutgoingGraphView(src_label, dst_label, edge_label);
      neug::NbrList edges = view.get_edges(src_vid);

      // Find the edge to dst_vid and get its property
      bool found = false;
      for (auto edge_it = edges.begin(); edge_it != edges.end(); ++edge_it) {
        if (*edge_it == dst_vid) {
          // Found the edge, get property value
          Value data_value = accessor.get_data(edge_it);

          // Check if value satisfies constraint
          if (!CheckValueConstraint(data_value, constraint._comp_type,
                                    constraint._value)) {
            return false;  // Constraint not satisfied
          }
          found = true;
          break;
        }
      }

      if (!found) {
        // Edge not found (shouldn't happen), skip constraint
        continue;
      }
    }
  } catch (...) {
    // If any error occurs during property lookup, skip property constraints
    return true;
  }

  return true;  // All constraints satisfied
}

// 辅助函数：检查值是否满足约束条件
// Note: neug::Value only supports == operator, other comparisons not
// yet implemented
bool CandidateSpace::CheckValueConstraint(const Value& data_value,
                                          CompType comp_type,
                                          const Value& constraint_value) {
  switch (comp_type) {
  case CompType::COMP_EQUAL:
    return data_value == constraint_value;
  // TODO: Implement other comparisons when Value supports them
  // Currently Value class doesn't have >, <, >=, <= operators
  case CompType::COMP_GREATER:
    return data_value > constraint_value;
  case CompType::COMP_LESS:
    return data_value < constraint_value;
  case CompType::COMP_GREATER_EQUAL:
    return data_value >= constraint_value;
  case CompType::COMP_LESS_EQUAL:
    return data_value <= constraint_value;
  case CompType::COMP_IN:
  case CompType::COMP_NOT_IN:
    WarnUnknownConstraintPropertyOnce(
        "op:in_not_in",
        "[SAMPLED_PATTERN_MATCH] Constraint operators 'in' / 'not_in' are "
        "accepted by the parser but not implemented at runtime; "
        "they currently behave as a no-op (always true).");
    return true;
  default:
    WarnUnknownConstraintPropertyOnce(
        "op:unknown_runtime",
        "[SAMPLED_PATTERN_MATCH] Unsupported comparison operator hit at "
        "runtime; treating as no-op (always true).");
    return true;
  }
}

}  // namespace SubgraphMatching
}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug
