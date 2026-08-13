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

#include "graph.h"

namespace neug {
namespace pattern_matching {
namespace graphlib {

void Graph::BuildNoEdgePairsFromSchema(
    std::shared_ptr<std::unordered_map<
        label_t, std::unordered_map<label_t, std::vector<label_t>>>>
        schema_graph) {
  // 基于schema构建no_edge_pairs，只检查schema中定义的边类型

  edge_index_map.resize(GetNumVertices());
  for (int i = 0; i < edge_list.size(); i++) {
    edge_index_map[edge_list[i].first][edge_list[i].second][edge_label[i]] = i;
  }

  if (schema_graph) {
    auto& schema = *schema_graph;

    for (int i = 0; i < GetNumVertices(); i++) {
      int i_label = GetVertexLabel(i);
      for (int j = 0; j < GetNumVertices(); j++) {
        if (i == j)
          continue;
        int j_label = GetVertexLabel(j);

        // 遍历schema中定义的所有边类型
        for (const auto& edge_type : schema[i_label][j_label]) {
          if (GetEdgeIndex(i, j, edge_type) == -1 &&
              GetEdgeIndex(i, j) == -1)  // to refine
          {
            no_edge_pairs[i].push_back(std::make_pair(j, edge_type));
          }
        }
      }
    }
  } else {
    // 如果没有schema信息，退回到遍历所有label的方式
    for (int i = 0; i < GetNumVertices(); i++) {
      for (int j = 0; j < GetNumVertices(); j++) {
        if (i == j)
          continue;
        for (int label = 0; label < GetNumEdgeLabels(); label++) {
          if (GetEdgeIndex(i, j, label) == -1 && GetEdgeIndex(i, j) == -1) {
            no_edge_pairs[i].push_back(std::make_pair(j, label));
          }
        }
      }
    }
  }
}

}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug

namespace neug {
namespace pattern_matching {
namespace graphlib {

std::vector<int>& Graph::GetAllOutIncidentEdges(int v) {
  return all_out_incident_edges[v];
}

std::vector<int>& Graph::GetAllInIncidentEdges(int v) {
  return all_in_incident_edges[v];
}

int Graph::GetSourcePoint(int edge_id) const {
  return edge_list[edge_id].first;
}

int Graph::GetEdgeIndex(int u, int v) {
  auto it_v = edge_index_map[u].find(v);
  if (it_v == edge_index_map[u].end() || it_v->second.empty()) {
    return -1;
  }
  // 返回第一个找到的边（任意label）
  return it_v->second.begin()->second;
}

int Graph::GetEdgeIndex(int u, int v, int label) {
  auto it_v = edge_index_map[u].find(v);
  if (it_v == edge_index_map[u].end()) {
    return -1;
  }
  auto it_label = it_v->second.find(label);
  return (it_label == it_v->second.end() ? -1 : it_label->second);
}

void Graph::ComputeCoreNum() {
  core_num.resize(num_vertex, 0);
  int* bin = new int[GetMaxDegree() + 1];
  int* pos = new int[GetNumVertices()];
  int* vert = new int[GetNumVertices()];

  std::fill(bin, bin + (GetMaxDegree() + 1), 0);

  for (int v = 0; v < GetNumVertices(); v++) {
    core_num[v] = adj_list[v].size();
    bin[core_num[v]] += 1;
  }

  int start = 0;
  int num;

  for (int d = 0; d <= GetMaxDegree(); d++) {
    num = bin[d];
    bin[d] = start;
    start += num;
  }

  for (int v = 0; v < GetNumVertices(); v++) {
    pos[v] = bin[core_num[v]];
    vert[pos[v]] = v;
    bin[core_num[v]] += 1;
  }

  for (int d = GetMaxDegree(); d--;)
    bin[d + 1] = bin[d];
  bin[0] = 0;

  for (int i = 0; i < GetNumVertices(); i++) {
    int v = vert[i];

    for (int u : GetNeighbors(v)) {
      if (core_num[u] > core_num[v]) {
        int du = core_num[u];
        int pu = pos[u];
        int pw = bin[du];
        int w = vert[pw];

        if (u != w) {
          pos[u] = pw;
          pos[w] = pu;
          vert[pu] = w;
          vert[pw] = u;
        }

        bin[du]++;
        core_num[u]--;
      }
    }
  }
  delete[] bin;
  delete[] pos;
  delete[] vert;
}

void Graph::BuildIncidenceList(
    bool load_no_edge_pairs,
    std::shared_ptr<std::unordered_map<
        label_t, std::unordered_map<label_t, std::vector<label_t>>>>
        schema_graph) {
  edge_index_map.resize(num_vertex);

  // Initialize structures for directed edges
  all_out_incident_edges.resize(num_vertex);
  all_in_incident_edges.resize(num_vertex);
  out_incident_edges.resize(num_vertex);
  in_incident_edges.resize(num_vertex);

  for (int i = 0; i < GetNumVertices(); i++) {
#ifndef HUGE_GRAPH
    out_incident_edges[i].resize(GetNumLabels());
    in_incident_edges[i].resize(GetNumLabels());
#endif
  }

  int edge_id = 0;
  for (auto& [u, v] : edge_list) {
    // For directed graph: edge goes from u to v
    int dst_label = GetVertexLabel(v);
    int src_label = GetVertexLabel(u);
    int edge_label_val = GetEdgeLabel(edge_id);

    // Build out-edge structures (edges going OUT from u)
    all_out_incident_edges[u].push_back(edge_id);
    out_incident_edges[u][dst_label].push_back(edge_id);

    // Build in-edge structures (edges coming IN to v)
    all_in_incident_edges[v].push_back(edge_id);
    in_incident_edges[v][src_label].push_back(edge_id);

    // Edge index map: src -> dst -> label -> edge_id
    edge_index_map[u][v][edge_label_val] = edge_id;
    edge_id++;
  }

  if (load_no_edge_pairs) {
    VLOG(1) << "[FaSTest] Build no-edge pairs from schema.";
    no_edge_pairs.resize(GetNumVertices());
    BuildNoEdgePairsFromSchema(schema_graph);
    VLOG(1) << "[FaSTest] Built no-edge pairs from schema.";
  }

  // Sort edges by degree of endpoint (using total degree for sorting)
  for (int i = 0; i < GetNumVertices(); i++) {
#ifdef HUGE_GRAPH
    // Sort out-incident edges
    for (auto& [l, vec] : out_incident_edges[i]) {
      std::stable_sort(vec.begin(), vec.end(),
                       [this](auto& a, auto& b) -> bool {
                         int opp_a = edge_list[a].second;
                         int opp_b = edge_list[b].second;
                         return adj_list[opp_a].size() > adj_list[opp_b].size();
                       });
    }
    // Sort in-incident edges
    for (auto& [l, vec] : in_incident_edges[i]) {
      std::stable_sort(
          vec.begin(), vec.end(), [this](auto& a, auto& b) -> bool {
            int opp_a = edge_list[a].first;  // Source vertex for in-edges
            int opp_b = edge_list[b].first;
            return adj_list[opp_a].size() > adj_list[opp_b].size();
          });
    }
#else
    // Sort out-incident edges
    for (auto& vec : out_incident_edges[i]) {
      std::stable_sort(vec.begin(), vec.end(),
                       [this](auto& a, auto& b) -> bool {
                         int opp_a = edge_list[a].second;
                         int opp_b = edge_list[b].second;
                         return adj_list[opp_a].size() > adj_list[opp_b].size();
                       });
    }
    // Sort in-incident edges
    for (auto& vec : in_incident_edges[i]) {
      std::stable_sort(
          vec.begin(), vec.end(), [this](auto& a, auto& b) -> bool {
            int opp_a = edge_list[a].first;  // Source vertex for in-edges
            int opp_b = edge_list[b].first;
            return adj_list[opp_a].size() > adj_list[opp_b].size();
          });
    }
#endif
    std::stable_sort(all_out_incident_edges[i].begin(),
                     all_out_incident_edges[i].end(),
                     [this](auto& a, auto& b) -> bool {
                       return adj_list[edge_list[a].second].size() >
                              adj_list[edge_list[b].second].size();
                     });
    std::stable_sort(all_in_incident_edges[i].begin(),
                     all_in_incident_edges[i].end(),
                     [this](auto& a, auto& b) -> bool {
                       return adj_list[edge_list[a].first].size() >
                              adj_list[edge_list[b].first].size();
                     });
  }
}

}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug
