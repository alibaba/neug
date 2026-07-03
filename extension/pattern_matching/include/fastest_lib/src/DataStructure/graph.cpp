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

std::vector<int>& Graph::GetAllIncidentEdges(int v) {
  return all_incident_edges[v];
}

std::vector<int>& Graph::GetIncidentEdges(int v, int label) {
  return incident_edges[v][label];
}

std::vector<int>& Graph::GetAllOutIncidentEdges(int v) {
  return all_out_incident_edges[v];
}

std::vector<int>& Graph::GetAllInIncidentEdges(int v) {
  return all_in_incident_edges[v];
}

std::vector<int>& Graph::GetOutIncidentEdges(int v, int label) {
  return out_incident_edges[v][label];
}

std::vector<int>& Graph::GetInIncidentEdges(int v, int label) {
  return in_incident_edges[v][label];
}

int Graph::GetSourcePoint(int edge_id) const {
  return edge_list[edge_id].first;
}

int Graph::GetDestPoint(int edge_id) const { return edge_list[edge_id].second; }

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

std::pair<int, int>& Graph::GetEdge(int edge_id) { return edge_list[edge_id]; }

std::vector<std::tuple<int, int, int>>& Graph::GetLocalTriangles(int edge_id) {
  return local_triangles[edge_id];
}

std::vector<Graph::FourMotif>& Graph::GetLocalFourCycles(int edge_id) {
  return local_four_cycles[edge_id];
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
  degeneracy_order.resize(GetNumVertices());
  for (int i = 0; i < GetNumVertices(); i++) {
    degeneracy_order[i] = vert[i];
  }
  std::reverse(degeneracy_order.begin(), degeneracy_order.end());

  degeneracy = 0;
  for (int i = 0; i < GetNumVertices(); i++) {
    degeneracy = std::max(core_num[i], degeneracy);
  }

  delete[] bin;
  delete[] pos;
  delete[] vert;
}

void Graph::AssignVertexColor() {
  vertex_color.resize(GetNumVertices(), -1);
  num_color = 0;
  bool* used = new bool[GetNumVertices()];
  for (int vertexID : degeneracy_order) {
    for (int neighbor : adj_list[vertexID]) {
      if (vertex_color[neighbor] == -1)
        continue;
      used[vertex_color[neighbor]] = true;
    }
    int c = 0;
    while (used[c])
      c++;
    vertex_color[vertexID] = c;
    num_color = std::max(num_color, c + 1);
    for (int neighbor : adj_list[vertexID]) {
      if (vertex_color[neighbor] == -1)
        continue;
      used[vertex_color[neighbor]] = false;
    }
  }
}

void Graph::LoadLabeledGraph(const std::string& filename, bool directed) {
  std::ifstream fin(filename);
  int v, e;
  std::string ignore, type, line;
  fin >> ignore >> v >> e;
  num_vertex = v;
  num_edge = directed
                 ? e
                 : e * 2;  // Directed: single edge, Undirected: both directions
  adj_list.resize(num_vertex);
  out_adj_list.resize(num_vertex);
  in_adj_list.resize(num_vertex);
  vertex_label.resize(num_vertex);
  edge_label.resize(num_edge);
  int num_lines = 0;
  while (getline(fin, line)) {
    if (line.empty())
      continue;
    auto tok = parse(line, " ");
    type = tok[0];
    tok.pop_front();
    if (type[0] == 'v') {
      int id = std::stoi(tok.front());
      tok.pop_front();
      int l;
      if (tok.empty())
        l = 0;
      else {
        l = std::stoi(tok.front());
        tok.pop_front();
      }
      vertex_label[id] = l;
    } else if (type[0] == 'e') {
      int v1, v2;
      v1 = std::stoi(tok.front());
      tok.pop_front();
      v2 = std::stoi(tok.front());
      tok.pop_front();
      int el = tok.empty() ? 0 : std::stoi(tok.front());

      if (directed) {
        // Directed graph: v1 -> v2
        out_adj_list[v1].push_back(v2);
        in_adj_list[v2].push_back(v1);
        adj_list[v1].push_back(v2);
        adj_list[v2].push_back(v1);
        edge_to.push_back(v2);
        edge_list.push_back({v1, v2});
        edge_label[edge_list.size() - 1] = el;
        max_out_degree =
            std::max(max_out_degree, (int) out_adj_list[v1].size());
        max_in_degree = std::max(max_in_degree, (int) in_adj_list[v2].size());
      } else {
        // Undirected graph: add both directions
        out_adj_list[v1].push_back(v2);
        out_adj_list[v2].push_back(v1);
        in_adj_list[v2].push_back(v1);
        in_adj_list[v1].push_back(v2);
        adj_list[v1].push_back(v2);
        adj_list[v2].push_back(v1);
        edge_to.push_back(v2);
        edge_to.push_back(v1);
        edge_list.push_back({v1, v2});
        edge_list.push_back({v2, v1});
        edge_label[edge_list.size() - 2] = edge_label[edge_list.size() - 1] =
            el;
        max_out_degree = std::max(
            max_out_degree,
            (int) std::max(out_adj_list[v1].size(), out_adj_list[v2].size()));
        max_in_degree = std::max(
            max_in_degree,
            (int) std::max(in_adj_list[v1].size(), in_adj_list[v2].size()));
      }
      max_degree = std::max(
          max_degree, (int) std::max(adj_list[v1].size(), adj_list[v2].size()));
    }
    num_lines++;
  }
}

void Graph::LoadLabeledGraph(
    const std::string& filename,
    std::unordered_map<std::string, int>& label2id_mapping,
    std::unordered_map<int, std::string>& /*id2label_mapping*/, bool directed) {
  std::ifstream fin(filename);
  int v, e;
  std::string ignore, type, line;
  fin >> ignore >> v >> e;
  num_vertex = v;
  num_edge = directed
                 ? e
                 : e * 2;  // Directed: single edge, Undirected: both directions
  adj_list.resize(num_vertex);
  out_adj_list.resize(num_vertex);
  in_adj_list.resize(num_vertex);
  vertex_label.resize(num_vertex);
  edge_label.resize(num_edge);
  int num_lines = 0;
  while (getline(fin, line)) {
    if (line.empty())
      continue;
    auto tok = parse(line, " ");
    type = tok[0];
    tok.pop_front();
    if (type[0] == 'v') {
      int id = std::stoi(tok.front());
      tok.pop_front();
      int l;
      if (tok.empty())
        l = label2id_mapping[""];
      else {
        l = label2id_mapping[tok.front()];
        tok.pop_front();
      }
      vertex_label[id] = l;
    } else if (type[0] == 'e') {
      int v1, v2;
      v1 = std::stoi(tok.front());
      tok.pop_front();
      v2 = std::stoi(tok.front());
      tok.pop_front();
      int el =
          tok.empty() ? label2id_mapping[""] : label2id_mapping[tok.front()];

      if (directed) {
        // Directed graph: v1 -> v2
        out_adj_list[v1].push_back(v2);
        in_adj_list[v2].push_back(v1);
        adj_list[v1].push_back(v2);
        adj_list[v2].push_back(v1);
        edge_to.push_back(v2);
        edge_list.push_back({v1, v2});
        edge_label[edge_list.size() - 1] = el;
        max_out_degree =
            std::max(max_out_degree, (int) out_adj_list[v1].size());
        max_in_degree = std::max(max_in_degree, (int) in_adj_list[v2].size());
      } else {
        // Undirected graph: add both directions
        out_adj_list[v1].push_back(v2);
        out_adj_list[v2].push_back(v1);
        in_adj_list[v2].push_back(v1);
        in_adj_list[v1].push_back(v2);
        adj_list[v1].push_back(v2);
        adj_list[v2].push_back(v1);
        edge_to.push_back(v2);
        edge_to.push_back(v1);
        edge_list.push_back({v1, v2});
        edge_list.push_back({v2, v1});
        edge_label[edge_list.size() - 2] = edge_label[edge_list.size() - 1] =
            el;
        max_out_degree = std::max(
            max_out_degree,
            (int) std::max(out_adj_list[v1].size(), out_adj_list[v2].size()));
        max_in_degree = std::max(
            max_in_degree,
            (int) std::max(in_adj_list[v1].size(), in_adj_list[v2].size()));
      }
      max_degree = std::max(
          max_degree, (int) std::max(adj_list[v1].size(), adj_list[v2].size()));
    }
    num_lines++;
  }
}

void Graph::LoadGraph(std::vector<int>& vertex_labels,
                      std::vector<std::pair<int, int>>& edges,
                      std::vector<int>& edge_labels, bool directed) {
  num_vertex = vertex_labels.size();
  num_edge = directed
                 ? edges.size()
                 : edges.size() *
                       2;  // Directed: single edge, Undirected: both directions

  // Initialize adjacency lists
  adj_list.resize(num_vertex);
  out_adj_list.resize(num_vertex);
  in_adj_list.resize(num_vertex);
  vertex_label.resize(num_vertex);
  edge_label.resize(num_edge);

  for (int i = 0; i < num_vertex; i++)
    vertex_label[i] = vertex_labels[i];

  for (size_t i = 0; i < edges.size(); i++) {
    auto& [v1, v2] = edges[i];
    int el = (edge_labels.size() > i) ? edge_labels[i] : 0;

    if (directed) {
      // Directed graph: v1 -> v2
      out_adj_list[v1].push_back(v2);
      in_adj_list[v2].push_back(v1);
      adj_list[v1].push_back(v2);
      adj_list[v2].push_back(v1);
      edge_to.push_back(v2);
      edge_list.push_back({v1, v2});
      edge_label[edge_list.size() - 1] = el;
      max_out_degree = std::max(max_out_degree, (int) out_adj_list[v1].size());
      max_in_degree = std::max(max_in_degree, (int) in_adj_list[v2].size());
    } else {
      // Undirected graph: add both directions
      out_adj_list[v1].push_back(v2);
      out_adj_list[v2].push_back(v1);
      in_adj_list[v2].push_back(v1);
      in_adj_list[v1].push_back(v2);
      adj_list[v1].push_back(v2);
      adj_list[v2].push_back(v1);
      edge_to.push_back(v2);
      edge_to.push_back(v1);
      edge_list.push_back({v1, v2});
      edge_list.push_back({v2, v1});
      edge_label[edge_list.size() - 2] = edge_label[edge_list.size() - 1] = el;
      max_out_degree = std::max(
          max_out_degree,
          (int) std::max(out_adj_list[v1].size(), out_adj_list[v2].size()));
      max_in_degree = std::max(
          max_in_degree,
          (int) std::max(in_adj_list[v1].size(), in_adj_list[v2].size()));
    }
    max_degree = std::max(
        max_degree, (int) std::max(adj_list[v1].size(), adj_list[v2].size()));
  }
}

void Graph::BuildIncidenceList(
    bool load_no_edge_pairs,
    std::shared_ptr<std::unordered_map<
        label_t, std::unordered_map<label_t, std::vector<label_t>>>>
        schema_graph) {
  // Initialize structures for combined (undirected) view
  all_incident_edges.resize(num_vertex);
  incident_edges.resize(num_vertex);
  edge_index_map.resize(num_vertex);

  // Initialize structures for directed edges
  all_out_incident_edges.resize(num_vertex);
  all_in_incident_edges.resize(num_vertex);
  out_incident_edges.resize(num_vertex);
  in_incident_edges.resize(num_vertex);

  for (int i = 0; i < GetNumVertices(); i++) {
#ifndef HUGE_GRAPH
    incident_edges[i].resize(GetNumLabels());
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

    // Combined structures for compatibility
    all_incident_edges[u].push_back(edge_id);
    incident_edges[u][dst_label].push_back(edge_id);

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
    // Sort combined incident edges
    for (auto& [l, vec] : incident_edges[i]) {
      std::stable_sort(vec.begin(), vec.end(),
                       [this](auto& a, auto& b) -> bool {
                         int opp_a = edge_list[a].second;
                         int opp_b = edge_list[b].second;
                         return adj_list[opp_a].size() > adj_list[opp_b].size();
                       });
    }
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
    // Sort combined incident edges
    for (auto& vec : incident_edges[i]) {
      std::stable_sort(vec.begin(), vec.end(),
                       [this](auto& a, auto& b) -> bool {
                         int opp_a = edge_list[a].second;
                         int opp_b = edge_list[b].second;
                         return adj_list[opp_a].size() > adj_list[opp_b].size();
                       });
    }
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
    // Sort all incident edges
    std::stable_sort(all_incident_edges[i].begin(), all_incident_edges[i].end(),
                     [this](auto& a, auto& b) -> bool {
                       return adj_list[edge_list[a].second].size() >
                              adj_list[edge_list[b].second].size();
                     });
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

void Graph::WriteToFile(std::string filename) {
  std::filesystem::path filepath = filename;
  std::filesystem::create_directories(filepath.parent_path());
  std::ofstream out(filename);
  out << "t " << GetNumVertices() << ' ' << GetNumEdges() / 2 << '\n';
  for (int i = 0; i < GetNumVertices(); i++) {
    out << "v " << i << ' ' << GetVertexLabel(i) << ' ' << GetDegree(i) << '\n';
  }
  int idx = 0;
  for (auto& e : edge_list) {
    if (e.first < e.second) {
      out << "e " << e.first << ' ' << e.second << ' ' << GetEdgeLabel(idx)
          << '\n';
    }
    idx++;
  }
}

}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug
