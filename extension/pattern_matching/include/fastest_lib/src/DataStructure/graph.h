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
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <unordered_map>

#include <glog/logging.h>

#include "Base/base.h"

// Include neug types
#include <any>
#include "neug/utils/property/types.h"

// Forward declarations removed - gbi types not used

// #define HUGE_GRAPH
namespace neug {
namespace pattern_matching {
namespace graphlib {

// Type alias for the core neug label type used throughout FaSTest.
using label_t = neug::label_t;

class Graph {
 public:
  // Adjacency Lists - for directed graphs
  std::vector<std::vector<int>>
      adj_list;  // Combined adjacency (for compatibility)
  std::vector<std::vector<int>> out_adj_list;  // Out-edge adjacency list
  std::vector<std::vector<int>> in_adj_list;   // In-edge adjacency list
  std::vector<int> core_num, vertex_color;
  std::vector<int> degeneracy_order;
  int num_vertex = 0, num_edge = 0, max_degree = 0, degeneracy = 0,
      num_color = 0;
  int max_out_degree = 0, max_in_degree = 0;
  int num_vertex_labels = 0;
  int num_edge_labels = 0;

  /**
   * Basic data structures for directed graph
   * @attribute (vertex/edge)_label : array of labels
   * @attribute edge_list : list of edges as pair<int, int> form (src, dst)
   * @attribute out_incident_edges[v][l] : list of indices of out-edges from v
   * to endpoint label l
   * @attribute in_incident_edges[v][l] : list of indices of in-edges to v from
   * source label l
   * @attribute all_out_incident_edges[v] : list of indices of all out-edges
   * from v
   * @attribute all_in_incident_edges[v] : list of indices of all in-edges to v
   * @attribute edge_index_map : Queryable as map[{u, v, label}] -> edge_id.
   */
  std::vector<int> vertex_label, edge_label, edge_to;
  std::vector<std::pair<int, int>> edge_list;
  std::vector<std::vector<int>>
      all_incident_edges;  // Combined (for compatibility)
  std::vector<std::vector<int>> all_out_incident_edges;  // All out-edges from v
  std::vector<std::vector<int>> all_in_incident_edges;   // All in-edges to v
  std::vector<std::vector<std::string>> vertex_property_names,
      edge_property_names;
  std::vector<std::vector<std::any>> vertex_properties, edge_properties;
#ifdef HUGE_GRAPH
  std::vector<std::map<int, std::vector<int>>> incident_edges;
  std::vector<std::map<int, std::vector<int>>>
      out_incident_edges;  // [v][label] -> out-edges
  std::vector<std::map<int, std::vector<int>>>
      in_incident_edges;  // [v][label] -> in-edges
#else
  std::vector<std::vector<std::vector<int>>> incident_edges;
  std::vector<std::vector<std::vector<int>>>
      out_incident_edges;  // [v][label] -> out-edges to label
  std::vector<std::vector<std::vector<int>>>
      in_incident_edges;  // [v][label] -> in-edges from label
#endif
  std::vector<std::unordered_map<int, std::unordered_map<int, int>>>
      edge_index_map;  // 起点->终点->label->边id
  std::vector<std::vector<std::pair<int, int>>>
      no_edge_pairs;  // 记录不存在的（终点，边类型）对

  /*
   * Enumeration of Small Cycles for Cyclic Substructure Filter
   * For each edge e, store local triangles and four-cycles
   */
  struct FourMotif {
    std::tuple<int, int, int, int> edges;
    std::tuple<int, int> diags;
    FourMotif(std::tuple<int, int, int, int> edges, std::tuple<int, int> diags)
        : edges(edges), diags(diags) {}
  };
  std::vector<std::vector<std::tuple<int, int, int>>> local_triangles;
  std::vector<std::vector<FourMotif>> local_four_cycles;

  Graph() {}
  ~Graph() {}
  Graph& operator=(const Graph&) = delete;

  std::vector<int>& GetNeighbors(int v) { return adj_list[v]; }

  // Directed graph neighbor accessors
  std::vector<int>& GetOutNeighbors(int v) { return out_adj_list[v]; }

  std::vector<int>& GetInNeighbors(int v) { return in_adj_list[v]; }

  inline int GetDegree(int v) const { return adj_list[v].size(); }

  inline int GetOutDegree(int v) const { return out_adj_list[v].size(); }

  inline int GetInDegree(int v) const { return in_adj_list[v].size(); }

  inline int GetNumVertices() const { return num_vertex; }

  inline int GetNumEdges() const { return num_edge; }

  inline int GetMaxDegree() const { return max_degree; }

  inline int GetMaxOutDegree() const { return max_out_degree; }

  inline int GetMaxInDegree() const { return max_in_degree; }

  void ComputeCoreNum();

  inline int GetCoreNum(int v) const { return core_num[v]; }

  inline int GetDegeneracy() const { return degeneracy; }

  void AssignVertexColor();

  inline int GetNumColors() const { return num_color; }

  inline int GetVertexColor(int v) const { return vertex_color[v]; }

  void LoadLabeledGraph(const std::string& filename, bool directed = true);
  void LoadLabeledGraph(const std::string& filename,
                        std::unordered_map<std::string, int>& label2id_mapping,
                        std::unordered_map<int, std::string>& id2label_mapping,
                        bool directed = true);
  // LoadProperty function removed - not needed for subgraph matching

  std::vector<int>& GetAllIncidentEdges(int v);
  std::vector<int>& GetIncidentEdges(int v, int label);

  // Directed graph incident edge accessors
  std::vector<int>& GetAllOutIncidentEdges(int v);
  std::vector<int>& GetAllInIncidentEdges(int v);
  std::vector<int>& GetOutIncidentEdges(int v, int label);
  std::vector<int>& GetInIncidentEdges(int v, int label);
  inline int GetVertexLabel(int v) const { return vertex_label[v]; }
  inline int GetEdgeLabel(int edge_id) const { return edge_label[edge_id]; }
  inline int GetNumLabels() const { return num_vertex_labels; }
  inline int GetNumEdgeLabels() const { return num_edge_labels; }
  inline int GetOppositeEdge(int edge_id) const { return edge_id ^ 1; }
  inline int GetOppositePoint(int edge_id) const { return edge_to[edge_id]; }
  // For directed graph: get edge source and destination
  int GetSourcePoint(int edge_id) const;
  int GetDestPoint(int edge_id) const;
  int GetEdgeIndex(int u, int v);

  int GetEdgeIndex(int u, int v, int label);
  std::pair<int, int>& GetEdge(int edge_id);

  std::vector<std::tuple<int, int, int>>& GetLocalTriangles(int edge_id);
  std::vector<FourMotif>& GetLocalFourCycles(int edge_id);

  bool EnumerateLocalTriangles();
  bool EnumerateLocalFourCycles();
  void ChibaNishizeki();
  bool build_four_cycle = false;
  bool build_triangle = false;

  /**
   * @brief Build the incidence list structure
   */
  void BuildIncidenceList(
      bool load_no_edge_pairs = false,
      std::shared_ptr<std::unordered_map<
          label_t, std::unordered_map<label_t, std::vector<label_t>>>>
          schema_graph = nullptr);

 protected:
  /**
   * @brief 构建no_edge_pairs，可以被子类重写以使用schema
   */
  virtual void BuildNoEdgePairsFromSchema(
      std::shared_ptr<std::unordered_map<
          label_t, std::unordered_map<label_t, std::vector<label_t>>>>
          schema_graph);

 public:
  void LoadGraph(std::vector<int>& vertex_labels,
                 std::vector<std::pair<int, int>>& edges,
                 std::vector<int>& edge_labels, bool directed = false);

  void WriteToFile(string filename);
};

/**
 * @brief Compute the core number of each vertex
 * @date Oct 21, 2022
 */

/**
 * @brief Greedy coloring of the graph, following the given initial order of
 * vertices.
 * @date Sep 16, 2022
 */

// LoadProperty function removed - not needed for subgraph matching

}  // namespace graphlib
}  // namespace pattern_matching
}  // namespace neug

// 实现在graph.cpp中，因为需要访问GraphStorage的完整定义
