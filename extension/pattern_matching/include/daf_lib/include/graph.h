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

#ifndef GRAPH_H_
#define GRAPH_H_

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "../global/global.h"
#include "fastest_lib/src/DataStructure/json.hpp"
#include "storage/graph_element.hpp"

namespace daf {

class Graph {
 public:
  explicit Graph(const std::string& filename);
  ~Graph();

  Graph& operator=(const Graph&) = delete;
  Graph(const Graph&) = delete;

  inline Size GetNumLabels() const;
  inline Size GetNumVertices() const;
  inline Size GetNumEdges() const;
  inline Size GetMaxDegree() const;

  inline Label GetLabel(Vertex v) const;
  inline Size GetStartOffset(Vertex v) const;
  inline Size GetEndOffset(Vertex v) const;
  inline Size GetDegree(Vertex v) const;
  inline Size GetCoreNum(Vertex v) const;

  inline Label GetLabelFrequency(Label l) const;

  inline Vertex GetNeighbor(Size i) const;

  // Property access methods (for DATA GRAPH)
  inline Label GetEdgeLabel(Size edge_idx) const;
  inline Size GetEdgeIndex(Vertex u, Vertex v) const;
  inline Size GetEdgeIndex(Vertex u, Vertex v, Label label) const;
  inline const std::vector<gbi::Value>& GetVertexProperties(Vertex v) const;
  inline const std::vector<gbi::Value>& GetEdgeProperties(Size edge_idx) const;
  inline const std::vector<std::string>& GetVertexPropertyNames(
      Label vertex_label) const;
  inline const std::vector<std::string>& GetEdgePropertyNames(
      Label edge_label) const;
  inline Size GetNumEdgeLabels() const;

  // Public method to load properties (for DataGraph and QueryGraph to use)
  void LoadGraphProperties(
      const std::string& vertex_property_filename,
      const std::string& edge_property_filename,
      std::unordered_map<std::string, int>& label2id_mapping,
      std::shared_ptr<gbi::SchemaGraph> schema_graph);

  // Property matching methods (for DATA GRAPH)
  bool CheckVertexPropertyConstraints(
      const std::vector<gbi::PropCons>& constraints, Vertex v) const;
  bool CheckEdgePropertyConstraints(
      const std::vector<gbi::PropCons>& constraints, Size edge_idx) const;
  bool HasVertexAttributes() const;
  bool HasEdgeAttributes() const;

  // Public wrapper for testing
  void LoadTestGraph() {
    std::vector<std::vector<Vertex>> adj_list;
    LoadRoughGraph(&adj_list);
  }

 public:
  void LoadRoughGraph(std::vector<std::vector<Vertex>>* graph);
  void LoadRoughGraph(std::vector<std::vector<Vertex>>* graph,
                      std::unordered_map<std::string, int>& label2id_mapping);
  void LoadProperty(const std::string& vertex_property_filename,
                    const std::string& edge_property_filename,
                    std::unordered_map<std::string, int>& label2id_mapping,
                    std::shared_ptr<gbi::SchemaGraph> schema_graph);
  void computeCoreNum();

  Size num_vertex_;
  Size num_edge_;
  Size num_label_;
  Size num_edge_labels_;

  Size max_degree_;

  Label* label_;
  Size* start_off_;
  Vertex* linear_adj_list_;
  Size* label_frequency_;

  // Property support (similar to FaSTest) - for DATA GRAPH
  Label* edge_labels_;  // Edge labels for each edge
  std::vector<std::vector<std::string>>
      vertex_property_names_;  // Property names for each vertex label
  std::vector<std::vector<std::string>>
      edge_property_names_;  // Property names for each edge label
  std::vector<std::vector<gbi::Value>>
      vertex_properties_;  // Property values for each vertex
  std::vector<std::vector<gbi::Value>>
      edge_properties_;  // Property values for each edge
  std::vector<std::pair<Vertex, Vertex>>
      edge_list_;  // List of edges as vertex pairs
  std::vector<std::unordered_map<int, std::unordered_map<int, int>>>
      edge_index_map_;  // Map from (u,v,label) to edge index

  Size* core_num_;

  const std::string& filename_;
  std::ifstream fin_;
};

inline Size Graph::GetNumLabels() const { return num_label_; }

inline Size Graph::GetNumVertices() const { return num_vertex_; }

inline Size Graph::GetNumEdges() const { return num_edge_; }

inline Size Graph::GetMaxDegree() const { return max_degree_; }

inline Label Graph::GetLabel(Vertex v) const { return label_[v]; }

inline Size Graph::GetStartOffset(Vertex v) const { return start_off_[v]; }

inline Size Graph::GetEndOffset(Vertex v) const { return start_off_[v + 1]; }

inline Size Graph::GetDegree(Vertex v) const {
  return start_off_[v + 1] - start_off_[v];
}

inline Size Graph::GetCoreNum(Vertex v) const { return core_num_[v]; }

inline Label Graph::GetLabelFrequency(Label l) const {
  return label_frequency_[l];
}

inline Vertex Graph::GetNeighbor(Size i) const { return linear_adj_list_[i]; }

// Edge attribute inline implementations
inline Label Graph::GetEdgeLabel(Size edge_idx) const {
  return edge_labels_ ? edge_labels_[edge_idx] : 0;
}

inline Size Graph::GetEdgeIndex(Vertex u, Vertex v) const {
  auto it_v = edge_index_map_[u].find(v);
  if (it_v != edge_index_map_[u].end() && !it_v->second.empty()) {
    // Return the first edge index if any label exists
    return it_v->second.begin()->second;
  }
  return INVALID_SZ;
}

inline Size Graph::GetEdgeIndex(Vertex u, Vertex v, Label label) const {
  auto it_v = edge_index_map_[u].find(v);
  if (it_v != edge_index_map_[u].end()) {
    auto it_label = it_v->second.find(label);
    if (it_label != it_v->second.end()) {
      return it_label->second;
    }
  }
  return INVALID_SZ;
}

inline const std::vector<gbi::Value>& Graph::GetVertexProperties(
    Vertex v) const {
  return vertex_properties_[v];
}

inline const std::vector<gbi::Value>& Graph::GetEdgeProperties(
    Size edge_idx) const {
  return edge_properties_[edge_idx];
}

inline const std::vector<std::string>& Graph::GetVertexPropertyNames(
    Label vertex_label) const {
  return vertex_property_names_[vertex_label];
}

inline const std::vector<std::string>& Graph::GetEdgePropertyNames(
    Label edge_label) const {
  return edge_property_names_[edge_label];
}

inline Size Graph::GetNumEdgeLabels() const { return num_edge_labels_; }

inline bool Graph::HasVertexAttributes() const {
  return !vertex_property_names_.empty();
}

inline bool Graph::HasEdgeAttributes() const {
  return edge_labels_ != nullptr && !edge_property_names_.empty();
}

}  // namespace daf

#endif  // GRAPH_H_
