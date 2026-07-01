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

#ifndef QUERY_GRAPH_H_
#define QUERY_GRAPH_H_

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "../include/data_graph.h"
#include "../include/graph.h"
#include "storage/graph_element.hpp"

namespace daf {
struct NECElement;

class QueryGraph : public Graph {
 public:
  explicit QueryGraph(const std::string& filename);
  ~QueryGraph();

  QueryGraph& operator=(const QueryGraph&) = delete;
  QueryGraph(const QueryGraph&) = delete;

  bool LoadAndProcessGraph(const DataGraph& data);

  inline bool IsNECRepresentation(Vertex v) const;
  inline bool IsInNEC(Vertex v) const;

  inline Size GetNumNECLabel() const;
  inline Size GetNECStartOffset(Size i) const;
  inline Size GetNECEndOffset(Size i) const;
  inline const NECElement& GetNECElement(Size i) const;
  inline bool IsTree() const;

  inline Size GetNumNonLeafVertices() const;
  inline Size GetNECSize(Vertex v) const;
  inline Size GetNECRepresentative(Vertex v) const;

  inline Label GetMaxLabel() const;

  // Property constraint support (similar to FaSTest PatternGraph)
  inline const std::vector<gbi::PropCons>& GetVertexPropertyConstraints(
      Vertex v) const;
  inline const std::vector<gbi::PropCons>& GetEdgePropertyConstraints(
      Size edge_idx) const;
  void SetVertexPropertyConstraints(
      Vertex v, const std::vector<gbi::PropCons>& constraints);
  void SetEdgePropertyConstraints(
      Size edge_idx, const std::vector<gbi::PropCons>& constraints);
  bool IsLeafVertex(Vertex u) const;

 public:
  void ExtractResidualStructure();

  std::vector<std::vector<std::pair<Vertex, Label>>>
      no_edge_pairs;  // 记录不存在的（终点，边类型）对
  std::vector<std::vector<std::pair<Vertex, Label>>> has_out_edge_pairs;
  std::vector<std::vector<std::pair<Vertex, Label>>> has_in_edge_pairs;
  Vertex* NEC_map_;
  NECElement* NEC_elems_;
  Size* NEC_start_offs_;
  Size* NEC_size_;

  Size num_NEC_label_;
  Size num_non_leaf_vertices_;
  Label max_label_;

  bool is_tree_;

  // Property constraints (like FaSTest PatternGraph)
  std::vector<std::vector<gbi::PropCons>>
      vertex_property_constraints_;  // per vertex
  std::vector<std::vector<gbi::PropCons>>
      edge_property_constraints_;  // per edge
};

struct NECElement {
  Label label;
  Label elabel;
  Vertex adjacent;
  Vertex represent;
  Size size;
  Size represent_idx;
};

inline bool QueryGraph::IsNECRepresentation(Vertex v) const {
  return NEC_map_[v] == v;
}

inline bool QueryGraph::IsInNEC(Vertex v) const {
  return NEC_map_[v] != INVALID_VTX;
}

inline Size QueryGraph::GetNumNECLabel() const { return num_NEC_label_; }

inline Size QueryGraph::GetNECStartOffset(Size i) const {
  return NEC_start_offs_[i];
}

inline Size QueryGraph::GetNECEndOffset(Size i) const {
  return NEC_start_offs_[i + 1];
}

inline const NECElement& QueryGraph::GetNECElement(Size i) const {
  return NEC_elems_[i];
}

inline bool QueryGraph::IsTree() const { return is_tree_; }

inline Size QueryGraph::GetNumNonLeafVertices() const {
  return num_non_leaf_vertices_;
}

inline Size QueryGraph::GetNECSize(Vertex v) const { return NEC_size_[v]; }

inline Size QueryGraph::GetNECRepresentative(Vertex v) const {
  if (IsInNEC(v)) {
    return NEC_map_[v];
  } else {
    return v;
  }
}

inline Label QueryGraph::GetMaxLabel() const { return max_label_; }

inline const std::vector<gbi::PropCons>&
QueryGraph::GetVertexPropertyConstraints(Vertex v) const {
  return vertex_property_constraints_[v];
}

inline const std::vector<gbi::PropCons>& QueryGraph::GetEdgePropertyConstraints(
    Size edge_idx) const {
  return edge_property_constraints_[edge_idx];
}
}  // namespace daf

#endif  // QUERY_GRAPH_H_
