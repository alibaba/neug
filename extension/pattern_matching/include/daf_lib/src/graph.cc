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

#include "include/graph.h"

#include <glog/logging.h>

namespace daf {
Graph::Graph(const std::string& filename)
    : filename_(filename), fin_(filename) {
  num_edge_labels_ = 0;
  edge_labels_ = nullptr;
}

Graph::~Graph() {
  delete[] start_off_;
  delete[] linear_adj_list_;
  delete[] label_;
  delete[] label_frequency_;
  delete[] core_num_;
  delete[] edge_labels_;
}

void Graph::LoadRoughGraph(std::vector<std::vector<Vertex>>* graph) {
  if (!fin_.is_open()) {
    LOG(ERROR) << "Graph file " << filename_ << " not found.";
    exit(EXIT_FAILURE);
  }

  Size v, e;
  char type;

  fin_ >> type >> v >> e;

  num_vertex_ = v;
  num_edge_ = e;
  label_ = new Label[v];
  edge_index_map_.resize(num_vertex_);

  // Initialize edge structures for undirected graph (each edge counted twice)
  edge_labels_ = new Label[e * 2];
  edge_list_.reserve(e * 2);
  edge_properties_.resize(e * 2);

  graph->resize(v);

  Size edge_idx = 0;
  Label max_edge_label = 0;

  // preprocessing
  while (fin_ >> type) {
    if (type == 'v') {
      Vertex id;
      Label l;
      fin_ >> id >> l;

      label_[id] = l;
    } else if (type == 'e') {
      Vertex v1, v2;
      Label edge_label = 0;  // Default edge label

      fin_ >> v1 >> v2;

      // Try to read edge label (optional)
      if (fin_.peek() != '\n' && fin_.peek() != EOF && fin_.peek() != '\r') {
        fin_ >> edge_label;
      }

      (*graph)[v1].push_back(v2);
      (*graph)[v2].push_back(v1);

      // Add both directions for undirected graph
      edge_list_.push_back({v1, v2});
      edge_list_.push_back({v2, v1});
      edge_labels_[edge_idx] = edge_label;
      edge_labels_[edge_idx + 1] = edge_label;

      // Build edge index map
      edge_index_map_[v1][v2][edge_label] = edge_idx;
      edge_index_map_[v2][v1][edge_label] = edge_idx + 1;

      // Track max edge label
      max_edge_label = std::max(max_edge_label, edge_label);

      edge_idx += 2;
    }
  }

  // Set number of edge labels
  num_edge_labels_ = max_edge_label + 1;

  fin_.close();
}

void Graph::LoadRoughGraph(
    std::vector<std::vector<Vertex>>* graph,
    std::unordered_map<std::string, int>& label2id_mapping) {
  if (!fin_.is_open()) {
    LOG(ERROR) << "Graph file " << filename_ << " not found.";
    exit(EXIT_FAILURE);
  }

  Size v, e;
  char type;

  fin_ >> type >> v >> e;

  num_vertex_ = v;
  num_edge_ = e;
  label_ = new Label[v];
  edge_index_map_.resize(num_vertex_);

  // Initialize edge structures for undirected graph (each edge counted twice)
  edge_labels_ = new Label[e * 2];
  edge_list_.reserve(e * 2);
  edge_properties_.resize(e * 2);

  graph->resize(v);

  Size edge_idx = 0;
  Label max_edge_label = 0;

  // preprocessing
  while (fin_ >> type) {
    if (type == 'v') {
      Vertex id;
      std::string label_str;
      fin_ >> id >> label_str;

      label_[id] = label2id_mapping[label_str];
    } else if (type == 'e') {
      Vertex v1, v2;
      Label edge_label = 0;  // Default edge label
      std::string edge_label_str;

      fin_ >> v1 >> v2;

      // Try to read edge label (optional)
      if (fin_.peek() != '\n' && fin_.peek() != EOF && fin_.peek() != '\r') {
        fin_ >> edge_label_str;
        edge_label = label2id_mapping[edge_label_str];
      }

      (*graph)[v1].push_back(v2);
      (*graph)[v2].push_back(v1);

      // Add both directions for undirected graph
      edge_list_.push_back({v1, v2});
      edge_list_.push_back({v2, v1});
      edge_labels_[edge_idx] = edge_label;
      edge_labels_[edge_idx + 1] = edge_label;

      // Build edge index map
      edge_index_map_[v1][v2][edge_label] = edge_idx;
      edge_index_map_[v2][v1][edge_label] = edge_idx + 1;

      // Track max edge label
      max_edge_label = std::max(max_edge_label, edge_label);

      edge_idx += 2;
    }
  }

  // Set number of edge labels
  num_edge_labels_ = max_edge_label + 1;

  fin_.close();
}

bool Graph::CheckVertexPropertyConstraints(
    const std::vector<gbi::PropCons>& constraints, Vertex v) const {
  if (constraints.empty())
    return true;
  if (!HasVertexAttributes())
    return constraints.empty();

  Label vertex_label = GetLabel(v);
  const auto& prop_names = GetVertexPropertyNames(vertex_label);
  const auto& prop_values = GetVertexProperties(v);
  // Check each constraint
  for (const auto& constraint : constraints) {
    if (constraint._use_expr) {
      // Expression-based constraint
      auto resolver = [&prop_names, &prop_values](
                          const std::string& alias,
                          const std::string& prop_name) -> gbi::Value {
        for (size_t i = 0; i < prop_names.size(); i++) {
          if (prop_names[i] == prop_name) {
            if (i < prop_values.size()) {
              if (prop_values[i].IsNull() ||
                  prop_values[i].is_orgininally_null) {
                return gbi::Value();
              }
              return prop_values[i];
            }
            break;
          }
        }
        return gbi::Value();
      };

      if (!constraint.CheckExpr(resolver)) {
        return false;
      }
    } else {
      // Traditional simple constraint
      // Find property index by name
      size_t prop_idx = SIZE_MAX;
      for (size_t i = 0; i < prop_names.size(); i++) {
        if (prop_names[i] == constraint._prop_name) {
          prop_idx = i;
          break;
        }
      }

      // Property not found - special handling for IS NULL
      if (prop_idx == SIZE_MAX) {
        if (constraint._comp_type == gbi::CompType::COMP_IS_NULL) {
          continue;  // IS NULL: property not found is considered as null
        }
        return false;
      }

      // Check constraint
      const gbi::Value& actual_value = prop_values[prop_idx];
      const gbi::Value& constraint_value = constraint._value;
      bool constraint_satisfied = false;
      switch (constraint._comp_type) {
      case gbi::CompType::COMP_EQUAL:
        constraint_satisfied = (actual_value == constraint_value);
        break;
      case gbi::CompType::COMP_GREATER:
        constraint_satisfied = (actual_value > constraint_value);
        break;
      case gbi::CompType::COMP_LESS:
        constraint_satisfied = (actual_value < constraint_value);
        break;
      case gbi::CompType::COMP_GREATER_EQUAL:
        constraint_satisfied = (actual_value >= constraint_value);
        break;
      case gbi::CompType::COMP_LESS_EQUAL:
        constraint_satisfied = (actual_value <= constraint_value);
        break;
      case gbi::CompType::COMP_IS_NULL:
        constraint_satisfied =
            (actual_value.IsNull() || actual_value.is_orgininally_null);
        break;
      case gbi::CompType::COMP_IS_NOT_NULL:
        constraint_satisfied =
            (!actual_value.IsNull() && !actual_value.is_orgininally_null);
        break;
      // TODO: Implement COMP_IN and COMP_NOT_IN
      default:
        constraint_satisfied = false;
      }

      if (!constraint_satisfied)
        return false;
    }
  }

  return true;
}

bool Graph::CheckEdgePropertyConstraints(
    const std::vector<gbi::PropCons>& constraints, Size edge_idx) const {
  if (constraints.empty())
    return true;
  if (!HasEdgeAttributes())
    return constraints.empty();

  Label edge_label = GetEdgeLabel(edge_idx);
  const auto& prop_names = GetEdgePropertyNames(edge_label);
  const auto& prop_values = GetEdgeProperties(edge_idx);

  // Check each constraint
  for (const auto& constraint : constraints) {
    if (constraint._use_expr) {
      // Expression-based constraint
      auto resolver = [&prop_names, &prop_values](
                          const std::string& alias,
                          const std::string& prop_name) -> gbi::Value {
        for (size_t i = 0; i < prop_names.size(); i++) {
          if (prop_names[i] == prop_name) {
            if (i < prop_values.size()) {
              if (prop_values[i].IsNull() ||
                  prop_values[i].is_orgininally_null) {
                return gbi::Value();
              }
              return prop_values[i];
            }
            break;
          }
        }
        return gbi::Value();
      };

      if (!constraint.CheckExpr(resolver)) {
        return false;
      }
    } else {
      // Traditional simple constraint
      // Find property index by name
      size_t prop_idx = SIZE_MAX;
      for (size_t i = 0; i < prop_names.size(); i++) {
        if (prop_names[i] == constraint._prop_name) {
          prop_idx = i;
          break;
        }
      }

      // Property not found - special handling for IS NULL
      if (prop_idx == SIZE_MAX) {
        if (constraint._comp_type == gbi::CompType::COMP_IS_NULL) {
          continue;  // IS NULL: property not found is considered as null
        }
        return false;
      }

      // Check constraint
      const gbi::Value& actual_value = prop_values[prop_idx];
      const gbi::Value& constraint_value = constraint._value;

      bool constraint_satisfied = false;
      switch (constraint._comp_type) {
      case gbi::CompType::COMP_EQUAL:
        constraint_satisfied = (actual_value == constraint_value);
        break;
      case gbi::CompType::COMP_GREATER:
        constraint_satisfied = (actual_value > constraint_value);
        break;
      case gbi::CompType::COMP_LESS:
        constraint_satisfied = (actual_value < constraint_value);
        break;
      case gbi::CompType::COMP_GREATER_EQUAL:
        constraint_satisfied = (actual_value >= constraint_value);
        break;
      case gbi::CompType::COMP_LESS_EQUAL:
        constraint_satisfied = (actual_value <= constraint_value);
        break;
      case gbi::CompType::COMP_IS_NULL:
        constraint_satisfied =
            (actual_value.IsNull() || actual_value.is_orgininally_null);
        break;
      case gbi::CompType::COMP_IS_NOT_NULL:
        constraint_satisfied =
            (!actual_value.IsNull() && !actual_value.is_orgininally_null);
        break;
      // TODO: Implement COMP_IN and COMP_NOT_IN
      default:
        constraint_satisfied = false;
      }

      if (!constraint_satisfied)
        return false;
    }
  }

  return true;
}

void Graph::computeCoreNum() {
  Size* bin = new Size[max_degree_ + 1];
  Size* pos = new Size[GetNumVertices()];
  Vertex* vert = new Vertex[GetNumVertices()];

  std::fill(bin, bin + (max_degree_ + 1), 0);

  for (Vertex v = 0; v < GetNumVertices(); ++v) {
    bin[core_num_[v]] += 1;
  }

  Size start = 0;
  Size num;

  for (Size d = 0; d <= max_degree_; ++d) {
    num = bin[d];
    bin[d] = start;
    start += num;
  }

  for (Vertex v = 0; v < GetNumVertices(); ++v) {
    pos[v] = bin[core_num_[v]];
    vert[pos[v]] = v;
    bin[core_num_[v]] += 1;
  }

  for (Size d = max_degree_; d--;)
    bin[d + 1] = bin[d];
  bin[0] = 0;

  for (Size i = 0; i < GetNumVertices(); ++i) {
    Vertex v = vert[i];

    for (Size j = GetStartOffset(v); j < GetEndOffset(v); j++) {
      Vertex u = GetNeighbor(j);

      if (core_num_[u] > core_num_[v]) {
        Size du = core_num_[u];
        Size pu = pos[u];

        Size pw = bin[du];
        Vertex w = vert[pw];

        if (u != w) {
          pos[u] = pw;
          pos[w] = pu;
          vert[pu] = w;
          vert[pw] = u;
        }

        bin[du]++;
        core_num_[u]--;
      }
    }
  }

  delete[] bin;
  delete[] pos;
  delete[] vert;
}
}  // namespace daf
