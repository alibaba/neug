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

void Graph::LoadProperty(const std::string& vertex_property_filename,
                         const std::string& edge_property_filename,
                         std::unordered_map<std::string, int>& label2id_mapping,
                         std::shared_ptr<gbi::SchemaGraph> schema_graph) {
  using json = nlohmann::json;

  // Clear existing properties
  vertex_property_names_.clear();
  edge_property_names_.clear();
  vertex_properties_.clear();
  edge_properties_.clear();

  // Initialize property structures
  vertex_property_names_.resize(num_label_);  // Use vertex label count
  vertex_properties_.resize(num_vertex_);
  edge_property_names_.resize(num_edge_labels_);
  edge_properties_.resize(num_edge_);

  // Load vertex properties from JSON file
  std::ifstream vertex_file(vertex_property_filename);
  if (vertex_file.is_open()) {
    std::string line;
    while (std::getline(vertex_file, line)) {
      if (line.empty())
        continue;
      try {
        json j = json::parse(line);
        int vertex_id = j["id"];
        int label = label2id_mapping[j["label"]];

        if (vertex_id >= num_vertex_ || label >= num_label_) {
          continue;  // Skip invalid vertices
        }

        // Check if this is first vertex with this label
        bool is_first_for_this_label = vertex_property_names_[label].empty();

        if (is_first_for_this_label) {
          // Set property schema for this vertex label
          for (auto& [key, value] : j.items()) {
            if (key != "label") {
              vertex_property_names_[label].push_back(key);
            }
          }
        }

        // Store property values
        vertex_properties_[vertex_id].resize(
            vertex_property_names_[label].size());

        for (size_t i = 0; i < vertex_property_names_[label].size(); i++) {
          const std::string& prop_name = vertex_property_names_[label][i];
          if (j.contains(prop_name)) {
            auto& val = j[prop_name];
            if (val.is_number_integer()) {
              vertex_properties_[vertex_id][i] = gbi::Value((int32_t) val);
            } else if (val.is_number_float()) {
              vertex_properties_[vertex_id][i] = gbi::Value((double) val);
            } else if (val.is_string()) {
              vertex_properties_[vertex_id][i] = gbi::Value((std::string) val);
            } else if (val.is_boolean()) {
              vertex_properties_[vertex_id][i] =
                  gbi::Value((int32_t) (val ? 1 : 0));
            } else if (val.is_null()) {
              gbi::EIdType schema_vertex_id = schema_graph->label_vertex[label];
              gbi::ValueType value_type =
                  schema_graph
                      ->vertex_value_type_map[schema_vertex_id][prop_name];
              vertex_properties_[vertex_id][i] =
                  gbi::GetDefaultValue(value_type);
            }
          }
        }
      } catch (const std::exception& e) {
        LOG(WARNING) << "Error parsing vertex property line: " << line << ": "
                     << e.what();
      }
    }
    vertex_file.close();
  }

  // Load edge properties from JSON file
  std::ifstream edge_file(edge_property_filename);
  if (edge_file.is_open()) {
    std::string line;
    Size edge_idx = 0;

    while (std::getline(edge_file, line) && edge_idx < num_edge_ / 2) {
      if (line.empty())
        continue;
      try {
        json j = json::parse(line);
        Label label = label2id_mapping[j["type"]];
        int src_id = j["src"];
        int dst_id = j["dst"];
        int src_label_id = label_[src_id];
        int dst_label_id = label_[dst_id];

        bool is_first_for_this_label = edge_property_names_[label].empty();

        if (is_first_for_this_label) {
          // Set property schema for this edge label
          for (auto& [key, value] : j.items()) {
            if (key != "src" && key != "dst" && key != "type") {
              edge_property_names_[label].push_back(key);
            }
          }
        }

        // Initialize property storage for both edge directions
        edge_properties_[edge_idx * 2].resize(
            edge_property_names_[label].size());
        edge_properties_[edge_idx * 2 + 1].resize(
            edge_property_names_[label].size());

        // Store property values for current edge
        if (edge_idx * 2 < num_edge_) {
          for (size_t i = 0; i < edge_property_names_[label].size(); i++) {
            const std::string& prop_name = edge_property_names_[label][i];
            if (j.contains(prop_name)) {
              auto& val = j[prop_name];
              gbi::Value prop_value;
              if (val.is_number_integer()) {
                prop_value = gbi::Value((int64_t) val);
              } else if (val.is_number_float()) {
                prop_value = gbi::Value((double) val);
              } else if (val.is_string()) {
                prop_value = gbi::Value((std::string) val);
              } else if (val.is_boolean()) {
                prop_value = gbi::Value::BOOLEAN(val ? 1 : 0);
              } else if (val.is_null()) {
                gbi::EIdType schema_edge_id =
                    schema_graph
                        ->label_edge[label][{src_label_id, dst_label_id}];
                gbi::ValueType value_type =
                    schema_graph
                        ->edge_value_type_map[schema_edge_id][prop_name];
                prop_value = gbi::GetDefaultValue(value_type);
              }

              // For undirected graph, both directions have same properties
              edge_properties_[edge_idx * 2][i] = prop_value;
              edge_properties_[edge_idx * 2 + 1][i] = prop_value;
            }
          }
        }
        edge_idx++;
      } catch (const std::exception& e) {
        LOG(WARNING) << "Error parsing edge property line: " << line << ": "
                     << e.what();
      }
    }
    edge_file.close();
  }

  LOG(INFO) << "Loaded properties for " << num_vertex_ << " vertices and "
            << num_edge_ / 2 << " edges.";
}

void Graph::LoadGraphProperties(
    const std::string& vertex_property_filename,
    const std::string& edge_property_filename,
    std::unordered_map<std::string, int>& label2id_mapping,
    std::shared_ptr<gbi::SchemaGraph> schema_graph) {
  LoadProperty(vertex_property_filename, edge_property_filename,
               label2id_mapping, schema_graph);
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
