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

#include "include/query_graph.h"

namespace daf {
QueryGraph::QueryGraph(const std::string& filename) : Graph(filename) {}
QueryGraph::~QueryGraph() {
  delete[] NEC_map_;
  delete[] NEC_elems_;

  if (NEC_start_offs_)
    delete[] NEC_start_offs_;
  delete[] NEC_size_;
}

void QueryGraph::SetVertexPropertyConstraints(
    Vertex v, const std::vector<gbi::PropCons>& constraints) {
  if (v < vertex_property_constraints_.size()) {
    vertex_property_constraints_[v] = constraints;
  }
}

void QueryGraph::SetEdgePropertyConstraints(
    Size edge_idx, const std::vector<gbi::PropCons>& constraints) {
  if (edge_idx < edge_property_constraints_.size()) {
    edge_property_constraints_[edge_idx] = constraints;
  }
}

bool QueryGraph::IsLeafVertex(Vertex u) const {
  // A leaf vertex is one with degree 1 (connected to only one other vertex)
  return GetDegree(u) == 1;
}

bool QueryGraph::LoadAndProcessGraph(const DataGraph& data) {
  std::vector<std::vector<Vertex>> adj_list;

  LoadRoughGraph(&adj_list);

  // no_edge_pairs 现在基于schema构建，在graph_storage.cpp中处理
  // 这里只初始化大小
  no_edge_pairs.resize(GetNumVertices());

  max_degree_ = 0;
  num_label_ = 0;
  max_label_ = 0;

  label_frequency_ = new Size[data.GetNumLabels()];
  start_off_ = new Size[GetNumVertices() + 1];
  linear_adj_list_ = new Vertex[GetNumEdges() * 2];
  core_num_ = new Size[GetNumVertices()];
  std::fill(label_frequency_, label_frequency_ + data.GetNumLabels(), 0);

  Size cur_idx = 0;

  // transfer label & construct adj list and label frequency
  for (Vertex v = 0; v < GetNumVertices(); ++v) {
    Label l = data.GetTransferredLabel(label_[v]);
    if (l == INVALID_LB)
      return false;
    if (label_frequency_[l] == 0)
      num_label_ += 1;
    label_[v] = l;
    max_label_ = std::max(max_label_, l);
    label_frequency_[l] += 1;
    if (adj_list[v].size() > max_degree_)
      max_degree_ = adj_list[v].size();

    start_off_[v] = cur_idx;
    core_num_[v] = adj_list[v].size();

    std::copy(adj_list[v].begin(), adj_list[v].end(),
              linear_adj_list_ + cur_idx);

    cur_idx += adj_list[v].size();
  }
  start_off_[GetNumVertices()] = num_edge_ * 2;

  // preprocess for query graph
  computeCoreNum();

  // Initialize property constraints vectors
  vertex_property_constraints_.resize(GetNumVertices());
  edge_property_constraints_.resize(GetNumEdges());

  is_tree_ = true;
  for (Vertex v = 0; v < GetNumVertices(); ++v) {
    if (GetCoreNum(v) > 1) {
      is_tree_ = false;
      break;
    }
  }

  ExtractResidualStructure();

  return true;
}

namespace {
struct NECInfo {
  bool visit = false;
  Vertex representative;
  Size NEC_elems_idx;
};
}  // namespace

void QueryGraph::ExtractResidualStructure() {
  NECInfo* NEC_infos_temp =
      new NECInfo[(max_label_ + 1) * num_edge_labels_ * GetNumVertices()];
  NEC_elems_ = new NECElement[GetNumVertices()];
  NEC_map_ = new Vertex[GetNumVertices()];
  NEC_size_ = new Size[GetNumVertices()];

  Size num_NEC_elems_ = 0;

  std::fill(NEC_map_, NEC_map_ + GetNumVertices(), INVALID_VTX);
  std::fill(NEC_size_, NEC_size_ + GetNumVertices(), 0);

  num_non_leaf_vertices_ = GetNumVertices();

  // construct NEC map
  for (Vertex v = 0; v < GetNumVertices(); ++v) {
    if (GetDegree(v) == 1) {
      Vertex p = GetNeighbor(GetStartOffset(v));
      Label l = GetLabel(v);
      Label el = GetEdgeLabel(GetEdgeIndex(v, p));

      NECInfo& info = NEC_infos_temp[l * num_edge_labels_ * GetNumVertices() +
                                     el * GetNumVertices() + p];
      if (!info.visit) {
        info = {true, v, num_NEC_elems_};
        NEC_map_[v] = v;

        for (Size nbr_idx = GetStartOffset(p); nbr_idx < GetEndOffset(p);
             ++nbr_idx) {
          Vertex nbr = GetNeighbor(nbr_idx);
          Label nbr_el = GetEdgeLabel(GetEdgeIndex(p, nbr));
          if (nbr == v && el == nbr_el) {
            NEC_elems_[num_NEC_elems_] = {l, el, p,
                                          v, 0,  nbr_idx - GetStartOffset(p)};
            break;
          }
        }
        num_NEC_elems_ += 1;
      } else {
        NEC_map_[v] = info.representative;
      }
      NEC_size_[info.representative] += 1;
      NEC_elems_[info.NEC_elems_idx].size += 1;
      num_non_leaf_vertices_ -= 1;
    } else {
      NEC_size_[v] += 1;
    }
  }

  for (Vertex v = 0; v < GetNumVertices(); ++v) {
    NEC_size_[v] = NEC_size_[GetNECRepresentative(v)];
  }

  num_NEC_label_ = 0;
  NEC_start_offs_ = nullptr;
  if (num_NEC_elems_ > 0) {
    // sort NEC elems by label
    std::sort(NEC_elems_, NEC_elems_ + num_NEC_elems_,
              [](const NECElement& a, const NECElement& b) -> bool {
                return a.label < b.label;
              });

    // construct start offsets of NEC elems for same label
    NEC_start_offs_ = new Size[GetNumVertices() + 1];
    NEC_start_offs_[0] = 0;
    num_NEC_label_ += 1;

    Label prev_label = NEC_elems_[0].label;
    for (Size i = 1; i < num_NEC_elems_; ++i) {
      if (NEC_elems_[i].label != prev_label) {
        prev_label = NEC_elems_[i].label;
        NEC_start_offs_[num_NEC_label_] = i;
        num_NEC_label_ += 1;
      }
    }
    NEC_start_offs_[num_NEC_label_] = num_NEC_elems_;
  }

  delete[] NEC_infos_temp;
}
}  // namespace daf
