/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "data_graph_meta.h"
#include <unordered_set>

namespace neug {
namespace function {

DataGraphMeta::DataGraphMeta(const StorageReadInterface& graph) 
    : graph_(graph) {
}

void DataGraphMeta::Preprocess() {
    LOG(INFO) << "[DataGraphMeta] Starting preprocessing...";
    
    // Step 1: Build neighbors_ and compute degree statistics
    BuildNeighbors();
    
    // Step 2: Compute k-core decomposition
    ComputeCoreNum();
    
    // Step 3: Compute label statistics
    ComputeLabelStatistics();
    
    LOG(INFO) << "[DataGraphMeta] Preprocessing complete.";
    LOG(INFO) << "  Vertices: " << num_vertex_ << ", Edges: " << num_edge_;
    LOG(INFO) << "  Max degree: " << max_degree_ 
              << ", Max in-degree: " << max_in_degree_ 
              << ", Max out-degree: " << max_out_degree_;
    LOG(INFO) << "  Degeneracy: " << degeneracy_;
}

void DataGraphMeta::BuildNeighbors() {
    const auto& schema = graph_.schema();
    label_t num_vertex_labels = schema.vertex_label_num();
    label_t num_edge_labels = schema.edge_label_num();
    num_labels_ = num_vertex_labels;
    num_edge_labels_ = num_edge_labels;
    
    // Step 1: Count total vertices (vid is 0 to size-1 for each label)
    // Assume vertices across all labels use the same vid space (0 to num_vertex-1)
    num_vertex_ = 0;
    for (label_t label = 0; label < num_vertex_labels; ++label) {
        if (!schema.vertex_label_valid(label)) continue;
        VertexSet vs = graph_.GetVertexSet(label);
        num_vertex_ = std::max(num_vertex_, static_cast<int>(vs.size()));
    }
    
    if (num_vertex_ == 0) {
        LOG(WARNING) << "[BuildNeighbors] No vertices found";
        return;
    }
    
    // Initialize vertex_label_ array (record each vertex's label)
    vertex_label_.resize(num_vertex_, 0);
    vertices_by_label_.resize(num_vertex_labels);
    for (label_t label = 0; label < num_vertex_labels; ++label) {
        if (!schema.vertex_label_valid(label)) continue;
        VertexSet vs = graph_.GetVertexSet(label);
        for (vid_t vid : vs) {
            if (static_cast<int>(vid) < num_vertex_) {
                vertex_label_[vid] = label;
                vertices_by_label_[label].push_back(static_cast<int>(vid));
            }
        }
    }
    
    // Initialize neighbors_ and temporary degree arrays
    in_degree_.resize(num_vertex_, 0);
    out_degree_.resize(num_vertex_, 0);
    degree_.resize(num_vertex_, 0);
    
    // Step 2: Iterate through all edge triplets using e_schemas_, compute degrees
    num_edge_ = 0;
    for (const auto& [key, edge_schema] : schema.e_schemas_) {
        auto [src_label, dst_label, e_label] = schema.parse_edge_label(key);
        
        try {
            GenericView out_view = graph_.GetGenericOutgoingGraphView(
                src_label, dst_label, e_label);
            
            VertexSet src_vs = graph_.GetVertexSet(src_label);
            for (vid_t src_vid : src_vs) {
                if (!graph_.IsValidVertex(src_label, src_vid)) continue;
                if (static_cast<int>(src_vid) >= num_vertex_) continue;
                
                NbrList edges = out_view.get_edges(src_vid);
                NbrIterator it;
                it.init(edges.start_ptr, edges.end_ptr, edges.cfg, edges.timestamp);
                NbrIterator end;
                end.init(edges.end_ptr, edges.end_ptr, edges.cfg, edges.timestamp);
                
                while (it != end) {
                    vid_t dst_vid = *it;
                    if (static_cast<int>(dst_vid) < num_vertex_) {
                        // Count degrees
                        out_degree_[src_vid]++;
                        in_degree_[dst_vid]++;
                        degree_[src_vid]++;
                        degree_[dst_vid]++;

                        num_edge_++;
                    }
                    ++it;
                }
            }
        } catch (...) {
            // Edge triplet doesn't exist in storage
        }
    }
    
    // Step 3: Compute max degrees
    max_degree_ = 0;
    max_in_degree_ = 0;
    max_out_degree_ = 0;
    
    for (int v = 0; v < num_vertex_; ++v) {
        max_degree_ = std::max(max_degree_, degree_[v]);
        max_in_degree_ = std::max(max_in_degree_, in_degree_[v]);
        max_out_degree_ = std::max(max_out_degree_, out_degree_[v]);
    }
    
    LOG(INFO) << "[BuildNeighbors] Built neighbors for " << num_vertex_ 
              << " vertices, " << num_edge_ << " edges";
}

/**
 * @brief Compute the core number of each vertex
 * 
 * Exact translation of the original ComputeCoreNum algorithm.
 * Uses bin-sort based k-core decomposition on the undirected neighbors_.
 */
void DataGraphMeta::ComputeCoreNum() {
    if (num_vertex_ == 0) {
        degeneracy_ = 0;
        return;
    }
    
    core_num_.resize(num_vertex_, 0);
    int* bin = new int[max_degree_ + 1];
    int* pos = new int[num_vertex_];
    int* vert = new int[num_vertex_];
    
    std::fill(bin, bin + (max_degree_ + 1), 0);
    
    // Initialize core numbers with degree (from neighbors_)
    for (int v = 0; v < num_vertex_; v++) {
        core_num_[v] = degree_[v];
        bin[core_num_[v]] += 1;
    }
    
    int start = 0;
    int num;
    
    // Compute bin boundaries (cumulative sum)
    for (int d = 0; d <= max_degree_; d++) {
        num = bin[d];
        bin[d] = start;
        start += num;
    }
    
    // Sort vertices into bins by degree
    for (int v = 0; v < num_vertex_; v++) {
        pos[v] = bin[core_num_[v]];
        vert[pos[v]] = v;
        bin[core_num_[v]] += 1;
    }
    
    // Restore bin boundaries
    for (int d = max_degree_; d--;)
        bin[d + 1] = bin[d];
    bin[0] = 0;
    
    // Core decomposition: process vertices in order of degree
    for (int i = 0; i < num_vertex_; i++) {
        int v = vert[i];
        
        // For each neighbor of v (using neighbors_)
        for (int u : GetNeighbors(v)) {
            if (core_num_[u] > core_num_[v]) {
                int du = core_num_[u];
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
                core_num_[u]--;
            }
        }
    }
    
    // Build degeneracy order (reverse of processing order)
    degeneracy_order_.resize(num_vertex_);
    for (int i = 0; i < num_vertex_; i++) {
        degeneracy_order_[i] = vert[i];
    }
    std::reverse(degeneracy_order_.begin(), degeneracy_order_.end());
    
    // Find maximum core number (degeneracy)
    degeneracy_ = 0;
    for (int i = 0; i < num_vertex_; i++) {
        degeneracy_ = std::max(core_num_[i], degeneracy_);
    }
    
    delete[] bin;
    delete[] pos;
    delete[] vert;
    
    LOG(INFO) << "[ComputeCoreNum] Degeneracy: " << degeneracy_;
}

/**
 * @brief Compute label statistics following original logic:
 * 
 * label_statistics.vertex_label_probability.resize(GetNumLabels(), 1e-4);
 * for (int i = 0; i < GetNumVertices(); i++) {
 *     label_statistics.vertex_label_probability[GetVertexLabel(i)] += 1.0;
 * }
 * for (int i = 0; i < GetNumLabels(); i++) {
 *     label_statistics.vertex_label_probability[i] /= (1.0 * GetNumVertices());
 * }
 * for (auto x : label_statistics.vertex_label_probability) {
 *     label_statistics.vertex_label_entropy -= x * log2(x);
 * }
 */
void DataGraphMeta::ComputeLabelStatistics() {
    if (num_vertex_ == 0) {
        LOG(WARNING) << "[ComputeLabelStatistics] No vertices found";
        return;
    }
    
    // Initialize with small epsilon (1e-4) as in original
    label_statistics_.vertex_label_probability.resize(GetNumLabels(), 1e-4);
    
    // Count vertices per label
    for (int i = 0; i < GetNumVertices(); i++) {
        label_statistics_.vertex_label_probability[GetVertexLabel(i)] += 1.0;
    }
    
    // Normalize to get probability
    for (int i = 0; i < GetNumLabels(); i++) {
        label_statistics_.vertex_label_probability[i] /= (1.0 * GetNumVertices());
    }
    
    // Compute entropy: H = -sum(p * log2(p))
    label_statistics_.vertex_label_entropy = 0.0;
    for (auto x : label_statistics_.vertex_label_probability) {
        label_statistics_.vertex_label_entropy -= x * std::log2(x);
    }
    
    LOG(INFO) << "[ComputeLabelStatistics] Labels: " << GetNumLabels()
              << ", Entropy: " << label_statistics_.vertex_label_entropy;
}

}  // namespace function
}  // namespace neug
