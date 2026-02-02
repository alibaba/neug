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

#pragma once

#include <algorithm>
#include <cmath>
#include <tuple>
#include <vector>
#include <unordered_set>
#include <unordered_map>

#include "glog/logging.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace function {

/**
 * @brief Statistics about vertex and edge labels in the graph
 */
struct LabelStatistics {
    std::vector<double> vertex_label_probability;
    std::vector<double> edge_label_probability;
    double vertex_label_entropy = 0.0;
    double edge_label_entropy = 0.0;
};

/**
 * @brief Metadata and statistics for the data graph
 * 
 * Vertex IDs are consecutive integers starting from 0.
 * Only stores neighbors_ (undirected adjacency) for k-core computation.
 * Does NOT store the full graph structure.
 */
class DataGraphMeta {
public:
    explicit DataGraphMeta(const StorageReadInterface& graph);
    ~DataGraphMeta() = default;

    // Main preprocessing function
    void Preprocess();

    // Getters
    inline int GetNumVertices() const { return num_vertex_; }
    inline int GetNumEdges() const { return num_edge_; }
    inline int GetNumLabels() const { return num_labels_; }
    inline int GetNumEdgeLabels() const { return num_edge_labels_; }
    inline int GetMaxDegree() const { return max_degree_; }
    inline int GetMaxInDegree() const { return max_in_degree_; }
    inline int GetMaxOutDegree() const { return max_out_degree_; }
    inline int GetDegeneracy() const { return degeneracy_; }
    
    inline int GetDegree(int v) const {
        return (v >= 0 && v < (int)degree_.size()) ? degree_[v] : 0;
    }
    inline int GetVertexLabel(int v) const {
        return (v >= 0 && v < (int)vertex_label_.size()) ? vertex_label_[v] : 0;
    }
    // Get undirected neighbors (dynamic query from graph_)
    inline std::vector<int> GetNeighbors(int v) const {
        std::vector<int> result;
        if (v < 0 || v >= num_vertex_) return result;
        std::unordered_set<int> seen;
        // Collect all out-neighbors
        for (const auto& edge : GetAllOutIncidentEdges(v)) {
            int dst = std::get<1>(edge);
            if (seen.find(dst) == seen.end()) {
                seen.insert(dst);
                result.push_back(dst);
            }
        }
        // Collect all in-neighbors
        for (const auto& edge : GetAllInIncidentEdges(v)) {
            int src = std::get<0>(edge);
            if (seen.find(src) == seen.end()) {
                seen.insert(src);
                result.push_back(src);
            }
        }
        return result;
    }
    inline int GetCoreNum(int v) const {
        return (v >= 0 && v < (int)core_num_.size()) ? core_num_[v] : 0;
    }
    inline const std::vector<int>& GetDegeneracyOrder() const { return degeneracy_order_; }
    inline const std::vector<int>& GetCoreNums() const { return core_num_; }
    inline const LabelStatistics& GetLabelStatistics() const { return label_statistics_; }
    
    // Edge representation: (src, dst, edge_label)
    using Edge = std::tuple<vid_t, vid_t, label_t>;
    
    // Edge lookup: check if edge exists, return edge or invalid edge
    inline Edge GetEdge(int u, int v, int label) const {
        if (u < 0 || u >= num_vertex_) return {-1, -1, -1};
        label_t src_label = vertex_label_[u];
        label_t dst_label = (v >= 0 && v < num_vertex_) ? vertex_label_[v] : 0;
        const auto& schema = graph_.schema();
        if (!schema.edge_triplet_valid(src_label, dst_label, label)) {
            return {-1, -1, -1};
        }
        try {
            GenericView view = graph_.GetGenericOutgoingGraphView(src_label, dst_label, label);
            NbrList edges = view.get_edges(u);
            for (auto it = edges.begin(); it != edges.end(); ++it) {
                if (*it == (vid_t)v) {
                    return {(vid_t)u, (vid_t)v, (label_t)label};
                }
            }
        } catch (...) {}
        return {-1, -1, -1};
    }
    
    // Check if edge exists (any label)
    inline int GetEdgeIndex(int u, int v) const {
        if (u < 0 || u >= num_vertex_ || v < 0 || v >= num_vertex_) return -1;
        label_t src_label = vertex_label_[u];
        label_t dst_label = vertex_label_[v];
        const auto& schema = graph_.schema();
        for (label_t e_label = 0; e_label < num_edge_labels_; ++e_label) {
            if (!schema.edge_triplet_valid(src_label, dst_label, e_label)) continue;
            try {
                GenericView view = graph_.GetGenericOutgoingGraphView(src_label, dst_label, e_label);
                NbrList edges = view.get_edges(u);
                for (auto it = edges.begin(); it != edges.end(); ++it) {
                    if (*it == (vid_t)v) return e_label;  // Return edge label as index
                }
            } catch (...) {}
        }
        return -1;
    }
    
    // Check if edge with specific label exists
    inline int GetEdgeIndex(int u, int v, int label) const {
        Edge e = GetEdge(u, v, label);
        return std::get<0>(e) != (vid_t)-1 ? label : -1;
    }
    
    // Vertex by label lookup
    inline const std::vector<int>& GetVerticesByLabel(int label) const {
        static const std::vector<int> empty;
        if (label < 0 || label >= (int)vertices_by_label_.size()) return empty;
        return vertices_by_label_[label];
    }
    
    // Degree accessors for individual vertices
    inline int GetInDegree(int v) const {
        return (v >= 0 && v < (int)in_degree_.size()) ? in_degree_[v] : 0;
    }
    inline int GetOutDegree(int v) const {
        return (v >= 0 && v < (int)out_degree_.size()) ? out_degree_[v] : 0;
    }
    
    // Get out-edges from vertex v to vertices with dst_label
    // Returns vector of Edge tuples
    // Iterates over e_schemas_ directly instead of triple loop
    inline std::vector<Edge> GetOutIncidentEdges(int v, int target_dst_label) const {
        std::vector<Edge> result;
        if (v < 0 || v >= num_vertex_) return result;
        label_t src_label = vertex_label_[v];
        const auto& schema = graph_.schema();
        // Iterate over all valid edge triplets in e_schemas_
        for (const auto& [key, edge_schema] : schema.e_schemas_) {
            auto [s_label, d_label, e_label] = schema.parse_edge_label(key);
            if (s_label != src_label || d_label != (label_t)target_dst_label) continue;
            try {
                GenericView view = graph_.GetGenericOutgoingGraphView(s_label, d_label, e_label);
                NbrList edges = view.get_edges(v);
                for (auto it = edges.begin(); it != edges.end(); ++it) {
                    result.push_back({(vid_t)v, *it, e_label});
                }
            } catch (...) {}
        }
        return result;
    }
    
    // Get in-edges to vertex v from vertices with src_label
    // Iterates over e_schemas_ directly instead of triple loop
    inline std::vector<Edge> GetInIncidentEdges(int v, int target_src_label) const {
        std::vector<Edge> result;
        if (v < 0 || v >= num_vertex_) return result;
        label_t dst_label = vertex_label_[v];
        const auto& schema = graph_.schema();
        // Iterate over all valid edge triplets in e_schemas_
        for (const auto& [key, edge_schema] : schema.e_schemas_) {
            auto [s_label, d_label, e_label] = schema.parse_edge_label(key);
            if (s_label != (label_t)target_src_label || d_label != dst_label) continue;
            try {
                GenericView view = graph_.GetGenericIncomingGraphView(s_label, d_label, e_label);
                NbrList edges = view.get_edges(v);
                for (auto it = edges.begin(); it != edges.end(); ++it) {
                    result.push_back({*it, (vid_t)v, e_label});
                }
            } catch (...) {}
        }
        return result;
    }
    
    // Get all out-edges from vertex v
    // Iterates over e_schemas_ directly instead of triple loop
    inline std::vector<Edge> GetAllOutIncidentEdges(int v) const {
        std::vector<Edge> result;
        if (v < 0 || v >= num_vertex_) return result;
        label_t src_label = vertex_label_[v];
        const auto& schema = graph_.schema();
        // Iterate over all valid edge triplets in e_schemas_
        for (const auto& [key, edge_schema] : schema.e_schemas_) {
            auto [s_label, d_label, e_label] = schema.parse_edge_label(key);
            if (s_label != src_label) continue;
            try {
                GenericView view = graph_.GetGenericOutgoingGraphView(s_label, d_label, e_label);
                NbrList edges = view.get_edges(v);
                for (auto it = edges.begin(); it != edges.end(); ++it) {
                    result.push_back({(vid_t)v, *it, e_label});
                }
            } catch (...) {}
        }
        return result;
    }
    
    // Get all in-edges to vertex v
    // Iterates over e_schemas_ directly instead of triple loop
    inline std::vector<Edge> GetAllInIncidentEdges(int v) const {
        std::vector<Edge> result;
        if (v < 0 || v >= num_vertex_) return result;
        label_t dst_label = vertex_label_[v];
        const auto& schema = graph_.schema();
        // Iterate over all valid edge triplets in e_schemas_
        for (const auto& [key, edge_schema] : schema.e_schemas_) {
            auto [s_label, d_label, e_label] = schema.parse_edge_label(key);
            if (d_label != dst_label) continue;
            try {
                GenericView view = graph_.GetGenericIncomingGraphView(s_label, d_label, e_label);
                NbrList edges = view.get_edges(v);
                for (auto it = edges.begin(); it != edges.end(); ++it) {
                    result.push_back({*it, (vid_t)v, e_label});
                }
            } catch (...) {}
        }
        return result;
    }
    
    // Get out-neighbors (directed)
    // Iterates over e_schemas_ directly instead of triple loop
    inline std::vector<int> GetOutNeighbors(int v) const {
        std::vector<int> result;
        if (v < 0 || v >= num_vertex_) return result;
        label_t src_label = vertex_label_[v];
        const auto& schema = graph_.schema();
        std::unordered_set<int> seen;
        // Iterate over all valid edge triplets in e_schemas_
        for (const auto& [key, edge_schema] : schema.e_schemas_) {
            auto [s_label, d_label, e_label] = schema.parse_edge_label(key);
            if (s_label != src_label) continue;
            try {
                GenericView view = graph_.GetGenericOutgoingGraphView(s_label, d_label, e_label);
                NbrList edges = view.get_edges(v);
                for (auto it = edges.begin(); it != edges.end(); ++it) {
                    int dst = *it;
                    if (seen.find(dst) == seen.end()) {
                        seen.insert(dst);
                        result.push_back(dst);
                    }
                }
            } catch (...) {}
        }
        return result;
    }
    
    // Get in-neighbors (directed)
    // Iterates over e_schemas_ directly instead of triple loop
    inline std::vector<int> GetInNeighbors(int v) const {
        std::vector<int> result;
        if (v < 0 || v >= num_vertex_) return result;
        label_t dst_label = vertex_label_[v];
        const auto& schema = graph_.schema();
        std::unordered_set<int> seen;
        // Iterate over all valid edge triplets in e_schemas_
        for (const auto& [key, edge_schema] : schema.e_schemas_) {
            auto [s_label, d_label, e_label] = schema.parse_edge_label(key);
            if (d_label != dst_label) continue;
            try {
                GenericView view = graph_.GetGenericIncomingGraphView(s_label, d_label, e_label);
                NbrList edges = view.get_edges(v);
                for (auto it = edges.begin(); it != edges.end(); ++it) {
                    int src = *it;
                    if (seen.find(src) == seen.end()) {
                        seen.insert(src);
                        result.push_back(src);
                    }
                }
            } catch (...) {}
        }
        return result;
    }
    
    // Edge accessors (using Edge tuple)
    inline label_t GetEdgeLabel(const Edge& edge) const {
        return std::get<2>(edge);
    }
    inline vid_t GetDestPoint(const Edge& edge) const {
        return std::get<1>(edge);
    }
    inline vid_t GetSourcePoint(const Edge& edge) const {
        return std::get<0>(edge);
    }
    
    // Convert Edge to unique integer index for BitsetEdgeCS etc.
    // index = src * num_vertex_ * num_edge_labels_ + dst * num_edge_labels_ + label
    inline int EdgeToIndex(const Edge& edge) const {
        vid_t src = std::get<0>(edge);
        vid_t dst = std::get<1>(edge);
        label_t label = std::get<2>(edge);
        if (src == (vid_t)-1) return -1;
        return src * num_vertex_ * num_edge_labels_ + dst * num_edge_labels_ + label;
    }
    
    // Maximum possible edge index (for sizing BitsetEdgeCS)
    inline int GetMaxEdgeIndex() const {
        return num_vertex_ * num_vertex_ * num_edge_labels_;
    }

    // Configuration flags
    bool build_triangle = false;
    bool build_four_cycle = false;

private:
    // Build neighbors_ and compute degree statistics
    void BuildNeighbors();

    // Compute core numbers (k-core decomposition)
    void ComputeCoreNum();

    // Compute label statistics
    void ComputeLabelStatistics();

    // Graph storage interface
    const StorageReadInterface& graph_;

    // Counts
    int num_vertex_ = 0;
    int num_edge_ = 0;
    int num_labels_ = 0;
    int num_edge_labels_ = 0;

    // Per-vertex label (vertex_label_[v] = label of vertex v)
    std::vector<int> vertex_label_;
    std::vector<std::vector<int>> vertices_by_label_;

    // Degree statistics
    int max_degree_ = 0;
    int max_in_degree_ = 0;
    int max_out_degree_ = 0;

    std::vector<int> in_degree_, out_degree_, degree_;

    // K-core decomposition results
    std::vector<int> core_num_;
    std::vector<int> degeneracy_order_;
    int degeneracy_ = 0;

    // Label statistics
    LabelStatistics label_statistics_;
};

}  // namespace function
}  // namespace neug
