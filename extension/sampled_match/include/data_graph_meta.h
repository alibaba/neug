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
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <any>
#include <functional>

#include "neug/storages/graph/graph_interface.h"
#include "neug/execution/common/types/value.h"

namespace neug {
namespace function {

// Hash function for std::pair<label_t, vid_t>
struct LabelVidHash {
    std::size_t operator()(const std::pair<label_t, vid_t>& p) const {
        return std::hash<uint64_t>()((static_cast<uint64_t>(p.first) << 32) | p.second);
    }
};

// Use Value from neug::execution
using Value = neug::execution::Value;

enum class CompType {
    COMP_EQUAL,
    COMP_GREATER,
    COMP_LESS,
    COMP_GREATER_EQUAL,
    COMP_LESS_EQUAL,
    COMP_IN,
    COMP_NOT_IN,
};

class PropCons {
public:
    PropCons() : _value(neug::DataTypeId::kUnknown) {}
    PropCons(std::string prop_name, CompType comp_type, Value value) :
        _prop_name(prop_name), _comp_type(comp_type), _value(std::move(value)) {}
    ~PropCons() {}

    std::string _prop_name;
    CompType _comp_type;
    Value _value;
};


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
    
    inline int GetDegree(int global_id) const {
        return (global_id >= 0 && global_id < (int)degree_.size()) ? degree_[global_id] : 0;
    }
    // Get vertex label by global_id (returns label_t as int)
    inline int GetVertexLabel(int global_id) const {
        return (global_id >= 0 && global_id < (int)vertex_label_.size()) ? vertex_label_[global_id] : 0;
    }
    // Get undirected neighbors by global_id (returns global_ids)
    inline std::vector<int> GetNeighbors(int global_id) const {
        std::vector<int> result;
        if (global_id < 0 || global_id >= num_vertex_) return result;
        std::unordered_set<int> seen;
        // Collect all out-neighbors (already returns global_ids)
        for (const auto& edge : GetAllOutIncidentEdges(global_id)) {
            int dst_global = std::get<1>(edge);
            if (seen.find(dst_global) == seen.end()) {
                seen.insert(dst_global);
                result.push_back(dst_global);
            }
        }
        // Collect all in-neighbors (already returns global_ids)
        for (const auto& edge : GetAllInIncidentEdges(global_id)) {
            int src_global = std::get<0>(edge);
            if (seen.find(src_global) == seen.end()) {
                seen.insert(src_global);
                result.push_back(src_global);
            }
        }
        return result;
    }
    inline int GetCoreNum(int global_id) const {
        return (global_id >= 0 && global_id < (int)core_num_.size()) ? core_num_[global_id] : 0;
    }
    inline const std::vector<int>& GetDegeneracyOrder() const { return degeneracy_order_; }
    inline const std::vector<int>& GetCoreNums() const { return core_num_; }
    inline const LabelStatistics& GetLabelStatistics() const { return label_statistics_; }
    
    // Edge representation: (src_global_id, dst_global_id, edge_label)
    using Edge = std::tuple<int, int, label_t>;
    
    // Edge lookup: check if edge exists, return edge or invalid edge
    // u, v are global_ids
    inline Edge GetEdge(int u, int v, int label) const {
        if (u < 0 || u >= num_vertex_ || v < 0 || v >= num_vertex_) return {-1, -1, 255};
        auto [src_label, src_vid] = ToLocalId(u);
        auto [dst_label, dst_vid] = ToLocalId(v);
        const auto& schema = graph_.schema();
        if (!schema.edge_triplet_valid(src_label, dst_label, label)) {
            return {-1, -1, 255};
        }
        try {
            GenericView view = graph_.GetGenericOutgoingGraphView(src_label, dst_label, label);
            NbrList edges = view.get_edges(src_vid);
            for (auto it = edges.begin(); it != edges.end(); ++it) {
                if (*it == dst_vid) {
                    return {u, v, (label_t)label};
                }
            }
        } catch (...) {}
        return {-1, -1, 255};
    }
    
    // Check if edge exists (any label), u, v are global_ids
    inline int GetEdgeIndex(int u, int v) const {
        if (u < 0 || u >= num_vertex_ || v < 0 || v >= num_vertex_) return -1;
        auto [src_label, src_vid] = ToLocalId(u);
        auto [dst_label, dst_vid] = ToLocalId(v);
        const auto& schema = graph_.schema();
        for (label_t e_label = 0; e_label < num_edge_labels_; ++e_label) {
            if (!schema.edge_triplet_valid(src_label, dst_label, e_label)) continue;
            try {
                GenericView view = graph_.GetGenericOutgoingGraphView(src_label, dst_label, e_label);
                NbrList edges = view.get_edges(src_vid);
                for (auto it = edges.begin(); it != edges.end(); ++it) {
                    if (*it == dst_vid) return e_label;  // Return edge label as index
                }
            } catch (...) {}
        }
        return -1;
    }
    
    // Check if edge with specific label exists, u, v are global_ids
    inline int GetEdgeIndex(int u, int v, int label) const {
        Edge e = GetEdge(u, v, label);
        return std::get<0>(e) != -1 ? label : -1;
    }
    
    // Vertex by label lookup
    inline const std::vector<int>& GetVerticesByLabel(int label) const {
        static const std::vector<int> empty;
        if (label < 0 || label >= (int)vertices_by_label_.size()) return empty;
        return vertices_by_label_[label];
    }
    
    // Degree accessors for individual vertices (by global_id)
    inline int GetInDegree(int global_id) const {
        return (global_id >= 0 && global_id < (int)in_degree_.size()) ? in_degree_[global_id] : 0;
    }
    inline int GetOutDegree(int global_id) const {
        return (global_id >= 0 && global_id < (int)out_degree_.size()) ? out_degree_[global_id] : 0;
    }
    
    // Get out-edges from vertex (by global_id) to vertices with target_dst_label
    // Returns vector of Edge tuples with global_ids
    inline std::vector<Edge> GetOutIncidentEdges(int global_id, int target_dst_label) const {
        std::vector<Edge> result;
        if (global_id < 0 || global_id >= num_vertex_) return result;
        auto [src_label, src_vid] = ToLocalId(global_id);
        const auto& schema = graph_.schema();
        // Iterate over all valid edge triplets in e_schemas_
        for (const auto& [key, edge_schema] : schema.get_all_edge_schemas()) {
            auto [s_label, d_label, e_label] = schema.parse_edge_label(key);
            if (s_label != src_label || d_label != (label_t)target_dst_label) continue;
            try {
                GenericView view = graph_.GetGenericOutgoingGraphView(s_label, d_label, e_label);
                NbrList edges = view.get_edges(src_vid);
                for (auto it = edges.begin(); it != edges.end(); ++it) {
                    int dst_global = ToGlobalId(d_label, *it);
                    if (dst_global >= 0) {
                        result.push_back({global_id, dst_global, e_label});
                    }
                }
            } catch (...) {}
        }
        return result;
    }
    
    // Get in-edges to vertex (by global_id) from vertices with target_src_label
    inline std::vector<Edge> GetInIncidentEdges(int global_id, int target_src_label) const {
        std::vector<Edge> result;
        if (global_id < 0 || global_id >= num_vertex_) return result;
        auto [dst_label, dst_vid] = ToLocalId(global_id);
        const auto& schema = graph_.schema();
        // Iterate over all valid edge triplets in e_schemas_
        for (const auto& [key, edge_schema] : schema.get_all_edge_schemas()) {
            auto [s_label, d_label, e_label] = schema.parse_edge_label(key);
            if (s_label != (label_t)target_src_label || d_label != dst_label) continue;
            try {
                GenericView view = graph_.GetGenericIncomingGraphView(d_label, s_label, e_label);
                NbrList edges = view.get_edges(dst_vid);
                for (auto it = edges.begin(); it != edges.end(); ++it) {
                    int src_global = ToGlobalId(s_label, *it);
                    if (src_global >= 0) {
                        result.push_back({src_global, global_id, e_label});
                    }
                }
            } catch (...) {}
        }
        return result;
    }
    
    // Get all out-edges from vertex (by global_id)
    inline std::vector<Edge> GetAllOutIncidentEdges(int global_id) const {
        std::vector<Edge> result;
        if (global_id < 0 || global_id >= num_vertex_) return result;
        auto [src_label, src_vid] = ToLocalId(global_id);
        const auto& schema = graph_.schema();
        // Iterate over all valid edge triplets in e_schemas_
        for (const auto& [key, edge_schema] : schema.get_all_edge_schemas()) {
            auto [s_label, d_label, e_label] = schema.parse_edge_label(key);
            if (s_label != src_label) continue;
            try {
                GenericView view = graph_.GetGenericOutgoingGraphView(s_label, d_label, e_label);
                NbrList edges = view.get_edges(src_vid);
                for (auto it = edges.begin(); it != edges.end(); ++it) {
                    int dst_global = ToGlobalId(d_label, *it);
                    if (dst_global >= 0) {
                        result.push_back({global_id, dst_global, e_label});
                    }
                }
            } catch (...) {}
        }
        return result;
    }
    
    // Get all in-edges to vertex (by global_id)
    inline std::vector<Edge> GetAllInIncidentEdges(int global_id) const {
        std::vector<Edge> result;
        if (global_id < 0 || global_id >= num_vertex_) return result;
        auto [dst_label, dst_vid] = ToLocalId(global_id);
        const auto& schema = graph_.schema();
        // Iterate over all valid edge triplets in e_schemas_
        for (const auto& [key, edge_schema] : schema.get_all_edge_schemas()) {
            auto [s_label, d_label, e_label] = schema.parse_edge_label(key);
            if (d_label != dst_label) continue;
            try {
                GenericView view = graph_.GetGenericIncomingGraphView(d_label, s_label, e_label);
                NbrList edges = view.get_edges(dst_vid);
                for (auto it = edges.begin(); it != edges.end(); ++it) {
                    int src_global = ToGlobalId(s_label, *it);
                    if (src_global >= 0) {
                        result.push_back({src_global, global_id, e_label});
                    }
                }
            } catch (...) {}
        }
        return result;
    }
    
    // Get out-neighbors (directed) by global_id, returns global_ids
    inline std::vector<int> GetOutNeighbors(int global_id) const {
        std::vector<int> result;
        if (global_id < 0 || global_id >= num_vertex_) return result;
        auto [src_label, src_vid] = ToLocalId(global_id);
        const auto& schema = graph_.schema();
        std::unordered_set<int> seen;
        // Iterate over all valid edge triplets in e_schemas_
        for (const auto& [key, edge_schema] : schema.get_all_edge_schemas()) {
            auto [s_label, d_label, e_label] = schema.parse_edge_label(key);
            if (s_label != src_label) continue;
            try {
                GenericView view = graph_.GetGenericOutgoingGraphView(s_label, d_label, e_label);
                NbrList edges = view.get_edges(src_vid);
                for (auto it = edges.begin(); it != edges.end(); ++it) {
                    int dst_global = ToGlobalId(d_label, *it);
                    if (dst_global >= 0 && seen.find(dst_global) == seen.end()) {
                        seen.insert(dst_global);
                        result.push_back(dst_global);
                    }
                }
            } catch (...) {}
        }
        return result;
    }
    
    // Get in-neighbors (directed) by global_id, returns global_ids
    inline std::vector<int> GetInNeighbors(int global_id) const {
        std::vector<int> result;
        if (global_id < 0 || global_id >= num_vertex_) return result;
        auto [dst_label, dst_vid] = ToLocalId(global_id);
        const auto& schema = graph_.schema();
        std::unordered_set<int> seen;
        // Iterate over all valid edge triplets in e_schemas_
        for (const auto& [key, edge_schema] : schema.get_all_edge_schemas()) {
            auto [s_label, d_label, e_label] = schema.parse_edge_label(key);
            if (d_label != dst_label) continue;
            try {
                GenericView view = graph_.GetGenericIncomingGraphView(d_label, s_label, e_label);
                NbrList edges = view.get_edges(dst_vid);
                for (auto it = edges.begin(); it != edges.end(); ++it) {
                    int src_global = ToGlobalId(s_label, *it);
                    if (src_global >= 0 && seen.find(src_global) == seen.end()) {
                        seen.insert(src_global);
                        result.push_back(src_global);
                    }
                }
            } catch (...) {}
        }
        return result;
    }
    
    // Edge accessors (using Edge tuple with global_ids)
    inline label_t GetEdgeLabel(const Edge& edge) const {
        return std::get<2>(edge);
    }
    inline int GetDestPoint(const Edge& edge) const {
        return std::get<1>(edge);  // Returns global_id
    }
    inline int GetSourcePoint(const Edge& edge) const {
        return std::get<0>(edge);  // Returns global_id
    }
    
    // Convert Edge to unique string key for BitsetEdgeCS etc.
    // Format: "src_global:dst_global:label"
    inline std::string EdgeToKey(const Edge& edge) const {
        int src = std::get<0>(edge);
        int dst = std::get<1>(edge);
        label_t label = std::get<2>(edge);
        if (src == -1) return "";
        return std::to_string(src) + ":" + std::to_string(dst) + ":" + std::to_string(label);
    }
    
    // Create edge key directly from components (for lookup without creating Edge tuple)
    inline std::string EdgeToKey(int src, int dst, label_t label) const {
        if (src == -1) return "";
        return std::to_string(src) + ":" + std::to_string(dst) + ":" + std::to_string(label);
    }

    // Configuration flags
    bool build_triangle = false;
    bool build_four_cycle = false;
    
    // ========== ID Mapping Methods ==========
    // Map (label, vid) to global_id
    inline int ToGlobalId(label_t label, vid_t vid) const {
        auto key = std::make_pair(label, vid);
        auto it = local_to_global_.find(key);
        return (it != local_to_global_.end()) ? it->second : -1;
    }
    
    // Map global_id back to (label, vid)
    inline std::pair<label_t, vid_t> ToLocalId(int global_id) const {
        if (global_id < 0 || global_id >= (int)global_to_local_.size()) {
            return {255, (vid_t)-1};  // Invalid
        }
        return global_to_local_[global_id];
    }
    
    // Get vertex label from global_id
    inline label_t GetVertexLabelFromGlobal(int global_id) const {
        if (global_id < 0 || global_id >= (int)global_to_local_.size()) return 255;
        return global_to_local_[global_id].first;
    }
    
    // Get original vid from global_id
    inline vid_t GetOriginalVid(int global_id) const {
        if (global_id < 0 || global_id >= (int)global_to_local_.size()) return (vid_t)-1;
        return global_to_local_[global_id].second;
    }

private:
    // Build ID mapping: (label, vid) <-> global_id
    void BuildIdMapping();
    
    // Build neighbors_ and compute degree statistics
    void BuildNeighbors();

    // Compute core numbers (k-core decomposition)
    void ComputeCoreNum();

    // Compute label statistics
    void ComputeLabelStatistics();

    // Graph storage interface
    const StorageReadInterface& graph_;

    // ========== ID Mapping ==========
    // (label, vid) -> global_id
    std::unordered_map<std::pair<label_t, vid_t>, int, LabelVidHash> local_to_global_;
    // global_id -> (label, vid)
    std::vector<std::pair<label_t, vid_t>> global_to_local_;

    // Counts
    int num_vertex_ = 0;  // Total number of vertices across all labels
    int num_edge_ = 0;
    int num_labels_ = 0;
    int num_edge_labels_ = 0;

    // Per-vertex label (indexed by global_id)
    std::vector<int> vertex_label_;
    std::vector<std::vector<int>> vertices_by_label_;  // vertices_by_label_[label] = list of global_ids

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
