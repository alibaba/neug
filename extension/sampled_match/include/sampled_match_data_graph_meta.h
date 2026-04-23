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

#pragma once

#include <algorithm>
#include <cmath>
#include <fstream>
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

// Integer-based edge key to replace string-based EdgeToKey for performance.
// Avoids heap allocation, string hashing, and string comparison.
struct EdgeKey {
    int32_t src;
    int32_t dst;
    uint8_t label;
    EdgeKey() : src(-1), dst(-1), label(255) {}
    EdgeKey(int32_t s, int32_t d, uint8_t l) : src(s), dst(d), label(l) {}
    bool operator==(const EdgeKey& o) const {
        return src == o.src && dst == o.dst && label == o.label;
    }
    bool invalid() const { return src == -1; }
};

struct EdgeKeyHash {
    size_t operator()(const EdgeKey& k) const {
        uint64_t h = (static_cast<uint64_t>(static_cast<uint32_t>(k.src)) << 32)
                   | static_cast<uint64_t>(static_cast<uint32_t>(k.dst));
        h ^= static_cast<uint64_t>(k.label) * 0x9e3779b97f4a7c15ULL;
        h ^= (h >> 33);
        h *= 0xff51afd7ed558ccdULL;
        h ^= (h >> 33);
        return static_cast<size_t>(h);
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

    // Checkpoint serialization: save/load precomputed metadata to/from binary file
    bool SaveToFile(const std::string& filepath) const;
    bool LoadFromFile(const std::string& filepath);

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
        for (const auto& edge : GetAllOutIncidentEdges(global_id)) {
            int dst_global = std::get<1>(edge);
            if (!nbr_seen_[dst_global]) {
                nbr_seen_[dst_global] = true;
                nbr_seen_reset_.push_back(dst_global);
                result.push_back(dst_global);
            }
        }
        for (const auto& edge : GetAllInIncidentEdges(global_id)) {
            int src_global = std::get<0>(edge);
            if (!nbr_seen_[src_global]) {
                nbr_seen_[src_global] = true;
                nbr_seen_reset_.push_back(src_global);
                result.push_back(src_global);
            }
        }
        for (int v : nbr_seen_reset_) nbr_seen_[v] = false;
        nbr_seen_reset_.clear();
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
    inline Edge GetEdge(int u, int v, int label) const {
        if (u < 0 || u >= num_vertex_ || v < 0 || v >= num_vertex_) return {-1, -1, 255};
        auto [src_label, src_vid] = ToLocalId(u);
        auto [dst_label, dst_vid] = ToLocalId(v);
        uint64_t vk = PackViewKey(src_label, dst_label, (label_t)label);
        auto vit = out_view_cache_.find(vk);
        if (vit == out_view_cache_.end()) return {-1, -1, 255};
        NbrList edges = vit->second.get_edges(src_vid);
        for (auto it = edges.begin(); it != edges.end(); ++it) {
            if (*it == dst_vid) return {u, v, (label_t)label};
        }
        return {-1, -1, 255};
    }
    
    // Check if edge exists (any label), u, v are global_ids
    inline int GetEdgeIndex(int u, int v) const {
        if (u < 0 || u >= num_vertex_ || v < 0 || v >= num_vertex_) return -1;
        auto [src_label, src_vid] = ToLocalId(u);
        auto [dst_label, dst_vid] = ToLocalId(v);
        uint32_t sk = PackLabelPair(src_label, dst_label);
        auto sit = out_schema_index_.find(sk);
        if (sit == out_schema_index_.end()) return -1;
        for (label_t e_label : sit->second) {
            uint64_t vk = PackViewKey(src_label, dst_label, e_label);
            auto vit = out_view_cache_.find(vk);
            if (vit == out_view_cache_.end()) continue;
            NbrList edges = vit->second.get_edges(src_vid);
            for (auto it = edges.begin(); it != edges.end(); ++it) {
                if (*it == dst_vid) return e_label;
            }
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
    inline std::vector<Edge> GetOutIncidentEdges(int global_id, int target_dst_label) const {
        std::vector<Edge> result;
        if (global_id < 0 || global_id >= num_vertex_) return result;
        auto [src_label, src_vid] = ToLocalId(global_id);
        uint32_t sk = PackLabelPair(src_label, (label_t)target_dst_label);
        auto sit = out_schema_index_.find(sk);
        if (sit == out_schema_index_.end()) return result;
        for (label_t e_label : sit->second) {
            uint64_t vk = PackViewKey(src_label, (label_t)target_dst_label, e_label);
            auto vit = out_view_cache_.find(vk);
            if (vit == out_view_cache_.end()) continue;
            NbrList edges = vit->second.get_edges(src_vid);
            for (auto it = edges.begin(); it != edges.end(); ++it) {
                int dst_global = FastToGlobalId((label_t)target_dst_label, *it);
                if (dst_global >= 0) result.push_back({global_id, dst_global, e_label});
            }
        }
        return result;
    }
    
    // Get in-edges to vertex (by global_id) from vertices with target_src_label
    inline std::vector<Edge> GetInIncidentEdges(int global_id, int target_src_label) const {
        std::vector<Edge> result;
        if (global_id < 0 || global_id >= num_vertex_) return result;
        auto [dst_label, dst_vid] = ToLocalId(global_id);
        uint32_t sk = PackLabelPair(dst_label, (label_t)target_src_label);
        auto sit = in_schema_index_.find(sk);
        if (sit == in_schema_index_.end()) return result;
        for (label_t e_label : sit->second) {
            uint64_t vk = PackViewKey(dst_label, (label_t)target_src_label, e_label);
            auto vit = in_view_cache_.find(vk);
            if (vit == in_view_cache_.end()) continue;
            NbrList edges = vit->second.get_edges(dst_vid);
            for (auto it = edges.begin(); it != edges.end(); ++it) {
                int src_global = FastToGlobalId((label_t)target_src_label, *it);
                if (src_global >= 0) result.push_back({src_global, global_id, e_label});
            }
        }
        return result;
    }
    
    // Get all out-edges from vertex (by global_id)
    inline std::vector<Edge> GetAllOutIncidentEdges(int global_id) const {
        std::vector<Edge> result;
        if (global_id < 0 || global_id >= num_vertex_) return result;
        auto [src_label, src_vid] = ToLocalId(global_id);
        auto sit = out_schemas_by_src_.find(src_label);
        if (sit == out_schemas_by_src_.end()) return result;
        for (const auto& [d_label, e_label] : sit->second) {
            uint64_t vk = PackViewKey(src_label, d_label, e_label);
            auto vit = out_view_cache_.find(vk);
            if (vit == out_view_cache_.end()) continue;
            NbrList edges = vit->second.get_edges(src_vid);
            for (auto it = edges.begin(); it != edges.end(); ++it) {
                int dst_global = FastToGlobalId(d_label, *it);
                if (dst_global >= 0) result.push_back({global_id, dst_global, e_label});
            }
        }
        return result;
    }
    
    // Get all in-edges to vertex (by global_id)
    inline std::vector<Edge> GetAllInIncidentEdges(int global_id) const {
        std::vector<Edge> result;
        if (global_id < 0 || global_id >= num_vertex_) return result;
        auto [dst_label, dst_vid] = ToLocalId(global_id);
        auto sit = in_schemas_by_dst_.find(dst_label);
        if (sit == in_schemas_by_dst_.end()) return result;
        for (const auto& [s_label, e_label] : sit->second) {
            uint64_t vk = PackViewKey(dst_label, s_label, e_label);
            auto vit = in_view_cache_.find(vk);
            if (vit == in_view_cache_.end()) continue;
            NbrList edges = vit->second.get_edges(dst_vid);
            for (auto it = edges.begin(); it != edges.end(); ++it) {
                int src_global = FastToGlobalId(s_label, *it);
                if (src_global >= 0) result.push_back({src_global, global_id, e_label});
            }
        }
        return result;
    }
    
    // Get out-neighbors with flat boolean dedup
    inline std::vector<int> GetOutNeighbors(int global_id) const {
        std::vector<int> result;
        if (global_id < 0 || global_id >= num_vertex_) return result;
        auto [src_label, src_vid] = ToLocalId(global_id);
        auto sit = out_schemas_by_src_.find(src_label);
        if (sit == out_schemas_by_src_.end()) return result;
        for (const auto& [d_label, e_label] : sit->second) {
            uint64_t vk = PackViewKey(src_label, d_label, e_label);
            auto vit = out_view_cache_.find(vk);
            if (vit == out_view_cache_.end()) continue;
            NbrList edges = vit->second.get_edges(src_vid);
            for (auto it = edges.begin(); it != edges.end(); ++it) {
                int dst_global = FastToGlobalId(d_label, *it);
                if (dst_global >= 0 && !nbr_seen_[dst_global]) {
                    nbr_seen_[dst_global] = true;
                    nbr_seen_reset_.push_back(dst_global);
                    result.push_back(dst_global);
                }
            }
        }
        for (int v : nbr_seen_reset_) nbr_seen_[v] = false;
        nbr_seen_reset_.clear();
        return result;
    }
    
    // Get in-neighbors with flat boolean dedup
    inline std::vector<int> GetInNeighbors(int global_id) const {
        std::vector<int> result;
        if (global_id < 0 || global_id >= num_vertex_) return result;
        auto [dst_label, dst_vid] = ToLocalId(global_id);
        auto sit = in_schemas_by_dst_.find(dst_label);
        if (sit == in_schemas_by_dst_.end()) return result;
        for (const auto& [s_label, e_label] : sit->second) {
            uint64_t vk = PackViewKey(dst_label, s_label, e_label);
            auto vit = in_view_cache_.find(vk);
            if (vit == in_view_cache_.end()) continue;
            NbrList edges = vit->second.get_edges(dst_vid);
            for (auto it = edges.begin(); it != edges.end(); ++it) {
                int src_global = FastToGlobalId(s_label, *it);
                if (src_global >= 0 && !nbr_seen_[src_global]) {
                    nbr_seen_[src_global] = true;
                    nbr_seen_reset_.push_back(src_global);
                    result.push_back(src_global);
                }
            }
        }
        for (int v : nbr_seen_reset_) nbr_seen_[v] = false;
        nbr_seen_reset_.clear();
        return result;
    }

    // Get the count of out-neighbors with vertex label target_dst_label that are in the mask.
    // Used by CheckNeighborSafety to count per-label masked neighbors directly.
    inline int GetOutNeighborCountMasked(int global_id, int target_dst_label, const bool* mask) const {
        if (global_id < 0 || global_id >= num_vertex_) return 0;
        auto [src_label, src_vid] = ToLocalId(global_id);
        uint32_t sk = PackLabelPair(src_label, (label_t)target_dst_label);
        auto sit = out_schema_index_.find(sk);
        if (sit == out_schema_index_.end()) return 0;
        int count = 0;
        for (label_t e_label : sit->second) {
            uint64_t vk = PackViewKey(src_label, (label_t)target_dst_label, e_label);
            auto vit = out_view_cache_.find(vk);
            if (vit == out_view_cache_.end()) continue;
            NbrList edges = vit->second.get_edges(src_vid);
            for (auto it = edges.begin(); it != edges.end(); ++it) {
                int dst_global = FastToGlobalId((label_t)target_dst_label, *it);
                if (dst_global >= 0 && mask[dst_global]) {
                    count++;
                }
            }
        }
        return count;
    }

    // Get the count of in-neighbors with vertex label target_src_label that are in the mask.
    // Used by CheckNeighborSafety to count per-label masked neighbors directly.
    inline int GetInNeighborCountMasked(int global_id, int target_src_label, const bool* mask) const {
        if (global_id < 0 || global_id >= num_vertex_) return 0;
        auto [dst_label, dst_vid] = ToLocalId(global_id);
        uint32_t sk = PackLabelPair(dst_label, (label_t)target_src_label);
        auto sit = in_schema_index_.find(sk);
        if (sit == in_schema_index_.end()) return 0;
        int count = 0;
        for (label_t e_label : sit->second) {
            uint64_t vk = PackViewKey(dst_label, (label_t)target_src_label, e_label);
            auto vit = in_view_cache_.find(vk);
            if (vit == in_view_cache_.end()) continue;
            NbrList edges = vit->second.get_edges(dst_vid);
            for (auto it = edges.begin(); it != edges.end(); ++it) {
                int src_global = FastToGlobalId((label_t)target_src_label, *it);
                if (src_global >= 0 && mask[src_global]) {
                    count++;
                }
            }
        }
        return count;
    }

    // Count out-neighbors of specific label that are in a boolean set, with early termination.
    // Skips dedup (slight over-count is conservative for safety checks).
    inline int CountOutNeighborsInSet(int global_id, int target_dst_label, const bool* set, int needed) const {
        if (global_id < 0 || global_id >= num_vertex_ || needed <= 0) return 0;
        auto [src_label, src_vid] = ToLocalId(global_id);
        uint32_t sk = PackLabelPair(src_label, (label_t)target_dst_label);
        auto sit = out_schema_index_.find(sk);
        if (sit == out_schema_index_.end()) return 0;
        int count = 0;
        for (label_t e_label : sit->second) {
            uint64_t vk = PackViewKey(src_label, (label_t)target_dst_label, e_label);
            auto vit = out_view_cache_.find(vk);
            if (vit == out_view_cache_.end()) continue;
            NbrList edges = vit->second.get_edges(src_vid);
            for (auto it = edges.begin(); it != edges.end(); ++it) {
                int dst_global = FastToGlobalId((label_t)target_dst_label, *it);
                if (dst_global >= 0 && set[dst_global]) {
                    if (++count >= needed) return count;
                }
            }
        }
        return count;
    }

    // Count in-neighbors of specific label that are in a boolean set, with early termination.
    inline int CountInNeighborsInSet(int global_id, int target_src_label, const bool* set, int needed) const {
        if (global_id < 0 || global_id >= num_vertex_ || needed <= 0) return 0;
        auto [dst_label, dst_vid] = ToLocalId(global_id);
        uint32_t sk = PackLabelPair(dst_label, (label_t)target_src_label);
        auto sit = in_schema_index_.find(sk);
        if (sit == in_schema_index_.end()) return 0;
        int count = 0;
        for (label_t e_label : sit->second) {
            uint64_t vk = PackViewKey(dst_label, (label_t)target_src_label, e_label);
            auto vit = in_view_cache_.find(vk);
            if (vit == in_view_cache_.end()) continue;
            NbrList edges = vit->second.get_edges(dst_vid);
            for (auto it = edges.begin(); it != edges.end(); ++it) {
                int src_global = FastToGlobalId((label_t)target_src_label, *it);
                if (src_global >= 0 && set[src_global]) {
                    if (++count >= needed) return count;
                }
            }
        }
        return count;
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
    
    // Convert Edge to unique string key (legacy, prefer EdgeToIntKey)
    inline std::string EdgeToKey(const Edge& edge) const {
        int src = std::get<0>(edge);
        int dst = std::get<1>(edge);
        label_t label = std::get<2>(edge);
        if (src == -1) return "";
        return std::to_string(src) + ":" + std::to_string(dst) + ":" + std::to_string(label);
    }
    
    inline std::string EdgeToKey(int src, int dst, label_t label) const {
        if (src == -1) return "";
        return std::to_string(src) + ":" + std::to_string(dst) + ":" + std::to_string(label);
    }

    // Integer-based edge key (no heap allocation, fast hash)
    inline EdgeKey EdgeToIntKey(const Edge& edge) const {
        return EdgeKey(std::get<0>(edge), std::get<1>(edge), std::get<2>(edge));
    }
    
    inline EdgeKey EdgeToIntKey(int src, int dst, label_t label) const {
        return EdgeKey(src, dst, label);
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

    // Fast ToGlobalId using direct array indexing (no hash map)
    inline int FastToGlobalId(label_t label, vid_t vid) const {
        if (label >= (label_t)label_vid_to_global_.size()) return -1;
        const auto& arr = label_vid_to_global_[label];
        if (vid >= (vid_t)arr.size()) return -1;
        return arr[vid];
    }

private:
    void BuildIdMapping();
    void BuildNeighbors();
    void ComputeCoreNum();
    void ComputeLabelStatistics();
    void BuildSchemaIndex();
    void InitDedupArrays();

    static inline uint32_t PackLabelPair(label_t a, label_t b) {
        return (static_cast<uint32_t>(a) << 16) | static_cast<uint32_t>(b);
    }
    static inline uint64_t PackViewKey(label_t src, label_t dst, label_t edge) {
        return (static_cast<uint64_t>(src) << 32) | (static_cast<uint64_t>(dst) << 16) | static_cast<uint64_t>(edge);
    }

    const StorageReadInterface& graph_;

    // ========== ID Mapping ==========
    std::unordered_map<std::pair<label_t, vid_t>, int, LabelVidHash> local_to_global_;
    std::vector<std::pair<label_t, vid_t>> global_to_local_;
    std::vector<std::vector<int>> label_vid_to_global_;

    int num_vertex_ = 0;
    int num_edge_ = 0;
    int num_labels_ = 0;
    int num_edge_labels_ = 0;

    std::vector<int> vertex_label_;
    std::vector<std::vector<int>> vertices_by_label_;

    int max_degree_ = 0;
    int max_in_degree_ = 0;
    int max_out_degree_ = 0;
    std::vector<int> in_degree_, out_degree_, degree_;

    std::vector<int> core_num_;
    std::vector<int> degeneracy_order_;
    int degeneracy_ = 0;

    LabelStatistics label_statistics_;

    // ========== Schema Index & View Cache (~1KB total, pointers only) ==========
    std::unordered_map<uint32_t, std::vector<label_t>> out_schema_index_;
    std::unordered_map<uint32_t, std::vector<label_t>> in_schema_index_;
    std::unordered_map<uint64_t, GenericView> out_view_cache_;
    std::unordered_map<uint64_t, GenericView> in_view_cache_;
    std::unordered_map<label_t, std::vector<std::pair<label_t, label_t>>> out_schemas_by_src_;
    std::unordered_map<label_t, std::vector<std::pair<label_t, label_t>>> in_schemas_by_dst_;

    // ========== Flat boolean dedup for GetOutNeighbors/GetInNeighbors ==========
    mutable std::vector<bool> nbr_seen_;
    mutable std::vector<int> nbr_seen_reset_;
};

}  // namespace function
}  // namespace neug
