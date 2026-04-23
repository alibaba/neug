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

#include "sampled_match_data_graph_meta.h"
#include <unordered_set>

namespace neug {
namespace function {

DataGraphMeta::DataGraphMeta(const StorageReadInterface& graph) 
    : graph_(graph) {
}

void DataGraphMeta::Preprocess() {
    LOG(INFO) << "[DataGraphMeta] Starting preprocessing...";
    
    // Step 0: Build ID mapping (label, vid) <-> global_id
    BuildIdMapping();
    
    // Step 1: Build neighbors_ and compute degree statistics
    BuildNeighbors();

    // Debug: Print all edges
    // LOG(INFO) << "[DataGraphMeta] === Printing all edges ===";
    // const auto& schema = graph_.schema();
    // int total_edges = 0;
    // for (const auto& [key, edge_schema] : schema.e_schemas_) {
    //     auto [src_label, dst_label, e_label] = schema.parse_edge_label(key);
    //     std::string src_name = schema.get_vertex_label_name(src_label);
    //     std::string dst_name = schema.get_vertex_label_name(dst_label);
    //     std::string e_name = schema.get_edge_label_name(e_label);
        
    //     LOG(INFO) << "  Edge type: " << src_name << " -[" << e_name << "]-> " << dst_name;
        
    //     try {
    //         GenericView out_view = graph_.GetGenericOutgoingGraphView(src_label, dst_label, e_label);
    //         VertexSet src_vs = graph_.GetVertexSet(src_label);
            
    //         for (vid_t src_vid : src_vs) {
    //             if (!graph_.IsValidVertex(src_label, src_vid)) continue;
                
    //             NbrList edges = out_view.get_edges(src_vid);
    //             for (auto it = edges.begin(); it != edges.end(); ++it) {
    //                 vid_t dst_vid = *it;
    //                 LOG(INFO) << "    " << src_name << "[" << src_vid << "] -> " 
    //                           << dst_name << "[" << dst_vid << "]";
    //                 total_edges++;
    //             }
    //         }
    //     } catch (const std::exception& e) {
    //         LOG(WARNING) << "    Exception: " << e.what();
    //     }
    // }
    // LOG(INFO) << "[DataGraphMeta] Total edges found: " << total_edges;
    // LOG(INFO) << "[DataGraphMeta] === End of edge list ===";
    
    
    // Step 2: Build schema index and cache views
    BuildSchemaIndex();
    InitDedupArrays();
    
    // Step 3: Compute k-core decomposition
    ComputeCoreNum();
    
    // Step 4: Compute label statistics
    ComputeLabelStatistics();
    
    LOG(INFO) << "[DataGraphMeta] Preprocessing complete.";
    LOG(INFO) << "  Vertices: " << num_vertex_ << ", Edges: " << num_edge_;
    LOG(INFO) << "  Max degree: " << max_degree_ 
              << ", Max in-degree: " << max_in_degree_ 
              << ", Max out-degree: " << max_out_degree_;
    LOG(INFO) << "  Degeneracy: " << degeneracy_;
}

void DataGraphMeta::BuildIdMapping() {
    const auto& schema = graph_.schema();
    label_t num_vertex_labels = schema.vertex_label_num();
    label_t num_edge_labels = schema.edge_label_num();
    num_labels_ = num_vertex_labels;
    num_edge_labels_ = num_edge_labels;
    
    LOG(INFO) << "[BuildIdMapping] Vertex labels: " << (int)num_vertex_labels 
              << ", Edge labels: " << (int)num_edge_labels;
    
    // Clear existing mappings
    local_to_global_.clear();
    global_to_local_.clear();
    label_vid_to_global_.clear();
    label_vid_to_global_.resize(num_vertex_labels);
    
    // First pass: find max vid per label to size the fast-lookup arrays
    std::vector<vid_t> max_vid_per_label(num_vertex_labels, 0);
    for (label_t label = 0; label < num_vertex_labels; ++label) {
        if (!schema.vertex_label_valid(label)) continue;
        VertexSet vs = graph_.GetVertexSet(label);
        for (vid_t vid : vs) {
            if (vid >= max_vid_per_label[label]) max_vid_per_label[label] = vid + 1;
        }
    }
    for (label_t label = 0; label < num_vertex_labels; ++label) {
        label_vid_to_global_[label].assign(max_vid_per_label[label], -1);
    }
    
    // Iterate through all vertex labels and assign global IDs
    int global_id = 0;
    for (label_t label = 0; label < num_vertex_labels; ++label) {
        if (!schema.vertex_label_valid(label)) continue;
        
        VertexSet vs = graph_.GetVertexSet(label);
        LOG(INFO) << "[BuildIdMapping] Label " << (int)label 
                  << " (" << schema.get_vertex_label_name(label) << ")"
                  << " has " << vs.size() << " vertices";
        
        for (vid_t vid : vs) {
            auto key = std::make_pair(label, vid);
            local_to_global_[key] = global_id;
            global_to_local_.push_back(key);
            label_vid_to_global_[label][vid] = global_id;
            global_id++;
        }
    }
    
    num_vertex_ = global_id;
    LOG(INFO) << "[BuildIdMapping] Total vertices (global): " << num_vertex_;
}

void DataGraphMeta::BuildNeighbors() {
    const auto& schema = graph_.schema();
    
    if (num_vertex_ == 0) {
        LOG(WARNING) << "[BuildNeighbors] No vertices found";
        return;
    }
    
    // Initialize vertex_label_ array (record each vertex's label by global_id)
    vertex_label_.resize(num_vertex_, 0);
    vertices_by_label_.resize(num_labels_);
    
    for (int global_id = 0; global_id < num_vertex_; ++global_id) {
        auto [label, vid] = global_to_local_[global_id];
        vertex_label_[global_id] = label;
        vertices_by_label_[label].push_back(global_id);
    }
    
    // Initialize degree arrays
    in_degree_.resize(num_vertex_, 0);
    out_degree_.resize(num_vertex_, 0);
    degree_.resize(num_vertex_, 0);
    
    // Iterate through all edge triplets using e_schemas_, compute degrees
    num_edge_ = 0;
    for (const auto& [key, edge_schema] : schema.get_all_edge_schemas()) {
        auto [src_label, dst_label, e_label] = schema.parse_edge_label(key);
        
        LOG(INFO) << "Processing edge type: src_label=" << (int)src_label 
                  << " (" << schema.get_vertex_label_name(src_label) << ")"
                  << " dst_label=" << (int)dst_label 
                  << " (" << schema.get_vertex_label_name(dst_label) << ")"
                  << " e_label=" << (int)e_label
                  << " (" << schema.get_edge_label_name(e_label) << ")";
        
        try {
            GenericView out_view = graph_.GetGenericOutgoingGraphView(
                src_label, dst_label, e_label);
            
            VertexSet src_vs = graph_.GetVertexSet(src_label);
            for (vid_t src_vid : src_vs) {
                if (!graph_.IsValidVertex(src_label, src_vid)) continue;
                
                int src_global = ToGlobalId(src_label, src_vid);
                if (src_global < 0) continue;
                
                NbrList edges = out_view.get_edges(src_vid);
                NbrIterator it;
                it.init(edges.start_ptr, edges.end_ptr, edges.cfg, edges.timestamp);
                NbrIterator end;
                end.init(edges.end_ptr, edges.end_ptr, edges.cfg, edges.timestamp);
                
                while (it != end) {
                    vid_t dst_vid = *it;
                    int dst_global = ToGlobalId(dst_label, dst_vid);
                    if (dst_global >= 0) {
                        out_degree_[src_global]++;
                        in_degree_[dst_global]++;
                        degree_[src_global]++;
                        degree_[dst_global]++;
                        num_edge_++;
                    }
                    ++it;
                }
            }
        } catch (const std::exception& e) {
            LOG(WARNING) << "Exception processing edge triplet: " << e.what();
        } catch (...) {
            // Edge triplet doesn't exist in storage
        }
    }
    
    // Compute max degrees
    max_degree_ = 0;
    max_in_degree_ = 0;
    max_out_degree_ = 0;
    
    for (int global_id = 0; global_id < num_vertex_; ++global_id) {
        max_degree_ = std::max(max_degree_, degree_[global_id]);
        max_in_degree_ = std::max(max_in_degree_, in_degree_[global_id]);
        max_out_degree_ = std::max(max_out_degree_, out_degree_[global_id]);
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

void DataGraphMeta::BuildSchemaIndex() {
    out_schema_index_.clear();
    in_schema_index_.clear();
    out_view_cache_.clear();
    in_view_cache_.clear();
    out_schemas_by_src_.clear();
    in_schemas_by_dst_.clear();

    const auto& schema = graph_.schema();
    for (const auto& [key, edge_schema] : schema.get_all_edge_schemas()) {
        auto [s_label, d_label, e_label] = schema.parse_edge_label(key);

        out_schema_index_[PackLabelPair(s_label, d_label)].push_back(e_label);
        in_schema_index_[PackLabelPair(d_label, s_label)].push_back(e_label);
        out_schemas_by_src_[s_label].push_back({d_label, e_label});
        in_schemas_by_dst_[d_label].push_back({s_label, e_label});

        try {
            out_view_cache_[PackViewKey(s_label, d_label, e_label)] =
                graph_.GetGenericOutgoingGraphView(s_label, d_label, e_label);
        } catch (...) {}
        try {
            in_view_cache_[PackViewKey(d_label, s_label, e_label)] =
                graph_.GetGenericIncomingGraphView(d_label, s_label, e_label);
        } catch (...) {}
    }

    LOG(INFO) << "[BuildSchemaIndex] Cached " << out_view_cache_.size()
              << " outgoing + " << in_view_cache_.size() << " incoming views";
}

void DataGraphMeta::InitDedupArrays() {
    nbr_seen_.assign(num_vertex_, false);
    nbr_seen_reset_.reserve(max_degree_ > 0 ? max_degree_ : 256);
}

// ============================================================================
// Checkpoint serialization
// ============================================================================

static constexpr char DGMC_MAGIC[] = "DGMC";
static constexpr int32_t DGMC_VERSION = 1;

bool DataGraphMeta::SaveToFile(const std::string& filepath) const {
    std::ofstream ofs(filepath, std::ios::binary);
    if (!ofs.is_open()) {
        LOG(ERROR) << "[DataGraphMeta] Failed to open file for writing: " << filepath;
        return false;
    }

    auto writeInt = [&](int32_t v) { ofs.write(reinterpret_cast<const char*>(&v), sizeof(v)); };
    auto writeDouble = [&](double v) { ofs.write(reinterpret_cast<const char*>(&v), sizeof(v)); };
    auto writeIntVec = [&](const std::vector<int>& vec) {
        writeInt(static_cast<int32_t>(vec.size()));
        if (!vec.empty()) {
            ofs.write(reinterpret_cast<const char*>(vec.data()), vec.size() * sizeof(int));
        }
    };
    auto writeDoubleVec = [&](const std::vector<double>& vec) {
        writeInt(static_cast<int32_t>(vec.size()));
        if (!vec.empty()) {
            ofs.write(reinterpret_cast<const char*>(vec.data()), vec.size() * sizeof(double));
        }
    };

    ofs.write(DGMC_MAGIC, 4);
    writeInt(DGMC_VERSION);

    writeInt(num_vertex_);
    writeInt(num_edge_);
    writeInt(num_labels_);
    writeInt(num_edge_labels_);
    writeInt(max_degree_);
    writeInt(max_in_degree_);
    writeInt(max_out_degree_);
    writeInt(degeneracy_);
    writeDouble(label_statistics_.vertex_label_entropy);

    writeInt(static_cast<int32_t>(global_to_local_.size()));
    for (const auto& [label, vid] : global_to_local_) {
        writeInt(static_cast<int32_t>(label));
        writeInt(static_cast<int32_t>(vid));
    }

    writeIntVec(vertex_label_);

    writeInt(static_cast<int32_t>(vertices_by_label_.size()));
    for (const auto& vec : vertices_by_label_) {
        writeIntVec(vec);
    }

    writeIntVec(in_degree_);
    writeIntVec(out_degree_);
    writeIntVec(degree_);
    writeIntVec(core_num_);
    writeIntVec(degeneracy_order_);
    writeDoubleVec(label_statistics_.vertex_label_probability);

    ofs.close();
    LOG(INFO) << "[DataGraphMeta] Saved checkpoint to: " << filepath
              << " (" << num_vertex_ << " vertices, " << num_edge_ << " edges)";
    return true;
}

bool DataGraphMeta::LoadFromFile(const std::string& filepath) {
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs.is_open()) {
        LOG(WARNING) << "[DataGraphMeta] Checkpoint file not found: " << filepath;
        return false;
    }

    auto readInt = [&]() -> int32_t {
        int32_t v;
        ifs.read(reinterpret_cast<char*>(&v), sizeof(v));
        return v;
    };
    auto readDouble = [&]() -> double {
        double v;
        ifs.read(reinterpret_cast<char*>(&v), sizeof(v));
        return v;
    };
    auto readIntVec = [&](std::vector<int>& vec) {
        int32_t sz = readInt();
        vec.resize(sz);
        if (sz > 0) {
            ifs.read(reinterpret_cast<char*>(vec.data()), sz * sizeof(int));
        }
    };
    auto readDoubleVec = [&](std::vector<double>& vec) {
        int32_t sz = readInt();
        vec.resize(sz);
        if (sz > 0) {
            ifs.read(reinterpret_cast<char*>(vec.data()), sz * sizeof(double));
        }
    };

    char magic[4];
    ifs.read(magic, 4);
    if (std::string(magic, 4) != DGMC_MAGIC) {
        LOG(ERROR) << "[DataGraphMeta] Invalid checkpoint magic in: " << filepath;
        return false;
    }
    int32_t version = readInt();
    if (version != DGMC_VERSION) {
        LOG(ERROR) << "[DataGraphMeta] Unsupported checkpoint version " << version;
        return false;
    }

    num_vertex_ = readInt();
    num_edge_ = readInt();
    num_labels_ = readInt();
    num_edge_labels_ = readInt();
    max_degree_ = readInt();
    max_in_degree_ = readInt();
    max_out_degree_ = readInt();
    degeneracy_ = readInt();
    label_statistics_.vertex_label_entropy = readDouble();

    int32_t gtl_size = readInt();
    global_to_local_.resize(gtl_size);
    local_to_global_.clear();
    local_to_global_.reserve(gtl_size);
    for (int i = 0; i < gtl_size; i++) {
        label_t label = static_cast<label_t>(readInt());
        vid_t vid = static_cast<vid_t>(readInt());
        global_to_local_[i] = {label, vid};
        local_to_global_[{label, vid}] = i;
    }

    readIntVec(vertex_label_);

    int32_t vbl_size = readInt();
    vertices_by_label_.resize(vbl_size);
    for (int i = 0; i < vbl_size; i++) {
        readIntVec(vertices_by_label_[i]);
    }

    readIntVec(in_degree_);
    readIntVec(out_degree_);
    readIntVec(degree_);
    readIntVec(core_num_);
    readIntVec(degeneracy_order_);
    readDoubleVec(label_statistics_.vertex_label_probability);

    if (ifs.fail()) {
        LOG(ERROR) << "[DataGraphMeta] Error reading checkpoint file: " << filepath;
        return false;
    }

    ifs.close();

    // Rebuild label_vid_to_global_ from loaded global_to_local_
    {
        label_vid_to_global_.clear();
        label_vid_to_global_.resize(num_labels_);
        std::vector<vid_t> max_vid_per_label(num_labels_, 0);
        for (int i = 0; i < (int)global_to_local_.size(); i++) {
            auto [label, vid] = global_to_local_[i];
            if (label < (label_t)num_labels_ && vid >= max_vid_per_label[label])
                max_vid_per_label[label] = vid + 1;
        }
        for (int l = 0; l < num_labels_; l++) {
            label_vid_to_global_[l].assign(max_vid_per_label[l], -1);
        }
        for (int i = 0; i < (int)global_to_local_.size(); i++) {
            auto [label, vid] = global_to_local_[i];
            if (label < (label_t)num_labels_)
                label_vid_to_global_[label][vid] = i;
        }
    }

    BuildSchemaIndex();
    InitDedupArrays();

    LOG(INFO) << "[DataGraphMeta] Loaded checkpoint from: " << filepath
              << " (" << num_vertex_ << " vertices, " << num_edge_ << " edges)";
    return true;
}

}  // namespace function
}  // namespace neug
