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
#include <any>
#include <cmath>
#include <fstream>
#include <functional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "neug/execution/common/types/value.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace pattern_matching {

// Hash function for std::pair<label_t, vid_t>
struct LabelVidHash {
  std::size_t operator()(const std::pair<label_t, vid_t>& p) const;
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
  size_t operator()(const EdgeKey& k) const;
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
  PropCons(std::string prop_name, CompType comp_type, Value value)
      : _prop_name(prop_name),
        _comp_type(comp_type),
        _value(std::move(value)) {}
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

  // Checkpoint serialization: save/load precomputed metadata to/from binary
  // file
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

  int GetDegree(int global_id) const;
  // Get vertex label by global_id (returns label_t as int)
  int GetVertexLabel(int global_id) const;
  // Get undirected neighbors by global_id (returns global_ids)
  std::vector<int> GetNeighbors(int global_id) const;
  int GetCoreNum(int global_id) const;
  inline const std::vector<int>& GetDegeneracyOrder() const {
    return degeneracy_order_;
  }
  inline const std::vector<int>& GetCoreNums() const { return core_num_; }
  inline const LabelStatistics& GetLabelStatistics() const {
    return label_statistics_;
  }

  // Edge representation: (src_global_id, dst_global_id, edge_label)
  using Edge = std::tuple<int, int, label_t>;

  // Edge lookup: check if edge exists, return edge or invalid edge
  Edge GetEdge(int u, int v, int label) const;

  // Check if edge exists (any label), u, v are global_ids
  int GetEdgeIndex(int u, int v) const;

  // Check if edge with specific label exists, u, v are global_ids
  int GetEdgeIndex(int u, int v, int label) const;

  // Vertex by label lookup
  const std::vector<int>& GetVerticesByLabel(int label) const;

  // Degree accessors for individual vertices (by global_id)
  int GetInDegree(int global_id) const;
  int GetOutDegree(int global_id) const;

  // Get out-edges from vertex (by global_id) to vertices with target_dst_label
  std::vector<Edge> GetOutIncidentEdges(int global_id,
                                        int target_dst_label) const;

  // Get in-edges to vertex (by global_id) from vertices with target_src_label
  std::vector<Edge> GetInIncidentEdges(int global_id,
                                       int target_src_label) const;

  // Get all out-edges from vertex (by global_id)
  std::vector<Edge> GetAllOutIncidentEdges(int global_id) const;

  // Get all in-edges to vertex (by global_id)
  std::vector<Edge> GetAllInIncidentEdges(int global_id) const;

  // Get out-neighbors with flat boolean dedup
  std::vector<int> GetOutNeighbors(int global_id) const;

  // Get in-neighbors with flat boolean dedup
  std::vector<int> GetInNeighbors(int global_id) const;

  // Get the count of out-neighbors with vertex label target_dst_label that are
  // in the mask. Used by CheckNeighborSafety to count per-label masked
  // neighbors directly.
  int GetOutNeighborCountMasked(int global_id, int target_dst_label,
                                const bool* mask) const;

  // Get the count of in-neighbors with vertex label target_src_label that are
  // in the mask. Used by CheckNeighborSafety to count per-label masked
  // neighbors directly.
  int GetInNeighborCountMasked(int global_id, int target_src_label,
                               const bool* mask) const;

  // Count out-neighbors of specific label that are in a boolean set, with early
  // termination. Skips dedup (slight over-count is conservative for safety
  // checks).
  int CountOutNeighborsInSet(int global_id, int target_dst_label,
                             const bool* set, int needed) const;

  // Count in-neighbors of specific label that are in a boolean set, with early
  // termination.
  int CountInNeighborsInSet(int global_id, int target_src_label,
                            const bool* set, int needed) const;

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
  std::string EdgeToKey(const Edge& edge) const;

  std::string EdgeToKey(int src, int dst, label_t label) const;

  // Integer-based edge key (no heap allocation, fast hash)
  EdgeKey EdgeToIntKey(const Edge& edge) const;

  EdgeKey EdgeToIntKey(int src, int dst, label_t label) const;

  // Configuration flags
  bool build_triangle = false;
  bool build_four_cycle = false;

  // ========== ID Mapping Methods ==========
  // Map (label, vid) to global_id
  int ToGlobalId(label_t label, vid_t vid) const;

  // Map global_id back to (label, vid)
  std::pair<label_t, vid_t> ToLocalId(int global_id) const;

  // Get vertex label from global_id
  label_t GetVertexLabelFromGlobal(int global_id) const;

  // Get original vid from global_id
  vid_t GetOriginalVid(int global_id) const;

  // Fast ToGlobalId using direct array indexing (no hash map)
  int FastToGlobalId(label_t label, vid_t vid) const;

 private:
  void BuildIdMapping();
  void BuildNeighbors();
  void ComputeCoreNum();
  void ComputeLabelStatistics();
  void BuildSchemaIndex();

  // Per-thread scratch for dedup in GetNeighbors / GetOutNeighbors /
  // GetInNeighbors. The DataGraphMeta instance is shared across threads via
  // GraphDataCache, so the dedup bitmap must NOT live on the object — it
  // would race. Each thread reuses its own scratch across calls (and across
  // DataGraphMeta instances of varying sizes); the bitmap only ever grows.
  struct DedupScratch {
    std::vector<bool> seen;
    std::vector<int> reset_list;
    void EnsureSize(int n);
  };
  static DedupScratch& GetDedupScratch();

  static uint32_t PackLabelPair(label_t a, label_t b);
  static uint64_t PackViewKey(label_t src, label_t dst, label_t edge);

  const StorageReadInterface& graph_;

  // ========== ID Mapping ==========
  std::unordered_map<std::pair<label_t, vid_t>, int, LabelVidHash>
      local_to_global_;
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
  std::unordered_map<uint64_t, CsrView> out_view_cache_;
  std::unordered_map<uint64_t, CsrView> in_view_cache_;
  std::unordered_map<label_t, std::vector<std::pair<label_t, label_t>>>
      out_schemas_by_src_;
  std::unordered_map<label_t, std::vector<std::pair<label_t, label_t>>>
      in_schemas_by_dst_;
};

}  // namespace pattern_matching
}  // namespace neug
