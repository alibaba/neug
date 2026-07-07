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

#include "pattern_matching_data_graph_meta.h"
#include <unordered_set>

namespace neug {
namespace pattern_matching {

DataGraphMeta::DataGraphMeta(const StorageReadInterface& graph)
    : graph_(graph) {}

void DataGraphMeta::Preprocess() {
  LOG(INFO) << "[DataGraphMeta] Starting preprocessing...";

  // Step 0: Build ID mapping (label, vid) <-> global_id
  BuildIdMapping();

  // Step 1: Build neighbors_ and compute degree statistics
  BuildNeighbors();

  // Step 2: Build schema index and cache views
  BuildSchemaIndex();

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

  LOG(INFO) << "[BuildIdMapping] Vertex labels: " << (int) num_vertex_labels
            << ", Edge labels: " << (int) num_edge_labels;

  // Clear existing mappings
  local_to_global_.clear();
  global_to_local_.clear();
  label_vid_to_global_.clear();
  label_vid_to_global_.resize(num_vertex_labels);

  // First pass: find max vid per label to size the fast-lookup arrays
  std::vector<vid_t> max_vid_per_label(num_vertex_labels, 0);
  for (label_t label = 0; label < num_vertex_labels; ++label) {
    if (!schema.is_vertex_label_valid(label))
      continue;
    VertexSet vs = graph_.GetVertexSet(label);
    for (vid_t vid : vs) {
      if (vid >= max_vid_per_label[label])
        max_vid_per_label[label] = vid + 1;
    }
  }
  for (label_t label = 0; label < num_vertex_labels; ++label) {
    label_vid_to_global_[label].assign(max_vid_per_label[label], -1);
  }

  // Iterate through all vertex labels and assign global IDs
  int global_id = 0;
  for (label_t label = 0; label < num_vertex_labels; ++label) {
    if (!schema.is_vertex_label_valid(label))
      continue;

    VertexSet vs = graph_.GetVertexSet(label);
    LOG(INFO) << "[BuildIdMapping] Label " << (int) label << " ("
              << schema.get_vertex_label_name(label) << ")"
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

    LOG(INFO) << "Processing edge type: src_label=" << (int) src_label << " ("
              << schema.get_vertex_label_name(src_label) << ")"
              << " dst_label=" << (int) dst_label << " ("
              << schema.get_vertex_label_name(dst_label) << ")"
              << " e_label=" << (int) e_label << " ("
              << schema.get_edge_label_name(e_label) << ")";

    try {
      CsrView out_view =
          graph_.GetGenericOutgoingGraphView(src_label, dst_label, e_label);

      VertexSet src_vs = graph_.GetVertexSet(src_label);
      for (vid_t src_vid : src_vs) {
        if (!graph_.IsValidVertex(src_label, src_vid))
          continue;

        int src_global = ToGlobalId(src_label, src_vid);
        if (src_global < 0)
          continue;

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

// ============================================================================
// Checkpoint serialization
// ============================================================================

static constexpr char DGMC_MAGIC[] = "DGMC";
static constexpr int32_t DGMC_VERSION = 1;

bool DataGraphMeta::SaveToFile(const std::string& filepath) const {
  std::ofstream ofs(filepath, std::ios::binary);
  if (!ofs.is_open()) {
    LOG(ERROR) << "[DataGraphMeta] Failed to open file for writing: "
               << filepath;
    return false;
  }

  auto write_int = [&](int32_t v) {
    ofs.write(reinterpret_cast<const char*>(&v), sizeof(v));
  };
  auto write_double = [&](double v) {
    ofs.write(reinterpret_cast<const char*>(&v), sizeof(v));
  };
  auto write_int_vec = [&](const std::vector<int>& vec) {
    write_int(static_cast<int32_t>(vec.size()));
    if (!vec.empty()) {
      ofs.write(reinterpret_cast<const char*>(vec.data()),
                vec.size() * sizeof(int));
    }
  };
  auto write_double_vec = [&](const std::vector<double>& vec) {
    write_int(static_cast<int32_t>(vec.size()));
    if (!vec.empty()) {
      ofs.write(reinterpret_cast<const char*>(vec.data()),
                vec.size() * sizeof(double));
    }
  };

  ofs.write(DGMC_MAGIC, 4);
  write_int(DGMC_VERSION);

  write_int(num_vertex_);
  write_int(num_edge_);
  write_int(num_labels_);
  write_int(num_edge_labels_);
  write_int(max_degree_);
  write_int(max_in_degree_);
  write_int(max_out_degree_);
  write_int(degeneracy_);
  write_double(label_statistics_.vertex_label_entropy);

  write_int(static_cast<int32_t>(global_to_local_.size()));
  for (const auto& [label, vid] : global_to_local_) {
    write_int(static_cast<int32_t>(label));
    write_int(static_cast<int32_t>(vid));
  }

  write_int_vec(vertex_label_);

  write_int(static_cast<int32_t>(vertices_by_label_.size()));
  for (const auto& vec : vertices_by_label_) {
    write_int_vec(vec);
  }

  write_int_vec(in_degree_);
  write_int_vec(out_degree_);
  write_int_vec(degree_);
  write_int_vec(core_num_);
  write_int_vec(degeneracy_order_);
  write_double_vec(label_statistics_.vertex_label_probability);

  ofs.close();
  LOG(INFO) << "[DataGraphMeta] Saved checkpoint to: " << filepath << " ("
            << num_vertex_ << " vertices, " << num_edge_ << " edges)";
  return true;
}

bool DataGraphMeta::LoadFromFile(const std::string& filepath) {
  std::ifstream ifs(filepath, std::ios::binary);
  if (!ifs.is_open()) {
    LOG(WARNING) << "[DataGraphMeta] Checkpoint file not found: " << filepath;
    return false;
  }

  // Cap any single vector at 1B entries (~4GB for int32/double). A corrupted
  // or malicious checkpoint can otherwise request unbounded allocations.
  constexpr int32_t kMaxVecSize = 1 << 30;

  auto bail = [&](const std::string& msg) {
    LOG(ERROR) << "[DataGraphMeta] " << msg << " in: " << filepath;
    return false;
  };

  auto read_int = [&]() -> int32_t {
    int32_t v = 0;
    ifs.read(reinterpret_cast<char*>(&v), sizeof(v));
    return v;
  };
  auto read_double = [&]() -> double {
    double v = 0;
    ifs.read(reinterpret_cast<char*>(&v), sizeof(v));
    return v;
  };
  // expected_size < 0 means "no consistency check" (variable-length per-label
  // bucket); otherwise the on-disk size must match exactly.
  auto read_int_vec = [&](std::vector<int>& vec, int32_t expected_size,
                          const char* tag) -> bool {
    int32_t sz = read_int();
    if (ifs.fail())
      return bail(std::string("Truncated size for ") + tag);
    if (sz < 0 || sz > kMaxVecSize)
      return bail(std::string("Invalid size ") + std::to_string(sz) + " for " +
                  tag);
    if (expected_size >= 0 && sz != expected_size)
      return bail(std::string("Size mismatch for ") + tag + ": got " +
                  std::to_string(sz) + ", expected " +
                  std::to_string(expected_size));
    vec.resize(sz);
    if (sz > 0) {
      ifs.read(reinterpret_cast<char*>(vec.data()),
               static_cast<std::streamsize>(sz) * sizeof(int));
    }
    if (ifs.fail())
      return bail(std::string("Truncated payload for ") + tag);
    return true;
  };
  auto read_double_vec = [&](std::vector<double>& vec, int32_t expected_size,
                             const char* tag) -> bool {
    int32_t sz = read_int();
    if (ifs.fail())
      return bail(std::string("Truncated size for ") + tag);
    if (sz < 0 || sz > kMaxVecSize)
      return bail(std::string("Invalid size ") + std::to_string(sz) + " for " +
                  tag);
    if (expected_size >= 0 && sz != expected_size)
      return bail(std::string("Size mismatch for ") + tag + ": got " +
                  std::to_string(sz) + ", expected " +
                  std::to_string(expected_size));
    vec.resize(sz);
    if (sz > 0) {
      ifs.read(reinterpret_cast<char*>(vec.data()),
               static_cast<std::streamsize>(sz) * sizeof(double));
    }
    if (ifs.fail())
      return bail(std::string("Truncated payload for ") + tag);
    return true;
  };

  char magic[4];
  ifs.read(magic, 4);
  if (ifs.fail() || std::string(magic, 4) != DGMC_MAGIC)
    return bail("Invalid checkpoint magic");
  int32_t version = read_int();
  if (ifs.fail() || version != DGMC_VERSION)
    return bail("unsupported checkpoint version " + std::to_string(version));

  num_vertex_ = read_int();
  num_edge_ = read_int();
  num_labels_ = read_int();
  num_edge_labels_ = read_int();
  max_degree_ = read_int();
  max_in_degree_ = read_int();
  max_out_degree_ = read_int();
  degeneracy_ = read_int();
  label_statistics_.vertex_label_entropy = read_double();
  if (ifs.fail())
    return bail("Truncated header");
  if (num_vertex_ < 0 || num_vertex_ > kMaxVecSize || num_edge_ < 0 ||
      num_labels_ < 0 || num_labels_ > kMaxVecSize || num_edge_labels_ < 0)
    return bail("Invalid scalar counts in header");

  int32_t gtl_size = read_int();
  if (ifs.fail())
    return bail("Truncated global_to_local size");
  if (gtl_size != num_vertex_)
    return bail("global_to_local size " + std::to_string(gtl_size) +
                " != num_vertex_ " + std::to_string(num_vertex_));
  global_to_local_.resize(gtl_size);
  local_to_global_.clear();
  local_to_global_.reserve(gtl_size);
  for (int i = 0; i < gtl_size; i++) {
    int32_t raw_label = read_int();
    int32_t raw_vid = read_int();
    if (ifs.fail())
      return bail("Truncated global_to_local entry");
    if (raw_label < 0 || raw_label >= num_labels_)
      return bail("Label " + std::to_string(raw_label) +
                  " out of range in global_to_local");
    label_t label = static_cast<label_t>(raw_label);
    vid_t vid = static_cast<vid_t>(raw_vid);
    global_to_local_[i] = {label, vid};
    local_to_global_[{label, vid}] = i;
  }

  if (!read_int_vec(vertex_label_, num_vertex_, "vertex_label_"))
    return false;

  int32_t vbl_size = read_int();
  if (ifs.fail())
    return bail("Truncated vertices_by_label outer size");
  if (vbl_size != num_labels_)
    return bail("vertices_by_label outer size " + std::to_string(vbl_size) +
                " != num_labels_ " + std::to_string(num_labels_));
  vertices_by_label_.resize(vbl_size);
  int64_t vbl_total = 0;
  for (int i = 0; i < vbl_size; i++) {
    if (!read_int_vec(vertices_by_label_[i], -1, "vertices_by_label_[i]"))
      return false;
    vbl_total += static_cast<int64_t>(vertices_by_label_[i].size());
    if (vbl_total > num_vertex_)
      return bail("vertices_by_label total exceeds num_vertex_");
  }

  if (!read_int_vec(in_degree_, num_vertex_, "in_degree_"))
    return false;
  if (!read_int_vec(out_degree_, num_vertex_, "out_degree_"))
    return false;
  if (!read_int_vec(degree_, num_vertex_, "degree_"))
    return false;
  if (!read_int_vec(core_num_, num_vertex_, "core_num_"))
    return false;
  if (!read_int_vec(degeneracy_order_, num_vertex_, "degeneracy_order_"))
    return false;
  if (!read_double_vec(label_statistics_.vertex_label_probability, num_labels_,
                       "vertex_label_probability"))
    return false;

  ifs.close();

  // Rebuild label_vid_to_global_ from loaded global_to_local_
  {
    label_vid_to_global_.clear();
    label_vid_to_global_.resize(num_labels_);
    std::vector<vid_t> max_vid_per_label(num_labels_, 0);
    for (int i = 0; i < (int) global_to_local_.size(); i++) {
      auto [label, vid] = global_to_local_[i];
      if (label < (label_t) num_labels_ && vid >= max_vid_per_label[label])
        max_vid_per_label[label] = vid + 1;
    }
    for (int l = 0; l < num_labels_; l++) {
      label_vid_to_global_[l].assign(max_vid_per_label[l], -1);
    }
    for (int i = 0; i < (int) global_to_local_.size(); i++) {
      auto [label, vid] = global_to_local_[i];
      if (label < (label_t) num_labels_)
        label_vid_to_global_[label][vid] = i;
    }
  }

  BuildSchemaIndex();

  LOG(INFO) << "[DataGraphMeta] Loaded checkpoint from: " << filepath << " ("
            << num_vertex_ << " vertices, " << num_edge_ << " edges)";
  return true;
}

size_t EdgeKeyHash::operator()(const EdgeKey& k) const {
  uint64_t h = (static_cast<uint64_t>(static_cast<uint32_t>(k.src)) << 32) |
               static_cast<uint64_t>(static_cast<uint32_t>(k.dst));
  h ^= static_cast<uint64_t>(k.label) * 0x9e3779b97f4a7c15ULL;
  h ^= (h >> 33);
  h *= 0xff51afd7ed558ccdULL;
  h ^= (h >> 33);
  return static_cast<size_t>(h);
}

std::vector<int> DataGraphMeta::GetNeighbors(int global_id) const {
  std::vector<int> result;
  if (global_id < 0 || global_id >= num_vertex_)
    return result;
  auto& scratch = GetDedupScratch();
  scratch.EnsureSize(num_vertex_);
  auto& seen = scratch.seen;
  auto& reset_list = scratch.reset_list;
  for (const auto& edge : GetAllOutIncidentEdges(global_id)) {
    int dst_global = std::get<1>(edge);
    if (!seen[dst_global]) {
      seen[dst_global] = true;
      reset_list.push_back(dst_global);
      result.push_back(dst_global);
    }
  }
  for (const auto& edge : GetAllInIncidentEdges(global_id)) {
    int src_global = std::get<0>(edge);
    if (!seen[src_global]) {
      seen[src_global] = true;
      reset_list.push_back(src_global);
      result.push_back(src_global);
    }
  }
  for (int v : reset_list)
    seen[v] = false;
  reset_list.clear();
  return result;
}

DataGraphMeta::Edge DataGraphMeta::GetEdge(int u, int v, int label) const {
  if (u < 0 || u >= num_vertex_ || v < 0 || v >= num_vertex_)
    return {-1, -1, 255};
  auto [src_label, src_vid] = ToLocalId(u);
  auto [dst_label, dst_vid] = ToLocalId(v);
  uint64_t vk = PackViewKey(src_label, dst_label, (label_t) label);
  auto vit = out_view_cache_.find(vk);
  if (vit == out_view_cache_.end())
    return {-1, -1, 255};
  NbrList edges = vit->second.get_edges(src_vid);
  for (auto it = edges.begin(); it != edges.end(); ++it) {
    if (*it == dst_vid)
      return {u, v, (label_t) label};
  }
  return {-1, -1, 255};
}

int DataGraphMeta::GetEdgeIndex(int u, int v) const {
  if (u < 0 || u >= num_vertex_ || v < 0 || v >= num_vertex_)
    return -1;
  auto [src_label, src_vid] = ToLocalId(u);
  auto [dst_label, dst_vid] = ToLocalId(v);
  uint32_t sk = PackLabelPair(src_label, dst_label);
  auto sit = out_schema_index_.find(sk);
  if (sit == out_schema_index_.end())
    return -1;
  for (label_t e_label : sit->second) {
    uint64_t vk = PackViewKey(src_label, dst_label, e_label);
    auto vit = out_view_cache_.find(vk);
    if (vit == out_view_cache_.end())
      continue;
    NbrList edges = vit->second.get_edges(src_vid);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      if (*it == dst_vid)
        return e_label;
    }
  }
  return -1;
}

std::vector<DataGraphMeta::Edge> DataGraphMeta::GetOutIncidentEdges(
    int global_id, int target_dst_label) const {
  std::vector<Edge> result;
  if (global_id < 0 || global_id >= num_vertex_)
    return result;
  auto [src_label, src_vid] = ToLocalId(global_id);
  uint32_t sk = PackLabelPair(src_label, (label_t) target_dst_label);
  auto sit = out_schema_index_.find(sk);
  if (sit == out_schema_index_.end())
    return result;
  for (label_t e_label : sit->second) {
    uint64_t vk = PackViewKey(src_label, (label_t) target_dst_label, e_label);
    auto vit = out_view_cache_.find(vk);
    if (vit == out_view_cache_.end())
      continue;
    NbrList edges = vit->second.get_edges(src_vid);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      int dst_global = FastToGlobalId((label_t) target_dst_label, *it);
      if (dst_global >= 0)
        result.push_back({global_id, dst_global, e_label});
    }
  }
  return result;
}

std::vector<DataGraphMeta::Edge> DataGraphMeta::GetInIncidentEdges(
    int global_id, int target_src_label) const {
  std::vector<Edge> result;
  if (global_id < 0 || global_id >= num_vertex_)
    return result;
  auto [dst_label, dst_vid] = ToLocalId(global_id);
  uint32_t sk = PackLabelPair(dst_label, (label_t) target_src_label);
  auto sit = in_schema_index_.find(sk);
  if (sit == in_schema_index_.end())
    return result;
  for (label_t e_label : sit->second) {
    uint64_t vk = PackViewKey(dst_label, (label_t) target_src_label, e_label);
    auto vit = in_view_cache_.find(vk);
    if (vit == in_view_cache_.end())
      continue;
    NbrList edges = vit->second.get_edges(dst_vid);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      int src_global = FastToGlobalId((label_t) target_src_label, *it);
      if (src_global >= 0)
        result.push_back({src_global, global_id, e_label});
    }
  }
  return result;
}

std::vector<DataGraphMeta::Edge> DataGraphMeta::GetAllOutIncidentEdges(
    int global_id) const {
  std::vector<Edge> result;
  if (global_id < 0 || global_id >= num_vertex_)
    return result;
  auto [src_label, src_vid] = ToLocalId(global_id);
  auto sit = out_schemas_by_src_.find(src_label);
  if (sit == out_schemas_by_src_.end())
    return result;
  for (const auto& [d_label, e_label] : sit->second) {
    uint64_t vk = PackViewKey(src_label, d_label, e_label);
    auto vit = out_view_cache_.find(vk);
    if (vit == out_view_cache_.end())
      continue;
    NbrList edges = vit->second.get_edges(src_vid);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      int dst_global = FastToGlobalId(d_label, *it);
      if (dst_global >= 0)
        result.push_back({global_id, dst_global, e_label});
    }
  }
  return result;
}

std::vector<DataGraphMeta::Edge> DataGraphMeta::GetAllInIncidentEdges(
    int global_id) const {
  std::vector<Edge> result;
  if (global_id < 0 || global_id >= num_vertex_)
    return result;
  auto [dst_label, dst_vid] = ToLocalId(global_id);
  auto sit = in_schemas_by_dst_.find(dst_label);
  if (sit == in_schemas_by_dst_.end())
    return result;
  for (const auto& [s_label, e_label] : sit->second) {
    uint64_t vk = PackViewKey(dst_label, s_label, e_label);
    auto vit = in_view_cache_.find(vk);
    if (vit == in_view_cache_.end())
      continue;
    NbrList edges = vit->second.get_edges(dst_vid);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      int src_global = FastToGlobalId(s_label, *it);
      if (src_global >= 0)
        result.push_back({src_global, global_id, e_label});
    }
  }
  return result;
}

std::vector<int> DataGraphMeta::GetOutNeighbors(int global_id) const {
  std::vector<int> result;
  if (global_id < 0 || global_id >= num_vertex_)
    return result;
  auto [src_label, src_vid] = ToLocalId(global_id);
  auto sit = out_schemas_by_src_.find(src_label);
  if (sit == out_schemas_by_src_.end())
    return result;
  auto& scratch = GetDedupScratch();
  scratch.EnsureSize(num_vertex_);
  auto& seen = scratch.seen;
  auto& reset_list = scratch.reset_list;
  for (const auto& [d_label, e_label] : sit->second) {
    uint64_t vk = PackViewKey(src_label, d_label, e_label);
    auto vit = out_view_cache_.find(vk);
    if (vit == out_view_cache_.end())
      continue;
    NbrList edges = vit->second.get_edges(src_vid);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      int dst_global = FastToGlobalId(d_label, *it);
      if (dst_global >= 0 && !seen[dst_global]) {
        seen[dst_global] = true;
        reset_list.push_back(dst_global);
        result.push_back(dst_global);
      }
    }
  }
  for (int v : reset_list)
    seen[v] = false;
  reset_list.clear();
  return result;
}

std::vector<int> DataGraphMeta::GetInNeighbors(int global_id) const {
  std::vector<int> result;
  if (global_id < 0 || global_id >= num_vertex_)
    return result;
  auto [dst_label, dst_vid] = ToLocalId(global_id);
  auto sit = in_schemas_by_dst_.find(dst_label);
  if (sit == in_schemas_by_dst_.end())
    return result;
  auto& scratch = GetDedupScratch();
  scratch.EnsureSize(num_vertex_);
  auto& seen = scratch.seen;
  auto& reset_list = scratch.reset_list;
  for (const auto& [s_label, e_label] : sit->second) {
    uint64_t vk = PackViewKey(dst_label, s_label, e_label);
    auto vit = in_view_cache_.find(vk);
    if (vit == in_view_cache_.end())
      continue;
    NbrList edges = vit->second.get_edges(dst_vid);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      int src_global = FastToGlobalId(s_label, *it);
      if (src_global >= 0 && !seen[src_global]) {
        seen[src_global] = true;
        reset_list.push_back(src_global);
        result.push_back(src_global);
      }
    }
  }
  for (int v : reset_list)
    seen[v] = false;
  reset_list.clear();
  return result;
}

int DataGraphMeta::GetOutNeighborCountMasked(int global_id,
                                             int target_dst_label,
                                             const bool* mask) const {
  if (global_id < 0 || global_id >= num_vertex_)
    return 0;
  auto [src_label, src_vid] = ToLocalId(global_id);
  uint32_t sk = PackLabelPair(src_label, (label_t) target_dst_label);
  auto sit = out_schema_index_.find(sk);
  if (sit == out_schema_index_.end())
    return 0;
  int count = 0;
  for (label_t e_label : sit->second) {
    uint64_t vk = PackViewKey(src_label, (label_t) target_dst_label, e_label);
    auto vit = out_view_cache_.find(vk);
    if (vit == out_view_cache_.end())
      continue;
    NbrList edges = vit->second.get_edges(src_vid);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      int dst_global = FastToGlobalId((label_t) target_dst_label, *it);
      if (dst_global >= 0 && mask[dst_global]) {
        count++;
      }
    }
  }
  return count;
}

int DataGraphMeta::GetInNeighborCountMasked(int global_id, int target_src_label,
                                            const bool* mask) const {
  if (global_id < 0 || global_id >= num_vertex_)
    return 0;
  auto [dst_label, dst_vid] = ToLocalId(global_id);
  uint32_t sk = PackLabelPair(dst_label, (label_t) target_src_label);
  auto sit = in_schema_index_.find(sk);
  if (sit == in_schema_index_.end())
    return 0;
  int count = 0;
  for (label_t e_label : sit->second) {
    uint64_t vk = PackViewKey(dst_label, (label_t) target_src_label, e_label);
    auto vit = in_view_cache_.find(vk);
    if (vit == in_view_cache_.end())
      continue;
    NbrList edges = vit->second.get_edges(dst_vid);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      int src_global = FastToGlobalId((label_t) target_src_label, *it);
      if (src_global >= 0 && mask[src_global]) {
        count++;
      }
    }
  }
  return count;
}

int DataGraphMeta::CountOutNeighborsInSet(int global_id, int target_dst_label,
                                          const bool* set, int needed) const {
  if (global_id < 0 || global_id >= num_vertex_ || needed <= 0)
    return 0;
  auto [src_label, src_vid] = ToLocalId(global_id);
  uint32_t sk = PackLabelPair(src_label, (label_t) target_dst_label);
  auto sit = out_schema_index_.find(sk);
  if (sit == out_schema_index_.end())
    return 0;
  int count = 0;
  for (label_t e_label : sit->second) {
    uint64_t vk = PackViewKey(src_label, (label_t) target_dst_label, e_label);
    auto vit = out_view_cache_.find(vk);
    if (vit == out_view_cache_.end())
      continue;
    NbrList edges = vit->second.get_edges(src_vid);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      int dst_global = FastToGlobalId((label_t) target_dst_label, *it);
      if (dst_global >= 0 && set[dst_global]) {
        if (++count >= needed)
          return count;
      }
    }
  }
  return count;
}

int DataGraphMeta::CountInNeighborsInSet(int global_id, int target_src_label,
                                         const bool* set, int needed) const {
  if (global_id < 0 || global_id >= num_vertex_ || needed <= 0)
    return 0;
  auto [dst_label, dst_vid] = ToLocalId(global_id);
  uint32_t sk = PackLabelPair(dst_label, (label_t) target_src_label);
  auto sit = in_schema_index_.find(sk);
  if (sit == in_schema_index_.end())
    return 0;
  int count = 0;
  for (label_t e_label : sit->second) {
    uint64_t vk = PackViewKey(dst_label, (label_t) target_src_label, e_label);
    auto vit = in_view_cache_.find(vk);
    if (vit == in_view_cache_.end())
      continue;
    NbrList edges = vit->second.get_edges(dst_vid);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      int src_global = FastToGlobalId((label_t) target_src_label, *it);
      if (src_global >= 0 && set[src_global]) {
        if (++count >= needed)
          return count;
      }
    }
  }
  return count;
}

std::string DataGraphMeta::EdgeToKey(const Edge& edge) const {
  int src = std::get<0>(edge);
  int dst = std::get<1>(edge);
  label_t label = std::get<2>(edge);
  if (src == -1)
    return "";
  return std::to_string(src) + ":" + std::to_string(dst) + ":" +
         std::to_string(label);
}

std::string DataGraphMeta::EdgeToKey(int src, int dst, label_t label) const {
  if (src == -1)
    return "";
  return std::to_string(src) + ":" + std::to_string(dst) + ":" +
         std::to_string(label);
}

std::size_t LabelVidHash::operator()(const std::pair<label_t, vid_t>& p) const {
  return std::hash<uint64_t>()((static_cast<uint64_t>(p.first) << 32) |
                               p.second);
}

int DataGraphMeta::GetDegree(int global_id) const {
  return (global_id >= 0 && global_id < (int) degree_.size())
             ? degree_[global_id]
             : 0;
}

int DataGraphMeta::GetVertexLabel(int global_id) const {
  return (global_id >= 0 && global_id < (int) vertex_label_.size())
             ? vertex_label_[global_id]
             : 0;
}

int DataGraphMeta::GetCoreNum(int global_id) const {
  return (global_id >= 0 && global_id < (int) core_num_.size())
             ? core_num_[global_id]
             : 0;
}

int DataGraphMeta::GetEdgeIndex(int u, int v, int label) const {
  Edge e = GetEdge(u, v, label);
  return std::get<0>(e) != -1 ? label : -1;
}

const std::vector<int>& DataGraphMeta::GetVerticesByLabel(int label) const {
  static const std::vector<int> empty;
  if (label < 0 || label >= (int) vertices_by_label_.size())
    return empty;
  return vertices_by_label_[label];
}

int DataGraphMeta::GetInDegree(int global_id) const {
  return (global_id >= 0 && global_id < (int) in_degree_.size())
             ? in_degree_[global_id]
             : 0;
}

int DataGraphMeta::GetOutDegree(int global_id) const {
  return (global_id >= 0 && global_id < (int) out_degree_.size())
             ? out_degree_[global_id]
             : 0;
}

EdgeKey DataGraphMeta::EdgeToIntKey(const Edge& edge) const {
  return EdgeKey(std::get<0>(edge), std::get<1>(edge), std::get<2>(edge));
}

EdgeKey DataGraphMeta::EdgeToIntKey(int src, int dst, label_t label) const {
  return EdgeKey(src, dst, label);
}

int DataGraphMeta::ToGlobalId(label_t label, vid_t vid) const {
  auto key = std::make_pair(label, vid);
  auto it = local_to_global_.find(key);
  return (it != local_to_global_.end()) ? it->second : -1;
}

std::pair<label_t, vid_t> DataGraphMeta::ToLocalId(int global_id) const {
  if (global_id < 0 || global_id >= (int) global_to_local_.size()) {
    return {255, (vid_t) -1};  // Invalid
  }
  return global_to_local_[global_id];
}

label_t DataGraphMeta::GetVertexLabelFromGlobal(int global_id) const {
  if (global_id < 0 || global_id >= (int) global_to_local_.size())
    return 255;
  return global_to_local_[global_id].first;
}

vid_t DataGraphMeta::GetOriginalVid(int global_id) const {
  if (global_id < 0 || global_id >= (int) global_to_local_.size())
    return (vid_t) -1;
  return global_to_local_[global_id].second;
}

int DataGraphMeta::FastToGlobalId(label_t label, vid_t vid) const {
  if (label >= (label_t) label_vid_to_global_.size())
    return -1;
  const auto& arr = label_vid_to_global_[label];
  if (vid >= (vid_t) arr.size())
    return -1;
  return arr[vid];
}

void DataGraphMeta::DedupScratch::EnsureSize(int n) {
  if (static_cast<int>(seen.size()) < n)
    seen.resize(n, false);
}

DataGraphMeta::DedupScratch& DataGraphMeta::GetDedupScratch() {
  thread_local DedupScratch scratch;
  return scratch;
}

uint32_t DataGraphMeta::PackLabelPair(label_t a, label_t b) {
  return (static_cast<uint32_t>(a) << 16) | static_cast<uint32_t>(b);
}

uint64_t DataGraphMeta::PackViewKey(label_t src, label_t dst, label_t edge) {
  return (static_cast<uint64_t>(src) << 32) |
         (static_cast<uint64_t>(dst) << 16) | static_cast<uint64_t>(edge);
}

}  // namespace pattern_matching
}  // namespace neug
