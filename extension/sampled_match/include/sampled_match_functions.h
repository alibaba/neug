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
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "rapidjson/document.h"
#include "rapidjson/error/en.h"
#include "rapidjson/filereadstream.h"

#include "neug/compiler/common/types/types.h"
#include "neug/compiler/function/function.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/storages/graph/property_graph.h"

#include "sampled_match_data_graph_meta.h"
#include "fastest_lib/src/SubgraphMatching/pattern_graph.h"
#include "fastest_lib/src/SubgraphCounting/cardinality_estimation.h"

namespace neug {
namespace function {

// ============================================================================
// Helper functions for parsing pattern JSON
// ============================================================================

// Helper function to parse comparison operator
inline CompType ParseOperator(const std::string& op) {
    if (op == "=" || op == "==") return CompType::COMP_EQUAL;
    if (op == ">") return CompType::COMP_GREATER;
    if (op == "<") return CompType::COMP_LESS;
    if (op == ">=") return CompType::COMP_GREATER_EQUAL;
    if (op == "<=") return CompType::COMP_LESS_EQUAL;
    if (op == "in") return CompType::COMP_IN;
    if (op == "not_in") return CompType::COMP_NOT_IN;
    return CompType::COMP_EQUAL; // default
}

// Helper function to create Value from rapidjson
inline Value CreateValueFromRapidjson(const rapidjson::Value& val) {
    if (val.IsInt()) {
        return Value::INT32(val.GetInt());
    } else if (val.IsInt64()) {
        return Value::INT64(val.GetInt64());
    } else if (val.IsDouble()) {
        return Value::DOUBLE(val.GetDouble());
    } else if (val.IsString()) {
        return Value::STRING(val.GetString());
    } else if (val.IsBool()) {
        return Value::BOOLEAN(val.GetBool());
    }
    return Value::INT32(0);
}

// Helper function: escape a string for JSON output
inline std::string EscapeJsonString(const std::string& s) {
    std::string escaped;
    escaped.reserve(s.size() + 8);
    for (char c : s) {
        switch (c) {
            case '"':  escaped += "\\\""; break;
            case '\\': escaped += "\\\\"; break;
            case '\n': escaped += "\\n";  break;
            case '\t': escaped += "\\t";  break;
            case '\r': escaped += "\\r";  break;
            case '\b': escaped += "\\b";  break;
            case '\f': escaped += "\\f";  break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    // Control character - encode as \uXXXX
                    char buf[8];
                    snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
                    escaped += buf;
                } else {
                    escaped += c;
                }
                break;
        }
    }
    return escaped;
}

/**
 * @brief Convert a neug::execution::Value to JSON format string (preserving types)
 * 
 * This function properly serializes different value types:
 * - INT32/INT64/UINT64: output as number (no quotes)
 * - DOUBLE/FLOAT: output as number (handle infinity/NaN)
 * - BOOL: output as true/false (no quotes)
 * - STRING/DATE/TIMESTAMP/etc: output as quoted, escaped string
 * - NULL/NONE: output as null
 * 
 * @param val The Value to serialize
 * @return JSON-formatted string representation
 */
inline std::string ValueToJsonString(const execution::Value& val) {
    // Check if value is null/none
    if (val.IsNull()) {
        return "null";
    }
    
    // Get the type and handle accordingly
    // Note: execution::Value uses DataTypeId, not LogicalTypeID
    DataTypeId type_id = val.type().id();
    
    switch (type_id) {
        case DataTypeId::kInt8:
        case DataTypeId::kInt16:
        case DataTypeId::kInt32:
        case DataTypeId::kInt64:
        case DataTypeId::kUInt32:
        case DataTypeId::kUInt64:
            // Integer types: output without quotes
            return val.to_string();
            
        case DataTypeId::kFloat:
        case DataTypeId::kDouble:
            {
                std::string num_str = val.to_string();
                // Handle special float values
                if (num_str == "inf" || num_str == "+inf") return "\"Infinity\"";
                if (num_str == "-inf") return "\"-Infinity\"";
                if (num_str == "nan" || num_str == "NaN") return "\"NaN\"";
                return num_str;
            }
            
        case DataTypeId::kBoolean:
            {
                // Boolean: output as true/false without quotes
                std::string bool_str = val.to_string();
                return (bool_str == "true" || bool_str == "True" || 
                        bool_str == "TRUE" || bool_str == "1") 
                       ? "true" : "false";
            }
            
        case DataTypeId::kVarchar:
        case DataTypeId::kDate:
        case DataTypeId::kTimestampMs:
        case DataTypeId::kInterval:
        default:
            // String and other types: output as quoted, escaped string
            return "\"" + EscapeJsonString(val.to_string()) + "\"";
    }
}

// Helper function to parse constraints from rapidjson
inline std::vector<PropCons> ParseConstraints(const rapidjson::Value& constraints_json) {
    std::vector<PropCons> constraints;
    if (!constraints_json.IsArray()) return constraints;
    
    for (rapidjson::SizeType i = 0; i < constraints_json.Size(); i++) {
        const auto& c = constraints_json[i];
        std::string prop_name = c.HasMember("property") ? c["property"].GetString() : "";
        std::string op_str = c.HasMember("operator") ? c["operator"].GetString() : "=";
        CompType op = ParseOperator(op_str);
        Value value = c.HasMember("value") ? CreateValueFromRapidjson(c["value"]) : Value::INT32(0);
        
        constraints.emplace_back(prop_name, op, std::move(value));
    }
    return constraints;
}

// ============================================================================
// Output file path helper: builds a unique path under /tmp/p/neug_sample.
// ============================================================================
inline std::string GenerateOutputFilePath(const std::string& prefix) {
  auto now = std::chrono::system_clock::now();
  auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch()).count();
  std::filesystem::create_directories("/tmp/p/neug_sample");
  return "/tmp/p/neug_sample/" + prefix + "_" + std::to_string(timestamp) + ".csv";
}

// ============================================================================
// GraphDataCache: caches preprocessed graph metadata so repeated SAMPLED_MATCH
// calls on the same graph avoid rebuilding DataGraphMeta every time.
// ============================================================================

class GraphDataCache {
 public:
  static GraphDataCache& Instance() {
    static GraphDataCache instance;
    return instance;
  }

  struct CachedData {
    std::unique_ptr<DataGraphMeta> data_meta;
    std::shared_ptr<std::unordered_map<label_t, std::unordered_map<label_t, std::vector<label_t>>>> schema_graph;
    bool preprocessed = false;
  };

  // Returns the cache slot for `graph_ptr`, creating it lazily on first use.
  CachedData& GetOrCreate(const StorageReadInterface* graph_ptr) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = cache_.find(graph_ptr);
    if (it == cache_.end()) {
      auto& data = cache_[graph_ptr];
      data.data_meta = std::make_unique<DataGraphMeta>(*graph_ptr);
      data.schema_graph = std::make_shared<std::unordered_map<label_t, std::unordered_map<label_t, std::vector<label_t>>>>();
      data.preprocessed = false;
    }
    return cache_[graph_ptr];
  }

  bool HasCache(const StorageReadInterface* graph_ptr) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_.find(graph_ptr);
    return it != cache_.end() && it->second.preprocessed;
  }

  void ClearCache(const StorageReadInterface* graph_ptr) {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.erase(graph_ptr);
  }

  void ClearAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.clear();
  }

 private:
  GraphDataCache() = default;
  ~GraphDataCache() = default;
  GraphDataCache(const GraphDataCache&) = delete;
  GraphDataCache& operator=(const GraphDataCache&) = delete;

  mutable std::mutex mutex_;
  std::unordered_map<const StorageReadInterface*, CachedData> cache_;
};

// ============================================================================
// Checkpoint file helpers: save/load schema_graph and DataGraphMeta
// ============================================================================

static constexpr char SGCH_MAGIC[] = "SGCH";
static constexpr int32_t SGCH_VERSION = 1;

inline bool SaveSchemaGraph(
    const std::unordered_map<label_t, std::unordered_map<label_t, std::vector<label_t>>>& sg,
    const std::string& filepath) {
  std::ofstream ofs(filepath, std::ios::binary);
  if (!ofs.is_open()) return false;

  auto writeInt = [&](int32_t v) { ofs.write(reinterpret_cast<const char*>(&v), sizeof(v)); };

  ofs.write(SGCH_MAGIC, 4);
  writeInt(SGCH_VERSION);
  writeInt(static_cast<int32_t>(sg.size()));
  for (const auto& [src, inner] : sg) {
    writeInt(static_cast<int32_t>(src));
    writeInt(static_cast<int32_t>(inner.size()));
    for (const auto& [dst, labels] : inner) {
      writeInt(static_cast<int32_t>(dst));
      writeInt(static_cast<int32_t>(labels.size()));
      for (label_t l : labels) writeInt(static_cast<int32_t>(l));
    }
  }
  ofs.close();
  return true;
}

inline bool LoadSchemaGraph(
    std::unordered_map<label_t, std::unordered_map<label_t, std::vector<label_t>>>& sg,
    const std::string& filepath) {
  std::ifstream ifs(filepath, std::ios::binary);
  if (!ifs.is_open()) return false;

  auto readInt = [&]() -> int32_t {
    int32_t v; ifs.read(reinterpret_cast<char*>(&v), sizeof(v)); return v;
  };

  char magic[4];
  ifs.read(magic, 4);
  if (std::string(magic, 4) != SGCH_MAGIC) return false;
  if (readInt() != SGCH_VERSION) return false;

  sg.clear();
  int32_t outer_sz = readInt();
  for (int32_t i = 0; i < outer_sz; i++) {
    label_t src = static_cast<label_t>(readInt());
    int32_t inner_sz = readInt();
    for (int32_t j = 0; j < inner_sz; j++) {
      label_t dst = static_cast<label_t>(readInt());
      int32_t cnt = readInt();
      auto& vec = sg[src][dst];
      vec.resize(cnt);
      for (int32_t k = 0; k < cnt; k++) vec[k] = static_cast<label_t>(readInt());
    }
  }
  return !ifs.fail();
}

/**
 * @brief Save all cached graph initialization data to checkpoint files.
 * Files written: {checkpoint_dir}/data_graph_meta.bin, {checkpoint_dir}/schema_graph.bin
 */
inline bool SaveGraphCheckpoint(const StorageReadInterface& graph, const std::string& checkpoint_dir) {
  auto& cache = GraphDataCache::Instance();
  if (!cache.HasCache(&graph)) {
    std::cerr << "[SaveGraphCheckpoint] No cached data to save." << std::endl;
    return false;
  }
  auto& cached_data = cache.GetOrCreate(&graph);

  std::filesystem::create_directories(checkpoint_dir);

  bool ok = true;
  ok = cached_data.data_meta->SaveToFile(checkpoint_dir + "/data_graph_meta.bin") && ok;
  ok = SaveSchemaGraph(*cached_data.schema_graph, checkpoint_dir + "/schema_graph.bin") && ok;

  if (ok) {
    std::cout << "[SaveGraphCheckpoint] Checkpoint saved to: " << checkpoint_dir << std::endl;
  } else {
    std::cerr << "[SaveGraphCheckpoint] Failed to save checkpoint to: " << checkpoint_dir << std::endl;
  }
  return ok;
}

/**
 * @brief Try to load graph initialization data from checkpoint files.
 * @return true if checkpoint was loaded successfully, false if not available.
 */
inline bool LoadGraphCheckpoint(const StorageReadInterface& graph, const std::string& checkpoint_dir) {
  std::string meta_path = checkpoint_dir + "/data_graph_meta.bin";
  std::string sg_path = checkpoint_dir + "/schema_graph.bin";

  if (!std::filesystem::exists(meta_path) || !std::filesystem::exists(sg_path)) {
    std::cout << "[LoadGraphCheckpoint] No checkpoint files in: " << checkpoint_dir << std::endl;
    return false;
  }

  auto& cache = GraphDataCache::Instance();
  auto& cached_data = cache.GetOrCreate(&graph);

  if (!cached_data.data_meta->LoadFromFile(meta_path)) {
    return false;
  }
  if (!LoadSchemaGraph(*cached_data.schema_graph, sg_path)) {
    return false;
  }

  cached_data.preprocessed = true;
  std::cout << "[LoadGraphCheckpoint] Checkpoint loaded from: " << checkpoint_dir << std::endl;
  std::cout << "  Vertices: " << cached_data.data_meta->GetNumVertices() << std::endl;
  std::cout << "  Edges: " << cached_data.data_meta->GetNumEdges() << std::endl;
  std::cout << "  Max degree: " << cached_data.data_meta->GetMaxDegree() << std::endl;
  std::cout << "  Degeneracy: " << cached_data.data_meta->GetDegeneracy() << std::endl;
  return true;
}

// ============================================================================
// Graph initialization: builds label mappings and runs DataGraphMeta
// preprocessing. Shared by the INITIALIZE CALL function and by match() on
// first use, so either path ends up with the same cached state.
// ============================================================================

/**
 * @brief Build the per-graph caches that SAMPLED_MATCH relies on.
 * @param graph           Graph storage to preprocess.
 * @param verbose         Emit progress logs when true.
 * @param checkpoint_dir  If non-empty, try loading the cache from this
 *                        directory before falling back to full preprocessing.
 * @return true on success.
 */
inline bool DoGraphInitialization(const StorageReadInterface& graph, bool verbose = true,
                                  const std::string& checkpoint_dir = "") {
  auto& cache = GraphDataCache::Instance();
  auto& cached_data = cache.GetOrCreate(&graph);
  
  if (cached_data.preprocessed) {
    if (verbose) {
      std::cout << "[Initialize] Graph already initialized, skipping..." << std::endl;
      std::cout << "  Vertices: " << cached_data.data_meta->GetNumVertices() << std::endl;
      std::cout << "  Edges: " << cached_data.data_meta->GetNumEdges() << std::endl;
    }
    return true;
  }

  // Try loading from checkpoint first
  if (!checkpoint_dir.empty()) {
    if (LoadGraphCheckpoint(graph, checkpoint_dir)) {
      if (verbose) {
        std::cout << "[Initialize] Graph loaded from checkpoint." << std::endl;
      }
      return true;
    }
    if (verbose) {
      std::cout << "[Initialize] Checkpoint not available, falling back to full initialization..." << std::endl;
    }
  }
  
  if (verbose) {
    std::cout << "[0] Building label mappings..." << std::endl;
  }
  
  // Build schema_graph: mapping (src_label, dst_label) -> [edge_labels]
  const auto& schema = graph.schema();
  for (const auto& [key, edge_schema] : schema.get_all_edge_schemas()) {
    auto [src_label, dst_label, e_label] = schema.parse_edge_label(key);
    (*cached_data.schema_graph)[src_label][dst_label].push_back(e_label);
    
    if (verbose) {
      const std::string& src_name = schema.get_vertex_label_name(src_label);
      const std::string& dst_name = schema.get_vertex_label_name(dst_label);
      const std::string& edge_name = schema.get_edge_label_name(e_label);
      std::cout << "  Edge triplet: " << src_name << " -[" << edge_name 
                << "]-> " << dst_name << std::endl;
    }
  }
  
  if (verbose) {
    std::cout << "  Found " << cached_data.schema_graph->size() << " source labels in schema" << std::endl;
    std::cout << std::endl;
    std::cout << "[1] Preprocessing data graph..." << std::endl;
  }
  
  cached_data.data_meta->Preprocess();
  cached_data.preprocessed = true;
  
  if (verbose) {
    std::cout << "  Vertices: " << cached_data.data_meta->GetNumVertices() << std::endl;
    std::cout << "  Edges: " << cached_data.data_meta->GetNumEdges() << std::endl;
    std::cout << "  Max degree: " << cached_data.data_meta->GetMaxDegree() << std::endl;
    std::cout << "  Degeneracy: " << cached_data.data_meta->GetDegeneracy() << std::endl;
    std::cout << std::endl;
    std::cout << "[Initialize] Graph initialization completed." << std::endl;
  }
  
  return true;
}

// ============================================================================
// SampledSubgraphMatcher: Subgraph matching using FaSTest algorithm
// ============================================================================

class SampledSubgraphMatcher {
 public:
  SampledSubgraphMatcher(const StorageReadInterface& graph,
                            const std::string& pattern_file,
                            long long sample_size)
    : graph_(graph), pattern_file_(pattern_file), sample_size_(sample_size) {
    }
  
  /**
   * @brief Execute subgraph matching
   * @return Estimated count of embeddings
   */
  double match() {
    auto& cache = GraphDataCache::Instance();
    auto& cached_data = cache.GetOrCreate(&graph_);

    // Steps 0-1: reuse the cache when possible; initialize lazily otherwise.
    if (!cached_data.preprocessed) {
      std::cout << "[match] Graph not initialized, calling DoGraphInitialization..." << std::endl;
      DoGraphInitialization(graph_, true);
    } else {
      std::cout << "[0-1] Using cached graph data..." << std::endl;
      std::cout << "  Vertices: " << cached_data.data_meta->GetNumVertices() << std::endl;
      std::cout << "  Edges: " << cached_data.data_meta->GetNumEdges() << std::endl;
    }
    std::cout << std::endl;

    // Step 2: always reload the pattern — callers can vary it per invocation.
    std::cout << "[2] Loading pattern graph from: " << pattern_file_ << std::endl;
    pattern_graph_ = CreatePatternFromJson(pattern_file_);
    if (!pattern_graph_ || pattern_graph_->GetNumVertices() == 0) {
        std::cerr << "  ERROR: Failed to load pattern!" << std::endl;
        return -1;
    }
    std::cout << "  Pattern: " << pattern_graph_->GetNumVertices() << " vertices, " 
              << pattern_graph_->GetNumEdges() << " edges" << std::endl;
    
    // Print pattern details
    std::cout << "  Pattern vertices:" << std::endl;
    for (int i = 0; i < pattern_graph_->GetNumVertices(); i++) {
        int label = pattern_graph_->vertex_label[i];
        std::cout << "    v" << i << ": label=" << label 
                  << " (out_deg=" << pattern_graph_->GetOutDegree(i) 
                  << ", in_deg=" << pattern_graph_->GetInDegree(i) << ")" << std::endl;
    }
    std::cout << "  Pattern edges:" << std::endl;
    for (int i = 0; i < pattern_graph_->GetNumEdges(); i++) {
        auto& [src, dst] = pattern_graph_->edge_list[i];
        int label = pattern_graph_->edge_label[i];
        std::cout << "    e" << i << ": " << src << " -[label=" << label << "]-> " << dst << std::endl;
    }
    std::cout << std::endl;

    // Step 3: Process pattern (compute core numbers, build incidence list, etc.)
    std::cout << "[3] Processing pattern..." << std::endl;
    pattern_graph_->ProcessPattern(*cached_data.data_meta, cached_data.schema_graph);
    std::cout << "  Done." << std::endl;
    std::cout << std::endl;
    
    // Step 4: Setup cardinality estimation options
    std::cout << "[4] Setting up cardinality estimation..." << std::endl;
    GraphLib::CardinalityEstimation::CardEstOption opt;
    opt.MAX_QUERY_VERTEX = std::max(12, pattern_graph_->GetNumVertices());
    opt.MAX_QUERY_EDGE = std::max(24, pattern_graph_->GetNumEdges());
    opt.structure_filter = GraphLib::SubgraphMatching::NO_STRUCTURE_FILTER;
    std::cout << "  MAX_QUERY_VERTEX: " << opt.MAX_QUERY_VERTEX << std::endl;
    std::cout << "  MAX_QUERY_EDGE: " << opt.MAX_QUERY_EDGE << std::endl;
    std::cout << std::endl;
    
    // Step 5: Run cardinality estimation
    std::cout << "[5] Running cardinality estimation..." << std::endl;
    std::cout << "  Sample size: " << sample_size_ << std::endl;
    
    GraphLib::CardinalityEstimation::FaSTestCardinalityEstimation estimator(graph_, *cached_data.data_meta, opt);
    double est = estimator.EstimateEmbeddings(pattern_graph_.get(), sample_size_);
    
    std::cout << std::endl;
    std::cout << "=== Results ===" << std::endl;
    std::cout << "  Estimated embedding count: " << (long long)est << std::endl;
    
    // Get and store sampled results
    sampled_results_ = estimator.GetSampledResult();
    int num_samples = sampled_results_.size() / pattern_graph_->GetNumVertices();
    std::cout << "  Number of sampled embeddings: " << num_samples << std::endl;
    
    // Print first few samples
    if (num_samples > 0) {
        std::cout << "  First 5 sampled embeddings:" << std::endl;
        int show_count = std::min(5, num_samples);
        for (int i = 0; i < show_count; i++) {
            std::cout << "    [" << i << "]: ";
            for (int j = 0; j < pattern_graph_->GetNumVertices(); j++) {
                if (j > 0) std::cout << " -> ";
                std::cout << sampled_results_[i * pattern_graph_->GetNumVertices() + j];
            }
            std::cout << std::endl;
        }
    }
    
    // Print estimation info
    auto result_info = estimator.GetResult();
    std::cout << std::endl;
    std::cout << "  Estimation details:" << std::endl;
    for (const auto& [key, value] : result_info) {
        std::cout << "    " << key << ": ";
        if (value.type() == typeid(double)) {
            std::cout << std::any_cast<double>(value);
        } else if (value.type() == typeid(int)) {
            std::cout << std::any_cast<int>(value);
        } else if (value.type() == typeid(std::string)) {
            std::cout << std::any_cast<std::string>(value);
        }
        std::cout << std::endl;
    }
    
    estimated_count_ = est;
    std::cout << std::endl;
    std::cout << "=== Matching completed ===" << std::endl;
    
    return est;
  }
  
  // Get sampled results after matching
  const std::vector<int>& GetSampledResults() const { return sampled_results_; }
  double GetEstimatedCount() const { return estimated_count_; }
  int GetPatternVertexCount() const { return pattern_graph_ ? pattern_graph_->GetNumVertices() : 0; }
  int GetPatternEdgeCount() const { return pattern_graph_ ? pattern_graph_->GetNumEdges() : 0; }
  
  // Get pattern edge list: [(src_pattern_idx, dst_pattern_idx, edge_label), ...]
  std::vector<std::tuple<int, int, label_t>> GetPatternEdgeList() const {
    std::vector<std::tuple<int, int, label_t>> result;
    if (!pattern_graph_) return result;
    for (int i = 0; i < pattern_graph_->GetNumEdges(); i++) {
      auto& [src, dst] = pattern_graph_->edge_list[i];
      label_t label = pattern_graph_->edge_label[i];
      result.emplace_back(src, dst, label);
    }
    return result;
  }
  
  // Get sampled edge keys for a specific sample and pattern edge
  // Returns edge key in format "src_global:dst_global:edge_label"
  std::string GetSampledEdgeKey(int sample_idx, int pattern_edge_idx) const {
    if (!pattern_graph_ || sample_idx < 0 || pattern_edge_idx < 0) return "";
    int patternVertexCount = pattern_graph_->GetNumVertices();
    int patternEdgeCount = pattern_graph_->GetNumEdges();
    if (pattern_edge_idx >= patternEdgeCount) return "";
    if (sample_idx * patternVertexCount >= (int)sampled_results_.size()) return "";
    
    auto& [src_pattern, dst_pattern] = pattern_graph_->edge_list[pattern_edge_idx];
    label_t edge_label = pattern_graph_->edge_label[pattern_edge_idx];
    
    int src_global = sampled_results_[sample_idx * patternVertexCount + src_pattern];
    int dst_global = sampled_results_[sample_idx * patternVertexCount + dst_pattern];
    
    return std::to_string(src_global) + ":" + std::to_string(dst_global) + ":" + std::to_string(edge_label);
  }

 private:
    // NOTE: BuildLabelMappings logic has been moved to DoGraphInitialization()
    // for better code reuse and explicit initialization via CALL Initialize().
    
    // Create pattern graph from JSON file using rapidjson
    std::unique_ptr<GraphLib::SubgraphMatching::PatternGraph> CreatePatternFromJson(
        const std::string& pattern_file) {
        
        const auto& schema = graph_.schema();

        // Read file content
        std::ifstream fin(pattern_file);
        if (!fin.is_open()) {
            std::cerr << "Error: Cannot open pattern file: " << pattern_file << std::endl;
            return nullptr;
        }
        
        std::stringstream buffer;
        buffer << fin.rdbuf();
        std::string json_content = buffer.str();
        fin.close();
        
        // Parse JSON
        rapidjson::Document doc;
        if (doc.Parse(json_content.c_str()).HasParseError()) {
            std::cerr << "Error parsing JSON: " << rapidjson::GetParseError_En(doc.GetParseError()) 
                      << " at offset " << doc.GetErrorOffset() << std::endl;
            return nullptr;
        }
        
        auto pattern = std::make_unique<GraphLib::SubgraphMatching::PatternGraph>();
        
        // Parse vertices
        if (!doc.HasMember("vertices") || !doc["vertices"].IsArray()) {
            std::cerr << "Error: JSON must have 'vertices' array" << std::endl;
            return nullptr;
        }
        if (!doc.HasMember("edges") || !doc["edges"].IsArray()) {
            std::cerr << "Error: JSON must have 'edges' array" << std::endl;
            return nullptr;
        }
        
        const auto& vertices = doc["vertices"];
        const auto& edges = doc["edges"];
        int v = vertices.Size();
        int e = edges.Size();
        
        pattern->num_vertex = v;
        pattern->num_edge = e;
        pattern->vertex_label.resize(v);
        pattern->edge_label.resize(e);
        pattern->adj_list.resize(v);
        pattern->out_adj_list.resize(v);
        pattern->in_adj_list.resize(v);
        pattern->edge_to.resize(e);
        pattern->edge_list.resize(e);
        pattern->vertex_property_constraints.resize(v);
        pattern->edge_property_constraints.resize(e);
        
        // Initialize required props vectors
        vertex_required_props_.resize(v);
        edge_required_props_.resize(e);
        
        for (rapidjson::SizeType i = 0; i < vertices.Size(); i++) {
            const auto& vertex = vertices[i];
            // Support both integer and string id (convert string to int if needed)
            int id;
            if (vertex["id"].IsInt()) {
                id = vertex["id"].GetInt();
            } else if (vertex["id"].IsString()) {
                id = std::stoi(vertex["id"].GetString());
            } else {
                std::cerr << "Error: vertex 'id' must be int or string" << std::endl;
                return nullptr;
            }
            std::string label = vertex["label"].GetString();
            
            if (!schema.contains_vertex_label(label)) {
                std::cerr << "Error: vertex label '" << label << "' not found in schema" << std::endl;
                return nullptr;
            }
            pattern->vertex_label[id] = schema.get_vertex_label_id(label);
            
            // Parse vertex property constraints
            if (vertex.HasMember("constraints") && vertex["constraints"].IsArray()) {
                pattern->vertex_property_constraints[id] = ParseConstraints(vertex["constraints"]);
            }
            
            // Parse required_props
            if (vertex.HasMember("required_props") && vertex["required_props"].IsArray()) {
                const auto& rp = vertex["required_props"];
                for (rapidjson::SizeType j = 0; j < rp.Size(); j++) {
                    if (rp[j].IsString()) {
                        vertex_required_props_[id].push_back(rp[j].GetString());
                    }
                }
            }
        }
        
        // Parse edges
        int edge_idx = 0;
        for (rapidjson::SizeType i = 0; i < edges.Size(); i++) {
            const auto& edge = edges[i];
            // Support both integer and string source/target (convert string to int if needed)
            int src, dst;
            if (edge["source"].IsInt()) {
                src = edge["source"].GetInt();
            } else if (edge["source"].IsString()) {
                src = std::stoi(edge["source"].GetString());
            } else {
                std::cerr << "Error: edge 'source' must be int or string" << std::endl;
                return nullptr;
            }
            if (edge["target"].IsInt()) {
                dst = edge["target"].GetInt();
            } else if (edge["target"].IsString()) {
                dst = std::stoi(edge["target"].GetString());
            } else {
                std::cerr << "Error: edge 'target' must be int or string" << std::endl;
                return nullptr;
            }
            
            std::string edge_type = "";
            if (edge.HasMember("label") && edge["label"].IsString()) {
                edge_type = edge["label"].GetString();
            }
            
            if (!edge_type.empty()) {
                if (!schema.contains_edge_label(edge_type)) {
                    std::cerr << "Error: edge label '" << edge_type << "' not found in schema" << std::endl;
                    return nullptr;
                }
                pattern->edge_label[edge_idx] = schema.get_edge_label_id(edge_type);
            } else {
                pattern->edge_label[edge_idx] = 0;
            }
            
            // Directed graph: src -> dst
            pattern->out_adj_list[src].push_back(dst);
            pattern->in_adj_list[dst].push_back(src);
            pattern->adj_list[src].push_back(dst);
            pattern->adj_list[dst].push_back(src);
            
            pattern->edge_to[edge_idx] = dst;
            pattern->edge_list[edge_idx] = {src, dst};
            
            pattern->max_out_degree = std::max(pattern->max_out_degree, (int)pattern->out_adj_list[src].size());
            pattern->max_in_degree = std::max(pattern->max_in_degree, (int)pattern->in_adj_list[dst].size());
            pattern->max_degree = std::max(pattern->max_degree, (int)std::max(pattern->adj_list[src].size(), pattern->adj_list[dst].size()));
            
            // Parse edge property constraints
            if (edge.HasMember("constraints") && edge["constraints"].IsArray()) {
                pattern->edge_property_constraints[edge_idx] = ParseConstraints(edge["constraints"]);
            }
            
            // Parse required_props for edge
            if (edge.HasMember("required_props") && edge["required_props"].IsArray()) {
                const auto& rp = edge["required_props"];
                for (rapidjson::SizeType j = 0; j < rp.Size(); j++) {
                    if (rp[j].IsString()) {
                        edge_required_props_[edge_idx].push_back(rp[j].GetString());
                    }
                }
            }
            
            edge_idx++;
        }
        
        return pattern;
    }

    // Member variables
    const StorageReadInterface& graph_;
    std::string pattern_file_;
    std::unique_ptr<GraphLib::SubgraphMatching::PatternGraph> pattern_graph_;
    long long sample_size_;
    
    // Results (per-call, not cached)
    std::vector<int> sampled_results_;
    double estimated_count_ = 0.0;
    
    // Required properties per pattern vertex/edge (parsed from pattern JSON)
    // pattern_vertex_idx -> list of property names (empty = none, ["*"] = all)
    std::vector<std::vector<std::string>> vertex_required_props_;
    // pattern_edge_idx -> list of property names
    std::vector<std::vector<std::string>> edge_required_props_;
    
  public:
    /**
     * @brief Convert NeuG DataTypeId to a portable type string for biagent.
     *
     * Mapping: kInt32->"int32", kInt64->"int64", kDouble->"double",
     *          kFloat->"float", kBoolean->"boolean", kVarchar->"string",
     *          kDate/kTimestampMs/kInterval->"string", others->"string".
     */
    static std::string DataTypeIdToString(DataTypeId type) {
        switch (type) {
            case DataTypeId::kInt8:    return "int8";
            case DataTypeId::kInt16:   return "int16";
            case DataTypeId::kInt32:   return "int32";
            case DataTypeId::kUInt32:  return "uint32";
            case DataTypeId::kInt64:   return "int64";
            case DataTypeId::kUInt64:  return "uint64";
            case DataTypeId::kFloat:   return "float";
            case DataTypeId::kDouble:  return "double";
            case DataTypeId::kBoolean: return "boolean";
            case DataTypeId::kVarchar: return "string";
            default:                   return "string";
        }
    }
    
    /**
     * @brief After match(), fetch required properties for all sampled results
     *        and write to a deduplicated JSON file with schema information.
     * @return Path to the JSON properties file, or "" if no properties needed.
     * 
     * JSON format:
     * {
     *   "schema": {
     *     "vertices": [
     *       {"label": "Publication", "properties": {"year": "int64", "title": "string"}}
     *     ],
     *     "edges": [
     *       {"label": "knows", "src_label": "Person", "dst_label": "Person",
     *        "properties": {"weight": "double"}}
     *     ]
     *   },
     *   "vertices": [
     *     {"id": 12345, "props": {"year": 2021, "title": "Paper A"}},
     *     ...
     *   ],
     *   "edges": [
     *     {"id": "100:200:3", "props": {"weight": 0.5}},
     *     ...
     *   ]
     * }
     */
    std::string FetchAndWriteProperties() {
        if (!pattern_graph_) return "";
        
        int patternVertexCount = pattern_graph_->GetNumVertices();
        int patternEdgeCount = pattern_graph_->GetNumEdges();
        int sampleCount = patternVertexCount > 0 ? 
            (int)sampled_results_.size() / patternVertexCount : 0;
        
        if (sampleCount == 0) return "";
        
        // Check if any properties are requested
        bool has_any_props = false;
        for (const auto& props : vertex_required_props_) {
            if (!props.empty()) { has_any_props = true; break; }
        }
        if (!has_any_props) {
            for (const auto& props : edge_required_props_) {
                if (!props.empty()) { has_any_props = true; break; }
            }
        }
        if (!has_any_props) return "";
        
        auto& cache = GraphDataCache::Instance();
        auto& cached_data = cache.GetOrCreate(&graph_);
        const auto& schema = graph_.schema();
        auto* readInterface = const_cast<StorageReadInterface*>(&graph_);
        
        // ---- Precompute property indices for each pattern vertex ----
        struct VertexPropInfo {
            label_t label_id;
            std::vector<std::string> prop_names;
            std::vector<int> prop_indices;
        };
        std::vector<VertexPropInfo> vertex_prop_infos(patternVertexCount);
        
        for (int pv = 0; pv < patternVertexCount; pv++) {
            if (pv >= (int)vertex_required_props_.size() || vertex_required_props_[pv].empty()) continue;
            
            label_t v_label = pattern_graph_->vertex_label[pv];
            auto all_names = schema.get_vertex_property_names(v_label);
            
            vertex_prop_infos[pv].label_id = v_label;
            
            bool want_all = (vertex_required_props_[pv].size() == 1 && vertex_required_props_[pv][0] == "*");
            
            if (want_all) {
                vertex_prop_infos[pv].prop_names = all_names;
                for (int j = 0; j < (int)all_names.size(); j++) {
                    vertex_prop_infos[pv].prop_indices.push_back(j);
                }
            } else {
                for (const auto& pname : vertex_required_props_[pv]) {
                    auto it = std::find(all_names.begin(), all_names.end(), pname);
                    if (it != all_names.end()) {
                        vertex_prop_infos[pv].prop_names.push_back(pname);
                        vertex_prop_infos[pv].prop_indices.push_back(std::distance(all_names.begin(), it));
                    }
                }
            }
        }
        
        // ---- Precompute property info for each pattern edge ----
        struct EdgePropInfo {
            label_t src_label, dst_label, edge_label;
            std::vector<std::string> prop_names;
            std::vector<int> prop_indices;
        };
        std::vector<EdgePropInfo> edge_prop_infos(patternEdgeCount);
        
        for (int pe = 0; pe < patternEdgeCount; pe++) {
            if (pe >= (int)edge_required_props_.size() || edge_required_props_[pe].empty()) continue;
            
            auto& [src_pv, dst_pv] = pattern_graph_->edge_list[pe];
            label_t src_label = pattern_graph_->vertex_label[src_pv];
            label_t dst_label = pattern_graph_->vertex_label[dst_pv];
            label_t e_label = pattern_graph_->edge_label[pe];
            
            edge_prop_infos[pe].src_label = src_label;
            edge_prop_infos[pe].dst_label = dst_label;
            edge_prop_infos[pe].edge_label = e_label;
            
            auto all_names = schema.get_edge_property_names(src_label, dst_label, e_label);
            
            bool want_all = (edge_required_props_[pe].size() == 1 && edge_required_props_[pe][0] == "*");
            
            if (want_all) {
                edge_prop_infos[pe].prop_names = all_names;
                for (int j = 0; j < (int)all_names.size(); j++) {
                    edge_prop_infos[pe].prop_indices.push_back(j);
                }
            } else {
                for (const auto& pname : edge_required_props_[pe]) {
                    auto it = std::find(all_names.begin(), all_names.end(), pname);
                    if (it != all_names.end()) {
                        edge_prop_infos[pe].prop_names.push_back(pname);
                        edge_prop_infos[pe].prop_indices.push_back(std::distance(all_names.begin(), it));
                    }
                }
            }
        }
        
        // ================================================================
        // Step 1: Collect all unique vertex IDs and edge keys across all
        //         samples, merging the required property names for each.
        // ================================================================
        
        // For vertices: global_id -> merged set of required prop names
        // We also need to know which VertexPropInfo to use for fetching (label-based).
        // A given global_id always has one label, so we pick the first matching pattern vertex.
        struct UniqueVertexInfo {
            int global_id;
            label_t label_id;
            std::unordered_set<std::string> needed_props;     // union of all pattern positions
            std::vector<std::string> ordered_prop_names;       // resolved after collection
            std::vector<int> ordered_prop_indices;             // resolved after collection
        };
        std::unordered_map<int, UniqueVertexInfo> unique_vertices; // global_id -> info
        
        struct UniqueEdgeInfo {
            std::string edge_key; // "src:dst:label"
            label_t src_label, dst_label, edge_label;
            int src_vid, dst_vid; // local vid for lookup
            std::unordered_set<std::string> needed_props;
            std::vector<std::string> ordered_prop_names;
            std::vector<int> ordered_prop_indices;
        };
        std::unordered_map<std::string, UniqueEdgeInfo> unique_edges; // edge_key -> info
        
        for (int s = 0; s < sampleCount; s++) {
            // Collect unique vertices
            for (int pv = 0; pv < patternVertexCount; pv++) {
                if (vertex_prop_infos[pv].prop_names.empty()) continue;
                
                int global_id = sampled_results_[s * patternVertexCount + pv];
                auto& uv = unique_vertices[global_id];
                if (uv.needed_props.empty() && uv.global_id == 0 && global_id != 0) {
                    // First time seeing this vertex
                    uv.global_id = global_id;
                    uv.label_id = vertex_prop_infos[pv].label_id;
                }
                uv.global_id = global_id; // always set (handles id=0 edge case)
                uv.label_id = vertex_prop_infos[pv].label_id;
                for (const auto& pname : vertex_prop_infos[pv].prop_names) {
                    uv.needed_props.insert(pname);
                }
            }
            
            // Collect unique edges
            for (int pe = 0; pe < patternEdgeCount; pe++) {
                if (edge_prop_infos[pe].prop_names.empty()) continue;
                
                auto& [src_pv, dst_pv] = pattern_graph_->edge_list[pe];
                int src_global = sampled_results_[s * patternVertexCount + src_pv];
                int dst_global = sampled_results_[s * patternVertexCount + dst_pv];
                label_t e_label = pattern_graph_->edge_label[pe];
                
                std::string edge_key = std::to_string(src_global) + ":" + 
                                       std::to_string(dst_global) + ":" + 
                                       std::to_string(e_label);
                
                auto& ue = unique_edges[edge_key];
                if (ue.edge_key.empty()) {
                    ue.edge_key = edge_key;
                    ue.src_label = edge_prop_infos[pe].src_label;
                    ue.dst_label = edge_prop_infos[pe].dst_label;
                    ue.edge_label = edge_prop_infos[pe].edge_label;
                    auto [sl, sv] = cached_data.data_meta->ToLocalId(src_global);
                    auto [dl, dv] = cached_data.data_meta->ToLocalId(dst_global);
                    ue.src_vid = sv;
                    ue.dst_vid = dv;
                }
                for (const auto& pname : edge_prop_infos[pe].prop_names) {
                    ue.needed_props.insert(pname);
                }
            }
        }
        
        LOG(INFO) << "[SAMPLED_MATCH] Unique vertices needing props: " << unique_vertices.size()
                  << ", unique edges needing props: " << unique_edges.size();
        
        // ================================================================
        // Step 2: Resolve merged property names to column indices, then
        //         fetch each unique vertex/edge's properties exactly once.
        // ================================================================
        
        // Resolve vertex property indices and fetch
        for (auto& [gid, uv] : unique_vertices) {
            auto all_names = schema.get_vertex_property_names(uv.label_id);
            for (const auto& pname : uv.needed_props) {
                auto it = std::find(all_names.begin(), all_names.end(), pname);
                if (it != all_names.end()) {
                    uv.ordered_prop_names.push_back(pname);
                    uv.ordered_prop_indices.push_back(std::distance(all_names.begin(), it));
                }
            }
        }
        
        // Resolve edge property indices
        for (auto& [key, ue] : unique_edges) {
            auto all_names = schema.get_edge_property_names(ue.src_label, ue.dst_label, ue.edge_label);
            for (const auto& pname : ue.needed_props) {
                auto it = std::find(all_names.begin(), all_names.end(), pname);
                if (it != all_names.end()) {
                    ue.ordered_prop_names.push_back(pname);
                    ue.ordered_prop_indices.push_back(std::distance(all_names.begin(), it));
                }
            }
        }
        
        // ================================================================
        // Step 3: Write deduplicated JSON with schema
        // ================================================================
        std::string propsFile = GenerateOutputFilePath("sampled_props") + ".json";
        std::filesystem::create_directories(std::filesystem::path(propsFile).parent_path());
        
        std::ofstream ofs(propsFile);
        if (!ofs.is_open()) {
            LOG(ERROR) << "[SAMPLED_MATCH] Failed to open props file: " << propsFile;
            return "";
        }
        
        ofs << "{";
        
        // ---- Write "schema" section ----
        // Merge all needed property names per vertex label and edge triplet,
        // then look up types from NeuG schema.
        
        // vertex label -> merged set of property names
        std::unordered_map<label_t, std::unordered_set<std::string>> vlabel_props;
        for (const auto& [gid, uv] : unique_vertices) {
            for (const auto& pname : uv.ordered_prop_names) {
                vlabel_props[uv.label_id].insert(pname);
            }
        }
        
        // edge triplet key -> (src_label, dst_label, edge_label, merged props)
        struct EdgeTripletSchema {
            label_t src_label, dst_label, edge_label;
            std::unordered_set<std::string> props;
        };
        std::unordered_map<uint32_t, EdgeTripletSchema> elabel_props;
        for (const auto& [key, ue] : unique_edges) {
            uint32_t triplet = schema.generate_edge_label(ue.src_label, ue.dst_label, ue.edge_label);
            auto& ets = elabel_props[triplet];
            ets.src_label = ue.src_label;
            ets.dst_label = ue.dst_label;
            ets.edge_label = ue.edge_label;
            for (const auto& pname : ue.ordered_prop_names) {
                ets.props.insert(pname);
            }
        }
        
        ofs << "\"schema\":{\"vertices\":[";
        bool first_sl = true;
        for (const auto& [vlabel, pnames] : vlabel_props) {
            if (pnames.empty()) continue;
            if (!first_sl) ofs << ",";
            first_sl = false;
            
            std::string label_name = schema.get_vertex_label_name(vlabel);
            auto v_schema = schema.get_vertex_schema(vlabel);
            
            ofs << "{\"label\":\"" << EscapeJsonString(label_name) << "\",\"properties\":{";
            bool first_prop = true;
            for (const auto& pname : pnames) {
                auto it = std::find(v_schema->property_names.begin(), v_schema->property_names.end(), pname);
                if (it != v_schema->property_names.end()) {
                    size_t idx = std::distance(v_schema->property_names.begin(), it);
                    if (!first_prop) ofs << ",";
                    first_prop = false;
                    ofs << "\"" << pname << "\":\"" << DataTypeIdToString(v_schema->property_types[idx].id()) << "\"";
                }
            }
            ofs << "}}";
        }
        
        ofs << "],\"edges\":[";
        first_sl = true;
        for (const auto& [triplet, ets] : elabel_props) {
            if (ets.props.empty()) continue;
            if (!first_sl) ofs << ",";
            first_sl = false;
            
            std::string src_name = schema.get_vertex_label_name(ets.src_label);
            std::string dst_name = schema.get_vertex_label_name(ets.dst_label);
            std::string edge_name = schema.get_edge_label_name(ets.edge_label);
            auto e_schema = schema.get_edge_schema(ets.src_label, ets.dst_label, ets.edge_label);
            
            ofs << "{\"label\":\"" << EscapeJsonString(edge_name) 
                << "\",\"src_label\":\"" << EscapeJsonString(src_name) 
                << "\",\"dst_label\":\"" << EscapeJsonString(dst_name) 
                << "\",\"properties\":{";
            bool first_prop = true;
            for (const auto& pname : ets.props) {
                auto it = std::find(e_schema->property_names.begin(), e_schema->property_names.end(), pname);
                if (it != e_schema->property_names.end()) {
                    size_t idx = std::distance(e_schema->property_names.begin(), it);
                    if (!first_prop) ofs << ",";
                    first_prop = false;
                    ofs << "\"" << pname << "\":\"" << DataTypeIdToString(e_schema->properties[idx].id()) << "\"";
                }
            }
            ofs << "}}";
        }
        ofs << "]},";
        // ---- End schema section ----
        
        ofs << "\"vertices\":[\n";
        
        // Write each unique vertex once
        bool first_v = true;
        for (auto& [gid, uv] : unique_vertices) {
            if (uv.ordered_prop_names.empty()) continue;
            
            auto [label, local_vid] = cached_data.data_meta->ToLocalId(gid);
            
            if (!first_v) ofs << ",\n";
            first_v = false;
            ofs << "{\"id\":" << gid << ",\"props\":{";
            
            bool first_p = true;
            for (size_t pi = 0; pi < uv.ordered_prop_names.size(); pi++) {
                if (!first_p) ofs << ",";
                first_p = false;
                
                std::string json_val = "null";  // default to null if not found
                if (label == uv.label_id) {
                    try {
                        Property prop = readInterface->GetVertexProperty(label, local_vid, uv.ordered_prop_indices[pi]);
                        execution::Value val = execution::property_to_value(prop);
                        // Use ValueToJsonString to preserve proper JSON types
                        json_val = ValueToJsonString(val);
                    } catch (...) {
                        json_val = "null";
                    }
                }
                ofs << "\"" << uv.ordered_prop_names[pi] << "\":" << json_val;
            }
            ofs << "}}";
        }
        
        ofs << "\n],\"edges\":[\n";
        
        // Write each unique edge once
        bool first_e = true;
        for (auto& [key, ue] : unique_edges) {
            if (ue.ordered_prop_names.empty()) continue;
            
            if (!first_e) ofs << ",\n";
            first_e = false;
            ofs << "{\"id\":\"" << EscapeJsonString(ue.edge_key) << "\",\"props\":{";
            
            bool first_p = true;
            for (size_t pi = 0; pi < ue.ordered_prop_names.size(); pi++) {
                if (!first_p) ofs << ",";
                first_p = false;
                
                std::string json_val = "null";  // default to null if not found
                try {
                    EdgeDataAccessor accessor = readInterface->GetEdgeDataAccessor(
                        ue.src_label, ue.dst_label, ue.edge_label, ue.ordered_prop_indices[pi]);
                    GenericView view = readInterface->GetGenericOutgoingGraphView(
                        ue.src_label, ue.dst_label, ue.edge_label);
                    
                    for (auto it = view.get_edges(ue.src_vid).begin(); 
                         it != view.get_edges(ue.src_vid).end(); ++it) {
                        if (*it == ue.dst_vid) {
                            Property prop = accessor.get_data(it);
                            execution::Value val = execution::property_to_value(prop);
                            // Use ValueToJsonString to preserve proper JSON types
                            json_val = ValueToJsonString(val);
                            break;
                        }
                    }
                } catch (...) {
                    json_val = "null";
                }
                
                ofs << "\"" << ue.ordered_prop_names[pi] << "\":" << json_val;
            }
            ofs << "}}";
        }
        
        ofs << "\n]}\n";
        ofs.close();
        
        LOG(INFO) << "[SAMPLED_MATCH] Deduplicated properties written to: " << propsFile 
                  << " (" << unique_vertices.size() << " unique vertices, " 
                  << unique_edges.size() << " unique edges)";
        return propsFile;
    }
};

// ============================================================================
// InitializeGraphFunction: CALL INITIALIZE([checkpoint_dir]) — populates the
// GraphDataCache, optionally restoring from a previously saved checkpoint.
// ============================================================================

struct InitializeGraphInput : public CallFuncInputBase {
  std::string checkpoint_dir;
  InitializeGraphInput() = default;
  explicit InitializeGraphInput(std::string dir) : checkpoint_dir(std::move(dir)) {}
  ~InitializeGraphInput() override = default;
};

struct InitializeGraphFunction {
  static constexpr const char* name = "INITIALIZE";
  
  static function_set getFunctionSet() {
    function_set functionSet;
    
    call_output_columns outputCols{
        {"status", common::LogicalTypeID::STRING},
        {"num_vertices", common::LogicalTypeID::INT64},
        {"num_edges", common::LogicalTypeID::INT64},
        {"max_degree", common::LogicalTypeID::INT64},
        {"degeneracy", common::LogicalTypeID::INT64}
    };
    
    // Overload 1: CALL INITIALIZE() — no checkpoint
    {
      auto func = std::make_unique<NeugCallFunction>(
          name,
          std::vector<common::LogicalTypeID>{},
          call_output_columns(outputCols));
      
      func->bindFunc = [](const Schema& schema, const execution::ContextMeta& ctx_meta,
                          const ::physical::PhysicalPlan& plan, int op_idx) 
          -> std::unique_ptr<CallFuncInputBase> {
        LOG(INFO) << "[INITIALIZE] Bind: no parameters (full initialization)";
        return std::make_unique<InitializeGraphInput>();
      };
      
      func->execFunc = [](const CallFuncInputBase& input, IStorageInterface& graph) 
          -> execution::Context {
        auto& initInput = static_cast<const InitializeGraphInput&>(input);
        LOG(INFO) << "[INITIALIZE] Executing graph initialization...";
        
        auto* readInterface = dynamic_cast<StorageReadInterface*>(&graph);
        if (!readInterface) {
          LOG(ERROR) << "[INITIALIZE] ERROR: graph is not a StorageReadInterface!";
          return execution::Context();
        }
        
        bool success = DoGraphInitialization(*readInterface, true, initInput.checkpoint_dir);
        
        auto& cache = GraphDataCache::Instance();
        auto& cached_data = cache.GetOrCreate(readInterface);
        
        execution::Context ctx;
        
        execution::ValueColumnBuilder<std::string> statusBuilder;
        statusBuilder.push_back_opt(success ? std::string("success") : std::string("failed"));
        ctx.set(0, statusBuilder.finish());
        
        execution::ValueColumnBuilder<int64_t> verticesBuilder;
        verticesBuilder.push_back_opt(static_cast<int64_t>(cached_data.data_meta->GetNumVertices()));
        ctx.set(1, verticesBuilder.finish());
        
        execution::ValueColumnBuilder<int64_t> edgesBuilder;
        edgesBuilder.push_back_opt(static_cast<int64_t>(cached_data.data_meta->GetNumEdges()));
        ctx.set(2, edgesBuilder.finish());
        
        execution::ValueColumnBuilder<int64_t> maxDegreeBuilder;
        maxDegreeBuilder.push_back_opt(static_cast<int64_t>(cached_data.data_meta->GetMaxDegree()));
        ctx.set(3, maxDegreeBuilder.finish());
        
        execution::ValueColumnBuilder<int64_t> degeneracyBuilder;
        degeneracyBuilder.push_back_opt(static_cast<int64_t>(cached_data.data_meta->GetDegeneracy()));
        ctx.set(4, degeneracyBuilder.finish());
        
        ctx.tag_ids = {0, 1, 2, 3, 4};
        
        LOG(INFO) << "[INITIALIZE] Initialization " << (success ? "successful" : "failed");
        return ctx;
      };
      
      functionSet.push_back(std::move(func));
    }
    
    // Overload 2: CALL INITIALIZE('/path/to/checkpoint') — try loading from checkpoint first
    {
      auto func = std::make_unique<NeugCallFunction>(
          name,
          std::vector<common::LogicalTypeID>{common::LogicalTypeID::STRING},
          call_output_columns(outputCols));
      
      func->bindFunc = [](const Schema& schema, const execution::ContextMeta& ctx_meta,
                          const ::physical::PhysicalPlan& plan, int op_idx) 
          -> std::unique_ptr<CallFuncInputBase> {
        auto& procedure = plan.plan(op_idx).opr().procedure_call();
        std::string checkpoint_dir;
        if (procedure.query().arguments_size() >= 1 &&
            procedure.query().arguments(0).has_const_()) {
          checkpoint_dir = procedure.query().arguments(0).const_().str();
        }
        LOG(INFO) << "[INITIALIZE] Bind: checkpoint_dir = " << checkpoint_dir;
        return std::make_unique<InitializeGraphInput>(std::move(checkpoint_dir));
      };
      
      func->execFunc = [](const CallFuncInputBase& input, IStorageInterface& graph) 
          -> execution::Context {
        auto& initInput = static_cast<const InitializeGraphInput&>(input);
        LOG(INFO) << "[INITIALIZE] Executing with checkpoint_dir: " << initInput.checkpoint_dir;
        
        auto* readInterface = dynamic_cast<StorageReadInterface*>(&graph);
        if (!readInterface) {
          LOG(ERROR) << "[INITIALIZE] ERROR: graph is not a StorageReadInterface!";
          return execution::Context();
        }
        
        bool success = DoGraphInitialization(*readInterface, true, initInput.checkpoint_dir);
        
        auto& cache = GraphDataCache::Instance();
        auto& cached_data = cache.GetOrCreate(readInterface);
        
        execution::Context ctx;
        
        execution::ValueColumnBuilder<std::string> statusBuilder;
        statusBuilder.push_back_opt(success ? std::string("success") : std::string("failed"));
        ctx.set(0, statusBuilder.finish());
        
        execution::ValueColumnBuilder<int64_t> verticesBuilder;
        verticesBuilder.push_back_opt(static_cast<int64_t>(cached_data.data_meta->GetNumVertices()));
        ctx.set(1, verticesBuilder.finish());
        
        execution::ValueColumnBuilder<int64_t> edgesBuilder;
        edgesBuilder.push_back_opt(static_cast<int64_t>(cached_data.data_meta->GetNumEdges()));
        ctx.set(2, edgesBuilder.finish());
        
        execution::ValueColumnBuilder<int64_t> maxDegreeBuilder;
        maxDegreeBuilder.push_back_opt(static_cast<int64_t>(cached_data.data_meta->GetMaxDegree()));
        ctx.set(3, maxDegreeBuilder.finish());
        
        execution::ValueColumnBuilder<int64_t> degeneracyBuilder;
        degeneracyBuilder.push_back_opt(static_cast<int64_t>(cached_data.data_meta->GetDegeneracy()));
        ctx.set(4, degeneracyBuilder.finish());
        
        ctx.tag_ids = {0, 1, 2, 3, 4};
        
        LOG(INFO) << "[INITIALIZE] Initialization " << (success ? "successful" : "failed");
        return ctx;
      };
      
      functionSet.push_back(std::move(func));
    }
    
    return functionSet;
  }
};

// ============================================================================
// SaveSampledmatchCheckpointFunction: persists the GraphDataCache contents so
// a later INITIALIZE call can skip preprocessing.
// CALL SAVE_SAMPLEDMATCH_CHECKPOINT('/path/to/checkpoint') RETURN *;
// ============================================================================

struct SaveSampledmatchCheckpointInput : public CallFuncInputBase {
  std::string checkpoint_dir;
  explicit SaveSampledmatchCheckpointInput(std::string dir) : checkpoint_dir(std::move(dir)) {}
  ~SaveSampledmatchCheckpointInput() override = default;
};

struct SaveSampledmatchCheckpointFunction {
  static constexpr const char* name = "SAVE_SAMPLEDMATCH_CHECKPOINT";
  
  static function_set getFunctionSet() {
    function_set functionSet;
    
    call_output_columns outputCols{
        {"status", common::LogicalTypeID::STRING},
        {"checkpoint_dir", common::LogicalTypeID::STRING}
    };
    
    auto func = std::make_unique<NeugCallFunction>(
        name,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::STRING},
        std::move(outputCols));
    
    func->bindFunc = [](const Schema& schema, const execution::ContextMeta& ctx_meta,
                        const ::physical::PhysicalPlan& plan, int op_idx) 
        -> std::unique_ptr<CallFuncInputBase> {
      auto& procedure = plan.plan(op_idx).opr().procedure_call();
      std::string checkpoint_dir;
      if (procedure.query().arguments_size() >= 1 &&
          procedure.query().arguments(0).has_const_()) {
        checkpoint_dir = procedure.query().arguments(0).const_().str();
      }
      LOG(INFO) << "[SAVE_SAMPLEDMATCH_CHECKPOINT] Bind: checkpoint_dir = " << checkpoint_dir;
      return std::make_unique<SaveSampledmatchCheckpointInput>(std::move(checkpoint_dir));
    };
    
    func->execFunc = [](const CallFuncInputBase& input, IStorageInterface& graph) 
        -> execution::Context {
      auto& ckptInput = static_cast<const SaveSampledmatchCheckpointInput&>(input);
      LOG(INFO) << "[SAVE_SAMPLEDMATCH_CHECKPOINT] Saving to: " << ckptInput.checkpoint_dir;
      
      auto* readInterface = dynamic_cast<StorageReadInterface*>(&graph);
      if (!readInterface) {
        LOG(ERROR) << "[SAVE_SAMPLEDMATCH_CHECKPOINT] ERROR: graph is not a StorageReadInterface!";
        return execution::Context();
      }
      
      bool success = SaveGraphCheckpoint(*readInterface, ckptInput.checkpoint_dir);
      
      execution::Context ctx;
      
      execution::ValueColumnBuilder<std::string> statusBuilder;
      statusBuilder.push_back_opt(success ? std::string("success") : std::string("failed"));
      ctx.set(0, statusBuilder.finish());
      
      execution::ValueColumnBuilder<std::string> dirBuilder;
      dirBuilder.push_back_opt(std::string(ckptInput.checkpoint_dir));
      ctx.set(1, dirBuilder.finish());
      
      ctx.tag_ids = {0, 1};
      
      LOG(INFO) << "[SAVE_SAMPLEDMATCH_CHECKPOINT] " << (success ? "Success" : "Failed");
      return ctx;
    };
    
    functionSet.push_back(std::move(func));
    return functionSet;
  }
};

// ============================================================================
// SampledMatchFunction: CALL SAMPLED_MATCH(pattern_file, sample_size) — the
// main entry point that runs subgraph sampling against the cached graph.
// ============================================================================

struct SampledMatchInput : public CallFuncInputBase {
  std::string patternFilePath;
  long long sampleSize;
  SampledMatchInput(std::string path, long long sample_size) : patternFilePath(std::move(path)), sampleSize(sample_size) {}
  ~SampledMatchInput() override = default;
};

struct SampledMatchFunction {
  static constexpr const char* name = "SAMPLED_MATCH";

  static function_set getFunctionSet() {
    function_set functionSet;

    // Four-column result layout:
    //   col 0: estimated_count (double) — estimated total number of embeddings
    //   col 1: sample_count    (int64)  — number of sampled embeddings returned
    //   col 2: result_file     (string) — CSV path holding the samples
    //   col 3: props_file      (string) — JSON path of extra properties, or empty
    call_output_columns outputCols{
        {"estimated_count", common::LogicalTypeID::DOUBLE},
        {"sample_count", common::LogicalTypeID::INT64},
        {"result_file", common::LogicalTypeID::STRING},
        {"props_file", common::LogicalTypeID::STRING}
    };

    auto func = std::make_unique<NeugCallFunction>(
        name,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::STRING, common::LogicalTypeID::INT64},
        std::move(outputCols));

    func->bindFunc = [](const Schema& schema, const execution::ContextMeta& ctx_meta,
                        const ::physical::PhysicalPlan& plan, int op_idx) 
        -> std::unique_ptr<CallFuncInputBase> {
      auto& procedure = plan.plan(op_idx).opr().procedure_call();
      std::string patternPath;
      long long sampleSize = 1000000;
      
      if (procedure.query().arguments_size() >= 2) {
        if (procedure.query().arguments(0).has_const_()) {
          const auto& arg = procedure.query().arguments(0);
          patternPath = arg.const_().str();
        }
        if (procedure.query().arguments(1).has_const_()) {
          const auto& arg = procedure.query().arguments(1);
          try { 
            sampleSize = arg.const_().i64();
            LOG(WARNING) << "[SAMPLED_MATCH] Sample size: " << sampleSize;
          } catch (const std::invalid_argument&) {
            sampleSize = 1000000;
            LOG(WARNING) << "[SAMPLED_MATCH] Invalid sample size: " << arg.const_().str()
                          << ", using default: " << sampleSize;
          } catch (const std::out_of_range&) {
            sampleSize = 1000000;
            LOG(WARNING) << "[SAMPLED_MATCH] Sample size out of range: " << arg.const_().str()
                          << ", using default: " << sampleSize;
          }
        }
      }
      
      LOG(INFO) << "[SAMPLED_MATCH] Bind: pattern file = " << patternPath;
      LOG(INFO) << "[SAMPLED_MATCH] Bind: sample size = " << sampleSize;
      return std::make_unique<SampledMatchInput>(patternPath, sampleSize);
    };
    
    func->execFunc = [](const CallFuncInputBase& input, IStorageInterface& graph) 
        -> execution::Context {
      auto& matchInput = static_cast<const SampledMatchInput&>(input);
      
      LOG(INFO) << "[SAMPLED_MATCH] Executing with graph access";
      LOG(INFO) << "[SAMPLED_MATCH] Pattern file: " << matchInput.patternFilePath;
      
      auto* readInterface = dynamic_cast<StorageReadInterface*>(&graph);
      if (!readInterface) {
        LOG(ERROR) << "[SAMPLED_MATCH] ERROR: graph is not a StorageReadInterface!";
        return execution::Context();
      }
      
      LOG(INFO) << "[SAMPLED_MATCH] Starting subgraph matching...";
      
      SampledSubgraphMatcher matcher(*readInterface, matchInput.patternFilePath, matchInput.sampleSize);
      double estimatedCount = matcher.match();
      
      const auto& sampledResults = matcher.GetSampledResults();
      int patternVertexCount = matcher.GetPatternVertexCount();
      int patternEdgeCount = matcher.GetPatternEdgeCount();
      auto patternEdgeList = matcher.GetPatternEdgeList();
      int sampleCount = patternVertexCount > 0 ? 
          sampledResults.size() / patternVertexCount : 0;
      
      LOG(INFO) << "[SAMPLED_MATCH] Estimated count: " << (long long)estimatedCount;
      LOG(INFO) << "[SAMPLED_MATCH] Sampled embeddings: " << sampleCount;
      LOG(INFO) << "[SAMPLED_MATCH] Pattern edges: " << patternEdgeCount;
      
      std::string outputFile = GenerateOutputFilePath("sampled_match");

      std::ofstream ofs(outputFile);
      if (!ofs.is_open()) {
        LOG(ERROR) << "[SAMPLED_MATCH] Failed to open output file: " << outputFile;
        return execution::Context();
      }

      // CSV header: vertex columns (v0, v1, ...) followed by edge columns
      // (vSRC-vDST) encoded as src_global:dst_global:edge_label.
      for (int v = 0; v < patternVertexCount; v++) {
        if (v > 0) ofs << ",";
        ofs << "v" << v;
      }
      for (int e = 0; e < patternEdgeCount; e++) {
        auto [src, dst, label] = patternEdgeList[e];
        ofs << ",v" << src << "-v" << dst;
      }
      ofs << "\n";

      for (int s = 0; s < sampleCount; s++) {
        for (int v = 0; v < patternVertexCount; v++) {
          if (v > 0) ofs << ",";
          ofs << sampledResults[s * patternVertexCount + v];
        }
        for (int e = 0; e < patternEdgeCount; e++) {
          ofs << "," << matcher.GetSampledEdgeKey(s, e);
        }
        ofs << "\n";
      }
      
      ofs.close();
      LOG(INFO) << "[SAMPLED_MATCH] Results written to: " << outputFile;
      
      // After matching, fetch required properties and write to JSON file
      std::string propsFile = matcher.FetchAndWriteProperties();
      if (!propsFile.empty()) {
          LOG(INFO) << "[SAMPLED_MATCH] Properties file: " << propsFile;
      } else {
          LOG(INFO) << "[SAMPLED_MATCH] No properties requested or no results";
      }
      
      // Assemble the 1-row, 4-column result Context.
      execution::Context ctx;

      execution::ValueColumnBuilder<double> estimatedCountBuilder;
      estimatedCountBuilder.push_back_opt(estimatedCount);
      ctx.set(0, estimatedCountBuilder.finish());

      execution::ValueColumnBuilder<int64_t> sampleCountBuilder;
      sampleCountBuilder.push_back_opt(static_cast<int64_t>(sampleCount));
      ctx.set(1, sampleCountBuilder.finish());

      execution::ValueColumnBuilder<std::string> filePathBuilder;
      filePathBuilder.push_back_opt(std::string(outputFile));
      ctx.set(2, filePathBuilder.finish());

      execution::ValueColumnBuilder<std::string> propsFileBuilder;
      propsFileBuilder.push_back_opt(std::string(propsFile));
      ctx.set(3, propsFileBuilder.finish());

      ctx.tag_ids = {0, 1, 2, 3};
      
      LOG(INFO) << "[SAMPLED_MATCH] Returned 1 row with result file: " << outputFile 
                << ", props file: " << propsFile;
      
      return ctx;
    };
    
    functionSet.push_back(std::move(func));
    return functionSet;
  }
};

// ============================================================================
// GetVertexPropertyFunction: looks up vertex properties and writes a CSV.
//   Inputs : vertex_ids (JSON array), vertex_label (string),
//            property_names (JSON array).
//   Output : path of the generated CSV file.
// ============================================================================

struct GetVertexPropertyInput : public CallFuncInputBase {
  std::vector<int64_t> vertex_ids;
  std::string vertex_label;
  std::vector<std::string> property_names;
  
  GetVertexPropertyInput(std::vector<int64_t> ids, std::string label, std::vector<std::string> props)
    : vertex_ids(std::move(ids)), vertex_label(std::move(label)), property_names(std::move(props)) {}
  ~GetVertexPropertyInput() override = default;
};

struct GetVertexPropertyFunction {
  static constexpr const char* name = "GET_VERTEX_PROPERTY";
  
  static function_set getFunctionSet() {
    function_set functionSet;
    
    // Output schema: single string column carrying the generated file path.
    call_output_columns outputCols{
        {"result_file", common::LogicalTypeID::STRING}
    };

    auto func = std::make_unique<NeugCallFunction>(
        name,
        std::vector<common::LogicalTypeID>{
            common::LogicalTypeID::STRING,  // vertex_ids as JSON array string
            common::LogicalTypeID::STRING,  // vertex_label
            common::LogicalTypeID::STRING   // property_names as JSON array string
        },
        std::move(outputCols));

    func->bindFunc = [](const Schema& schema, const execution::ContextMeta& ctx_meta,
                        const ::physical::PhysicalPlan& plan, int op_idx)
        -> std::unique_ptr<CallFuncInputBase> {
      auto& procedure = plan.plan(op_idx).opr().procedure_call();

      std::vector<int64_t> vertex_ids;
      std::string vertex_label;
      std::vector<std::string> property_names;

      if (procedure.query().arguments_size() >= 3) {
        // Arg 0: vertex_ids — JSON array of ints.
        if (procedure.query().arguments(0).has_const_()) {
          std::string ids_str = procedure.query().arguments(0).const_().str();
          rapidjson::Document doc;
          if (!doc.Parse(ids_str.c_str()).HasParseError() && doc.IsArray()) {
            for (auto& v : doc.GetArray()) {
              if (v.IsInt64()) vertex_ids.push_back(v.GetInt64());
              else if (v.IsInt()) vertex_ids.push_back(v.GetInt());
            }
          }
        }

        // Arg 1: vertex_label name.
        if (procedure.query().arguments(1).has_const_()) {
          vertex_label = procedure.query().arguments(1).const_().str();
        }

        // Arg 2: property_names — JSON array of strings.
        if (procedure.query().arguments(2).has_const_()) {
          std::string props_str = procedure.query().arguments(2).const_().str();
          rapidjson::Document doc;
          if (!doc.Parse(props_str.c_str()).HasParseError() && doc.IsArray()) {
            for (auto& v : doc.GetArray()) {
              if (v.IsString()) property_names.push_back(v.GetString());
            }
          }
        }
      }
      
      LOG(INFO) << "[GET_VERTEX_PROPERTY] Bind: " << vertex_ids.size() << " vertices, "
                << "label=" << vertex_label << ", " << property_names.size() << " properties";
      
      return std::make_unique<GetVertexPropertyInput>(
          std::move(vertex_ids), std::move(vertex_label), std::move(property_names));
    };
    
    func->execFunc = [](const CallFuncInputBase& input, IStorageInterface& graph) 
        -> execution::Context {
      auto& propInput = static_cast<const GetVertexPropertyInput&>(input);
      
      auto* readInterface = dynamic_cast<StorageReadInterface*>(&graph);
      if (!readInterface) {
        LOG(ERROR) << "[GET_VERTEX_PROPERTY] ERROR: graph is not a StorageReadInterface!";
        return execution::Context();
      }
      
      auto& cache = GraphDataCache::Instance();
      auto& cached_data = cache.GetOrCreate(readInterface);
      if (!cached_data.preprocessed) {
        LOG(WARNING) << "[GET_VERTEX_PROPERTY] Cache not preprocessed, calling DoGraphInitialization...";
        DoGraphInitialization(*readInterface, false);
      }

      const auto& schema = readInterface->schema();
      if (!schema.contains_vertex_label(propInput.vertex_label)) {
        LOG(ERROR) << "[GET_VERTEX_PROPERTY] vertex label '" << propInput.vertex_label << "' not found in schema";
        return execution::Context();
      }
      label_t vertex_label_id = schema.get_vertex_label_id(propInput.vertex_label);

      int numVertices = propInput.vertex_ids.size();
      int numProps = propInput.property_names.size();

      std::string outputFile = GenerateOutputFilePath("vertex_property");

      // Resolve user-facing property names into storage indices; -1 marks
      // properties the schema does not define (written as empty cells).
      std::vector<std::string> all_prop_names = schema.get_vertex_property_names(vertex_label_id);
      std::vector<int> prop_indices;
      for (const auto& pname : propInput.property_names) {
        auto it = std::find(all_prop_names.begin(), all_prop_names.end(), pname);
        if (it != all_prop_names.end()) {
          prop_indices.push_back(std::distance(all_prop_names.begin(), it));
        } else {
          prop_indices.push_back(-1);
        }
      }

      std::ofstream ofs(outputFile);
      if (!ofs.is_open()) {
        LOG(ERROR) << "[GET_VERTEX_PROPERTY] Failed to open output file: " << outputFile;
        return execution::Context();
      }

      // Header row: vertex_id, prop1, prop2, ...
      ofs << "vertex_id";
      for (const auto& pname : propInput.property_names) {
        ofs << "," << pname;
      }
      ofs << "\n";

      for (int64_t global_id : propInput.vertex_ids) {
        ofs << global_id;

        auto [label, local_vid] = cached_data.data_meta->ToLocalId(global_id);

        for (int p = 0; p < numProps; p++) {
          ofs << ",";
          if (label == vertex_label_id && prop_indices[p] >= 0) {
            try {
              Property prop = readInterface->GetVertexProperty(label, local_vid, prop_indices[p]);
              execution::Value val = execution::property_to_value(prop);
              // Quote and escape per RFC 4180 when the value contains a
              // comma or a quote; otherwise emit verbatim.
              std::string val_str = val.to_string();
              if (val_str.find(',') != std::string::npos || val_str.find('"') != std::string::npos) {
                std::string escaped;
                for (char c : val_str) {
                  if (c == '"') escaped += "\"\"";
                  else escaped += c;
                }
                ofs << "\"" << escaped << "\"";
              } else {
                ofs << val_str;
              }
            } catch (...) {
              // Leave cell empty on fetch failure.
            }
          }
        }
        ofs << "\n";
      }

      ofs.close();
      LOG(INFO) << "[GET_VERTEX_PROPERTY] Results written to: " << outputFile;

      execution::Context ctx;
      
      execution::ValueColumnBuilder<std::string> filePathBuilder;
      filePathBuilder.push_back_opt(std::string(outputFile));
      ctx.set(0, filePathBuilder.finish());
      
      ctx.tag_ids = {0};
      
      LOG(INFO) << "[GET_VERTEX_PROPERTY] Returned file: " << outputFile 
                << " with " << numVertices << " vertices, " << numProps << " properties";
      
      return ctx;
    };
    
    functionSet.push_back(std::move(func));
    return functionSet;
  }
};

// ============================================================================
// GetEdgePropertyFunction: looks up edge properties and writes a CSV.
//   Inputs : edge_keys (JSON array), edge_label (string),
//            property_names (JSON array).
//   edge_key format: "src_global:dst_global:edge_label_id".
//   Output : path of the generated CSV file.
// ============================================================================

struct GetEdgePropertyInput : public CallFuncInputBase {
  std::vector<std::string> edge_keys;
  std::string edge_label;
  std::vector<std::string> property_names;
  
  GetEdgePropertyInput(std::vector<std::string> keys, std::string label, std::vector<std::string> props)
    : edge_keys(std::move(keys)), edge_label(std::move(label)), property_names(std::move(props)) {}
  ~GetEdgePropertyInput() override = default;
};

struct GetEdgePropertyFunction {
  static constexpr const char* name = "GET_EDGE_PROPERTY";
  
  static function_set getFunctionSet() {
    function_set functionSet;
    
    // Output schema: single string column carrying the generated file path.
    call_output_columns outputCols{
        {"result_file", common::LogicalTypeID::STRING}
    };

    auto func = std::make_unique<NeugCallFunction>(
        name,
        std::vector<common::LogicalTypeID>{
            common::LogicalTypeID::STRING,  // edge_keys as JSON array string
            common::LogicalTypeID::STRING,  // edge_label
            common::LogicalTypeID::STRING   // property_names as JSON array string
        },
        std::move(outputCols));

    func->bindFunc = [](const Schema& schema, const execution::ContextMeta& ctx_meta,
                        const ::physical::PhysicalPlan& plan, int op_idx)
        -> std::unique_ptr<CallFuncInputBase> {
      auto& procedure = plan.plan(op_idx).opr().procedure_call();

      std::vector<std::string> edge_keys;
      std::string edge_label;
      std::vector<std::string> property_names;

      if (procedure.query().arguments_size() >= 3) {
        // Arg 0: edge_keys — JSON array of "src:dst:label" strings.
        if (procedure.query().arguments(0).has_const_()) {
          std::string keys_str = procedure.query().arguments(0).const_().str();
          rapidjson::Document doc;
          if (!doc.Parse(keys_str.c_str()).HasParseError() && doc.IsArray()) {
            for (auto& v : doc.GetArray()) {
              if (v.IsString()) edge_keys.push_back(v.GetString());
            }
          }
        }

        // Arg 1: edge_label name.
        if (procedure.query().arguments(1).has_const_()) {
          edge_label = procedure.query().arguments(1).const_().str();
        }

        // Arg 2: property_names — JSON array of strings.
        if (procedure.query().arguments(2).has_const_()) {
          std::string props_str = procedure.query().arguments(2).const_().str();
          rapidjson::Document doc;
          if (!doc.Parse(props_str.c_str()).HasParseError() && doc.IsArray()) {
            for (auto& v : doc.GetArray()) {
              if (v.IsString()) property_names.push_back(v.GetString());
            }
          }
        }
      }
      
      LOG(INFO) << "[GET_EDGE_PROPERTY] Bind: " << edge_keys.size() << " edges, "
                << "label=" << edge_label << ", " << property_names.size() << " properties";
      
      return std::make_unique<GetEdgePropertyInput>(
          std::move(edge_keys), std::move(edge_label), std::move(property_names));
    };
    
    func->execFunc = [](const CallFuncInputBase& input, IStorageInterface& graph) 
        -> execution::Context {
      auto& propInput = static_cast<const GetEdgePropertyInput&>(input);
      
      auto* readInterface = dynamic_cast<StorageReadInterface*>(&graph);
      if (!readInterface) {
        LOG(ERROR) << "[GET_EDGE_PROPERTY] ERROR: graph is not a StorageReadInterface!";
        return execution::Context();
      }
      
      auto& cache = GraphDataCache::Instance();
      auto& cached_data = cache.GetOrCreate(readInterface);
      if (!cached_data.preprocessed) {
        LOG(WARNING) << "[GET_EDGE_PROPERTY] Cache not preprocessed, calling DoGraphInitialization...";
        DoGraphInitialization(*readInterface, false);
      }

      const auto& schema = readInterface->schema();

      int numEdges = propInput.edge_keys.size();
      int numProps = propInput.property_names.size();

      std::string outputFile = GenerateOutputFilePath("edge_property");

      // Split each "src:dst:label" key into its components and resolve the
      // (label, vid) pair for both endpoints via the graph cache.
      struct ParsedEdge {
        std::string key;
        int64_t src_global;
        int64_t dst_global;
        label_t edge_label_id;
        label_t src_label;
        vid_t src_vid;
        label_t dst_label;
        vid_t dst_vid;
        bool valid;
      };
      
      std::vector<ParsedEdge> parsed_edges;
      parsed_edges.reserve(numEdges);
      
      for (const auto& key : propInput.edge_keys) {
        ParsedEdge pe;
        pe.key = key;
        pe.valid = false;
        
        size_t pos1 = key.find(':');
        size_t pos2 = key.rfind(':');
        if (pos1 != std::string::npos && pos2 != std::string::npos && pos1 != pos2) {
          try {
            pe.src_global = std::stoll(key.substr(0, pos1));
            pe.dst_global = std::stoll(key.substr(pos1 + 1, pos2 - pos1 - 1));
            pe.edge_label_id = std::stoi(key.substr(pos2 + 1));
            
            auto [src_l, src_v] = cached_data.data_meta->ToLocalId(pe.src_global);
            auto [dst_l, dst_v] = cached_data.data_meta->ToLocalId(pe.dst_global);
            pe.src_label = src_l;
            pe.src_vid = src_v;
            pe.dst_label = dst_l;
            pe.dst_vid = dst_v;
            pe.valid = (src_l != 255 && dst_l != 255);
          } catch (...) {}
        }
        
        parsed_edges.push_back(pe);
      }
      
      // Resolve property names to schema indices using the first valid edge
      // as a reference — edges in one call share the same endpoint labels.
      std::vector<int> prop_indices;
      bool has_valid_edge = false;
      label_t sample_src_label = 0, sample_dst_label = 0, sample_edge_label = 0;
      
      for (const auto& pe : parsed_edges) {
        if (pe.valid) {
          sample_src_label = pe.src_label;
          sample_dst_label = pe.dst_label;
          sample_edge_label = pe.edge_label_id;
          has_valid_edge = true;
          break;
        }
      }
      
      if (has_valid_edge) {
        std::vector<std::string> all_prop_names = schema.get_edge_property_names(
            sample_src_label, sample_dst_label, sample_edge_label);
        
        for (const auto& pname : propInput.property_names) {
          auto it = std::find(all_prop_names.begin(), all_prop_names.end(), pname);
          if (it != all_prop_names.end()) {
            prop_indices.push_back(std::distance(all_prop_names.begin(), it));
          } else {
            prop_indices.push_back(-1);
          }
        }
      }
      
      std::ofstream ofs(outputFile);
      if (!ofs.is_open()) {
        LOG(ERROR) << "[GET_EDGE_PROPERTY] Failed to open output file: " << outputFile;
        return execution::Context();
      }

      // Header row: edge_key, src_id, dst_id, prop1, prop2, ...
      ofs << "edge_key,src_id,dst_id";
      for (const auto& pname : propInput.property_names) {
        ofs << "," << pname;
      }
      ofs << "\n";

      for (const auto& pe : parsed_edges) {
        ofs << pe.key << "," << pe.src_global << "," << pe.dst_global;

        for (int p = 0; p < numProps; p++) {
          ofs << ",";
          if (pe.valid && has_valid_edge && p < (int)prop_indices.size() && prop_indices[p] >= 0) {
            try {
              EdgeDataAccessor accessor = readInterface->GetEdgeDataAccessor(
                  pe.src_label, pe.dst_label, pe.edge_label_id, prop_indices[p]);
              GenericView view = readInterface->GetGenericOutgoingGraphView(
                  pe.src_label, pe.dst_label, pe.edge_label_id);

              // Locate the requested dst vertex among the src's out-edges.
              for (auto it = view.get_edges(pe.src_vid).begin();
                   it != view.get_edges(pe.src_vid).end(); ++it) {
                if (*it == pe.dst_vid) {
                  Property prop = accessor.get_data(it);
                  execution::Value val = execution::property_to_value(prop);
                  std::string val_str = val.to_string();
                  // Quote and escape per RFC 4180 when needed.
                  if (val_str.find(',') != std::string::npos || val_str.find('"') != std::string::npos) {
                    std::string escaped;
                    for (char c : val_str) {
                      if (c == '"') escaped += "\"\"";
                      else escaped += c;
                    }
                    ofs << "\"" << escaped << "\"";
                  } else {
                    ofs << val_str;
                  }
                  break;
                }
              }
            } catch (...) {
              // Leave cell empty on fetch failure.
            }
          }
        }
        ofs << "\n";
      }

      ofs.close();
      LOG(INFO) << "[GET_EDGE_PROPERTY] Results written to: " << outputFile;

      execution::Context ctx;
      
      execution::ValueColumnBuilder<std::string> filePathBuilder;
      filePathBuilder.push_back_opt(std::string(outputFile));
      ctx.set(0, filePathBuilder.finish());
      
      ctx.tag_ids = {0};
      
      LOG(INFO) << "[GET_EDGE_PROPERTY] Returned file: " << outputFile 
                << " with " << numEdges << " edges, " << numProps << " properties";
      
      return ctx;
    };
    
    functionSet.push_back(std::move(func));
    return functionSet;
  }
};

}  // namespace function
}  // namespace neug
