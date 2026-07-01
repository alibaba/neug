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
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>
#include "rapidjson/document.h"
#include "rapidjson/error/en.h"
#include "rapidjson/filereadstream.h"

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/expression/rel_expression.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/function/function.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/bind_input.h"
#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"

#include "daf_lib/include/backtrack.h"
#include "daf_lib/include/candidate_space.h"
#include "daf_lib/include/dag.h"
#include "daf_lib/include/data_graph.h"
#include "daf_lib/include/query_graph.h"
#include "fastest_lib/src/SubgraphCounting/cardinality_estimation.h"
#include "fastest_lib/src/SubgraphMatching/pattern_graph.h"
#include "pattern_cypher_translator.h"
#include "pattern_matching_data_graph_meta.h"
#include "storage/graph_storage.hpp"

namespace neug {
namespace function {

inline execution::Context MakeSingleChunkContext(
    std::vector<std::shared_ptr<execution::IContextColumn>> columns) {
  execution::Context ctx;
  execution::DataChunk chunk;
  ctx.tag_ids.reserve(columns.size());
  for (size_t i = 0; i < columns.size(); ++i) {
    chunk.set(static_cast<int>(i), std::move(columns[i]));
    ctx.tag_ids.push_back(static_cast<int>(i));
  }
  ctx.append_chunk(std::move(chunk));
  return ctx;
}

// ============================================================================
// Helper functions for parsing pattern JSON
// ============================================================================

// Helper function to parse comparison operator. Unknown operator strings
// (e.g. "and", "or", "like") fall back to COMP_EQUAL — flag the fallback so
// users notice instead of silently getting equality semantics. Dedup by op
// string so a typo'd operator only warns once per process.
inline CompType ParseOperator(const std::string& op_in) {
  // Normalize to lowercase so word operators ("IN", "Not_In") are recognized
  // case-insensitively (Cypher keywords are case-insensitive); symbol
  // operators ("=", ">=") are unaffected by tolower.
  std::string op = op_in;
  std::transform(op.begin(), op.end(), op.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  if (op == "=" || op == "==")
    return CompType::COMP_EQUAL;
  if (op == ">")
    return CompType::COMP_GREATER;
  if (op == "<")
    return CompType::COMP_LESS;
  if (op == ">=")
    return CompType::COMP_GREATER_EQUAL;
  if (op == "<=")
    return CompType::COMP_LESS_EQUAL;
  if (op == "in")
    return CompType::COMP_IN;
  if (op == "not_in")
    return CompType::COMP_NOT_IN;

  static std::mutex op_warn_mu;
  static std::unordered_set<std::string> op_warn_seen;
  bool fresh = false;
  {
    std::lock_guard<std::mutex> lk(op_warn_mu);
    fresh = op_warn_seen.insert(op).second;
  }
  if (fresh) {
    LOG(WARNING) << "[SAMPLED_PATTERN_MATCH] Unknown constraint operator '"
                 << op << "'; falling back to '=' (COMP_EQUAL). "
                 << "Boolean combinators like 'and'/'or' are not supported — "
                 << "constraints in an array are AND-combined implicitly.";
  }
  return CompType::COMP_EQUAL;
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
inline std::string EscapeJsonString(const std::string& s);

/**
 * @brief Convert a neug::execution::Value to JSON format string (preserving
 * types)
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
inline std::string ValueToJsonString(const execution::Value& val);

// Helper function to parse constraints from rapidjson
inline std::vector<PropCons> ParseConstraints(
    const rapidjson::Value& constraints_json) {
  std::vector<PropCons> constraints;
  if (!constraints_json.IsArray())
    return constraints;

  for (rapidjson::SizeType i = 0; i < constraints_json.Size(); i++) {
    const auto& c = constraints_json[i];
    // A constraint must be an object; rapidjson's HasMember()/GetString()
    // assert on the wrong type, so guard before touching any field.
    if (!c.IsObject())
      continue;
    std::string prop_name =
        (c.HasMember("property") && c["property"].IsString())
            ? c["property"].GetString()
            : "";
    std::string op_str = (c.HasMember("operator") && c["operator"].IsString())
                             ? c["operator"].GetString()
                             : "=";
    CompType op = ParseOperator(op_str);
    // Set-membership ('in'/'not_in') is not implemented: the value path below
    // only parses scalars (arrays collapse to 0) and every evaluator returns
    // false for these ops, so accepting them would silently drop all matches.
    // Reject loudly instead of returning a wrong (empty) result.
    if (op == CompType::COMP_IN || op == CompType::COMP_NOT_IN) {
      THROW_EXTENSION_EXCEPTION(
          std::string("[PATTERN_MATCH] Constraint operator '") + op_str +
          "' is not supported. Supported operators: =, ==, >, <, >=, <=. "
          "Set membership ('in'/'not_in') is not implemented.");
    }
    Value value = c.HasMember("value") ? CreateValueFromRapidjson(c["value"])
                                       : Value::INT32(0);

    constraints.emplace_back(prop_name, op, std::move(value));
  }
  return constraints;
}

// ============================================================================
// Temp file path helper: builds a collision-resistant path under
// <temp_dir>/neug_sample. Returns "" if the directory can't be created.
// Filename = <prefix>_<ms>_<counter>_<rand-hex><extension>. Millisecond
// timestamp alone collides under burst load; the per-process atomic counter
// and 64-bit random suffix keep concurrent calls distinct.
// ============================================================================
inline std::string GenerateTempFilePath(const std::string& prefix,
                                        const std::string& extension) {
  auto now = std::chrono::system_clock::now();
  auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                       now.time_since_epoch())
                       .count();

  static std::atomic<uint64_t> counter{0};
  uint64_t seq = counter.fetch_add(1, std::memory_order_relaxed);

  // Each thread keeps its own RNG seeded from random_device + a high-res
  // clock — random_device alone has been observed to repeat on some libcs
  // when multiple threads construct one back-to-back.
  thread_local std::mt19937_64 rng{
      static_cast<uint64_t>(std::random_device{}()) ^
      static_cast<uint64_t>(std::chrono::high_resolution_clock::now()
                                .time_since_epoch()
                                .count())};
  uint64_t rand_bits = rng();

  std::ostringstream name;
  name << prefix << "_" << timestamp << "_" << seq << "_" << std::hex
       << std::setw(16) << std::setfill('0') << rand_bits << extension;

  std::filesystem::path dir =
      std::filesystem::temp_directory_path() / "neug_sample";
  std::error_code ec;
  std::filesystem::create_directories(dir, ec);
  if (ec) {
    LOG(ERROR) << "[SAMPLED_PATTERN_MATCH] Failed to create output directory "
               << dir << ": " << ec.message();
    return "";
  }
  return (dir / name.str()).string();
}

inline std::string GenerateOutputFilePath(const std::string& prefix) {
  return GenerateTempFilePath(prefix, ".csv");
}

inline std::string TrimCopy(std::string_view input) {
  size_t begin = 0;
  while (begin < input.size() &&
         std::isspace(static_cast<unsigned char>(input[begin]))) {
    begin++;
  }
  size_t end = input.size();
  while (end > begin &&
         std::isspace(static_cast<unsigned char>(input[end - 1]))) {
    end--;
  }
  return std::string(input.substr(begin, end - begin));
}

inline bool ReadTextFile(const std::string& path, std::string* out) {
  std::ifstream ifs(path);
  if (!ifs.is_open())
    return false;
  std::ostringstream oss;
  oss << ifs.rdbuf();
  *out = oss.str();
  return true;
}

inline bool WriteTextFile(const std::string& path, const std::string& text) {
  std::ofstream ofs(path);
  if (!ofs.is_open())
    return false;
  ofs << text;
  return static_cast<bool>(ofs);
}

inline bool LooksLikeJsonPattern(std::string_view text) {
  std::string trimmed = TrimCopy(text);
  return !trimmed.empty() && trimmed.front() == '{';
}

inline std::string WritePatternJsonTempFile(const std::string& pattern_json) {
  std::string path = GenerateTempFilePath("pattern_matching_pattern", ".json");
  if (path.empty())
    return "";
  if (!WriteTextFile(path, pattern_json)) {
    LOG(ERROR) << "[PATTERN_MATCHING] Failed to write pattern JSON file: "
               << path;
    return "";
  }
  return path;
}

// Normalize a user pattern argument to a JSON file path. The argument may be:
//   * path to a JSON pattern file
//   * path to a Cypher pattern file
//   * inline JSON pattern text
//   * inline Cypher pattern text
std::string NormalizePatternInputToJsonFile(const std::string& arg,
                                            const char* log_tag);

// ============================================================================
// GraphDataCache: caches preprocessed graph metadata so repeated
// SAMPLED_PATTERN_MATCH calls on the same graph avoid rebuilding DataGraphMeta
// every time.
// ============================================================================

class GraphDataCache {
 public:
  static GraphDataCache& Instance() {
    static GraphDataCache instance;
    return instance;
  }

  struct CachedData {
    std::unique_ptr<DataGraphMeta> data_meta;
    std::shared_ptr<std::unordered_map<
        label_t, std::unordered_map<label_t, std::vector<label_t>>>>
        schema_graph;
    bool preprocessed = false;
  };

  // Returns the cache slot for `graph`, creating it lazily on first use.
  //
  // Keyed by the address of the graph's Schema (&graph.schema()), obtained
  // purely through the public StorageReadInterface API. The Schema is a
  // value member of the underlying graph, so its address is stable for the
  // graph's lifetime and distinct per graph -- unlike the per-query
  // StorageReadInterface wrapper, which is a stack-local rebuilt every query
  // (keying on it would miss across queries and rebuild the preprocessing every
  // call). This needs no graph-object exposure from the storage layer.
  //
  // Residual: if a graph is destroyed and a different graph is later allocated
  // with its Schema at the same address, a stale entry could be served. The
  // cache is process-global and not cleared on graph teardown, so callers
  // recycling graph objects should ClearAll() between distinct graphs.
  static const void* KeyOf(const StorageReadInterface& graph) {
    return static_cast<const void*>(&graph.schema());
  }

  CachedData& GetOrCreate(const StorageReadInterface& graph) {
    const void* key = KeyOf(graph);
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = cache_.find(key);
    if (it == cache_.end()) {
      auto& data = cache_[key];
      data.data_meta = std::make_unique<DataGraphMeta>(graph);
      data.schema_graph = std::make_shared<std::unordered_map<
          label_t, std::unordered_map<label_t, std::vector<label_t>>>>();
      data.preprocessed = false;
    }
    return cache_[key];
  }

  bool HasCache(const StorageReadInterface& graph) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_.find(KeyOf(graph));
    return it != cache_.end() && it->second.preprocessed;
  }

  void ClearCache(const StorageReadInterface& graph) {
    std::lock_guard<std::mutex> lock(mutex_);
    cache_.erase(KeyOf(graph));
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
  std::unordered_map<const void*, CachedData> cache_;
};

// ============================================================================
// Checkpoint file helpers: save/load schema_graph and DataGraphMeta
// ============================================================================

static constexpr char SGCH_MAGIC[] = "SGCH";
static constexpr int32_t SGCH_VERSION = 1;

inline bool SaveSchemaGraph(
    const std::unordered_map<
        label_t, std::unordered_map<label_t, std::vector<label_t>>>& sg,
    const std::string& filepath);

bool LoadSchemaGraph(
    std::unordered_map<label_t,
                       std::unordered_map<label_t, std::vector<label_t>>>& sg,
    const std::string& filepath);

/**
 * @brief Save all cached graph initialization data to checkpoint files.
 * Files written: {checkpoint_dir}/data_graph_meta.bin,
 * {checkpoint_dir}/schema_graph.bin
 */
inline bool SaveGraphCheckpoint(const StorageReadInterface& graph,
                                const std::string& checkpoint_dir);

/**
 * @brief Try to load graph initialization data from checkpoint files.
 * @return true if checkpoint was loaded successfully, false if not available.
 */
bool LoadGraphCheckpoint(const StorageReadInterface& graph,
                         const std::string& checkpoint_dir);

// ============================================================================
// Graph initialization: builds label mappings and runs DataGraphMeta
// preprocessing. Shared by the INITIALIZE CALL function and by match() on
// first use, so either path ends up with the same cached state.
// ============================================================================

/**
 * @brief Build the per-graph caches that SAMPLED_PATTERN_MATCH relies on.
 * @param graph           Graph storage to preprocess.
 * @param verbose         Emit progress logs when true.
 * @param checkpoint_dir  If non-empty, try loading the cache from this
 *                        directory before falling back to full preprocessing.
 * @return true on success.
 */
bool DoGraphInitialization(const StorageReadInterface& graph,
                           bool verbose = true,
                           const std::string& checkpoint_dir = "");

enum class PatternOutputKind {
  kVertex,
  kEdge,
};

struct PatternOutputColumn {
  PatternOutputKind kind = PatternOutputKind::kVertex;
  int index = -1;
  std::string alias;
  // Label name of the pattern element (vertex label or edge type). Empty when
  // the pattern is anonymous/unlabeled, in which case the output column falls
  // back to a plain typed variable without property schema.
  std::string label;
  // For kEdge columns only: the pattern-vertex indices of the endpoints, so the
  // binder can wire the RelExpression to the corresponding NodeExpressions.
  int edge_src = -1;
  int edge_dst = -1;

  common::DataTypeId type_id() const {
    return kind == PatternOutputKind::kVertex ? common::DataTypeId::kVertex
                                              : common::DataTypeId::kEdge;
  }
};

struct PatternOutputEdgeInfo {
  int src = -1;
  int dst = -1;
  std::string alias;
  std::string label;
};

struct PatternOrderBySpec {
  PatternOutputKind kind = PatternOutputKind::kVertex;
  int index = -1;
  std::string variable;
  std::string property;
  bool ascending = true;
};

struct PatternExecutionModifiers {
  std::vector<PatternOrderBySpec> order_by;
  uint64_t skip = 0;
  uint64_t limit = std::numeric_limits<uint64_t>::max();
  bool has_skip = false;
  bool has_limit = false;

  bool HasOrderBy() const { return !order_by.empty(); }
  bool HasSkipOrLimit() const { return has_skip || has_limit; }
};

inline bool ReadJsonId(const rapidjson::Value& obj, const char* key, int* out) {
  if (!obj.HasMember(key))
    return false;
  const auto& v = obj[key];
  if (v.IsInt()) {
    *out = v.GetInt();
    return true;
  }
  if (v.IsString()) {
    try {
      *out = std::stoi(v.GetString());
      return true;
    } catch (...) { return false; }
  }
  return false;
}

inline std::string ReadPatternAlias(const rapidjson::Value& obj,
                                    const std::string& fallback_prefix,
                                    int fallback_id) {
  for (const char* key : {"alias", "variable", "name"}) {
    if (obj.HasMember(key) && obj[key].IsString() &&
        obj[key].GetStringLength() > 0) {
      return obj[key].GetString();
    }
  }
  return fallback_prefix + std::to_string(fallback_id);
}

inline std::string MakeUniquePatternAlias(
    const std::string& alias, std::unordered_map<std::string, int>* seen) {
  auto [it, inserted] = seen->emplace(alias, 0);
  if (inserted)
    return alias;
  int next = ++it->second;
  return alias + "_" + std::to_string(next);
}

inline bool ReadJsonUint64(const rapidjson::Value& value, uint64_t* out);

std::optional<PatternExecutionModifiers> ParsePatternExecutionModifiers(
    const rapidjson::Document& doc,
    const std::vector<std::string>& vertex_aliases,
    const std::vector<PatternOutputEdgeInfo>& edge_aliases,
    const char* log_tag);

std::vector<PatternOutputColumn> BuildPatternOutputColumnsFromAliases(
    const std::vector<std::string>& vertex_aliases,
    const std::vector<std::string>& vertex_labels,
    const std::vector<PatternOutputEdgeInfo>& edges);

std::optional<std::vector<PatternOutputColumn>>
ParsePatternOutputColumnsJsonFile(const std::string& pattern_json_file,
                                  const char* log_tag);

// ============================================================================
// SampledSubgraphMatcher: Subgraph matching using FaSTest algorithm
// ============================================================================

class SampledSubgraphMatcher {
 public:
  // Tag struct used to construct the matcher with an in-memory JSON pattern
  // (so callers that already have the JSON in hand can skip the temp-file
  // round-trip that file-based callers do).
  struct PatternJsonText {
    std::string json;
  };

  SampledSubgraphMatcher(const StorageReadInterface& graph,
                         const std::string& pattern_file, long long sample_size)
      : graph_(graph), pattern_file_(pattern_file), sample_size_(sample_size) {}

  SampledSubgraphMatcher(const StorageReadInterface& graph,
                         PatternJsonText pattern, long long sample_size)
      : graph_(graph),
        pattern_json_(std::move(pattern.json)),
        sample_size_(sample_size) {}

  /**
   * @brief Execute subgraph matching
   * @return Estimated count of embeddings
   */
  double match();

  // Get sampled results after matching
  const std::vector<int>& GetSampledResults() const { return sampled_results_; }
  double GetEstimatedCount() const { return estimated_count_; }
  int GetPatternVertexCount() const {
    return pattern_graph_ ? pattern_graph_->GetNumVertices() : 0;
  }
  int GetPatternEdgeCount() const {
    return pattern_graph_ ? pattern_graph_->GetNumEdges() : 0;
  }
  label_t GetPatternVertexLabel(int pattern_vertex_idx) const {
    if (!pattern_graph_ || pattern_vertex_idx < 0 ||
        pattern_vertex_idx >= pattern_graph_->GetNumVertices()) {
      return 0;
    }
    return pattern_graph_->vertex_label[pattern_vertex_idx];
  }
  label_t GetPatternEdgeLabel(int pattern_edge_idx) const {
    if (!pattern_graph_ || pattern_edge_idx < 0 ||
        pattern_edge_idx >= pattern_graph_->GetNumEdges()) {
      return 0;
    }
    return pattern_graph_->edge_label[pattern_edge_idx];
  }
  std::string GetPatternVertexLabelName(int pattern_vertex_idx) const {
    label_t label = GetPatternVertexLabel(pattern_vertex_idx);
    const auto& schema = graph_.schema();
    if (schema.is_vertex_label_valid(label)) {
      return schema.get_vertex_label_name(label);
    }
    return std::to_string(label);
  }
  std::string GetPatternEdgeLabelName(int pattern_edge_idx) const {
    label_t label = GetPatternEdgeLabel(pattern_edge_idx);
    const auto& schema = graph_.schema();
    if (schema.is_edge_label_valid(label)) {
      return schema.get_edge_label_name(label);
    }
    return std::to_string(label);
  }
  const std::vector<std::string>& GetVertexRequiredProps(
      int pattern_vertex_idx) const {
    static const std::vector<std::string> kEmpty;
    if (pattern_vertex_idx < 0 ||
        pattern_vertex_idx >= static_cast<int>(vertex_required_props_.size())) {
      return kEmpty;
    }
    return vertex_required_props_[pattern_vertex_idx];
  }
  const std::vector<std::string>& GetEdgeRequiredProps(
      int pattern_edge_idx) const {
    static const std::vector<std::string> kEmpty;
    if (pattern_edge_idx < 0 ||
        pattern_edge_idx >= static_cast<int>(edge_required_props_.size())) {
      return kEmpty;
    }
    return edge_required_props_[pattern_edge_idx];
  }

  // Get pattern edge list: [(src_pattern_idx, dst_pattern_idx, edge_label),
  // ...]
  std::vector<std::tuple<int, int, label_t>> GetPatternEdgeList() const {
    std::vector<std::tuple<int, int, label_t>> result;
    if (!pattern_graph_)
      return result;
    for (int i = 0; i < pattern_graph_->GetNumEdges(); i++) {
      auto& [src, dst] = pattern_graph_->edge_list[i];
      label_t label = pattern_graph_->edge_label[i];
      result.emplace_back(src, dst, label);
    }
    return result;
  }

  const std::vector<PatternOutputColumn>& GetPatternOutputColumns() const {
    return output_columns_;
  }

  const PatternExecutionModifiers& GetPatternExecutionModifiers() const {
    return modifiers_;
  }

  std::string GetPatternVertexAlias(int pattern_vertex_idx) const {
    if (pattern_vertex_idx < 0 ||
        pattern_vertex_idx >= static_cast<int>(vertex_aliases_.size())) {
      return "v" + std::to_string(pattern_vertex_idx);
    }
    return vertex_aliases_[pattern_vertex_idx];
  }

  std::string GetPatternEdgeAlias(int pattern_edge_idx) const {
    if (pattern_edge_idx < 0 ||
        pattern_edge_idx >= static_cast<int>(edge_aliases_.size())) {
      return "e" + std::to_string(pattern_edge_idx);
    }
    return edge_aliases_[pattern_edge_idx].alias;
  }

  // Get sampled edge keys for a specific sample and pattern edge
  // Returns edge key in format "src_global:dst_global:edge_label"
  std::string GetSampledEdgeKey(int sample_idx, int pattern_edge_idx) const {
    if (!pattern_graph_ || sample_idx < 0 || pattern_edge_idx < 0)
      return "";
    int patternVertexCount = pattern_graph_->GetNumVertices();
    int patternEdgeCount = pattern_graph_->GetNumEdges();
    if (pattern_edge_idx >= patternEdgeCount)
      return "";
    if (sample_idx * patternVertexCount >= (int) sampled_results_.size())
      return "";

    auto& [src_pattern, dst_pattern] =
        pattern_graph_->edge_list[pattern_edge_idx];
    label_t edge_label = pattern_graph_->edge_label[pattern_edge_idx];

    int src_global =
        sampled_results_[sample_idx * patternVertexCount + src_pattern];
    int dst_global =
        sampled_results_[sample_idx * patternVertexCount + dst_pattern];

    return std::to_string(src_global) + ":" + std::to_string(dst_global) + ":" +
           std::to_string(edge_label);
  }

 private:
  // NOTE: BuildLabelMappings logic has been moved to DoGraphInitialization()
  // for better code reuse and explicit initialization via CALL Initialize().

  // Thin wrapper: read the file off disk and delegate to the in-memory
  // text parser. The legacy SAMPLED_PATTERN_MATCH JSON path uses this.
  std::unique_ptr<GraphLib::SubgraphMatching::PatternGraph>
  CreatePatternFromJsonFile(const std::string& pattern_file) {
    std::ifstream fin(pattern_file);
    if (!fin.is_open()) {
      LOG(WARNING) << "[SAMPLED_PATTERN_MATCH] Cannot open pattern file: "
                   << pattern_file;
      return nullptr;
    }
    std::stringstream buffer;
    buffer << fin.rdbuf();
    return CreatePatternFromJsonText(buffer.str(), pattern_file);
  }

  // Core pattern loader. Takes the JSON text directly so Cypher callers can
  // skip the write-tempfile / re-read / re-parse round-trip. The
  // `origin_label` is purely for log lines (file path or "<inline>").
  std::unique_ptr<GraphLib::SubgraphMatching::PatternGraph>
  CreatePatternFromJsonText(const std::string& json_content,
                            const std::string& origin_label);

  // Member variables
  const StorageReadInterface& graph_;
  // Exactly one of these is non-empty: pattern_file_ for the legacy JSON
  // path that reads a file off disk, pattern_json_ for callers that
  // already have the JSON text in memory (e.g. the Cypher translator).
  std::string pattern_file_;
  std::string pattern_json_;
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
  std::vector<std::string> vertex_aliases_;
  std::vector<std::string> vertex_labels_;
  std::vector<PatternOutputEdgeInfo> edge_aliases_;
  std::vector<PatternOutputColumn> output_columns_;
  PatternExecutionModifiers modifiers_;

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
    case DataTypeId::kInt8:
      return "int8";
    case DataTypeId::kInt16:
      return "int16";
    case DataTypeId::kInt32:
      return "int32";
    case DataTypeId::kUInt32:
      return "uint32";
    case DataTypeId::kInt64:
      return "int64";
    case DataTypeId::kUInt64:
      return "uint64";
    case DataTypeId::kFloat:
      return "float";
    case DataTypeId::kDouble:
      return "double";
    case DataTypeId::kBoolean:
      return "boolean";
    case DataTypeId::kVarchar:
      return "string";
    default:
      return "string";
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
   *       {"label": "Publication", "properties": {"year": "int64", "title":
   * "string"}}
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
  std::string FetchAndWriteProperties();
};

// ============================================================================
// InitializeGraphFunction: CALL INITIALIZE([checkpoint_dir]) — populates the
// GraphDataCache, optionally restoring from a previously saved checkpoint.
// ============================================================================

struct InitializeGraphInput : public CallFuncInputBase {
  std::string checkpoint_dir;
  InitializeGraphInput() = default;
  explicit InitializeGraphInput(std::string dir)
      : checkpoint_dir(std::move(dir)) {}
  ~InitializeGraphInput() override = default;
};

struct InitializeGraphFunction {
  static constexpr const char* name = "INITIALIZE";

  static function_set getFunctionSet();
};

// ============================================================================
// SaveSampledmatchCheckpointFunction: persists the GraphDataCache contents so
// a later INITIALIZE call can skip preprocessing.
// CALL SAVE_SAMPLEDMATCH_CHECKPOINT('/path/to/checkpoint') RETURN *;
// ============================================================================

struct SaveSampledmatchCheckpointInput : public CallFuncInputBase {
  std::string checkpoint_dir;
  explicit SaveSampledmatchCheckpointInput(std::string dir)
      : checkpoint_dir(std::move(dir)) {}
  ~SaveSampledmatchCheckpointInput() override = default;
};

struct SaveSampledmatchCheckpointFunction {
  static constexpr const char* name = "SAVE_SAMPLEDMATCH_CHECKPOINT";

  static function_set getFunctionSet();
};

// ============================================================================
// ExactPatternMatcher: DAF-backed exact pattern matching. DAF works on a
// compact text graph format, so NeuG's cached DataGraphMeta is materialized
// into temporary DAF data/query graph files per call. The final result is
// validated against NeuG's directed edge index and property storage.
// ============================================================================

struct ExactPatternSpec {
  struct VertexSpec {
    int id = -1;
    label_t label = 0;
    std::string label_name;
    std::string alias;
    std::vector<PropCons> constraints;
    std::vector<std::string> required_props;
  };

  struct EdgeSpec {
    int src = -1;
    int dst = -1;
    label_t label = 0;
    std::string label_name;
    std::string alias;
    std::vector<PropCons> constraints;
    std::vector<std::string> required_props;
  };

  std::vector<VertexSpec> vertices;
  std::vector<EdgeSpec> edges;
  std::vector<PatternOutputColumn> output_columns;
  PatternExecutionModifiers modifiers;
};

inline std::vector<std::string> ParseRequiredProps(
    const rapidjson::Value& obj) {
  std::vector<std::string> props;
  if (!obj.HasMember("required_props") || !obj["required_props"].IsArray()) {
    return props;
  }
  for (const auto& item : obj["required_props"].GetArray()) {
    if (item.IsString())
      props.emplace_back(item.GetString());
  }
  return props;
}

std::optional<ExactPatternSpec> ParseExactPatternJsonFile(
    const std::string& pattern_json_file, const Schema& schema);

inline bool IsNumericValue(const execution::Value& value, double* out) {
  if (value.IsNull())
    return false;
  try {
    switch (value.type().id()) {
    case DataTypeId::kInt8:
    case DataTypeId::kInt16:
    case DataTypeId::kUInt8:
    case DataTypeId::kUInt16:
      *out = std::stod(value.to_string());
      return true;
    case DataTypeId::kInt32:
      *out = static_cast<double>(value.GetValue<int32_t>());
      return true;
    case DataTypeId::kInt64:
      *out = static_cast<double>(value.GetValue<int64_t>());
      return true;
    case DataTypeId::kUInt32:
      *out = static_cast<double>(value.GetValue<uint32_t>());
      return true;
    case DataTypeId::kUInt64:
      *out = static_cast<double>(value.GetValue<uint64_t>());
      return true;
    case DataTypeId::kFloat:
      *out = static_cast<double>(value.GetValue<float>());
      return true;
    case DataTypeId::kDouble:
      *out = value.GetValue<double>();
      return true;
    default:
      return false;
    }
  } catch (...) { return false; }
}

bool ComparePropertyValue(const execution::Value& actual, CompType op,
                          const execution::Value& expected);

inline bool CheckVertexConstraints(const StorageReadInterface& graph,
                                   const DataGraphMeta& data_meta,
                                   int global_id,
                                   const ExactPatternSpec::VertexSpec& spec) {
  if (spec.constraints.empty())
    return true;
  auto [label, local_vid] = data_meta.ToLocalId(global_id);
  if (label != spec.label)
    return false;
  const auto prop_names = graph.schema().get_vertex_property_names(label);
  for (const auto& constraint : spec.constraints) {
    auto it =
        std::find(prop_names.begin(), prop_names.end(), constraint._prop_name);
    if (it == prop_names.end())
      return false;
    int prop_idx = static_cast<int>(std::distance(prop_names.begin(), it));
    execution::Value actual =
        graph.GetVertexProperty(label, local_vid, prop_idx);
    if (!ComparePropertyValue(actual, constraint._comp_type,
                              constraint._value)) {
      return false;
    }
  }
  return true;
}

inline std::optional<execution::Value> GetDirectedEdgeProperty(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    int src_global, int dst_global, label_t edge_label, int prop_idx) {
  auto [src_label, src_vid] = data_meta.ToLocalId(src_global);
  auto [dst_label, dst_vid] = data_meta.ToLocalId(dst_global);
  try {
    EdgeDataAccessor accessor =
        graph.GetEdgeDataAccessor(src_label, dst_label, edge_label, prop_idx);
    CsrView view =
        graph.GetGenericOutgoingGraphView(src_label, dst_label, edge_label);
    NbrList edges = view.get_edges(src_vid);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      if (*it == dst_vid)
        return accessor.get_data(it);
    }
  } catch (...) { return std::nullopt; }
  return std::nullopt;
}

inline std::optional<execution::Value> GetVertexPropertyByName(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    int global_id, label_t expected_label, const std::string& prop_name) {
  auto [label, local_vid] = data_meta.ToLocalId(global_id);
  if (label != expected_label) {
    return std::nullopt;
  }
  const auto prop_names = graph.schema().get_vertex_property_names(label);
  auto it = std::find(prop_names.begin(), prop_names.end(), prop_name);
  if (it == prop_names.end()) {
    return std::nullopt;
  }
  int prop_idx = static_cast<int>(std::distance(prop_names.begin(), it));
  try {
    return graph.GetVertexProperty(label, local_vid, prop_idx);
  } catch (...) { return std::nullopt; }
}

inline std::optional<execution::Value> GetEdgePropertyByName(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    int src_global, int dst_global, label_t edge_label,
    const std::string& prop_name) {
  auto [src_label, src_vid] = data_meta.ToLocalId(src_global);
  auto [dst_label, dst_vid] = data_meta.ToLocalId(dst_global);
  (void) src_vid;
  (void) dst_vid;
  const auto prop_names =
      graph.schema().get_edge_property_names(src_label, dst_label, edge_label);
  auto it = std::find(prop_names.begin(), prop_names.end(), prop_name);
  if (it == prop_names.end()) {
    return std::nullopt;
  }
  int prop_idx = static_cast<int>(std::distance(prop_names.begin(), it));
  return GetDirectedEdgeProperty(graph, data_meta, src_global, dst_global,
                                 edge_label, prop_idx);
}

int CompareExecutionValues(const std::optional<execution::Value>& lhs,
                           const std::optional<execution::Value>& rhs);

template <class Rows>
inline void ApplyPatternWindow(const PatternExecutionModifiers& modifiers,
                               Rows* rows) {
  if (rows == nullptr || !modifiers.HasSkipOrLimit()) {
    return;
  }
  if (modifiers.has_skip && modifiers.skip >= rows->size()) {
    rows->clear();
    return;
  }
  if (modifiers.has_skip && modifiers.skip > 0) {
    rows->erase(rows->begin(),
                rows->begin() + static_cast<typename Rows::difference_type>(
                                    modifiers.skip));
  }
  if (modifiers.has_limit && modifiers.limit < rows->size()) {
    rows->resize(static_cast<typename Rows::size_type>(modifiers.limit));
  }
}

inline bool CheckEdgeConstraints(const StorageReadInterface& graph,
                                 const DataGraphMeta& data_meta, int src_global,
                                 int dst_global,
                                 const ExactPatternSpec::EdgeSpec& spec) {
  if (spec.constraints.empty())
    return true;
  auto [src_label, src_vid] = data_meta.ToLocalId(src_global);
  auto [dst_label, dst_vid] = data_meta.ToLocalId(dst_global);
  (void) src_vid;
  (void) dst_vid;
  const auto prop_names =
      graph.schema().get_edge_property_names(src_label, dst_label, spec.label);
  for (const auto& constraint : spec.constraints) {
    auto it =
        std::find(prop_names.begin(), prop_names.end(), constraint._prop_name);
    if (it == prop_names.end())
      return false;
    int prop_idx = static_cast<int>(std::distance(prop_names.begin(), it));
    auto actual = GetDirectedEdgeProperty(graph, data_meta, src_global,
                                          dst_global, spec.label, prop_idx);
    if (!actual.has_value() ||
        !ComparePropertyValue(*actual, constraint._comp_type,
                              constraint._value)) {
      return false;
    }
  }
  return true;
}

inline bool ValidateExactEmbedding(const StorageReadInterface& graph,
                                   const DataGraphMeta& data_meta,
                                   const ExactPatternSpec& spec,
                                   const std::vector<daf::Vertex>& mapping) {
  if (mapping.size() < spec.vertices.size())
    return false;
  for (const auto& vertex : spec.vertices) {
    int data_v = static_cast<int>(mapping[vertex.id]);
    if (data_meta.GetVertexLabel(data_v) != vertex.label)
      return false;
    if (!CheckVertexConstraints(graph, data_meta, data_v, vertex)) {
      return false;
    }
  }
  for (const auto& edge : spec.edges) {
    int src = static_cast<int>(mapping[edge.src]);
    int dst = static_cast<int>(mapping[edge.dst]);
    if (data_meta.GetEdgeIndex(src, dst, edge.label) == -1) {
      return false;
    }
    if (!CheckEdgeConstraints(graph, data_meta, src, dst, edge)) {
      return false;
    }
  }
  return true;
}

inline std::optional<execution::Value> ResolveExactOrderValue(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const ExactPatternSpec& spec, const std::vector<daf::Vertex>& match,
    const PatternOrderBySpec& order_by);

void ApplyExactPatternModifiers(const StorageReadInterface& graph,
                                const DataGraphMeta& data_meta,
                                const ExactPatternSpec& spec,
                                std::vector<std::vector<daf::Vertex>>* matches);

std::vector<std::vector<daf::Vertex>> EnumerateExactMatchesWithNeug(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const ExactPatternSpec& spec, uint64_t limit);

struct DafGraphStorageAdapter final : public gbi::GraphStorage {
  explicit DafGraphStorageAdapter(const DataGraphMeta& data_meta)
      : data_meta_(data_meta) {}

  bool HasEdge(gbi::EIdType source, gbi::EIdType target) override {
    return data_meta_.GetEdgeIndex(static_cast<int>(source),
                                   static_cast<int>(target)) != -1;
  }

  bool HasEdge(gbi::EIdType source, gbi::EIdType target, int label) override {
    return data_meta_.GetEdgeIndex(static_cast<int>(source),
                                   static_cast<int>(target), label) != -1;
  }

  const DataGraphMeta& data_meta_;
};

std::string WriteDafDataGraphFile(
    const DataGraphMeta& data_meta,
    std::unordered_map<std::string, int>* label_mapping);

inline std::string WriteDafQueryGraphFile(const ExactPatternSpec& spec) {
  std::string path =
      GenerateTempFilePath("pattern_matching_daf_query", ".graph");
  std::ofstream ofs(path);
  if (!ofs.is_open()) {
    LOG(ERROR) << "[PATTERN_MATCH] Failed to write DAF query graph: " << path;
    return "";
  }
  ofs << "t " << spec.vertices.size() << " " << spec.edges.size() << "\n";
  for (const auto& vertex : spec.vertices) {
    ofs << "v " << vertex.id << " " << static_cast<int>(vertex.label) << "\n";
  }
  for (const auto& edge : spec.edges) {
    ofs << "e " << edge.src << " " << edge.dst << " "
        << static_cast<int>(edge.label) << "\n";
  }
  return path;
}

inline void PopulateDafDirectedEdgeChecks(daf::QueryGraph* query,
                                          const ExactPatternSpec& spec) {
  query->has_out_edge_pairs.assign(spec.vertices.size(), {});
  query->has_in_edge_pairs.assign(spec.vertices.size(), {});
  for (const auto& edge : spec.edges) {
    query->has_out_edge_pairs[edge.src].push_back({edge.dst, edge.label});
    query->has_in_edge_pairs[edge.dst].push_back({edge.src, edge.label});
  }
}

std::string FetchAndWriteExactProperties(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const ExactPatternSpec& spec,
    const std::vector<std::vector<daf::Vertex>>& matches);

inline bool FindDirectedEdgeDataPtr(const StorageReadInterface& graph,
                                    const DataGraphMeta& data_meta,
                                    int src_global, int dst_global,
                                    label_t edge_label, const void** data_ptr) {
  if (data_ptr != nullptr) {
    *data_ptr = nullptr;
  }
  auto [src_label, src_vid] = data_meta.ToLocalId(src_global);
  auto [dst_label, dst_vid] = data_meta.ToLocalId(dst_global);
  if (src_label == static_cast<label_t>(255) ||
      dst_label == static_cast<label_t>(255)) {
    return false;
  }
  try {
    CsrView view =
        graph.GetGenericOutgoingGraphView(src_label, dst_label, edge_label);
    NbrList edges = view.get_edges(src_vid);
    for (auto it = edges.begin(); it != edges.end(); ++it) {
      if (*it == dst_vid) {
        if (data_ptr != nullptr) {
          *data_ptr = it.get_data_ptr();
        }
        return true;
      }
    }
  } catch (...) { return false; }
  return false;
}

struct NativePatternColumnBuilder {
  PatternOutputColumn column;
  std::unique_ptr<execution::MSVertexColumnBuilder> vertex_builder;
  std::unique_ptr<execution::MSEdgeColumnBuilder> edge_builder;
};

inline execution::Context MakeNativePatternContext(
    std::vector<NativePatternColumnBuilder>& builders) {
  std::vector<std::shared_ptr<execution::IContextColumn>> columns;
  columns.reserve(builders.size());
  for (auto& builder : builders) {
    if (builder.column.kind == PatternOutputKind::kVertex) {
      columns.push_back(builder.vertex_builder->finish());
    } else {
      columns.push_back(builder.edge_builder->finish());
    }
  }
  return MakeSingleChunkContext(std::move(columns));
}

execution::Context BuildExactNativePatternContext(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const ExactPatternSpec& spec,
    const std::vector<std::vector<daf::Vertex>>& matches);

inline std::optional<execution::Value> ResolveSampledOrderValue(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const SampledSubgraphMatcher& matcher, const std::vector<int>& results,
    int pattern_vertex_count,
    const std::vector<std::tuple<int, int, label_t>>& pattern_edges,
    int sample_idx, const PatternOrderBySpec& order_by);

execution::Context BuildSampledNativePatternContext(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const SampledSubgraphMatcher& matcher,
    const std::vector<int>& sampled_results, int pattern_vertex_count,
    int sample_count);

std::unique_ptr<TableFuncBindData> BindPatternNativeOutputColumns(
    const TableFuncBindInput* input, const char* log_tag);

struct PatternMatchInput : public CallFuncInputBase {
  std::string patternFilePath;
  long long limit;
  PatternMatchInput(std::string path, long long limit)
      : patternFilePath(std::move(path)), limit(limit) {}
  ~PatternMatchInput() override = default;
};

execution::Context ExecutePatternMatchPipeline(const PatternMatchInput& input,
                                               IStorageInterface& graph);

// PatternMatchFunction is the single unified CALL PATTERN_MATCH(...) entry; it
// is defined after the sampled-match helpers below because its sampled overload
// reuses ExecuteSampledMatchPipeline / SampledMatchInput.

// ============================================================================
// Sampled subgraph matching helpers, reused by the sampled overload of the
// unified PATTERN_MATCH. The first argument accepts the same Cypher/JSON
// text-or-file forms as the exact path and is normalized to a JSON file before
// FaSTest reads it.
// ============================================================================

struct SampledMatchInput : public CallFuncInputBase {
  std::string patternFilePath;  // legacy JSON-file path; empty for text flow
  // In-memory JSON pattern, populated by the Cypher text flow.
  std::string patternJsonText;
  long long sampleSize;
  SampledMatchInput(std::string path, long long sample_size)
      : patternFilePath(std::move(path)), sampleSize(sample_size) {}
  // Tag-dispatched ctor for the in-memory variant; keeps the call site
  // explicit about which pattern source it is using.
  struct InlineJsonTag {};
  SampledMatchInput(InlineJsonTag, std::string json, long long sample_size)
      : patternJsonText(std::move(json)), sampleSize(sample_size) {}
  ~SampledMatchInput() override = default;
};

// Runs the FaSTest sampler on a fully prepared pattern file. Factored out of
// SampledPatternMatchFunction can reuse it after normalizing its input to a
// temporary JSON pattern file.
execution::Context ExecuteSampledMatchPipeline(
    const SampledMatchInput& matchInput, IStorageInterface& graph);

// ============================================================================
// PatternMatchFunction: the single unified subgraph-matching entry point.
//
//   CALL PATTERN_MATCH(cypher_or_file)
//       -> exact matching (DAF), enumerates ALL matches.
//
//   CALL PATTERN_MATCH(cypher_or_file, size, is_sampled)
//       size       : positive integer (>= 1).
//       is_sampled : boolean flag selecting the algorithm.
//         * is_sampled = false  -> EXACT matching (DAF) that early-terminates
//                                  after the first `size` matches are found.
//         * is_sampled = true   -> SAMPLED matching (FaSTest) with sample
//                                  size = `size`.
//
// The first argument accepts inline Cypher pattern text, a Cypher pattern file,
// inline JSON, or a JSON pattern file in either mode.
// ============================================================================

struct PatternMatchFunction {
  static constexpr const char* name = "PATTERN_MATCH";

  static function_set getFunctionSet();
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

  GetVertexPropertyInput(std::vector<int64_t> ids, std::string label,
                         std::vector<std::string> props)
      : vertex_ids(std::move(ids)),
        vertex_label(std::move(label)),
        property_names(std::move(props)) {}
  ~GetVertexPropertyInput() override = default;
};

struct GetVertexPropertyFunction {
  static constexpr const char* name = "GET_VERTEX_PROPERTY";

  static function_set getFunctionSet();
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

  GetEdgePropertyInput(std::vector<std::string> keys, std::string label,
                       std::vector<std::string> props)
      : edge_keys(std::move(keys)),
        edge_label(std::move(label)),
        property_names(std::move(props)) {}
  ~GetEdgePropertyInput() override = default;
};

struct GetEdgePropertyFunction {
  static constexpr const char* name = "GET_EDGE_PROPERTY";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug
