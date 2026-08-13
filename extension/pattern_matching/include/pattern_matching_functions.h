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

#include "neug/common/columns/edge_columns.h"
#include "neug/common/columns/value_columns.h"
#include "neug/common/columns/vertex_columns.h"
#include "neug/common/types/data_chunk.h"
#include "neug/common/types/i_context_column.h"
#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/expression/node_expression.h"
#include "neug/compiler/binder/expression/rel_expression.h"
#include "neug/compiler/common/types/types.h"
#include "neug/compiler/function/function.h"
#include "neug/compiler/function/neug_call_function.h"
#include "neug/compiler/function/table/bind_data.h"
#include "neug/compiler/function/table/bind_input.h"
#include "neug/execution/common/context.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"
#include "pattern_matching_value.h"

#include "fastest_lib/src/SubgraphCounting/cardinality_estimation.h"
#include "fastest_lib/src/SubgraphMatching/pattern_graph.h"
#include "pattern_cypher_translator.h"
#include "pattern_matching_data_graph_meta.h"

namespace neug {
namespace pattern_matching {

// Data-graph vertex id used in an exact-match embedding.
using MatchVertex = uint32_t;
inline constexpr MatchVertex kInvalidMatchVertex =
    std::numeric_limits<MatchVertex>::max();

execution::Context make_single_chunk_context(
    std::vector<std::shared_ptr<neug::IContextColumn>> columns);

// ============================================================================
// Helper functions for parsing pattern JSON
// ============================================================================

// Helper function to parse comparison operator. Unknown operator strings
// (e.g. "and", "or", "like") fall back to COMP_EQUAL — flag the fallback so
// users notice instead of silently getting equality semantics. Dedup by op
// string so a typo'd operator only warns once per process.
CompType parse_operator(const std::string& op_in);

// Helper function to create Value from rapidjson
Value create_value_from_rapidjson(const rapidjson::Value& val);

// Helper function to parse constraints from rapidjson
std::vector<PropCons> parse_constraints(
    const rapidjson::Value& constraints_json);

// ============================================================================
// Temp file path helper: builds a collision-resistant path under
// <temp_dir>/neug_sample. Returns "" if the directory can't be created.
// Filename = <prefix>_<ms>_<counter>_<rand-hex><extension>. Millisecond
// timestamp alone collides under burst load; the per-process atomic counter
// and 64-bit random suffix keep concurrent calls distinct.
// ============================================================================
std::string generate_temp_file_path(const std::string& prefix,
                                    const std::string& extension);

std::string generate_output_file_path(const std::string& prefix);

std::string trim_copy(std::string_view input);

bool read_text_file(const std::string& path, std::string* out);

bool write_text_file(const std::string& path, const std::string& text);

bool looks_like_json_pattern(std::string_view text);

std::string write_pattern_json_temp_file(const std::string& pattern_json);

// Normalize a user pattern argument to a JSON file path. The argument may be:
//   * path to a JSON pattern file
//   * path to a Cypher pattern file
//   * inline JSON pattern text
//   * inline Cypher pattern text
std::string normalize_pattern_input_to_json_file(const std::string& arg,
                                                 const char* log_tag);

// ============================================================================
// GraphDataCache: catalog-owned cache for one database. Every registered
// function captures the same instance, so repeated LOADs in the same catalog
// preserve the warm cache and destroying the catalog releases it.
// ============================================================================

class GraphDataCache {
 public:
  struct CachedData {
    std::unique_ptr<DataGraphMeta> data_meta =
        std::make_unique<DataGraphMeta>();
    std::shared_ptr<std::unordered_map<
        label_t, std::unordered_map<label_t, std::vector<label_t>>>>
        schema_graph = std::make_shared<std::unordered_map<
            label_t, std::unordered_map<label_t, std::vector<label_t>>>>();
    // Guards the first initialization and checkpoint I/O. Once preprocessed
    // becomes true, data_meta and schema_graph are immutable.
    mutable std::mutex mutex;
    bool preprocessed = false;
  };

  using Handle = std::shared_ptr<CachedData>;

  GraphDataCache() = default;
  ~GraphDataCache() = default;

  Handle get() const { return cached_data_; }

 private:
  GraphDataCache(const GraphDataCache&) = delete;
  GraphDataCache& operator=(const GraphDataCache&) = delete;

  Handle cached_data_ = std::make_shared<CachedData>();
};

// ============================================================================
// Checkpoint file helpers: save/load schema_graph and DataGraphMeta
// ============================================================================

static constexpr char SGCH_MAGIC[] = "SGCH";
static constexpr int32_t SGCH_VERSION = 1;

inline bool save_schema_graph(
    const std::unordered_map<
        label_t, std::unordered_map<label_t, std::vector<label_t>>>& sg,
    const std::string& filepath);

bool load_schema_graph(
    std::unordered_map<label_t,
                       std::unordered_map<label_t, std::vector<label_t>>>& sg,
    const std::string& filepath);

/**
 * @brief Save all cached graph initialization data to checkpoint files.
 * Files written: {checkpoint_dir}/data_graph_meta.bin,
 * {checkpoint_dir}/schema_graph.bin
 */
inline bool save_graph_checkpoint(GraphDataCache& cache,
                                  const std::string& checkpoint_dir);

// ============================================================================
// Graph initialization: builds label mappings and runs DataGraphMeta
// preprocessing. Shared by the INITIALIZE CALL function and by match() on
// first use, so either path ends up with the same cached state.
// ============================================================================

/**
 * @brief Build the per-graph caches that SAMPLED_PATTERN_MATCH relies on.
 * @param graph           Graph storage to Preprocess.
 * @param verbose         Emit progress logs when true.
 * @param checkpoint_dir  If non-empty, try loading the cache from this
 *                        directory before falling back to full preprocessing.
 * @return true on success.
 */
bool do_graph_initialization(GraphDataCache& cache,
                             const StorageReadInterface& graph,
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

  bool has_order_by() const { return !order_by.empty(); }
  bool has_skip_or_limit() const { return has_skip || has_limit; }
};

bool read_json_id(const rapidjson::Value& obj, const char* key, int* out);

std::string read_pattern_alias(const rapidjson::Value& obj,
                               const std::string& fallback_prefix,
                               int fallback_id);

std::string make_unique_pattern_alias(
    const std::string& alias, std::unordered_map<std::string, int>* seen);

inline bool read_json_uint64(const rapidjson::Value& value, uint64_t* out);

std::optional<PatternExecutionModifiers> parse_pattern_execution_modifiers(
    const rapidjson::Document& doc,
    const std::vector<std::string>& vertex_aliases,
    const std::vector<PatternOutputEdgeInfo>& edge_aliases,
    const char* log_tag);

std::vector<PatternOutputColumn> build_pattern_output_columns_from_aliases(
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
  SampledSubgraphMatcher(GraphDataCache& cache,
                         const StorageReadInterface& graph,
                         const std::string& pattern_file, long long sample_size)
      : cache_(cache),
        graph_(graph),
        pattern_file_(pattern_file),
        sample_size_(sample_size) {}

  /**
   * @brief Execute subgraph matching
   * @return Estimated count of embeddings
   */
  double match();

  // Get sampled results after matching
  const std::vector<int>& get_sampled_results() const {
    return sampled_results_;
  }
  int get_pattern_vertex_count() const {
    return pattern_graph_ ? pattern_graph_->GetNumVertices() : 0;
  }
  int get_pattern_edge_count() const {
    return pattern_graph_ ? pattern_graph_->GetNumEdges() : 0;
  }
  label_t get_pattern_vertex_label(int pattern_vertex_idx) const;

  // Get pattern edge list: [(src_pattern_idx, dst_pattern_idx, edge_label),
  // ...]
  std::vector<std::tuple<int, int, label_t>> get_pattern_edge_list() const;

  const std::vector<PatternOutputColumn>& get_pattern_output_columns() const {
    return output_columns_;
  }

  const PatternExecutionModifiers& get_pattern_execution_modifiers() const {
    return modifiers_;
  }

 private:
  // NOTE: BuildLabelMappings logic has been moved to do_graph_initialization()
  // for better code reuse and explicit initialization via CALL Initialize().

  // Thin wrapper: read the file off disk and delegate to the in-memory
  // text parser. The legacy SAMPLED_PATTERN_MATCH JSON path uses this.
  std::unique_ptr<
      neug::pattern_matching::graphlib::SubgraphMatching::PatternGraph>
  create_pattern_from_json_file(const std::string& pattern_file);

  // Core pattern loader. The origin label is used only in diagnostics.
  std::unique_ptr<
      neug::pattern_matching::graphlib::SubgraphMatching::PatternGraph>
  create_pattern_from_json_text(const std::string& json_content,
                                const std::string& origin_label);

  // Member variables
  GraphDataCache& cache_;
  const StorageReadInterface& graph_;
  std::string pattern_file_;
  std::unique_ptr<
      neug::pattern_matching::graphlib::SubgraphMatching::PatternGraph>
      pattern_graph_;
  long long sample_size_;

  // Results (per-call, not cached)
  std::vector<int> sampled_results_;

  std::vector<std::string> vertex_aliases_;
  std::vector<std::string> vertex_labels_;
  std::vector<PatternOutputEdgeInfo> edge_aliases_;
  std::vector<PatternOutputColumn> output_columns_;
  PatternExecutionModifiers modifiers_;
};

// ============================================================================
// InitializeGraphFunction: CALL INITIALIZE([checkpoint_dir]) — populates the
// GraphDataCache, optionally restoring from a previously saved checkpoint.
// ============================================================================

struct InitializeGraphInput : public function::CallFuncInputBase {
  std::string checkpoint_dir;
  InitializeGraphInput() = default;
  explicit InitializeGraphInput(std::string dir)
      : checkpoint_dir(std::move(dir)) {}
  ~InitializeGraphInput() override = default;
};

struct InitializeGraphFunction {
  static constexpr const char* name = "INITIALIZE";

  static function::function_set getFunctionSet(
      const std::shared_ptr<GraphDataCache>& cache);
};

// ============================================================================
// SaveSampledmatchCheckpointFunction: persists the GraphDataCache contents so
// a later INITIALIZE call can skip preprocessing.
// CALL SAVE_SAMPLEDMATCH_CHECKPOINT('/path/to/checkpoint') RETURN *;
// ============================================================================

struct SaveSampledmatchCheckpointInput : public function::CallFuncInputBase {
  std::string checkpoint_dir;
  explicit SaveSampledmatchCheckpointInput(std::string dir)
      : checkpoint_dir(std::move(dir)) {}
  ~SaveSampledmatchCheckpointInput() override = default;
};

struct SaveSampledmatchCheckpointFunction {
  static constexpr const char* name = "SAVE_SAMPLEDMATCH_CHECKPOINT";

  static function::function_set getFunctionSet(
      const std::shared_ptr<GraphDataCache>& cache);
};

// ============================================================================
// Exact pattern matching: directed, isomorphism-preserving subgraph enumeration
// (enumerate_exact_matches_with_neug) run directly on NeuG's cached
// DataGraphMeta — no external matcher and no temporary graph files.
// ============================================================================

struct ExactPatternSpec {
  struct VertexSpec {
    int id = -1;
    label_t label = 0;
    std::string label_name;
    std::string alias;
    std::vector<PropCons> constraints;
  };

  struct EdgeSpec {
    int src = -1;
    int dst = -1;
    label_t label = 0;
    std::string label_name;
    std::string alias;
    std::vector<PropCons> constraints;
  };

  std::vector<VertexSpec> vertices;
  std::vector<EdgeSpec> edges;
  std::vector<PatternOutputColumn> output_columns;
  PatternExecutionModifiers modifiers;
};

std::optional<ExactPatternSpec> parse_exact_pattern_json_file(
    const std::string& pattern_json_file, const Schema& schema);

bool is_numeric_value(const neug::Value& value, double* out);

bool compare_property_value(const neug::Value& actual, CompType op,
                            const neug::Value& expected);

bool check_vertex_constraints(const StorageReadInterface& graph,
                              const DataGraphMeta& data_meta, int global_id,
                              const ExactPatternSpec::VertexSpec& spec);

std::optional<neug::Value> get_directed_edge_property(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    int src_global, int dst_global, label_t edge_label, int prop_idx);

std::optional<neug::Value> get_vertex_property_by_name(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    int global_id, label_t expected_label, const std::string& prop_name);

std::optional<neug::Value> get_edge_property_by_name(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    int src_global, int dst_global, label_t edge_label,
    const std::string& prop_name);

int compare_execution_values(const std::optional<neug::Value>& lhs,
                             const std::optional<neug::Value>& rhs);

template <class Rows>
inline void apply_pattern_window(const PatternExecutionModifiers& modifiers,
                                 Rows* rows) {
  if (rows == nullptr || !modifiers.has_skip_or_limit()) {
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

bool check_edge_constraints(const StorageReadInterface& graph,
                            const DataGraphMeta& data_meta, int src_global,
                            int dst_global,
                            const ExactPatternSpec::EdgeSpec& spec);

inline std::optional<neug::Value> resolve_exact_order_value(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const ExactPatternSpec& spec, const std::vector<MatchVertex>& match,
    const PatternOrderBySpec& order_by);

void apply_exact_pattern_modifiers(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const ExactPatternSpec& spec,
    std::vector<std::vector<MatchVertex>>* matches);

std::vector<std::vector<MatchVertex>> enumerate_exact_matches_with_neug(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const ExactPatternSpec& spec, uint64_t limit);

bool find_directed_edge_data_ptr(const StorageReadInterface& graph,
                                 const DataGraphMeta& data_meta, int src_global,
                                 int dst_global, label_t edge_label,
                                 const void** data_ptr);

struct NativePatternColumnBuilder {
  PatternOutputColumn column;
  std::unique_ptr<neug::MSVertexColumnBuilder> vertex_builder;
  std::unique_ptr<neug::MSEdgeColumnBuilder> edge_builder;
};

execution::Context make_native_pattern_context(
    std::vector<NativePatternColumnBuilder>& builders);

execution::Context build_exact_native_pattern_context(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const ExactPatternSpec& spec,
    const std::vector<std::vector<MatchVertex>>& matches);

inline std::optional<neug::Value> resolve_sampled_order_value(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const SampledSubgraphMatcher& matcher, const std::vector<int>& results,
    int pattern_vertex_count,
    const std::vector<std::tuple<int, int, label_t>>& pattern_edges,
    int sample_idx, const PatternOrderBySpec& order_by);

execution::Context build_sampled_native_pattern_context(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const SampledSubgraphMatcher& matcher,
    const std::vector<int>& sampled_results, int pattern_vertex_count,
    int sample_count);

std::unique_ptr<function::TableFuncBindData> bind_pattern_native_output_columns(
    const function::TableFuncBindInput* input, const char* log_tag);

struct PatternMatchInput : public function::CallFuncInputBase {
  std::string pattern_file_path;
  long long limit;
  PatternMatchInput(std::string path, long long limit)
      : pattern_file_path(std::move(path)), limit(limit) {}
  ~PatternMatchInput() override = default;
};

execution::Context execute_pattern_match_pipeline(
    GraphDataCache& cache, const PatternMatchInput& input,
    IStorageInterface& graph);

// PatternMatchFunction is the single unified CALL PATTERN_MATCH(...) entry; it
// is defined after the sampled-match helpers below because its sampled overload
// reuses execute_sampled_match_pipeline / SampledMatchInput.

// ============================================================================
// Sampled subgraph matching helpers, reused by the sampled overload of the
// unified PATTERN_MATCH. The first argument accepts the same Cypher/JSON
// text-or-file forms as the exact path and is normalized to a JSON file before
// FaSTest reads it.
// ============================================================================

struct SampledMatchInput : public function::CallFuncInputBase {
  std::string pattern_file_path;
  long long sample_size;
  SampledMatchInput(std::string path, long long sample_size)
      : pattern_file_path(std::move(path)), sample_size(sample_size) {}
  ~SampledMatchInput() override = default;
};

// Runs the FaSTest sampler on a fully prepared pattern file. Factored out of
// SampledPatternMatchFunction can reuse it after normalizing its input to a
// temporary JSON pattern file.
execution::Context execute_sampled_match_pipeline(
    GraphDataCache& cache, const SampledMatchInput& match_input,
    IStorageInterface& graph);

// ============================================================================
// PatternMatchFunction: the single unified subgraph-matching entry point.
//
//   CALL PATTERN_MATCH(cypher_or_file)
//       -> exact matching, enumerates ALL matches.
//
//   CALL PATTERN_MATCH(cypher_or_file, size, is_sampled)
//       size       : positive integer (>= 1).
//       is_sampled : boolean flag selecting the algorithm.
//         * is_sampled = false  -> EXACT matching that early-terminates
//                                  after the first `size` matches are found.
//         * is_sampled = true   -> SAMPLED matching (FaSTest) with sample
//                                  size = `size`.
//
// The first argument accepts inline Cypher pattern text, a Cypher pattern file,
// inline JSON, or a JSON pattern file in either mode.
// ============================================================================

struct PatternMatchFunction {
  static constexpr const char* name = "PATTERN_MATCH";

  static function::function_set getFunctionSet(
      const std::shared_ptr<GraphDataCache>& cache);
};

// ============================================================================
// GetVertexPropertyFunction: looks up vertex properties and writes a CSV.
//   Inputs : vertex_ids (JSON array), vertex_label (string),
//            property_names (JSON array).
//   Output : path of the generated CSV file.
// ============================================================================

struct GetVertexPropertyInput : public function::CallFuncInputBase {
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

  static function::function_set getFunctionSet(
      const std::shared_ptr<GraphDataCache>& cache);
};

// ============================================================================
// GetEdgePropertyFunction: looks up edge properties and writes a CSV.
//   Inputs : edge_keys (JSON array), edge_label (string),
//            property_names (JSON array).
//   edge_key format: "src_global:dst_global:edge_label_id".
//   Output : path of the generated CSV file.
// ============================================================================

struct GetEdgePropertyInput : public function::CallFuncInputBase {
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

  static function::function_set getFunctionSet(
      const std::shared_ptr<GraphDataCache>& cache);
};

}  // namespace pattern_matching
}  // namespace neug
