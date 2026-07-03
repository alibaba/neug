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

#include "pattern_matching_functions.h"

namespace neug {
namespace pattern_matching {

// Exact subgraph-isomorphism enumeration via candidate-filtered,
// adjacency-intersection backtracking (the shared core of RI/GraphQL/CFL).
// Unlike a naive all-pairs enumerator, it places each query vertex by walking
// the *adjacency* of an already-mapped neighbour (in the correct direction and
// edge label) instead of scanning every candidate of its label -- turning an
// O(product of candidate-set sizes) search into one that is output-sensitive
// (~O(matches x degree)). Directed, label-aware, injective (isomorphism), and
// deterministic. Used as the reliable exact matcher.
std::vector<std::vector<MatchVertex>> enumerate_exact_matches_with_neug(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const ExactPatternSpec& spec, uint64_t limit) {
  const int nq = static_cast<int>(spec.vertices.size());
  const int nv = data_meta.GetNumVertices();

  // Query adjacency: each incident pattern edge from a query vertex's view.
  struct QEdge {
    int other;         // the other query vertex
    label_t elabel;    // pattern edge label
    bool this_is_src;  // is *this* vertex the src of the pattern edge?
    int edge_idx;      // index into spec.edges (for constraints)
  };
  std::vector<std::vector<QEdge>> qadj(nq);
  for (int i = 0; i < static_cast<int>(spec.edges.size()); ++i) {
    const auto& e = spec.edges[i];
    qadj[e.src].push_back({e.dst, e.label, true, i});
    qadj[e.dst].push_back({e.src, e.label, false, i});
  }

  // Candidate sets (label + vertex constraints) and O(1) membership markers.
  std::vector<std::vector<int>> cand(nq);
  std::vector<std::vector<char>> is_cand(nq, std::vector<char>(nv, 0));
  for (const auto& vtx : spec.vertices) {
    auto& c = cand[vtx.id];
    auto& mark = is_cand[vtx.id];
    for (int v = 0; v < nv; ++v) {
      if (data_meta.GetVertexLabel(v) != vtx.label)
        continue;
      if (!check_vertex_constraints(graph, data_meta, v, vtx))
        continue;
      c.push_back(v);
      mark[v] = 1;
    }
  }

  // Connectivity-first matching order: start at the most selective vertex,
  // then always extend to a query vertex adjacent to the already-ordered set
  // (so every non-first vertex has a mapped neighbour to walk from).
  std::vector<int> order;
  std::vector<char> ordered(nq, 0);
  {
    int start = 0;
    size_t best = std::numeric_limits<size_t>::max();
    for (int u = 0; u < nq; ++u) {
      if (cand[u].size() < best) {
        best = cand[u].size();
        start = u;
      }
    }
    order.push_back(start);
    ordered[start] = 1;
    while (static_cast<int>(order.size()) < nq) {
      int pick = -1;
      size_t pbest = std::numeric_limits<size_t>::max();
      bool pconn = false;
      for (int u = 0; u < nq; ++u) {
        if (ordered[u])
          continue;
        bool conn = false;
        for (const auto& qe : qadj[u]) {
          if (ordered[qe.other]) {
            conn = true;
            break;
          }
        }
        if ((conn && !pconn) || (conn == pconn && cand[u].size() < pbest)) {
          pick = u;
          pbest = cand[u].size();
          pconn = conn;
        }
      }
      order.push_back(pick);
      ordered[pick] = 1;
    }
  }

  std::vector<std::vector<MatchVertex>> results;
  std::vector<MatchVertex> mapping(nq, kInvalidMatchVertex);
  std::vector<char> used(nv, 0);

  // All pattern edges from query vertex u (tentatively mapped to data vertex v)
  // to already-mapped vertices hold in the data graph, directed + constrained.
  auto edges_ok = [&](int u, int v) -> bool {
    for (const auto& qe : qadj[u]) {
      if (mapping[qe.other] == kInvalidMatchVertex)
        continue;
      int s = qe.this_is_src ? v : static_cast<int>(mapping[qe.other]);
      int d = qe.this_is_src ? static_cast<int>(mapping[qe.other]) : v;
      if (data_meta.GetEdgeIndex(s, d, qe.elabel) == -1)
        return false;
      if (!check_edge_constraints(graph, data_meta, s, d,
                                  spec.edges[qe.edge_idx]))
        return false;
    }
    return true;
  };

  auto dfs = [&](auto&& self, int depth) -> void {
    if (results.size() >= limit)
      return;
    if (depth == nq) {
      results.push_back(mapping);
      return;
    }
    const int u = order[depth];

    // Find an already-mapped neighbour to drive the enumeration.
    const QEdge* pivot = nullptr;
    for (const auto& qe : qadj[u]) {
      if (mapping[qe.other] != kInvalidMatchVertex) {
        pivot = &qe;
        break;
      }
    }

    auto try_candidate = [&](int candv) {
      if (used[candv] || !is_cand[u][candv])
        return;
      mapping[u] = candv;
      if (edges_ok(u, candv)) {
        used[candv] = 1;
        self(self, depth + 1);
        used[candv] = 0;
      }
      mapping[u] = kInvalidMatchVertex;
    };

    if (pivot != nullptr) {
      const int pv = static_cast<int>(mapping[pivot->other]);
      const int u_label = static_cast<int>(spec.vertices[u].label);
      // pivot edge is u->other (this_is_src): data v->pv, so pv has an in-edge
      // from v -> enumerate pv's in-neighbours of u's label.
      // Otherwise edge is other->u: data pv->v, pv has an out-edge to v ->
      // enumerate pv's out-neighbours of u's label.
      const auto inc = pivot->this_is_src
                           ? data_meta.GetInIncidentEdges(pv, u_label)
                           : data_meta.GetOutIncidentEdges(pv, u_label);
      for (const auto& e : inc) {
        if (std::get<2>(e) != pivot->elabel)
          continue;
        int candv = pivot->this_is_src ? std::get<0>(e) : std::get<1>(e);
        try_candidate(candv);
        if (results.size() >= limit)
          return;
      }
    } else {
      // Root / disconnected-component start: scan the candidate list once.
      for (int candv : cand[u]) {
        try_candidate(candv);
        if (results.size() >= limit)
          return;
      }
    }
  };

  dfs(dfs, 0);
  return results;
}

execution::Context build_exact_native_pattern_context(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const ExactPatternSpec& spec,
    const std::vector<std::vector<MatchVertex>>& matches) {
  std::vector<NativePatternColumnBuilder> builders;
  builders.reserve(spec.output_columns.size());
  for (const auto& column : spec.output_columns) {
    NativePatternColumnBuilder builder;
    builder.column = column;
    if (column.kind == PatternOutputKind::kVertex) {
      label_t label = spec.vertices[column.index].label;
      builder.vertex_builder =
          std::make_unique<execution::MSVertexColumnBuilder>(label);
      builder.vertex_builder->reserve(matches.size());
    } else {
      const auto& edge = spec.edges[column.index];
      label_t src_label = spec.vertices[edge.src].label;
      label_t dst_label = spec.vertices[edge.dst].label;
      builder.edge_builder = std::make_unique<execution::MSEdgeColumnBuilder>();
      builder.edge_builder->reserve(matches.size());
      builder.edge_builder->start_label_dir(
          execution::LabelTriplet(src_label, dst_label, edge.label),
          execution::Direction::kOut);
    }
    builders.push_back(std::move(builder));
  }

  for (const auto& match : matches) {
    for (auto& builder : builders) {
      if (builder.column.kind == PatternOutputKind::kVertex) {
        int pattern_vertex = builder.column.index;
        int global_id = static_cast<int>(match[pattern_vertex]);
        auto [label, local_vid] = data_meta.ToLocalId(global_id);
        if (label == static_cast<label_t>(255)) {
          builder.vertex_builder->push_back_null();
        } else {
          builder.vertex_builder->push_back_opt(local_vid);
        }
        continue;
      }

      const auto& edge = spec.edges[builder.column.index];
      int src_global = static_cast<int>(match[edge.src]);
      int dst_global = static_cast<int>(match[edge.dst]);
      auto [src_label, src_vid] = data_meta.ToLocalId(src_global);
      auto [dst_label, dst_vid] = data_meta.ToLocalId(dst_global);
      const void* data_ptr = nullptr;
      const bool found = find_directed_edge_data_ptr(
          graph, data_meta, src_global, dst_global, edge.label, &data_ptr);
      if (src_label == static_cast<label_t>(255) ||
          dst_label == static_cast<label_t>(255) || !found) {
        builder.edge_builder->push_back_null();
      } else {
        builder.edge_builder->push_back_opt(src_vid, dst_vid, data_ptr);
      }
    }
  }
  return make_native_pattern_context(builders);
}

execution::Context build_sampled_native_pattern_context(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const SampledSubgraphMatcher& matcher,
    const std::vector<int>& sampled_results, int pattern_vertex_count,
    int sample_count) {
  const auto& output_columns = matcher.get_pattern_output_columns();
  const auto pattern_edges = matcher.get_pattern_edge_list();
  const auto& modifiers = matcher.get_pattern_execution_modifiers();

  std::vector<int> row_indices(sample_count);
  std::iota(row_indices.begin(), row_indices.end(), 0);
  if (modifiers.has_order_by()) {
    std::stable_sort(
        row_indices.begin(), row_indices.end(), [&](int lhs, int rhs) {
          for (const auto& order_by : modifiers.order_by) {
            int cmp = compare_execution_values(
                resolve_sampled_order_value(
                    graph, data_meta, matcher, sampled_results,
                    pattern_vertex_count, pattern_edges, lhs, order_by),
                resolve_sampled_order_value(
                    graph, data_meta, matcher, sampled_results,
                    pattern_vertex_count, pattern_edges, rhs, order_by));
            if (cmp == 0) {
              continue;
            }
            return order_by.ascending ? cmp < 0 : cmp > 0;
          }
          return lhs < rhs;
        });
  }
  apply_pattern_window(modifiers, &row_indices);

  std::vector<NativePatternColumnBuilder> builders;
  builders.reserve(output_columns.size());
  for (const auto& column : output_columns) {
    NativePatternColumnBuilder builder;
    builder.column = column;
    if (column.kind == PatternOutputKind::kVertex) {
      label_t label = matcher.get_pattern_vertex_label(column.index);
      builder.vertex_builder =
          std::make_unique<execution::MSVertexColumnBuilder>(label);
      builder.vertex_builder->reserve(row_indices.size());
    } else {
      auto [src, dst, edge_label] = pattern_edges[column.index];
      label_t src_label = matcher.get_pattern_vertex_label(src);
      label_t dst_label = matcher.get_pattern_vertex_label(dst);
      builder.edge_builder = std::make_unique<execution::MSEdgeColumnBuilder>();
      builder.edge_builder->reserve(row_indices.size());
      builder.edge_builder->start_label_dir(
          execution::LabelTriplet(src_label, dst_label, edge_label),
          execution::Direction::kOut);
    }
    builders.push_back(std::move(builder));
  }

  for (int sample_idx : row_indices) {
    for (auto& builder : builders) {
      if (builder.column.kind == PatternOutputKind::kVertex) {
        int pattern_vertex = builder.column.index;
        int global_id =
            sampled_results[sample_idx * pattern_vertex_count + pattern_vertex];
        auto [label, local_vid] = data_meta.ToLocalId(global_id);
        if (label == static_cast<label_t>(255)) {
          builder.vertex_builder->push_back_null();
        } else {
          builder.vertex_builder->push_back_opt(local_vid);
        }
        continue;
      }

      auto [src, dst, edge_label] = pattern_edges[builder.column.index];
      int src_global = sampled_results[sample_idx * pattern_vertex_count + src];
      int dst_global = sampled_results[sample_idx * pattern_vertex_count + dst];
      auto [src_label, src_vid] = data_meta.ToLocalId(src_global);
      auto [dst_label, dst_vid] = data_meta.ToLocalId(dst_global);
      const void* data_ptr = nullptr;
      const bool found = find_directed_edge_data_ptr(
          graph, data_meta, src_global, dst_global, edge_label, &data_ptr);
      if (src_label == static_cast<label_t>(255) ||
          dst_label == static_cast<label_t>(255) || !found) {
        builder.edge_builder->push_back_null();
      } else {
        builder.edge_builder->push_back_opt(src_vid, dst_vid, data_ptr);
      }
    }
  }
  return make_native_pattern_context(builders);
}

std::unique_ptr<function::TableFuncBindData> bind_pattern_native_output_columns(
    const function::TableFuncBindInput* input, const char* log_tag) {
  if (input == nullptr || input->binder == nullptr) {
    THROW_BINDER_EXCEPTION(std::string("[") + log_tag +
                           "] Internal error: table binder is not set.");
  }
  std::string pattern_arg = input->getLiteralVal<std::string>(0);
  std::string pattern_path =
      normalize_pattern_input_to_json_file(pattern_arg, log_tag);
  if (pattern_path.empty()) {
    THROW_BINDER_EXCEPTION(std::string("[") + log_tag +
                           "] Failed to parse pattern input.");
  }
  auto output_columns =
      ParsePatternOutputColumnsJsonFile(pattern_path, log_tag);
  if (!output_columns.has_value() || output_columns->empty()) {
    THROW_BINDER_EXCEPTION(std::string("[") + log_tag +
                           "] Pattern produced no output columns.");
  }

  // Every vertex must declare a label. A label-less vertex (only possible via a
  // raw JSON pattern; the Cypher translator and the exact-match parser both
  // require labels) would otherwise be bound below as a bare kVertex variable
  // with no property schema, and resolving `a.<prop>` on it dereferences a null
  // StructTypeInfo in the binder. Reject it here with a clean error instead.
  for (const auto& output_column : *output_columns) {
    if (output_column.kind == PatternOutputKind::kVertex &&
        output_column.label.empty()) {
      THROW_BINDER_EXCEPTION(std::string("[") + log_tag + "] Pattern vertex '" +
                             output_column.alias +
                             "' has no label; every vertex must declare one.");
    }
  }

  // Bind each output column as a proper NodeExpression / RelExpression (the
  // same kind a `MATCH (a:Label)` would produce) so the kVertex/kEdge variables
  // carry catalog entries and a property schema. Without this metadata the
  // binder treats them as bare struct-typed variables, and resolving `a.prop`
  // (e.g. for an outer ORDER BY / WHERE / projection) dereferences a null
  // StructTypeInfo and crashes. With it, `a.age`, `ORDER BY a.age`, `count(a)`
  // and friends resolve through the populated property maps. Pattern elements
  // without a known label fall back to a plain typed variable.
  auto* binder = input->binder;

  // Resolve the YIELD clause (if any) against the pattern output columns,
  // mirroring how the GDS table function handles YIELD: each yielded name must
  // match a pattern output column (by its natural alias), `AS` renames the
  // scope variable, and a strict subset is allowed.
  //
  // The executor always materializes every pattern column, positionally, in
  // pattern order. So the bind data must keep ALL columns in that same order to
  // stay aligned with the executor — YIELD therefore only controls (a) which
  // columns are visible in the binder scope (and thus to the trailing RETURN)
  // and (b) the name each visible column is bound under. Selecting a subset or
  // renaming does not change the physical columns the executor produces; it
  // only changes which ones the surrounding query can reference.
  const auto& yieldVariables = input->yieldVariables;
  const bool has_yield = !yieldVariables.empty();
  // Map: pattern output-column alias -> scope name to bind it under. A column
  // not present in the map is hidden from the surrounding query.
  std::unordered_map<std::string, std::string> yield_scope_names;
  if (has_yield) {
    for (const auto& yield_var : yieldVariables) {
      bool found = false;
      for (const auto& output_column : *output_columns) {
        if (output_column.alias == yield_var.name) {
          found = true;
          break;
        }
      }
      if (!found) {
        THROW_BINDER_EXCEPTION(
            std::string("[") + log_tag + "] YIELD variable '" + yield_var.name +
            "' is not an output column of this pattern. Available columns are "
            "the pattern's vertex/relationship aliases.");
      }
      yield_scope_names[yield_var.name] =
          yield_var.hasAlias() ? yield_var.alias : yield_var.name;
    }
  }

  // Returns the scope name a column should be bound under, or std::nullopt when
  // the column is hidden by a YIELD that does not list it.
  auto scope_name_for =
      [&](const PatternOutputColumn& col) -> std::optional<std::string> {
    if (!has_yield) {
      return col.alias;
    }
    auto it = yield_scope_names.find(col.alias);
    if (it == yield_scope_names.end()) {
      return std::nullopt;
    }
    return it->second;
  };

  // Pre-create a NodeExpression for every labeled vertex (keyed by pattern
  // vertex index), because an edge column needs its endpoint nodes even when
  // those vertices are hidden by YIELD.
  std::unordered_map<int, std::shared_ptr<binder::NodeExpression>>
      nodes_by_vertex_index;
  for (const auto& output_column : *output_columns) {
    if (output_column.kind != PatternOutputKind::kVertex ||
        output_column.label.empty()) {
      continue;
    }
    auto entries = binder->bindNodeTableEntries({output_column.label});
    nodes_by_vertex_index[output_column.index] =
        binder->createQueryNode(output_column.alias, entries);
  }

  // Emit ALL columns (pattern order) into the bind data so the schema stays
  // aligned with the executor; only add the YIELD-visible ones to scope.
  binder::expression_vector columns;
  columns.reserve(output_columns->size());
  for (const auto& output_column : *output_columns) {
    auto scope_name = scope_name_for(output_column);
    std::shared_ptr<binder::Expression> expr;
    if (output_column.kind == PatternOutputKind::kVertex) {
      auto it = nodes_by_vertex_index.find(output_column.index);
      if (it != nodes_by_vertex_index.end()) {
        expr = it->second;
      }
    } else if (!output_column.label.empty()) {
      auto src_it = nodes_by_vertex_index.find(output_column.edge_src);
      auto dst_it = nodes_by_vertex_index.find(output_column.edge_dst);
      if (src_it != nodes_by_vertex_index.end() &&
          dst_it != nodes_by_vertex_index.end()) {
        auto entries = binder->bindRelTableEntries({output_column.label});
        expr = binder->createNonRecursiveQueryRel(
            output_column.alias, entries, src_it->second, dst_it->second,
            binder::RelDirectionType::SINGLE);
      }
    }
    if (!expr) {
      // Fallback: anonymous/unlabeled element — keep a bare typed variable.
      // Use the invisible variant so it is only scoped when YIELD makes it
      // visible (createVariable would unconditionally add it to scope).
      expr = binder->createInvisibleVariable(output_column.alias,
                                             output_column.type_id());
    }
    columns.push_back(expr);
    if (scope_name) {
      binder->addToScope(*scope_name, expr);
    }
  }
  return std::make_unique<function::TableFuncBindData>(std::move(columns), 0,
                                                       input->params);
}

execution::Context execute_pattern_match_pipeline(
    const PatternMatchInput& input, IStorageInterface& graph) {
  auto* read_interface = dynamic_cast<StorageReadInterface*>(&graph);
  if (!read_interface) {
    LOG(ERROR) << "[PATTERN_MATCH] ERROR: graph is not a StorageReadInterface!";
    return execution::Context();
  }

  auto& cache = GraphDataCache::instance();
  auto& cached_data = cache.get_or_create(*read_interface);
  if (!cached_data.preprocessed) {
    do_graph_initialization(*read_interface, true);
  }
  DataGraphMeta& data_meta = *cached_data.data_meta;

  auto spec_opt = parse_exact_pattern_json_file(input.pattern_file_path,
                                                read_interface->schema());
  if (!spec_opt.has_value())
    return execution::Context();
  const ExactPatternSpec& spec = *spec_opt;

  const uint64_t caller_limit = input.limit <= 0
                                    ? std::numeric_limits<uint64_t>::max()
                                    : static_cast<uint64_t>(input.limit);
  uint64_t match_limit = caller_limit;
  if (spec.modifiers.has_order_by()) {
    match_limit = std::numeric_limits<uint64_t>::max();
  } else if (spec.modifiers.has_limit) {
    uint64_t needed = spec.modifiers.limit;
    if (spec.modifiers.has_skip) {
      if (needed > std::numeric_limits<uint64_t>::max() - spec.modifiers.skip) {
        needed = std::numeric_limits<uint64_t>::max();
      } else {
        needed += spec.modifiers.skip;
      }
    }
    match_limit = std::min(match_limit, needed);
  }

  // Exact matching runs the deterministic adjacency-intersection matcher
  // directly on data_meta: it is directed-correct, isomorphism-preserving, and
  // output-sensitive (walks adjacency instead of scanning all candidates).
  std::vector<std::vector<MatchVertex>> valid_matches =
      enumerate_exact_matches_with_neug(*read_interface, data_meta, spec,
                                        match_limit);

  apply_exact_pattern_modifiers(*read_interface, data_meta, spec,
                                &valid_matches);
  return build_exact_native_pattern_context(*read_interface, data_meta, spec,
                                            valid_matches);
}

execution::Context execute_sampled_match_pipeline(
    const SampledMatchInput& match_input, IStorageInterface& graph) {
  LOG(INFO) << "[SAMPLED_PATTERN_MATCH] Executing with graph access";
  if (!match_input.pattern_json_text.empty()) {
    LOG(INFO) << "[SAMPLED_PATTERN_MATCH] Pattern: in-memory JSON ("
              << match_input.pattern_json_text.size() << " bytes)";
  } else {
    LOG(INFO) << "[SAMPLED_PATTERN_MATCH] Pattern file: "
              << match_input.pattern_file_path;
  }

  auto* read_interface = dynamic_cast<StorageReadInterface*>(&graph);
  if (!read_interface) {
    LOG(ERROR) << "[SAMPLED_PATTERN_MATCH] ERROR: graph is not a "
                  "StorageReadInterface!";
    return execution::Context();
  }

  LOG(INFO) << "[SAMPLED_PATTERN_MATCH] Starting subgraph matching...";

  // Pick the ctor that matches the caller's input flavour; the matcher's
  // match() routine handles both internally without an extra file write.
  std::unique_ptr<SampledSubgraphMatcher> matcher_ptr;
  if (!match_input.pattern_json_text.empty()) {
    matcher_ptr = std::make_unique<SampledSubgraphMatcher>(
        *read_interface,
        SampledSubgraphMatcher::PatternJsonText{match_input.pattern_json_text},
        match_input.sample_size);
  } else {
    matcher_ptr = std::make_unique<SampledSubgraphMatcher>(
        *read_interface, match_input.pattern_file_path,
        match_input.sample_size);
  }
  SampledSubgraphMatcher& matcher = *matcher_ptr;
  double estimated_count = matcher.match();

  const auto& sampled_results = matcher.get_sampled_results();
  int pattern_vertex_count = matcher.get_pattern_vertex_count();
  int pattern_edge_count = matcher.get_pattern_edge_count();
  auto pattern_edge_list = matcher.get_pattern_edge_list();
  int sample_count = pattern_vertex_count > 0
                         ? sampled_results.size() / pattern_vertex_count
                         : 0;

  LOG(INFO) << "[SAMPLED_PATTERN_MATCH] Estimated count: "
            << (long long) estimated_count;
  LOG(INFO) << "[SAMPLED_PATTERN_MATCH] Sampled embeddings: " << sample_count;
  LOG(INFO) << "[SAMPLED_PATTERN_MATCH] Pattern edges: " << pattern_edge_count;

  auto& cached_data = GraphDataCache::instance().get_or_create(*read_interface);
  DataGraphMeta& data_meta = *cached_data.data_meta;
  (void) pattern_edge_list;
  return build_sampled_native_pattern_context(
      *read_interface, data_meta, matcher, sampled_results,
      pattern_vertex_count, sample_count);
}

std::string normalize_pattern_input_to_json_file(const std::string& arg,
                                                 const char* log_tag) {
  std::string content;
  std::error_code ec;
  const bool is_existing_file = !arg.empty() && arg.size() <= 4096 &&
                                std::filesystem::exists(arg, ec) && !ec &&
                                !std::filesystem::is_directory(arg, ec);

  if (is_existing_file) {
    if (!read_text_file(arg, &content)) {
      LOG(ERROR) << "[" << log_tag << "] Cannot open pattern file: " << arg;
      return "";
    }
    if (looks_like_json_pattern(content)) {
      return arg;
    }
  } else {
    content = arg;
    if (looks_like_json_pattern(content)) {
      return write_pattern_json_temp_file(content);
    }
  }

  std::string pattern_json =
      ::neug::pattern_matching::translate_pattern_cypher_to_json(content);
  if (pattern_json.empty()) {
    LOG(ERROR) << "[" << log_tag << "] Cypher pattern translation failed";
    return "";
  }
  return write_pattern_json_temp_file(pattern_json);
}

bool load_schema_graph(
    std::unordered_map<label_t,
                       std::unordered_map<label_t, std::vector<label_t>>>& sg,
    const std::string& filepath) {
  std::ifstream ifs(filepath, std::ios::binary);
  if (!ifs.is_open())
    return false;

  auto read_int = [&]() -> int32_t {
    int32_t v;
    ifs.read(reinterpret_cast<char*>(&v), sizeof(v));
    return v;
  };

  char magic[4];
  ifs.read(magic, 4);
  if (std::string(magic, 4) != SGCH_MAGIC)
    return false;
  if (read_int() != SGCH_VERSION)
    return false;

  sg.clear();
  int32_t outer_sz = read_int();
  for (int32_t i = 0; i < outer_sz; i++) {
    label_t src = static_cast<label_t>(read_int());
    int32_t inner_sz = read_int();
    for (int32_t j = 0; j < inner_sz; j++) {
      label_t dst = static_cast<label_t>(read_int());
      int32_t cnt = read_int();
      auto& vec = sg[src][dst];
      vec.resize(cnt);
      for (int32_t k = 0; k < cnt; k++)
        vec[k] = static_cast<label_t>(read_int());
    }
  }
  return !ifs.fail();
}

bool load_graph_checkpoint(const StorageReadInterface& graph,
                           const std::string& checkpoint_dir) {
  std::string meta_path = checkpoint_dir + "/data_graph_meta.bin";
  std::string sg_path = checkpoint_dir + "/schema_graph.bin";

  if (!std::filesystem::exists(meta_path) ||
      !std::filesystem::exists(sg_path)) {
    VLOG(1) << "[load_graph_checkpoint] No checkpoint files in: "
            << checkpoint_dir;
    return false;
  }

  auto& cache = GraphDataCache::instance();
  auto& cached_data = cache.get_or_create(graph);

  if (!cached_data.data_meta->LoadFromFile(meta_path)) {
    return false;
  }
  if (!load_schema_graph(*cached_data.schema_graph, sg_path)) {
    return false;
  }

  cached_data.preprocessed = true;
  LOG(INFO) << "[load_graph_checkpoint] Checkpoint loaded from: "
            << checkpoint_dir
            << ", vertices=" << cached_data.data_meta->GetNumVertices()
            << ", edges=" << cached_data.data_meta->GetNumEdges()
            << ", max_degree=" << cached_data.data_meta->GetMaxDegree()
            << ", degeneracy=" << cached_data.data_meta->GetDegeneracy();
  return true;
}

bool do_graph_initialization(const StorageReadInterface& graph, bool verbose,
                             const std::string& checkpoint_dir) {
  auto& cache = GraphDataCache::instance();
  auto& cached_data = cache.get_or_create(graph);

  if (cached_data.preprocessed) {
    if (verbose) {
      LOG(INFO) << "[Initialize] Graph already initialized, skipping. "
                << "vertices=" << cached_data.data_meta->GetNumVertices()
                << ", edges=" << cached_data.data_meta->GetNumEdges();
    }
    return true;
  }

  // Try loading from checkpoint first
  if (!checkpoint_dir.empty()) {
    if (load_graph_checkpoint(graph, checkpoint_dir)) {
      if (verbose) {
        LOG(INFO) << "[Initialize] Graph loaded from checkpoint.";
      }
      return true;
    }
    if (verbose) {
      LOG(INFO) << "[Initialize] Checkpoint not available, falling back to "
                   "full initialization.";
    }
  }

  if (verbose) {
    LOG(INFO) << "[Initialize] Building label mappings.";
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
      VLOG(2) << "[Initialize] Edge triplet: " << src_name << " -[" << edge_name
              << "]-> " << dst_name;
    }
  }

  if (verbose) {
    LOG(INFO) << "[Initialize] Found " << cached_data.schema_graph->size()
              << " source labels in schema.";
    LOG(INFO) << "[Initialize] Preprocessing data graph.";
  }

  cached_data.data_meta->Preprocess();
  cached_data.preprocessed = true;

  if (verbose) {
    LOG(INFO) << "[Initialize] Graph initialization completed. vertices="
              << cached_data.data_meta->GetNumVertices()
              << ", edges=" << cached_data.data_meta->GetNumEdges()
              << ", max_degree=" << cached_data.data_meta->GetMaxDegree()
              << ", degeneracy=" << cached_data.data_meta->GetDegeneracy();
  }

  return true;
}

std::optional<PatternExecutionModifiers> parse_pattern_execution_modifiers(
    const rapidjson::Document& doc,
    const std::vector<std::string>& vertex_aliases,
    const std::vector<PatternOutputEdgeInfo>& edge_aliases,
    const char* log_tag) {
  PatternExecutionModifiers modifiers;
  std::unordered_map<std::string, PatternOrderBySpec> by_alias;
  std::unordered_set<std::string> ambiguous_aliases;

  auto add_alias = [&](std::string alias, PatternOutputKind kind, int index) {
    if (alias.empty()) {
      return;
    }
    PatternOrderBySpec spec;
    spec.kind = kind;
    spec.index = index;
    spec.variable = alias;
    auto [it, inserted] = by_alias.emplace(alias, spec);
    if (!inserted) {
      ambiguous_aliases.insert(alias);
    }
  };

  for (int i = 0; i < static_cast<int>(vertex_aliases.size()); ++i) {
    add_alias(vertex_aliases[i], PatternOutputKind::kVertex, i);
  }
  for (int i = 0; i < static_cast<int>(edge_aliases.size()); ++i) {
    add_alias(edge_aliases[i].alias, PatternOutputKind::kEdge, i);
  }

  if (doc.HasMember("order_by")) {
    if (!doc["order_by"].IsArray()) {
      LOG(ERROR) << "[" << log_tag << "] order_by must be an array";
      return std::nullopt;
    }
    for (const auto& item : doc["order_by"].GetArray()) {
      if (!item.IsObject()) {
        LOG(ERROR) << "[" << log_tag << "] order_by entries must be objects";
        return std::nullopt;
      }
      std::string variable;
      for (const char* key : {"variable", "alias", "name"}) {
        if (item.HasMember(key) && item[key].IsString()) {
          variable = item[key].GetString();
          break;
        }
      }
      if (variable.empty()) {
        LOG(ERROR) << "[" << log_tag << "] order_by entry missing variable";
        return std::nullopt;
      }
      if (ambiguous_aliases.contains(variable)) {
        LOG(ERROR) << "[" << log_tag << "] order_by variable '" << variable
                   << "' is ambiguous";
        return std::nullopt;
      }
      auto found = by_alias.find(variable);
      if (found == by_alias.end()) {
        LOG(ERROR) << "[" << log_tag << "] order_by variable '" << variable
                   << "' does not exist in the pattern";
        return std::nullopt;
      }
      if (!item.HasMember("property") || !item["property"].IsString() ||
          item["property"].GetStringLength() == 0) {
        LOG(ERROR) << "[" << log_tag << "] order_by entry missing property";
        return std::nullopt;
      }

      auto spec = found->second;
      spec.property = item["property"].GetString();
      spec.ascending = true;
      if (item.HasMember("ascending") && item["ascending"].IsBool()) {
        spec.ascending = item["ascending"].GetBool();
      } else if (item.HasMember("order") && item["order"].IsString()) {
        std::string order = item["order"].GetString();
        std::transform(order.begin(), order.end(), order.begin(),
                       [](unsigned char c) { return std::toupper(c); });
        if (order == "DESC" || order == "DESCENDING") {
          spec.ascending = false;
        }
      }
      modifiers.order_by.push_back(std::move(spec));
    }
  }

  if (doc.HasMember("skip")) {
    if (!read_json_uint64(doc["skip"], &modifiers.skip)) {
      LOG(ERROR) << "[" << log_tag << "] skip must be a non-negative integer";
      return std::nullopt;
    }
    modifiers.has_skip = true;
  }
  if (doc.HasMember("limit")) {
    if (!read_json_uint64(doc["limit"], &modifiers.limit)) {
      LOG(ERROR) << "[" << log_tag << "] limit must be a non-negative integer";
      return std::nullopt;
    }
    modifiers.has_limit = true;
  }
  return modifiers;
}

std::vector<PatternOutputColumn> build_pattern_output_columns_from_aliases(
    const std::vector<std::string>& vertex_aliases,
    const std::vector<std::string>& vertex_labels,
    const std::vector<PatternOutputEdgeInfo>& edges) {
  std::vector<PatternOutputColumn> output;
  std::vector<bool> emitted_vertices(vertex_aliases.size(), false);
  std::unordered_map<std::string, int> seen_aliases;

  auto vertex_label = [&](int vertex_idx) -> std::string {
    if (vertex_idx < 0 ||
        vertex_idx >= static_cast<int>(vertex_labels.size())) {
      return "";
    }
    return vertex_labels[vertex_idx];
  };

  auto emit_vertex = [&](int vertex_idx) {
    if (vertex_idx < 0 ||
        vertex_idx >= static_cast<int>(vertex_aliases.size()) ||
        emitted_vertices[vertex_idx]) {
      return;
    }
    std::string alias = vertex_aliases[vertex_idx].empty()
                            ? "v" + std::to_string(vertex_idx)
                            : vertex_aliases[vertex_idx];
    PatternOutputColumn column{PatternOutputKind::kVertex, vertex_idx,
                               make_unique_pattern_alias(alias, &seen_aliases)};
    column.label = vertex_label(vertex_idx);
    output.push_back(std::move(column));
    emitted_vertices[vertex_idx] = true;
  };

  auto emit_edge = [&](int edge_idx, const PatternOutputEdgeInfo& edge) {
    std::string alias =
        edge.alias.empty() ? "e" + std::to_string(edge_idx) : edge.alias;
    PatternOutputColumn column{PatternOutputKind::kEdge, edge_idx,
                               make_unique_pattern_alias(alias, &seen_aliases)};
    column.label = edge.label;
    column.edge_src = edge.src;
    column.edge_dst = edge.dst;
    output.push_back(std::move(column));
  };

  for (int edge_idx = 0; edge_idx < static_cast<int>(edges.size());
       ++edge_idx) {
    emit_vertex(edges[edge_idx].src);
    emit_edge(edge_idx, edges[edge_idx]);
    emit_vertex(edges[edge_idx].dst);
  }
  for (int vertex_idx = 0; vertex_idx < static_cast<int>(vertex_aliases.size());
       ++vertex_idx) {
    emit_vertex(vertex_idx);
  }
  return output;
}

std::optional<std::vector<PatternOutputColumn>>
ParsePatternOutputColumnsJsonFile(const std::string& pattern_json_file,
                                  const char* log_tag) {
  std::string json_text;
  if (!read_text_file(pattern_json_file, &json_text)) {
    LOG(ERROR) << "[" << log_tag
               << "] Cannot read pattern JSON file: " << pattern_json_file;
    return std::nullopt;
  }

  rapidjson::Document doc;
  if (doc.Parse(json_text.c_str()).HasParseError()) {
    LOG(ERROR) << "[" << log_tag << "] JSON parse error in pattern file '"
               << pattern_json_file
               << "': " << rapidjson::GetParseError_En(doc.GetParseError())
               << " at offset " << doc.GetErrorOffset();
    return std::nullopt;
  }
  if (!doc.HasMember("vertices") || !doc["vertices"].IsArray() ||
      !doc.HasMember("edges") || !doc["edges"].IsArray()) {
    LOG(ERROR) << "[" << log_tag
               << "] Pattern JSON must contain vertices[] and edges[]";
    return std::nullopt;
  }

  std::vector<std::string> vertex_aliases(doc["vertices"].Size());
  std::vector<std::string> vertex_labels(doc["vertices"].Size());
  for (const auto& vertex : doc["vertices"].GetArray()) {
    int id = -1;
    if (!read_json_id(vertex, "id", &id) || id < 0 ||
        id >= static_cast<int>(vertex_aliases.size())) {
      LOG(ERROR) << "[" << log_tag
                 << "] Pattern vertex id must be dense in [0, "
                 << vertex_aliases.size() << ")";
      return std::nullopt;
    }
    vertex_aliases[id] = read_pattern_alias(vertex, "v", id);
    if (vertex.HasMember("label") && vertex["label"].IsString()) {
      vertex_labels[id] = vertex["label"].GetString();
    }
  }

  std::vector<PatternOutputEdgeInfo> edges;
  edges.reserve(doc["edges"].Size());
  for (const auto& edge : doc["edges"].GetArray()) {
    int src = -1;
    int dst = -1;
    if (!read_json_id(edge, "source", &src) ||
        !read_json_id(edge, "target", &dst) || src < 0 || dst < 0 ||
        src >= static_cast<int>(vertex_aliases.size()) ||
        dst >= static_cast<int>(vertex_aliases.size())) {
      LOG(ERROR) << "[" << log_tag
                 << "] Pattern edge has invalid source/target";
      return std::nullopt;
    }
    PatternOutputEdgeInfo edge_info{
        src, dst, read_pattern_alias(edge, "e", static_cast<int>(edges.size())),
        ""};
    if (edge.HasMember("label") && edge["label"].IsString()) {
      edge_info.label = edge["label"].GetString();
    }
    edges.push_back(std::move(edge_info));
  }
  return build_pattern_output_columns_from_aliases(vertex_aliases,
                                                   vertex_labels, edges);
}

std::optional<ExactPatternSpec> parse_exact_pattern_json_file(
    const std::string& pattern_json_file, const Schema& schema) {
  std::string json_text;
  if (!read_text_file(pattern_json_file, &json_text)) {
    LOG(ERROR) << "[PATTERN_MATCH] Cannot read pattern JSON file: "
               << pattern_json_file;
    return std::nullopt;
  }

  rapidjson::Document doc;
  if (doc.Parse(json_text.c_str()).HasParseError()) {
    LOG(ERROR) << "[PATTERN_MATCH] JSON parse error in pattern file '"
               << pattern_json_file
               << "': " << rapidjson::GetParseError_En(doc.GetParseError())
               << " at offset " << doc.GetErrorOffset();
    return std::nullopt;
  }
  if (!doc.HasMember("vertices") || !doc["vertices"].IsArray() ||
      !doc.HasMember("edges") || !doc["edges"].IsArray()) {
    LOG(ERROR)
        << "[PATTERN_MATCH] Pattern JSON must contain vertices[] and edges[]";
    return std::nullopt;
  }

  ExactPatternSpec spec;
  spec.vertices.resize(doc["vertices"].Size());
  for (const auto& vertex : doc["vertices"].GetArray()) {
    int id = -1;
    if (!read_json_id(vertex, "id", &id) || id < 0 ||
        id >= static_cast<int>(spec.vertices.size())) {
      LOG(ERROR) << "[PATTERN_MATCH] Pattern vertex id must be dense in [0, "
                 << spec.vertices.size() << ")";
      return std::nullopt;
    }
    if (!vertex.HasMember("label") || !vertex["label"].IsString()) {
      LOG(ERROR) << "[PATTERN_MATCH] Pattern vertex " << id
                 << " missing string label";
      return std::nullopt;
    }
    std::string label_name = vertex["label"].GetString();
    if (!schema.is_vertex_label_valid(label_name)) {
      LOG(ERROR) << "[PATTERN_MATCH] Vertex label '" << label_name
                 << "' not found in schema";
      return std::nullopt;
    }
    auto& out = spec.vertices[id];
    out.id = id;
    out.label = schema.get_vertex_label_id(label_name);
    out.label_name = std::move(label_name);
    out.alias = read_pattern_alias(vertex, "v", id);
    if (vertex.HasMember("constraints") && vertex["constraints"].IsArray()) {
      out.constraints = parse_constraints(vertex["constraints"]);
    }
    out.required_props = parse_required_props(vertex);
  }

  for (const auto& edge : doc["edges"].GetArray()) {
    int src = -1;
    int dst = -1;
    if (!read_json_id(edge, "source", &src) ||
        !read_json_id(edge, "target", &dst) || src < 0 || dst < 0 ||
        src >= static_cast<int>(spec.vertices.size()) ||
        dst >= static_cast<int>(spec.vertices.size())) {
      LOG(ERROR) << "[PATTERN_MATCH] Pattern edge has invalid source/target";
      return std::nullopt;
    }

    ExactPatternSpec::EdgeSpec out;
    out.src = src;
    out.dst = dst;
    out.alias =
        read_pattern_alias(edge, "e", static_cast<int>(spec.edges.size()));
    if (edge.HasMember("label") && edge["label"].IsString()) {
      out.label_name = edge["label"].GetString();
      if (!schema.is_edge_label_valid(out.label_name)) {
        LOG(ERROR) << "[PATTERN_MATCH] Edge label '" << out.label_name
                   << "' not found in schema";
        return std::nullopt;
      }
      out.label = schema.get_edge_label_id(out.label_name);
    } else {
      out.label = 0;
      out.label_name = schema.is_edge_label_valid(out.label)
                           ? schema.get_edge_label_name(out.label)
                           : std::string("0");
    }
    if (edge.HasMember("constraints") && edge["constraints"].IsArray()) {
      out.constraints = parse_constraints(edge["constraints"]);
    }
    out.required_props = parse_required_props(edge);
    spec.edges.push_back(std::move(out));
  }

  std::vector<std::string> vertex_aliases(spec.vertices.size());
  std::vector<std::string> vertex_labels(spec.vertices.size());
  for (const auto& vertex : spec.vertices) {
    vertex_aliases[vertex.id] = vertex.alias;
    vertex_labels[vertex.id] = vertex.label_name;
  }
  std::vector<PatternOutputEdgeInfo> edge_aliases;
  edge_aliases.reserve(spec.edges.size());
  for (const auto& edge : spec.edges) {
    edge_aliases.push_back(
        PatternOutputEdgeInfo{edge.src, edge.dst, edge.alias, edge.label_name});
  }
  spec.output_columns = build_pattern_output_columns_from_aliases(
      vertex_aliases, vertex_labels, edge_aliases);
  auto modifiers = parse_pattern_execution_modifiers(
      doc, vertex_aliases, edge_aliases, "PATTERN_MATCH");
  if (!modifiers.has_value()) {
    return std::nullopt;
  }
  spec.modifiers = std::move(*modifiers);

  return spec;
}

namespace {

// Sign-magnitude representation of an exact integer. Using magnitude+sign
// instead of int64 lets us represent the full ranges of BOTH int64 and uint64
// without any lossy double conversion, so large ids/counts past 2^53 compare
// exactly under every operator (=, <, <=, >, >=) and ORDER BY.
struct IntRepr {
  bool negative = false;
  uint64_t magnitude = 0;
};

bool try_exact_int(const execution::Value& v, IntRepr* out) {
  if (v.IsNull())
    return false;
  auto set_signed = [&](int64_t s) {
    out->negative = s < 0;
    // Compute |s| without overflowing at INT64_MIN.
    out->magnitude = out->negative ? static_cast<uint64_t>(-(s + 1)) + 1ULL
                                   : static_cast<uint64_t>(s);
  };
  try {
    switch (v.type().id()) {
    case DataTypeId::kInt8:
    case DataTypeId::kInt16:
      set_signed(static_cast<int64_t>(std::stoll(v.to_string())));
      return true;
    case DataTypeId::kUInt8:
    case DataTypeId::kUInt16:
      out->negative = false;
      out->magnitude = static_cast<uint64_t>(std::stoull(v.to_string()));
      return true;
    case DataTypeId::kInt32:
      set_signed(static_cast<int64_t>(v.GetValue<int32_t>()));
      return true;
    case DataTypeId::kInt64:
      set_signed(v.GetValue<int64_t>());
      return true;
    case DataTypeId::kUInt32:
      out->negative = false;
      out->magnitude = v.GetValue<uint32_t>();
      return true;
    case DataTypeId::kUInt64:
      out->negative = false;
      out->magnitude = v.GetValue<uint64_t>();
      return true;
    default:
      return false;
    }
  } catch (...) { return false; }
}

int compare_int_repr(const IntRepr& a, const IntRepr& b) {
  if (a.negative != b.negative)
    return a.negative ? -1 : 1;
  if (a.magnitude == b.magnitude)
    return 0;
  // Both same sign: for negatives a larger magnitude means a smaller value.
  const bool a_smaller =
      a.negative ? (a.magnitude > b.magnitude) : (a.magnitude < b.magnitude);
  return a_smaller ? -1 : 1;
}

// Three-way numeric comparison. Compares integers exactly (no double rounding);
// falls back to double only when at least one operand is floating-point. The
// double path imposes a deterministic total order on NaN (NaN sorts last) so it
// remains a valid strict-weak-ordering comparator for std::sort/ORDER BY.
// Returns false when either operand is non-numeric.
bool compare_numeric_values(const execution::Value& a,
                            const execution::Value& b, int* cmp) {
  IntRepr ia;
  IntRepr ib;
  if (try_exact_int(a, &ia) && try_exact_int(b, &ib)) {
    *cmp = compare_int_repr(ia, ib);
    return true;
  }
  double da = 0.0;
  double db = 0.0;
  if (is_numeric_value(a, &da) && is_numeric_value(b, &db)) {
    const bool na = std::isnan(da);
    const bool nb = std::isnan(db);
    if (na || nb) {
      *cmp = (na == nb) ? 0 : (na ? 1 : -1);
      return true;
    }
    *cmp = (da < db) ? -1 : (da > db ? 1 : 0);
    return true;
  }
  return false;
}

}  // namespace

bool compare_property_value(const execution::Value& actual, CompType op,
                            const execution::Value& expected) {
  if (actual.IsNull())
    return false;
  int cmp = 0;
  if (compare_numeric_values(actual, expected, &cmp)) {
    // '=' uses the same exact comparison as the relational operators so they
    // stay mutually consistent (a value equal to X must also be <= X and >= X).
    // Floating-point '=' is therefore exact, matching standard SQL/Cypher
    // semantics; callers needing tolerance must round before querying.
    switch (op) {
    case CompType::COMP_EQUAL:
      return cmp == 0;
    case CompType::COMP_GREATER:
      return cmp > 0;
    case CompType::COMP_LESS:
      return cmp < 0;
    case CompType::COMP_GREATER_EQUAL:
      return cmp >= 0;
    case CompType::COMP_LESS_EQUAL:
      return cmp <= 0;
    default:
      return false;
    }
  }

  if (actual.type() == expected.type()) {
    switch (op) {
    case CompType::COMP_EQUAL:
      return actual == expected;
    case CompType::COMP_GREATER:
      return expected < actual;
    case CompType::COMP_LESS:
      return actual < expected;
    case CompType::COMP_GREATER_EQUAL:
      return (actual == expected) || (expected < actual);
    case CompType::COMP_LESS_EQUAL:
      return (actual == expected) || (actual < expected);
    default:
      return false;
    }
  }

  const std::string lhs_s = actual.to_string();
  const std::string rhs_s = expected.to_string();
  switch (op) {
  case CompType::COMP_EQUAL:
    return lhs_s == rhs_s;
  case CompType::COMP_GREATER:
    return lhs_s > rhs_s;
  case CompType::COMP_LESS:
    return lhs_s < rhs_s;
  case CompType::COMP_GREATER_EQUAL:
    return lhs_s >= rhs_s;
  case CompType::COMP_LESS_EQUAL:
    return lhs_s <= rhs_s;
  default:
    return false;
  }
}

int compare_execution_values(const std::optional<execution::Value>& lhs,
                             const std::optional<execution::Value>& rhs) {
  const bool lhs_null = !lhs.has_value() || lhs->IsNull();
  const bool rhs_null = !rhs.has_value() || rhs->IsNull();
  if (lhs_null && rhs_null)
    return 0;
  if (lhs_null)
    return 1;
  if (rhs_null)
    return -1;

  int num_cmp = 0;
  if (compare_numeric_values(*lhs, *rhs, &num_cmp)) {
    return num_cmp;
  }

  try {
    if (lhs->type() == rhs->type()) {
      if (*lhs == *rhs)
        return 0;
      return *lhs < *rhs ? -1 : 1;
    }
  } catch (...) {}

  const std::string lhs_s = lhs->to_string();
  const std::string rhs_s = rhs->to_string();
  if (lhs_s < rhs_s)
    return -1;
  if (lhs_s > rhs_s)
    return 1;
  return 0;
}

void apply_exact_pattern_modifiers(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const ExactPatternSpec& spec,
    std::vector<std::vector<MatchVertex>>* matches) {
  if (matches == nullptr) {
    return;
  }
  if (spec.modifiers.has_order_by()) {
    std::stable_sort(matches->begin(), matches->end(),
                     [&](const auto& lhs, const auto& rhs) {
                       for (const auto& order_by : spec.modifiers.order_by) {
                         int cmp = compare_execution_values(
                             resolve_exact_order_value(graph, data_meta, spec,
                                                       lhs, order_by),
                             resolve_exact_order_value(graph, data_meta, spec,
                                                       rhs, order_by));
                         if (cmp == 0) {
                           continue;
                         }
                         return order_by.ascending ? cmp < 0 : cmp > 0;
                       }
                       return lhs < rhs;
                     });
  }
  apply_pattern_window(spec.modifiers, matches);
}

std::string fetch_and_write_exact_properties(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const ExactPatternSpec& spec,
    const std::vector<std::vector<MatchVertex>>& matches) {
  bool has_props = false;
  for (const auto& vertex : spec.vertices)
    has_props |= !vertex.required_props.empty();
  for (const auto& edge : spec.edges)
    has_props |= !edge.required_props.empty();
  if (!has_props || matches.empty())
    return "";

  std::string path = generate_temp_file_path("pattern_matching_props", ".json");
  std::ofstream ofs(path);
  if (!ofs.is_open())
    return "";

  ofs << "{\"vertices\":[";
  bool first = true;
  std::unordered_set<int> emitted_vertices;
  for (const auto& match : matches) {
    for (const auto& vertex : spec.vertices) {
      if (vertex.required_props.empty())
        continue;
      int global_id = static_cast<int>(match[vertex.id]);
      if (!emitted_vertices.insert(global_id).second)
        continue;
      auto [label, local_vid] = data_meta.ToLocalId(global_id);
      auto all_names = graph.schema().get_vertex_property_names(label);
      if (!first)
        ofs << ",";
      first = false;
      ofs << "{\"id\":" << global_id << ",\"props\":{";
      bool first_prop = true;
      for (const auto& prop_name : vertex.required_props) {
        auto it = std::find(all_names.begin(), all_names.end(), prop_name);
        if (it == all_names.end())
          continue;
        int prop_idx = static_cast<int>(std::distance(all_names.begin(), it));
        execution::Value value =
            graph.GetVertexProperty(label, local_vid, prop_idx);
        if (!first_prop)
          ofs << ",";
        first_prop = false;
        ofs << "\"" << escape_json_string(prop_name)
            << "\":" << value_to_json_string(value);
      }
      ofs << "}}";
    }
  }

  ofs << "],\"edges\":[";
  first = true;
  std::unordered_set<std::string> emitted_edges;
  for (const auto& match : matches) {
    for (const auto& edge : spec.edges) {
      if (edge.required_props.empty())
        continue;
      int src = static_cast<int>(match[edge.src]);
      int dst = static_cast<int>(match[edge.dst]);
      std::string edge_key = std::to_string(src) + ":" + std::to_string(dst) +
                             ":" + std::to_string(edge.label);
      if (!emitted_edges.insert(edge_key).second)
        continue;
      auto [src_label, src_vid] = data_meta.ToLocalId(src);
      auto [dst_label, dst_vid] = data_meta.ToLocalId(dst);
      (void) src_vid;
      (void) dst_vid;
      auto all_names = graph.schema().get_edge_property_names(
          src_label, dst_label, edge.label);
      if (!first)
        ofs << ",";
      first = false;
      ofs << "{\"id\":\"" << escape_json_string(edge_key) << "\",\"props\":{";
      bool first_prop = true;
      for (const auto& prop_name : edge.required_props) {
        auto it = std::find(all_names.begin(), all_names.end(), prop_name);
        if (it == all_names.end())
          continue;
        int prop_idx = static_cast<int>(std::distance(all_names.begin(), it));
        auto value = get_directed_edge_property(graph, data_meta, src, dst,
                                                edge.label, prop_idx);
        if (!value.has_value())
          continue;
        if (!first_prop)
          ofs << ",";
        first_prop = false;
        ofs << "\"" << escape_json_string(prop_name)
            << "\":" << value_to_json_string(*value);
      }
      ofs << "}}";
    }
  }
  ofs << "]}\n";
  return path;
}

double SampledSubgraphMatcher::match() {
  // All progress traces go through glog: VLOG(1) for per-step progress,
  // VLOG(2) for per-vertex/per-edge dumps. Enable with `GLOG_v=1` (or 2)
  // — by default `CALL SAMPLED_PATTERN_MATCH` produces no chatter on stdout.
  auto& cache = GraphDataCache::instance();
  auto& cached_data = cache.get_or_create(graph_);

  // Steps 0-1: reuse the cache when possible; initialize lazily otherwise.
  if (!cached_data.preprocessed) {
    VLOG(1) << "[SAMPLED_PATTERN_MATCH] Graph not initialized, running "
               "do_graph_initialization...";
    do_graph_initialization(graph_, true);
  } else {
    VLOG(1) << "[SAMPLED_PATTERN_MATCH] Using cached graph data: "
            << cached_data.data_meta->GetNumVertices() << " vertices, "
            << cached_data.data_meta->GetNumEdges() << " edges";
  }

  // Step 2: always reload the pattern — callers can vary it per
  // invocation. Two flavours: file path (legacy JSON callers) or in-memory
  // JSON text (Cypher translator caller; spares disk I/O).
  if (!pattern_json_.empty()) {
    VLOG(1) << "[SAMPLED_PATTERN_MATCH] Loading pattern graph from in-memory "
               "JSON ("
            << pattern_json_.size() << " bytes)";
    pattern_graph_ = create_pattern_from_json_text(pattern_json_, "<inline>");
  } else {
    VLOG(1) << "[SAMPLED_PATTERN_MATCH] Loading pattern graph from: "
            << pattern_file_;
    pattern_graph_ = create_pattern_from_json_file(pattern_file_);
  }
  if (!pattern_graph_ || pattern_graph_->GetNumVertices() == 0) {
    LOG(ERROR) << "[SAMPLED_PATTERN_MATCH] Failed to load pattern from: "
               << (pattern_json_.empty() ? pattern_file_
                                         : std::string("<inline>"));
    return -1;
  }
  VLOG(1) << "[SAMPLED_PATTERN_MATCH] Pattern: "
          << pattern_graph_->GetNumVertices() << " vertices, "
          << pattern_graph_->GetNumEdges() << " edges";

  if (VLOG_IS_ON(2)) {
    VLOG(2) << "[SAMPLED_PATTERN_MATCH] Pattern vertices:";
    for (int i = 0; i < pattern_graph_->GetNumVertices(); i++) {
      int label = pattern_graph_->vertex_label[i];
      VLOG(2) << "  v" << i << ": label=" << label
              << " (out_deg=" << pattern_graph_->GetOutDegree(i)
              << ", in_deg=" << pattern_graph_->GetInDegree(i) << ")";
    }
    VLOG(2) << "[SAMPLED_PATTERN_MATCH] Pattern edges:";
    for (int i = 0; i < pattern_graph_->GetNumEdges(); i++) {
      auto& [src, dst] = pattern_graph_->edge_list[i];
      int label = pattern_graph_->edge_label[i];
      VLOG(2) << "  e" << i << ": " << src << " -[label=" << label << "]-> "
              << dst;
    }
  }

  // Step 3: Process pattern (compute core numbers, build incidence list,
  // etc.)
  VLOG(1) << "[SAMPLED_PATTERN_MATCH] Processing pattern...";
  pattern_graph_->ProcessPattern(*cached_data.data_meta,
                                 cached_data.schema_graph);

  // Step 4: Setup cardinality estimation options
  neug::pattern_matching::graphlib::CardinalityEstimation::CardEstOption opt;
  opt.MAX_QUERY_VERTEX = std::max(12, pattern_graph_->GetNumVertices());
  opt.MAX_QUERY_EDGE = std::max(24, pattern_graph_->GetNumEdges());
  opt.structure_filter =
      neug::pattern_matching::graphlib::SubgraphMatching::NO_STRUCTURE_FILTER;
  VLOG(1) << "[SAMPLED_PATTERN_MATCH] CardEst options: MAX_QUERY_VERTEX="
          << opt.MAX_QUERY_VERTEX << ", MAX_QUERY_EDGE=" << opt.MAX_QUERY_EDGE;

  // Step 5: Run cardinality estimation
  VLOG(1) << "[SAMPLED_PATTERN_MATCH] Running cardinality estimation, sample "
             "size: "
          << sample_size_;
  neug::pattern_matching::graphlib::CardinalityEstimation::
      FaSTestCardinalityEstimation estimator(graph_, *cached_data.data_meta,
                                             opt);
  double est = estimator.EstimateEmbeddings(pattern_graph_.get(), sample_size_);

  sampled_results_ = estimator.GetSampledResult();
  int num_samples = sampled_results_.size() / pattern_graph_->GetNumVertices();
  VLOG(1) << "[SAMPLED_PATTERN_MATCH] Estimated embedding count: "
          << (long long) est << ", sampled embeddings: " << num_samples;

  if (VLOG_IS_ON(2) && num_samples > 0) {
    VLOG(2) << "[SAMPLED_PATTERN_MATCH] First 5 sampled embeddings:";
    int show_count = std::min(5, num_samples);
    for (int i = 0; i < show_count; i++) {
      std::ostringstream oss;
      for (int j = 0; j < pattern_graph_->GetNumVertices(); j++) {
        if (j > 0)
          oss << " -> ";
        oss << sampled_results_[i * pattern_graph_->GetNumVertices() + j];
      }
      VLOG(2) << "  [" << i << "]: " << oss.str();
    }
  }

  if (VLOG_IS_ON(2)) {
    auto result_info = estimator.GetResult();
    VLOG(2) << "[SAMPLED_PATTERN_MATCH] Estimation details:";
    for (const auto& [key, value] : result_info) {
      std::ostringstream oss;
      if (value.type() == typeid(double)) {
        oss << std::any_cast<double>(value);
      } else if (value.type() == typeid(int)) {
        oss << std::any_cast<int>(value);
      } else if (value.type() == typeid(std::string)) {
        oss << std::any_cast<std::string>(value);
      }
      VLOG(2) << "  " << key << ": " << oss.str();
    }
  }

  estimated_count_ = est;
  return est;
}

std::unique_ptr<
    neug::pattern_matching::graphlib::SubgraphMatching::PatternGraph>
SampledSubgraphMatcher::create_pattern_from_json_text(
    const std::string& json_content, const std::string& origin_label) {
  const auto& schema = graph_.schema();
  const auto& pattern_file = origin_label;  // retained for log fidelity

  // Parse JSON
  rapidjson::Document doc;
  if (doc.Parse(json_content.c_str()).HasParseError()) {
    LOG(WARNING) << "[SAMPLED_PATTERN_MATCH] JSON parse error in pattern file '"
                 << pattern_file
                 << "': " << rapidjson::GetParseError_En(doc.GetParseError())
                 << " at offset " << doc.GetErrorOffset();
    return nullptr;
  }

  auto pattern = std::make_unique<
      neug::pattern_matching::graphlib::SubgraphMatching::PatternGraph>();

  // Parse vertices
  if (!doc.HasMember("vertices") || !doc["vertices"].IsArray()) {
    LOG(WARNING)
        << "[SAMPLED_PATTERN_MATCH] Pattern JSON missing 'vertices' array: "
        << pattern_file;
    return nullptr;
  }
  if (!doc.HasMember("edges") || !doc["edges"].IsArray()) {
    LOG(WARNING)
        << "[SAMPLED_PATTERN_MATCH] Pattern JSON missing 'edges' array: "
        << pattern_file;
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
  vertex_aliases_.assign(v, "");
  vertex_labels_.assign(v, "");
  edge_aliases_.clear();
  edge_aliases_.reserve(e);
  output_columns_.clear();

  for (rapidjson::SizeType i = 0; i < vertices.Size(); i++) {
    const auto& vertex = vertices[i];
    if (!vertex.IsObject() || !vertex.HasMember("id")) {
      LOG(WARNING)
          << "[SAMPLED_PATTERN_MATCH] Pattern vertex must be an object "
             "with an 'id'";
      return nullptr;
    }
    // Support both integer and string id (convert string to int if needed)
    int id;
    const auto& id_value = vertex["id"];
    if (id_value.IsInt()) {
      id = id_value.GetInt();
    } else if (id_value.IsString()) {
      try {
        id = std::stoi(id_value.GetString());
      } catch (const std::exception&) {
        LOG(WARNING) << "[SAMPLED_PATTERN_MATCH] Pattern vertex 'id' string is "
                        "not an integer: "
                     << id_value.GetString();
        return nullptr;
      }
    } else {
      LOG(WARNING) << "[SAMPLED_PATTERN_MATCH] Pattern vertex 'id' must be "
                      "int or string";
      return nullptr;
    }
    if (id < 0 || id >= v) {
      LOG(WARNING) << "[SAMPLED_PATTERN_MATCH] Pattern vertex id " << id
                   << " is out of range [0, " << v << ")";
      return nullptr;
    }
    if (!vertex.HasMember("label") || !vertex["label"].IsString()) {
      LOG(WARNING) << "[SAMPLED_PATTERN_MATCH] Pattern vertex " << id
                   << " missing string 'label'";
      return nullptr;
    }
    std::string label = vertex["label"].GetString();

    if (!schema.is_vertex_label_valid(label)) {
      LOG(WARNING) << "[SAMPLED_PATTERN_MATCH] Pattern vertex label '" << label
                   << "' not found in schema; aborting pattern load";
      return nullptr;
    }
    pattern->vertex_label[id] = schema.get_vertex_label_id(label);
    vertex_aliases_[id] = read_pattern_alias(vertex, "v", id);
    vertex_labels_[id] = label;

    // Parse vertex property constraints
    if (vertex.HasMember("constraints") && vertex["constraints"].IsArray()) {
      pattern->vertex_property_constraints[id] =
          parse_constraints(vertex["constraints"]);
    }

    // Parse required_props
    if (vertex.HasMember("required_props") &&
        vertex["required_props"].IsArray()) {
      const auto& rp = vertex["required_props"];
      for (rapidjson::SizeType j = 0; j < rp.Size(); j++) {
        if (rp[j].IsString()) {
          vertex_required_props_[id].push_back(rp[j].GetString());
        }
      }
    }
  }

  // Parse an edge endpoint id (int or numeric string), validating that it is
  // present, well-formed, and within the dense vertex-id range [0, v).
  auto parse_endpoint = [&](const rapidjson::Value& edge, const char* key,
                            int* out) -> bool {
    if (!edge.HasMember(key)) {
      return false;
    }
    const auto& val = edge[key];
    if (val.IsInt()) {
      *out = val.GetInt();
    } else if (val.IsString()) {
      try {
        *out = std::stoi(val.GetString());
      } catch (const std::exception&) { return false; }
    } else {
      return false;
    }
    return *out >= 0 && *out < v;
  };

  // Parse edges
  int edge_idx = 0;
  for (rapidjson::SizeType i = 0; i < edges.Size(); i++) {
    const auto& edge = edges[i];
    int src, dst;
    if (!edge.IsObject() || !parse_endpoint(edge, "source", &src) ||
        !parse_endpoint(edge, "target", &dst)) {
      LOG(WARNING) << "[SAMPLED_PATTERN_MATCH] Pattern edge has missing, "
                      "non-integer, or out-of-range source/target";
      return nullptr;
    }

    std::string edge_type = "";
    if (edge.HasMember("label") && edge["label"].IsString()) {
      edge_type = edge["label"].GetString();
    }
    std::string edge_alias = read_pattern_alias(edge, "e", edge_idx);

    if (!edge_type.empty()) {
      if (!schema.is_edge_label_valid(edge_type)) {
        LOG(WARNING) << "[SAMPLED_PATTERN_MATCH] Pattern edge label '"
                     << edge_type
                     << "' not found in schema; aborting pattern load";
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
    edge_aliases_.push_back(
        PatternOutputEdgeInfo{src, dst, edge_alias, edge_type});

    pattern->max_out_degree = std::max(pattern->max_out_degree,
                                       (int) pattern->out_adj_list[src].size());
    pattern->max_in_degree = std::max(pattern->max_in_degree,
                                      (int) pattern->in_adj_list[dst].size());
    pattern->max_degree = std::max(
        pattern->max_degree, (int) std::max(pattern->adj_list[src].size(),
                                            pattern->adj_list[dst].size()));

    // Parse edge property constraints
    if (edge.HasMember("constraints") && edge["constraints"].IsArray()) {
      pattern->edge_property_constraints[edge_idx] =
          parse_constraints(edge["constraints"]);
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

  output_columns_ = build_pattern_output_columns_from_aliases(
      vertex_aliases_, vertex_labels_, edge_aliases_);
  auto modifiers = parse_pattern_execution_modifiers(
      doc, vertex_aliases_, edge_aliases_, "SAMPLED_PATTERN_MATCH");
  if (!modifiers.has_value()) {
    return nullptr;
  }
  modifiers_ = std::move(*modifiers);

  return pattern;
}

std::string SampledSubgraphMatcher::fetch_and_write_properties() {
  if (!pattern_graph_)
    return "";

  int pattern_vertex_count = pattern_graph_->GetNumVertices();
  int pattern_edge_count = pattern_graph_->GetNumEdges();
  int sample_count = pattern_vertex_count > 0
                         ? (int) sampled_results_.size() / pattern_vertex_count
                         : 0;

  if (sample_count == 0)
    return "";

  // Check if any properties are requested
  bool has_any_props = false;
  for (const auto& props : vertex_required_props_) {
    if (!props.empty()) {
      has_any_props = true;
      break;
    }
  }
  if (!has_any_props) {
    for (const auto& props : edge_required_props_) {
      if (!props.empty()) {
        has_any_props = true;
        break;
      }
    }
  }
  if (!has_any_props)
    return "";

  auto& cache = GraphDataCache::instance();
  auto& cached_data = cache.get_or_create(graph_);
  const auto& schema = graph_.schema();
  auto* read_interface = const_cast<StorageReadInterface*>(&graph_);

  // ---- Precompute property indices for each pattern vertex ----
  struct VertexPropInfo {
    label_t label_id;
    std::vector<std::string> prop_names;
    std::vector<int> prop_indices;
  };
  std::vector<VertexPropInfo> vertex_prop_infos(pattern_vertex_count);

  for (int pv = 0; pv < pattern_vertex_count; pv++) {
    if (pv >= (int) vertex_required_props_.size() ||
        vertex_required_props_[pv].empty())
      continue;

    label_t v_label = pattern_graph_->vertex_label[pv];
    auto all_names = schema.get_vertex_property_names(v_label);

    vertex_prop_infos[pv].label_id = v_label;

    bool want_all = (vertex_required_props_[pv].size() == 1 &&
                     vertex_required_props_[pv][0] == "*");

    if (want_all) {
      vertex_prop_infos[pv].prop_names = all_names;
      for (int j = 0; j < (int) all_names.size(); j++) {
        vertex_prop_infos[pv].prop_indices.push_back(j);
      }
    } else {
      for (const auto& pname : vertex_required_props_[pv]) {
        auto it = std::find(all_names.begin(), all_names.end(), pname);
        if (it != all_names.end()) {
          vertex_prop_infos[pv].prop_names.push_back(pname);
          vertex_prop_infos[pv].prop_indices.push_back(
              std::distance(all_names.begin(), it));
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
  std::vector<EdgePropInfo> edge_prop_infos(pattern_edge_count);

  for (int pe = 0; pe < pattern_edge_count; pe++) {
    if (pe >= (int) edge_required_props_.size() ||
        edge_required_props_[pe].empty())
      continue;

    auto& [src_pv, dst_pv] = pattern_graph_->edge_list[pe];
    label_t src_label = pattern_graph_->vertex_label[src_pv];
    label_t dst_label = pattern_graph_->vertex_label[dst_pv];
    label_t e_label = pattern_graph_->edge_label[pe];

    edge_prop_infos[pe].src_label = src_label;
    edge_prop_infos[pe].dst_label = dst_label;
    edge_prop_infos[pe].edge_label = e_label;

    auto all_names =
        schema.get_edge_property_names(src_label, dst_label, e_label);

    bool want_all = (edge_required_props_[pe].size() == 1 &&
                     edge_required_props_[pe][0] == "*");

    if (want_all) {
      edge_prop_infos[pe].prop_names = all_names;
      for (int j = 0; j < (int) all_names.size(); j++) {
        edge_prop_infos[pe].prop_indices.push_back(j);
      }
    } else {
      for (const auto& pname : edge_required_props_[pe]) {
        auto it = std::find(all_names.begin(), all_names.end(), pname);
        if (it != all_names.end()) {
          edge_prop_infos[pe].prop_names.push_back(pname);
          edge_prop_infos[pe].prop_indices.push_back(
              std::distance(all_names.begin(), it));
        }
      }
    }
  }

  // ================================================================
  // Step 1: Collect all unique vertex IDs and edge keys across all
  //         samples, merging the required property names for each.
  // ================================================================

  // For vertices: global_id -> merged set of required prop names
  // We also need to know which VertexPropInfo to use for fetching
  // (label-based). A given global_id always has one label, so we pick the
  // first matching pattern vertex.
  struct UniqueVertexInfo {
    int global_id;
    label_t label_id;
    std::unordered_set<std::string>
        needed_props;  // union of all pattern positions
    std::vector<std::string> ordered_prop_names;  // resolved after collection
    std::vector<int> ordered_prop_indices;        // resolved after collection
  };
  std::unordered_map<int, UniqueVertexInfo>
      unique_vertices;  // global_id -> info

  struct UniqueEdgeInfo {
    std::string edge_key;  // "src:dst:label"
    label_t src_label, dst_label, edge_label;
    int src_vid, dst_vid;  // local vid for lookup
    std::unordered_set<std::string> needed_props;
    std::vector<std::string> ordered_prop_names;
    std::vector<int> ordered_prop_indices;
  };
  std::unordered_map<std::string, UniqueEdgeInfo>
      unique_edges;  // edge_key -> info

  for (int s = 0; s < sample_count; s++) {
    // Collect unique vertices
    for (int pv = 0; pv < pattern_vertex_count; pv++) {
      if (vertex_prop_infos[pv].prop_names.empty())
        continue;

      int global_id = sampled_results_[s * pattern_vertex_count + pv];
      auto& uv = unique_vertices[global_id];
      if (uv.needed_props.empty() && uv.global_id == 0 && global_id != 0) {
        // First time seeing this vertex
        uv.global_id = global_id;
        uv.label_id = vertex_prop_infos[pv].label_id;
      }
      uv.global_id = global_id;  // always set (handles id=0 edge case)
      uv.label_id = vertex_prop_infos[pv].label_id;
      for (const auto& pname : vertex_prop_infos[pv].prop_names) {
        uv.needed_props.insert(pname);
      }
    }

    // Collect unique edges
    for (int pe = 0; pe < pattern_edge_count; pe++) {
      if (edge_prop_infos[pe].prop_names.empty())
        continue;

      auto& [src_pv, dst_pv] = pattern_graph_->edge_list[pe];
      int src_global = sampled_results_[s * pattern_vertex_count + src_pv];
      int dst_global = sampled_results_[s * pattern_vertex_count + dst_pv];
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

  LOG(INFO) << "[SAMPLED_PATTERN_MATCH] Unique vertices needing props: "
            << unique_vertices.size()
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
    auto all_names = schema.get_edge_property_names(ue.src_label, ue.dst_label,
                                                    ue.edge_label);
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
  std::string props_file = generate_output_file_path("sampled_props") + ".json";
  std::filesystem::create_directories(
      std::filesystem::path(props_file).parent_path());

  std::ofstream ofs(props_file);
  if (!ofs.is_open()) {
    LOG(ERROR) << "[SAMPLED_PATTERN_MATCH] Failed to open props file: "
               << props_file;
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
    uint32_t triplet =
        schema.generate_edge_label(ue.src_label, ue.dst_label, ue.edge_label);
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
    if (pnames.empty())
      continue;
    if (!first_sl)
      ofs << ",";
    first_sl = false;

    std::string label_name = schema.get_vertex_label_name(vlabel);
    auto v_schema = schema.get_vertex_schema(vlabel);

    ofs << "{\"label\":\"" << escape_json_string(label_name)
        << "\",\"properties\":{";
    bool first_prop = true;
    for (const auto& pname : pnames) {
      auto it = std::find(v_schema->property_names.begin(),
                          v_schema->property_names.end(), pname);
      if (it != v_schema->property_names.end()) {
        size_t idx = std::distance(v_schema->property_names.begin(), it);
        if (!first_prop)
          ofs << ",";
        first_prop = false;
        ofs << "\"" << pname << "\":\""
            << data_type_id_to_string(v_schema->property_types[idx].id())
            << "\"";
      }
    }
    ofs << "}}";
  }

  ofs << "],\"edges\":[";
  first_sl = true;
  for (const auto& [triplet, ets] : elabel_props) {
    if (ets.props.empty())
      continue;
    if (!first_sl)
      ofs << ",";
    first_sl = false;

    std::string src_name = schema.get_vertex_label_name(ets.src_label);
    std::string dst_name = schema.get_vertex_label_name(ets.dst_label);
    std::string edge_name = schema.get_edge_label_name(ets.edge_label);
    auto e_schema =
        schema.get_edge_schema(ets.src_label, ets.dst_label, ets.edge_label);

    ofs << "{\"label\":\"" << escape_json_string(edge_name)
        << "\",\"src_label\":\"" << escape_json_string(src_name)
        << "\",\"dst_label\":\"" << escape_json_string(dst_name)
        << "\",\"properties\":{";
    bool first_prop = true;
    for (const auto& pname : ets.props) {
      auto it = std::find(e_schema->property_names.begin(),
                          e_schema->property_names.end(), pname);
      if (it != e_schema->property_names.end()) {
        size_t idx = std::distance(e_schema->property_names.begin(), it);
        if (!first_prop)
          ofs << ",";
        first_prop = false;
        ofs << "\"" << pname << "\":\""
            << data_type_id_to_string(e_schema->properties[idx].id()) << "\"";
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
    if (uv.ordered_prop_names.empty())
      continue;

    auto [label, local_vid] = cached_data.data_meta->ToLocalId(gid);

    if (!first_v)
      ofs << ",\n";
    first_v = false;
    ofs << "{\"id\":" << gid << ",\"props\":{";

    bool first_p = true;
    for (size_t pi = 0; pi < uv.ordered_prop_names.size(); pi++) {
      if (!first_p)
        ofs << ",";
      first_p = false;

      std::string json_val = "null";  // default to null if not found
      if (label == uv.label_id) {
        try {
          execution::Value val = read_interface->GetVertexProperty(
              label, local_vid, uv.ordered_prop_indices[pi]);
          // Use value_to_json_string to preserve proper JSON types
          json_val = value_to_json_string(val);
        } catch (...) { json_val = "null"; }
      }
      ofs << "\"" << uv.ordered_prop_names[pi] << "\":" << json_val;
    }
    ofs << "}}";
  }

  ofs << "\n],\"edges\":[\n";

  // Write each unique edge once
  bool first_e = true;
  for (auto& [key, ue] : unique_edges) {
    if (ue.ordered_prop_names.empty())
      continue;

    if (!first_e)
      ofs << ",\n";
    first_e = false;
    ofs << "{\"id\":\"" << escape_json_string(ue.edge_key) << "\",\"props\":{";

    bool first_p = true;
    for (size_t pi = 0; pi < ue.ordered_prop_names.size(); pi++) {
      if (!first_p)
        ofs << ",";
      first_p = false;

      std::string json_val = "null";  // default to null if not found
      try {
        EdgeDataAccessor accessor = read_interface->GetEdgeDataAccessor(
            ue.src_label, ue.dst_label, ue.edge_label,
            ue.ordered_prop_indices[pi]);
        CsrView view = read_interface->GetGenericOutgoingGraphView(
            ue.src_label, ue.dst_label, ue.edge_label);

        for (auto it = view.get_edges(ue.src_vid).begin();
             it != view.get_edges(ue.src_vid).end(); ++it) {
          if (*it == ue.dst_vid) {
            execution::Value val = accessor.get_data(it);
            // Use value_to_json_string to preserve proper JSON types
            json_val = value_to_json_string(val);
            break;
          }
        }
      } catch (...) { json_val = "null"; }

      ofs << "\"" << ue.ordered_prop_names[pi] << "\":" << json_val;
    }
    ofs << "}}";
  }

  ofs << "\n]}\n";
  ofs.close();

  LOG(INFO) << "[SAMPLED_PATTERN_MATCH] Deduplicated properties written to: "
            << props_file << " (" << unique_vertices.size()
            << " unique vertices, " << unique_edges.size() << " unique edges)";
  return props_file;
}

std::string escape_json_string(const std::string& s) {
  std::string escaped;
  escaped.reserve(s.size() + 8);
  for (char c : s) {
    switch (c) {
    case '"':
      escaped += "\\\"";
      break;
    case '\\':
      escaped += "\\\\";
      break;
    case '\n':
      escaped += "\\n";
      break;
    case '\t':
      escaped += "\\t";
      break;
    case '\r':
      escaped += "\\r";
      break;
    case '\b':
      escaped += "\\b";
      break;
    case '\f':
      escaped += "\\f";
      break;
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

std::string value_to_json_string(const execution::Value& val) {
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
  case DataTypeId::kDouble: {
    std::string num_str = val.to_string();
    // Handle special float values
    if (num_str == "inf" || num_str == "+inf")
      return "\"Infinity\"";
    if (num_str == "-inf")
      return "\"-Infinity\"";
    if (num_str == "nan" || num_str == "NaN")
      return "\"NaN\"";
    return num_str;
  }

  case DataTypeId::kBoolean: {
    // Boolean: output as true/false without quotes
    std::string bool_str = val.to_string();
    return (bool_str == "true" || bool_str == "True" || bool_str == "TRUE" ||
            bool_str == "1")
               ? "true"
               : "false";
  }

  case DataTypeId::kVarchar:
  case DataTypeId::kDate:
  case DataTypeId::kTimestampMs:
  case DataTypeId::kInterval:
  default:
    // String and other types: output as quoted, escaped string
    return "\"" + escape_json_string(val.to_string()) + "\"";
  }
}

bool save_schema_graph(
    const std::unordered_map<
        label_t, std::unordered_map<label_t, std::vector<label_t>>>& sg,
    const std::string& filepath) {
  std::ofstream ofs(filepath, std::ios::binary);
  if (!ofs.is_open())
    return false;

  auto write_int = [&](int32_t v) {
    ofs.write(reinterpret_cast<const char*>(&v), sizeof(v));
  };

  ofs.write(SGCH_MAGIC, 4);
  write_int(SGCH_VERSION);
  write_int(static_cast<int32_t>(sg.size()));
  for (const auto& [src, inner] : sg) {
    write_int(static_cast<int32_t>(src));
    write_int(static_cast<int32_t>(inner.size()));
    for (const auto& [dst, labels] : inner) {
      write_int(static_cast<int32_t>(dst));
      write_int(static_cast<int32_t>(labels.size()));
      for (label_t l : labels)
        write_int(static_cast<int32_t>(l));
    }
  }
  ofs.close();
  return true;
}

bool save_graph_checkpoint(const StorageReadInterface& graph,
                           const std::string& checkpoint_dir) {
  auto& cache = GraphDataCache::instance();
  if (!cache.has_cache(graph)) {
    LOG(WARNING) << "[save_graph_checkpoint] No cached data to save.";
    return false;
  }
  auto& cached_data = cache.get_or_create(graph);

  std::filesystem::create_directories(checkpoint_dir);

  bool ok = true;
  ok = cached_data.data_meta->SaveToFile(checkpoint_dir +
                                         "/data_graph_meta.bin") &&
       ok;
  ok = save_schema_graph(*cached_data.schema_graph,
                         checkpoint_dir + "/schema_graph.bin") &&
       ok;

  if (ok) {
    LOG(INFO) << "[save_graph_checkpoint] Checkpoint saved to: "
              << checkpoint_dir;
  } else {
    LOG(ERROR) << "[save_graph_checkpoint] Failed to save checkpoint to: "
               << checkpoint_dir;
  }
  return ok;
}

bool read_json_uint64(const rapidjson::Value& value, uint64_t* out) {
  if (value.IsUint64()) {
    *out = value.GetUint64();
    return true;
  }
  if (value.IsInt64() && value.GetInt64() >= 0) {
    *out = static_cast<uint64_t>(value.GetInt64());
    return true;
  }
  if (value.IsString()) {
    try {
      std::string raw = value.GetString();
      size_t pos = 0;
      auto parsed = std::stoull(raw, &pos);
      if (pos == raw.size()) {
        *out = parsed;
        return true;
      }
    } catch (...) { return false; }
  }
  return false;
}

function::function_set InitializeGraphFunction::getFunctionSet() {
  function::function_set func_set;

  function::call_output_columns output_cols{
      {"status", common::DataTypeId::kVarchar},
      {"num_vertices", common::DataTypeId::kInt64},
      {"num_edges", common::DataTypeId::kInt64},
      {"max_degree", common::DataTypeId::kInt64},
      {"degeneracy", common::DataTypeId::kInt64}};

  // Overload 1: CALL INITIALIZE() — no checkpoint
  {
    auto func = std::make_unique<function::NeugCallFunction>(
        name, std::vector<common::DataTypeId>{},
        function::call_output_columns(output_cols));

    func->bindFunc =
        [](const Schema& schema, const execution::ContextMeta& ctx_meta,
           const ::physical::PhysicalPlan& plan,
           int op_idx) -> std::unique_ptr<function::CallFuncInputBase> {
      LOG(INFO) << "[INITIALIZE] Bind: no parameters (full initialization)";
      return std::make_unique<InitializeGraphInput>();
    };

    func->execFunc = [](const function::CallFuncInputBase& input,
                        IStorageInterface& graph) -> execution::Context {
      auto& init_input = static_cast<const InitializeGraphInput&>(input);
      LOG(INFO) << "[INITIALIZE] Executing graph initialization...";

      auto* read_interface = dynamic_cast<StorageReadInterface*>(&graph);
      if (!read_interface) {
        LOG(ERROR)
            << "[INITIALIZE] ERROR: graph is not a StorageReadInterface!";
        return execution::Context();
      }

      bool success = do_graph_initialization(*read_interface, true,
                                             init_input.checkpoint_dir);

      auto& cache = GraphDataCache::instance();
      auto& cached_data = cache.get_or_create(*read_interface);

      execution::ValueColumnBuilder<std::string> status_builder;
      status_builder.push_back_opt(success ? std::string("success")
                                           : std::string("failed"));

      execution::ValueColumnBuilder<int64_t> vertices_builder;
      vertices_builder.push_back_opt(
          static_cast<int64_t>(cached_data.data_meta->GetNumVertices()));

      execution::ValueColumnBuilder<int64_t> edges_builder;
      edges_builder.push_back_opt(
          static_cast<int64_t>(cached_data.data_meta->GetNumEdges()));

      execution::ValueColumnBuilder<int64_t> max_degree_builder;
      max_degree_builder.push_back_opt(
          static_cast<int64_t>(cached_data.data_meta->GetMaxDegree()));

      execution::ValueColumnBuilder<int64_t> degeneracy_builder;
      degeneracy_builder.push_back_opt(
          static_cast<int64_t>(cached_data.data_meta->GetDegeneracy()));

      LOG(INFO) << "[INITIALIZE] Initialization "
                << (success ? "successful" : "failed");
      return make_single_chunk_context(
          {status_builder.finish(), vertices_builder.finish(),
           edges_builder.finish(), max_degree_builder.finish(),
           degeneracy_builder.finish()});
    };

    func_set.push_back(std::move(func));
  }

  // Overload 2: CALL INITIALIZE('/path/to/checkpoint') — try loading from
  // checkpoint first
  {
    auto func = std::make_unique<function::NeugCallFunction>(
        name, std::vector<common::DataTypeId>{common::DataTypeId::kVarchar},
        function::call_output_columns(output_cols));

    func->bindFunc =
        [](const Schema& schema, const execution::ContextMeta& ctx_meta,
           const ::physical::PhysicalPlan& plan,
           int op_idx) -> std::unique_ptr<function::CallFuncInputBase> {
      auto& procedure = plan.plan(op_idx).opr().procedure_call();
      std::string checkpoint_dir;
      if (procedure.query().arguments_size() >= 1 &&
          procedure.query().arguments(0).has_const_()) {
        checkpoint_dir = procedure.query().arguments(0).const_().str();
      }
      LOG(INFO) << "[INITIALIZE] Bind: checkpoint_dir = " << checkpoint_dir;
      return std::make_unique<InitializeGraphInput>(std::move(checkpoint_dir));
    };

    func->execFunc = [](const function::CallFuncInputBase& input,
                        IStorageInterface& graph) -> execution::Context {
      auto& init_input = static_cast<const InitializeGraphInput&>(input);
      LOG(INFO) << "[INITIALIZE] Executing with checkpoint_dir: "
                << init_input.checkpoint_dir;

      auto* read_interface = dynamic_cast<StorageReadInterface*>(&graph);
      if (!read_interface) {
        LOG(ERROR)
            << "[INITIALIZE] ERROR: graph is not a StorageReadInterface!";
        return execution::Context();
      }

      bool success = do_graph_initialization(*read_interface, true,
                                             init_input.checkpoint_dir);

      auto& cache = GraphDataCache::instance();
      auto& cached_data = cache.get_or_create(*read_interface);

      execution::ValueColumnBuilder<std::string> status_builder;
      status_builder.push_back_opt(success ? std::string("success")
                                           : std::string("failed"));

      execution::ValueColumnBuilder<int64_t> vertices_builder;
      vertices_builder.push_back_opt(
          static_cast<int64_t>(cached_data.data_meta->GetNumVertices()));

      execution::ValueColumnBuilder<int64_t> edges_builder;
      edges_builder.push_back_opt(
          static_cast<int64_t>(cached_data.data_meta->GetNumEdges()));

      execution::ValueColumnBuilder<int64_t> max_degree_builder;
      max_degree_builder.push_back_opt(
          static_cast<int64_t>(cached_data.data_meta->GetMaxDegree()));

      execution::ValueColumnBuilder<int64_t> degeneracy_builder;
      degeneracy_builder.push_back_opt(
          static_cast<int64_t>(cached_data.data_meta->GetDegeneracy()));

      LOG(INFO) << "[INITIALIZE] Initialization "
                << (success ? "successful" : "failed");
      return make_single_chunk_context(
          {status_builder.finish(), vertices_builder.finish(),
           edges_builder.finish(), max_degree_builder.finish(),
           degeneracy_builder.finish()});
    };

    func_set.push_back(std::move(func));
  }

  return func_set;
}

function::function_set SaveSampledmatchCheckpointFunction::getFunctionSet() {
  function::function_set func_set;

  function::call_output_columns output_cols{
      {"status", common::DataTypeId::kVarchar},
      {"checkpoint_dir", common::DataTypeId::kVarchar}};

  auto func = std::make_unique<function::NeugCallFunction>(
      name, std::vector<common::DataTypeId>{common::DataTypeId::kVarchar},
      std::move(output_cols));

  func->bindFunc =
      [](const Schema& schema, const execution::ContextMeta& ctx_meta,
         const ::physical::PhysicalPlan& plan,
         int op_idx) -> std::unique_ptr<function::CallFuncInputBase> {
    auto& procedure = plan.plan(op_idx).opr().procedure_call();
    std::string checkpoint_dir;
    if (procedure.query().arguments_size() >= 1 &&
        procedure.query().arguments(0).has_const_()) {
      checkpoint_dir = procedure.query().arguments(0).const_().str();
    }
    LOG(INFO) << "[SAVE_SAMPLEDMATCH_CHECKPOINT] Bind: checkpoint_dir = "
              << checkpoint_dir;
    return std::make_unique<SaveSampledmatchCheckpointInput>(
        std::move(checkpoint_dir));
  };

  func->execFunc = [](const function::CallFuncInputBase& input,
                      IStorageInterface& graph) -> execution::Context {
    auto& ckpt_input =
        static_cast<const SaveSampledmatchCheckpointInput&>(input);
    LOG(INFO) << "[SAVE_SAMPLEDMATCH_CHECKPOINT] Saving to: "
              << ckpt_input.checkpoint_dir;

    auto* read_interface = dynamic_cast<StorageReadInterface*>(&graph);
    if (!read_interface) {
      LOG(ERROR) << "[SAVE_SAMPLEDMATCH_CHECKPOINT] ERROR: graph is not a "
                    "StorageReadInterface!";
      return execution::Context();
    }

    bool success =
        save_graph_checkpoint(*read_interface, ckpt_input.checkpoint_dir);

    execution::ValueColumnBuilder<std::string> status_builder;
    status_builder.push_back_opt(success ? std::string("success")
                                         : std::string("failed"));

    execution::ValueColumnBuilder<std::string> dir_builder;
    dir_builder.push_back_opt(std::string(ckpt_input.checkpoint_dir));

    LOG(INFO) << "[SAVE_SAMPLEDMATCH_CHECKPOINT] "
              << (success ? "Success" : "Failed");
    return make_single_chunk_context(
        {status_builder.finish(), dir_builder.finish()});
  };

  func_set.push_back(std::move(func));
  return func_set;
}

std::optional<execution::Value> resolve_exact_order_value(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const ExactPatternSpec& spec, const std::vector<MatchVertex>& match,
    const PatternOrderBySpec& order_by) {
  if (order_by.kind == PatternOutputKind::kVertex) {
    if (order_by.index < 0 ||
        order_by.index >= static_cast<int>(spec.vertices.size()) ||
        order_by.index >= static_cast<int>(match.size())) {
      return std::nullopt;
    }
    const auto& vertex = spec.vertices[order_by.index];
    return get_vertex_property_by_name(graph, data_meta,
                                       static_cast<int>(match[order_by.index]),
                                       vertex.label, order_by.property);
  }

  if (order_by.index < 0 ||
      order_by.index >= static_cast<int>(spec.edges.size())) {
    return std::nullopt;
  }
  const auto& edge = spec.edges[order_by.index];
  if (edge.src >= static_cast<int>(match.size()) ||
      edge.dst >= static_cast<int>(match.size())) {
    return std::nullopt;
  }
  return get_edge_property_by_name(
      graph, data_meta, static_cast<int>(match[edge.src]),
      static_cast<int>(match[edge.dst]), edge.label, order_by.property);
}

std::optional<execution::Value> resolve_sampled_order_value(
    const StorageReadInterface& graph, const DataGraphMeta& data_meta,
    const SampledSubgraphMatcher& matcher, const std::vector<int>& results,
    int pattern_vertex_count,
    const std::vector<std::tuple<int, int, label_t>>& pattern_edges,
    int sample_idx, const PatternOrderBySpec& order_by) {
  if (sample_idx < 0 || pattern_vertex_count <= 0) {
    return std::nullopt;
  }
  const int row_offset = sample_idx * pattern_vertex_count;
  if (row_offset < 0 || row_offset >= static_cast<int>(results.size())) {
    return std::nullopt;
  }
  if (order_by.kind == PatternOutputKind::kVertex) {
    if (order_by.index < 0 || order_by.index >= pattern_vertex_count ||
        row_offset + order_by.index >= static_cast<int>(results.size())) {
      return std::nullopt;
    }
    return get_vertex_property_by_name(
        graph, data_meta, results[row_offset + order_by.index],
        matcher.get_pattern_vertex_label(order_by.index), order_by.property);
  }

  if (order_by.index < 0 ||
      order_by.index >= static_cast<int>(pattern_edges.size())) {
    return std::nullopt;
  }
  auto [src, dst, edge_label] = pattern_edges[order_by.index];
  if (src < 0 || dst < 0 || src >= pattern_vertex_count ||
      dst >= pattern_vertex_count ||
      row_offset + src >= static_cast<int>(results.size()) ||
      row_offset + dst >= static_cast<int>(results.size())) {
    return std::nullopt;
  }
  return get_edge_property_by_name(graph, data_meta, results[row_offset + src],
                                   results[row_offset + dst], edge_label,
                                   order_by.property);
}

function::function_set PatternMatchFunction::getFunctionSet() {
  function::function_set func_set;

  // ---- Overload 1: PATTERN_MATCH(cypher) -> exact, enumerate all ----
  {
    auto func = std::make_unique<function::NeugCallFunction>(
        name, std::vector<common::DataTypeId>{common::DataTypeId::kVarchar});

    auto* table_func = static_cast<function::TableFunction*>(func.get());
    table_func->bindFunc = [](main::ClientContext* /*client_context*/,
                              const function::TableFuncBindInput* input)
        -> std::unique_ptr<function::TableFuncBindData> {
      return bind_pattern_native_output_columns(input, "PATTERN_MATCH");
    };

    func->bindFunc =
        [](const Schema& schema, const execution::ContextMeta& ctx_meta,
           const ::physical::PhysicalPlan& plan,
           int op_idx) -> std::unique_ptr<function::CallFuncInputBase> {
      (void) schema;
      (void) ctx_meta;
      auto& procedure = plan.plan(op_idx).opr().procedure_call();
      std::string pattern_arg;
      if (procedure.query().arguments_size() >= 1 &&
          procedure.query().arguments(0).has_const_()) {
        pattern_arg = procedure.query().arguments(0).const_().str();
      }
      std::string json_file =
          normalize_pattern_input_to_json_file(pattern_arg, "PATTERN_MATCH");
      // No positional limit -> enumerate all matches (limit 0 == unbounded);
      // any in-Cypher LIMIT is still applied by the exact modifier pass.
      return std::make_unique<PatternMatchInput>(std::move(json_file), 0);
    };

    func->execFunc = [](const function::CallFuncInputBase& input,
                        IStorageInterface& graph) -> execution::Context {
      return execute_pattern_match_pipeline(
          static_cast<const PatternMatchInput&>(input), graph);
    };

    func_set.push_back(std::move(func));
  }

  // ---- Overload 2: PATTERN_MATCH(cypher, size>=1, is_sampled) ----
  //   is_sampled = false -> exact with early termination after `size` matches
  //   is_sampled = true  -> sampled (FaSTest) with sample size `size`
  {
    auto func = std::make_unique<function::NeugCallFunction>(
        name, std::vector<common::DataTypeId>{common::DataTypeId::kVarchar,
                                              common::DataTypeId::kInt64,
                                              common::DataTypeId::kBoolean});

    auto* table_func = static_cast<function::TableFunction*>(func.get());
    table_func->bindFunc = [](main::ClientContext* /*client_context*/,
                              const function::TableFuncBindInput* input)
        -> std::unique_ptr<function::TableFuncBindData> {
      // `size` must be a positive integer (>= 1) in both modes: it is the
      // sample size when sampled, and the early-termination bound when exact.
      if (input != nullptr && input->params.size() >= 2) {
        int64_t size = input->getLiteralVal<int64_t>(1);
        if (size < 1) {
          THROW_BINDER_EXCEPTION(
              "[PATTERN_MATCH] size must be >= 1, got " + std::to_string(size) +
              ". Call PATTERN_MATCH(cypher) with no size argument to "
              "enumerate all exact matches.");
        }
      }
      return bind_pattern_native_output_columns(input, "PATTERN_MATCH");
    };

    func->bindFunc =
        [](const Schema& schema, const execution::ContextMeta& ctx_meta,
           const ::physical::PhysicalPlan& plan,
           int op_idx) -> std::unique_ptr<function::CallFuncInputBase> {
      (void) schema;
      (void) ctx_meta;
      auto& procedure = plan.plan(op_idx).opr().procedure_call();
      std::string pattern_arg;
      long long size = 1;
      bool is_sampled = false;
      if (procedure.query().arguments_size() >= 1 &&
          procedure.query().arguments(0).has_const_()) {
        pattern_arg = procedure.query().arguments(0).const_().str();
      }
      if (procedure.query().arguments_size() >= 2 &&
          procedure.query().arguments(1).has_const_()) {
        size = procedure.query().arguments(1).const_().i64();
      }
      if (procedure.query().arguments_size() >= 3 &&
          procedure.query().arguments(2).has_const_()) {
        is_sampled = procedure.query().arguments(2).const_().boolean();
      }
      // Defensive clamp; the bind-time check above already rejects size < 1.
      if (size < 1) {
        size = 1;
      }
      std::string pattern_path =
          normalize_pattern_input_to_json_file(pattern_arg, "PATTERN_MATCH");
      if (is_sampled) {
        // FaSTest sampler; `size` is the sample size.
        return std::make_unique<SampledMatchInput>(pattern_path, size);
      }
      // Exact matching with early termination: `size` caps enumeration so the
      // matcher stops once `size` matches have been found.
      return std::make_unique<PatternMatchInput>(std::move(pattern_path), size);
    };

    func->execFunc = [](const function::CallFuncInputBase& input,
                        IStorageInterface& graph) -> execution::Context {
      // Dispatch on the bound input flavour: SampledMatchInput -> sampler,
      // PatternMatchInput -> exact (early-terminating) matcher.
      if (const auto* sampled =
              dynamic_cast<const SampledMatchInput*>(&input)) {
        return execute_sampled_match_pipeline(*sampled, graph);
      }
      return execute_pattern_match_pipeline(
          static_cast<const PatternMatchInput&>(input), graph);
    };

    func_set.push_back(std::move(func));
  }

  return func_set;
}

function::function_set GetVertexPropertyFunction::getFunctionSet() {
  function::function_set func_set;

  // Output schema: single string column carrying the generated file path.
  function::call_output_columns output_cols{
      {"result_file", common::DataTypeId::kVarchar}};

  auto func = std::make_unique<function::NeugCallFunction>(
      name,
      std::vector<common::DataTypeId>{
          common::DataTypeId::kVarchar,  // vertex_ids as JSON array string
          common::DataTypeId::kVarchar,  // vertex_label
          common::DataTypeId::kVarchar   // property_names as JSON array string
      },
      std::move(output_cols));

  func->bindFunc =
      [](const Schema& schema, const execution::ContextMeta& ctx_meta,
         const ::physical::PhysicalPlan& plan,
         int op_idx) -> std::unique_ptr<function::CallFuncInputBase> {
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
            if (v.IsInt64())
              vertex_ids.push_back(v.GetInt64());
            else if (v.IsInt())
              vertex_ids.push_back(v.GetInt());
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
            if (v.IsString())
              property_names.push_back(v.GetString());
          }
        }
      }
    }

    LOG(INFO) << "[GET_VERTEX_PROPERTY] Bind: " << vertex_ids.size()
              << " vertices, "
              << "label=" << vertex_label << ", " << property_names.size()
              << " properties";

    return std::make_unique<GetVertexPropertyInput>(std::move(vertex_ids),
                                                    std::move(vertex_label),
                                                    std::move(property_names));
  };

  func->execFunc = [](const function::CallFuncInputBase& input,
                      IStorageInterface& graph) -> execution::Context {
    auto& prop_input = static_cast<const GetVertexPropertyInput&>(input);

    auto* read_interface = dynamic_cast<StorageReadInterface*>(&graph);
    if (!read_interface) {
      LOG(ERROR) << "[GET_VERTEX_PROPERTY] ERROR: graph is not a "
                    "StorageReadInterface!";
      return execution::Context();
    }

    auto& cache = GraphDataCache::instance();
    auto& cached_data = cache.get_or_create(*read_interface);
    if (!cached_data.preprocessed) {
      LOG(WARNING) << "[GET_VERTEX_PROPERTY] Cache not preprocessed, calling "
                      "do_graph_initialization...";
      do_graph_initialization(*read_interface, false);
    }

    const auto& schema = read_interface->schema();
    if (!schema.is_vertex_label_valid(prop_input.vertex_label)) {
      LOG(ERROR) << "[GET_VERTEX_PROPERTY] vertex label '"
                 << prop_input.vertex_label << "' not found in schema";
      THROW_EXTENSION_EXCEPTION("[GET_VERTEX_PROPERTY] vertex label '" +
                                prop_input.vertex_label +
                                "' not found in schema");
    }
    label_t vertex_label_id =
        schema.get_vertex_label_id(prop_input.vertex_label);

    int num_vertices = prop_input.vertex_ids.size();
    int num_props = prop_input.property_names.size();

    std::string output_file = generate_output_file_path("vertex_property");

    // Resolve user-facing property names into storage indices; -1 marks
    // properties the schema does not define (written as empty cells).
    std::vector<std::string> all_prop_names =
        schema.get_vertex_property_names(vertex_label_id);
    std::vector<int> prop_indices;
    for (const auto& pname : prop_input.property_names) {
      auto it = std::find(all_prop_names.begin(), all_prop_names.end(), pname);
      if (it != all_prop_names.end()) {
        prop_indices.push_back(std::distance(all_prop_names.begin(), it));
      } else {
        prop_indices.push_back(-1);
      }
    }

    std::ofstream ofs(output_file);
    if (!ofs.is_open()) {
      LOG(ERROR) << "[GET_VERTEX_PROPERTY] Failed to open output file: "
                 << output_file;
      return execution::Context();
    }

    // Header row: vertex_id, prop1, prop2, ...
    ofs << "vertex_id";
    for (const auto& pname : prop_input.property_names) {
      ofs << "," << pname;
    }
    ofs << "\n";

    for (int64_t global_id : prop_input.vertex_ids) {
      ofs << global_id;

      auto [label, local_vid] = cached_data.data_meta->ToLocalId(global_id);

      for (int p = 0; p < num_props; p++) {
        ofs << ",";
        if (label == vertex_label_id && prop_indices[p] >= 0) {
          try {
            execution::Value val = read_interface->GetVertexProperty(
                label, local_vid, prop_indices[p]);
            // Quote and escape per RFC 4180 when the value contains a
            // comma or a quote; otherwise emit verbatim.
            std::string val_str = val.to_string();
            if (val_str.find(',') != std::string::npos ||
                val_str.find('"') != std::string::npos) {
              std::string escaped;
              for (char c : val_str) {
                if (c == '"')
                  escaped += "\"\"";
                else
                  escaped += c;
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
    LOG(INFO) << "[GET_VERTEX_PROPERTY] Results written to: " << output_file;

    execution::ValueColumnBuilder<std::string> file_path_builder;
    file_path_builder.push_back_opt(std::string(output_file));

    LOG(INFO) << "[GET_VERTEX_PROPERTY] Returned file: " << output_file
              << " with " << num_vertices << " vertices, " << num_props
              << " properties";

    return make_single_chunk_context({file_path_builder.finish()});
  };

  func_set.push_back(std::move(func));
  return func_set;
}

function::function_set GetEdgePropertyFunction::getFunctionSet() {
  function::function_set func_set;

  // Output schema: single string column carrying the generated file path.
  function::call_output_columns output_cols{
      {"result_file", common::DataTypeId::kVarchar}};

  auto func = std::make_unique<function::NeugCallFunction>(
      name,
      std::vector<common::DataTypeId>{
          common::DataTypeId::kVarchar,  // edge_keys as JSON array string
          common::DataTypeId::kVarchar,  // edge_label
          common::DataTypeId::kVarchar   // property_names as JSON array string
      },
      std::move(output_cols));

  func->bindFunc =
      [](const Schema& schema, const execution::ContextMeta& ctx_meta,
         const ::physical::PhysicalPlan& plan,
         int op_idx) -> std::unique_ptr<function::CallFuncInputBase> {
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
            if (v.IsString())
              edge_keys.push_back(v.GetString());
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
            if (v.IsString())
              property_names.push_back(v.GetString());
          }
        }
      }
    }

    LOG(INFO) << "[GET_EDGE_PROPERTY] Bind: " << edge_keys.size() << " edges, "
              << "label=" << edge_label << ", " << property_names.size()
              << " properties";

    return std::make_unique<GetEdgePropertyInput>(
        std::move(edge_keys), std::move(edge_label), std::move(property_names));
  };

  func->execFunc = [](const function::CallFuncInputBase& input,
                      IStorageInterface& graph) -> execution::Context {
    auto& prop_input = static_cast<const GetEdgePropertyInput&>(input);

    auto* read_interface = dynamic_cast<StorageReadInterface*>(&graph);
    if (!read_interface) {
      LOG(ERROR) << "[GET_EDGE_PROPERTY] ERROR: graph is not a "
                    "StorageReadInterface!";
      return execution::Context();
    }

    auto& cache = GraphDataCache::instance();
    auto& cached_data = cache.get_or_create(*read_interface);
    if (!cached_data.preprocessed) {
      LOG(WARNING) << "[GET_EDGE_PROPERTY] Cache not preprocessed, calling "
                      "do_graph_initialization...";
      do_graph_initialization(*read_interface, false);
    }

    const auto& schema = read_interface->schema();

    int num_edges = prop_input.edge_keys.size();
    int num_props = prop_input.property_names.size();

    std::string output_file = generate_output_file_path("edge_property");

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
    parsed_edges.reserve(num_edges);

    for (const auto& key : prop_input.edge_keys) {
      ParsedEdge pe;
      pe.key = key;
      pe.valid = false;

      size_t pos1 = key.find(':');
      size_t pos2 = key.rfind(':');
      if (pos1 != std::string::npos && pos2 != std::string::npos &&
          pos1 != pos2) {
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

      for (const auto& pname : prop_input.property_names) {
        auto it =
            std::find(all_prop_names.begin(), all_prop_names.end(), pname);
        if (it != all_prop_names.end()) {
          prop_indices.push_back(std::distance(all_prop_names.begin(), it));
        } else {
          prop_indices.push_back(-1);
        }
      }
    }

    std::ofstream ofs(output_file);
    if (!ofs.is_open()) {
      LOG(ERROR) << "[GET_EDGE_PROPERTY] Failed to open output file: "
                 << output_file;
      return execution::Context();
    }

    // Header row: edge_key, src_id, dst_id, prop1, prop2, ...
    ofs << "edge_key,src_id,dst_id";
    for (const auto& pname : prop_input.property_names) {
      ofs << "," << pname;
    }
    ofs << "\n";

    for (const auto& pe : parsed_edges) {
      ofs << pe.key << "," << pe.src_global << "," << pe.dst_global;

      for (int p = 0; p < num_props; p++) {
        ofs << ",";
        if (pe.valid && has_valid_edge && p < (int) prop_indices.size() &&
            prop_indices[p] >= 0) {
          try {
            EdgeDataAccessor accessor = read_interface->GetEdgeDataAccessor(
                pe.src_label, pe.dst_label, pe.edge_label_id, prop_indices[p]);
            CsrView view = read_interface->GetGenericOutgoingGraphView(
                pe.src_label, pe.dst_label, pe.edge_label_id);

            // Locate the requested dst vertex among the src's out-edges.
            for (auto it = view.get_edges(pe.src_vid).begin();
                 it != view.get_edges(pe.src_vid).end(); ++it) {
              if (*it == pe.dst_vid) {
                execution::Value val = accessor.get_data(it);
                std::string val_str = val.to_string();
                // Quote and escape per RFC 4180 when needed.
                if (val_str.find(',') != std::string::npos ||
                    val_str.find('"') != std::string::npos) {
                  std::string escaped;
                  for (char c : val_str) {
                    if (c == '"')
                      escaped += "\"\"";
                    else
                      escaped += c;
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
    LOG(INFO) << "[GET_EDGE_PROPERTY] Results written to: " << output_file;

    execution::ValueColumnBuilder<std::string> file_path_builder;
    file_path_builder.push_back_opt(std::string(output_file));

    LOG(INFO) << "[GET_EDGE_PROPERTY] Returned file: " << output_file
              << " with " << num_edges << " edges, " << num_props
              << " properties";

    return make_single_chunk_context({file_path_builder.finish()});
  };

  func_set.push_back(std::move(func));
  return func_set;
}

// --- Definitions moved out of pattern_matching_functions.h (decl/impl split)
// ---

execution::Context make_single_chunk_context(
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

CompType parse_operator(const std::string& op_in) {
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

Value create_value_from_rapidjson(const rapidjson::Value& val) {
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

std::vector<PropCons> parse_constraints(
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
    CompType op = parse_operator(op_str);
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
    Value value = c.HasMember("value") ? create_value_from_rapidjson(c["value"])
                                       : Value::INT32(0);

    constraints.emplace_back(prop_name, op, std::move(value));
  }
  return constraints;
}

std::string generate_temp_file_path(const std::string& prefix,
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

std::string generate_output_file_path(const std::string& prefix) {
  return generate_temp_file_path(prefix, ".csv");
}

std::string trim_copy(std::string_view input) {
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

bool read_text_file(const std::string& path, std::string* out) {
  std::ifstream ifs(path);
  if (!ifs.is_open())
    return false;
  std::ostringstream oss;
  oss << ifs.rdbuf();
  *out = oss.str();
  return true;
}

bool write_text_file(const std::string& path, const std::string& text) {
  std::ofstream ofs(path);
  if (!ofs.is_open())
    return false;
  ofs << text;
  return static_cast<bool>(ofs);
}

bool looks_like_json_pattern(std::string_view text) {
  std::string trimmed = trim_copy(text);
  return !trimmed.empty() && trimmed.front() == '{';
}

std::string write_pattern_json_temp_file(const std::string& pattern_json) {
  std::string path =
      generate_temp_file_path("pattern_matching_pattern", ".json");
  if (path.empty())
    return "";
  if (!write_text_file(path, pattern_json)) {
    LOG(ERROR) << "[PATTERN_MATCHING] Failed to write pattern JSON file: "
               << path;
    return "";
  }
  return path;
}

bool read_json_id(const rapidjson::Value& obj, const char* key, int* out) {
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

std::string read_pattern_alias(const rapidjson::Value& obj,
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

std::string make_unique_pattern_alias(
    const std::string& alias, std::unordered_map<std::string, int>* seen) {
  auto [it, inserted] = seen->emplace(alias, 0);
  if (inserted)
    return alias;
  int next = ++it->second;
  return alias + "_" + std::to_string(next);
}

std::vector<std::string> parse_required_props(const rapidjson::Value& obj) {
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

bool is_numeric_value(const execution::Value& value, double* out) {
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

bool check_vertex_constraints(const StorageReadInterface& graph,
                              const DataGraphMeta& data_meta, int global_id,
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
    if (!compare_property_value(actual, constraint._comp_type,
                                constraint._value)) {
      return false;
    }
  }
  return true;
}

std::optional<execution::Value> get_directed_edge_property(
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

std::optional<execution::Value> get_vertex_property_by_name(
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

std::optional<execution::Value> get_edge_property_by_name(
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
  return get_directed_edge_property(graph, data_meta, src_global, dst_global,
                                    edge_label, prop_idx);
}

bool check_edge_constraints(const StorageReadInterface& graph,
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
    auto actual = get_directed_edge_property(graph, data_meta, src_global,
                                             dst_global, spec.label, prop_idx);
    if (!actual.has_value() ||
        !compare_property_value(*actual, constraint._comp_type,
                                constraint._value)) {
      return false;
    }
  }
  return true;
}

bool find_directed_edge_data_ptr(const StorageReadInterface& graph,
                                 const DataGraphMeta& data_meta, int src_global,
                                 int dst_global, label_t edge_label,
                                 const void** data_ptr) {
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

execution::Context make_native_pattern_context(
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
  return make_single_chunk_context(std::move(columns));
}

// --- Class method definitions moved out of the header (decl/impl split) ---

GraphDataCache::CachedData& GraphDataCache::get_or_create(
    const StorageReadInterface& graph) {
  const void* key = key_of(graph);
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

bool GraphDataCache::has_cache(const StorageReadInterface& graph) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = cache_.find(key_of(graph));
  return it != cache_.end() && it->second.preprocessed;
}

label_t SampledSubgraphMatcher::get_pattern_vertex_label(
    int pattern_vertex_idx) const {
  if (!pattern_graph_ || pattern_vertex_idx < 0 ||
      pattern_vertex_idx >= pattern_graph_->GetNumVertices()) {
    return 0;
  }
  return pattern_graph_->vertex_label[pattern_vertex_idx];
}

label_t SampledSubgraphMatcher::get_pattern_edge_label(
    int pattern_edge_idx) const {
  if (!pattern_graph_ || pattern_edge_idx < 0 ||
      pattern_edge_idx >= pattern_graph_->GetNumEdges()) {
    return 0;
  }
  return pattern_graph_->edge_label[pattern_edge_idx];
}

std::string SampledSubgraphMatcher::get_pattern_vertex_label_name(
    int pattern_vertex_idx) const {
  label_t label = get_pattern_vertex_label(pattern_vertex_idx);
  const auto& schema = graph_.schema();
  if (schema.is_vertex_label_valid(label)) {
    return schema.get_vertex_label_name(label);
  }
  return std::to_string(label);
}

std::string SampledSubgraphMatcher::get_pattern_edge_label_name(
    int pattern_edge_idx) const {
  label_t label = get_pattern_edge_label(pattern_edge_idx);
  const auto& schema = graph_.schema();
  if (schema.is_edge_label_valid(label)) {
    return schema.get_edge_label_name(label);
  }
  return std::to_string(label);
}

std::vector<std::tuple<int, int, label_t>>
SampledSubgraphMatcher::get_pattern_edge_list() const {
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

std::string SampledSubgraphMatcher::get_pattern_vertex_alias(
    int pattern_vertex_idx) const {
  if (pattern_vertex_idx < 0 ||
      pattern_vertex_idx >= static_cast<int>(vertex_aliases_.size())) {
    return "v" + std::to_string(pattern_vertex_idx);
  }
  return vertex_aliases_[pattern_vertex_idx];
}

std::string SampledSubgraphMatcher::get_pattern_edge_alias(
    int pattern_edge_idx) const {
  if (pattern_edge_idx < 0 ||
      pattern_edge_idx >= static_cast<int>(edge_aliases_.size())) {
    return "e" + std::to_string(pattern_edge_idx);
  }
  return edge_aliases_[pattern_edge_idx].alias;
}

std::string SampledSubgraphMatcher::get_sampled_edge_key(
    int sample_idx, int pattern_edge_idx) const {
  if (!pattern_graph_ || sample_idx < 0 || pattern_edge_idx < 0)
    return "";
  int pattern_vertex_count = pattern_graph_->GetNumVertices();
  int pattern_edge_count = pattern_graph_->GetNumEdges();
  if (pattern_edge_idx >= pattern_edge_count)
    return "";
  if (sample_idx * pattern_vertex_count >= (int) sampled_results_.size())
    return "";

  auto& [src_pattern, dst_pattern] =
      pattern_graph_->edge_list[pattern_edge_idx];
  label_t edge_label = pattern_graph_->edge_label[pattern_edge_idx];

  int src_global =
      sampled_results_[sample_idx * pattern_vertex_count + src_pattern];
  int dst_global =
      sampled_results_[sample_idx * pattern_vertex_count + dst_pattern];

  return std::to_string(src_global) + ":" + std::to_string(dst_global) + ":" +
         std::to_string(edge_label);
}

std::string SampledSubgraphMatcher::data_type_id_to_string(DataTypeId type) {
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

std::unique_ptr<
    neug::pattern_matching::graphlib::SubgraphMatching::PatternGraph>
SampledSubgraphMatcher::create_pattern_from_json_file(
    const std::string& pattern_file) {
  std::ifstream fin(pattern_file);
  if (!fin.is_open()) {
    LOG(WARNING) << "[SAMPLED_PATTERN_MATCH] Cannot open pattern file: "
                 << pattern_file;
    return nullptr;
  }
  std::stringstream buffer;
  buffer << fin.rdbuf();
  return create_pattern_from_json_text(buffer.str(), pattern_file);
}

}  // namespace pattern_matching
}  // namespace neug
