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

#include "label_propagation.h"

#include <string>
#include <unordered_map>
#include <unordered_set>

#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/expression/expr.h"
#include "neug/execution/expression/predicates.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace gds {

namespace {

size_t GetMaxIterations(const function::options_t& options) {
  constexpr size_t kDefaultMaxIterations = 10;
  auto it = options.find("maxIterations");
  if (it == options.end() || it->second.empty()) {
    return kDefaultMaxIterations;
  }
  try {
    auto value = std::stoll(it->second);
    if (value <= 0) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "label_propagation maxIterations must be greater than 0.");
    }
    return static_cast<size_t>(value);
  } catch (const std::invalid_argument&) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "label_propagation maxIterations must be an integer.");
  } catch (const std::out_of_range&) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "label_propagation maxIterations is out of range.");
  }
}

}  // namespace

execution::Context LabelPropagationFunction::LabelPropagationExec(
    execution::Context& ctx, const ::physical::Subgraph& subgraph,
    const function::options_t& options, IStorageInterface& g) {
  const auto& graph = dynamic_cast<const StorageReadInterface&>(g);
  const auto max_iterations = GetMaxIterations(options);

  std::vector<std::vector<vid_t>> vertices_by_label;
  std::unordered_set<label_t> vertex_labels;
  std::unordered_map<execution::VertexRecord, size_t> vertex_record_to_label_id;
  size_t next_label_id = 0;
  for (int i = 0; i < subgraph.vertex_entries_size(); ++i) {
    const auto& vertex_entry = subgraph.vertex_entries(i);
    std::vector<vid_t> vertex_ids;
    label_t label = static_cast<label_t>(vertex_entry.label_id());
    auto vertex_set = graph.GetVertexSet(label);

    execution::ContextMeta ctx_meta;
    auto expr = execution::parse_expression(vertex_entry.predicate(), ctx_meta,
                                            execution::VarType::kVertex);
    auto binded_expr = expr->bind(&graph, {});
    execution::GeneralPred general_pred(std::move(binded_expr));
    for (vid_t v : vertex_set) {
      if (general_pred(label, v)) {
        vertex_ids.push_back(v);
        vertex_record_to_label_id.emplace(execution::VertexRecord(label, v),
                                          next_label_id++);
      }
    }
    if (vertex_ids.empty()) {
      continue;
    }
    if (label >= vertices_by_label.size()) {
      vertices_by_label.resize(label + 1);
    }
    vertex_labels.insert(label);

    vertices_by_label[label] = std::move(vertex_ids);
  }
  std::vector<execution::LabelTriplet> edge_triplets;
  std::vector<execution::GeneralPred> edge_preds;

  for (int i = 0; i < subgraph.edge_entries_size(); ++i) {
    const auto& edge_entry = subgraph.edge_entries(i);
    label_t src_label = static_cast<label_t>(edge_entry.src_label_id());
    label_t dst_label = static_cast<label_t>(edge_entry.dst_label_id());
    if (vertex_labels.find(src_label) == vertex_labels.end() ||
        vertex_labels.find(dst_label) == vertex_labels.end()) {
      continue;
    }
    label_t edge_label = static_cast<label_t>(edge_entry.edge_label_id());
    edge_triplets.emplace_back(src_label, dst_label, edge_label);

    execution::ContextMeta ctx_meta;

    auto expr = execution::parse_expression(edge_entry.predicate(), ctx_meta,
                                            execution::VarType::kEdge);
    auto binded_expr = expr->bind(&graph, {});
    edge_preds.emplace_back(std::move(binded_expr));
  }
  for (size_t iteration = 0; iteration < max_iterations; ++iteration) {
    bool updated = false;
    for (label_t dst_label = 0; dst_label < vertices_by_label.size();
         ++dst_label) {
      if (vertices_by_label[dst_label].empty()) {
        continue;
      }
      std::map<vid_t, std::vector<size_t>> neighbor_labels;
      for (size_t edge_id = 0; edge_id < edge_triplets.size(); ++edge_id) {
        const auto& triplet = edge_triplets[edge_id];
        if (triplet.dst_label != dst_label) {
          continue;
        }
        const auto& edge_pred = edge_preds[edge_id];
        const auto& ie_view = graph.GetGenericIncomingGraphView(
            triplet.dst_label, triplet.src_label, triplet.edge_label);
        for (vid_t dst_vid : vertices_by_label[dst_label]) {
          auto edges = ie_view.get_edges(dst_vid);
          for (auto it = edges.begin(); it != edges.end(); ++it) {
            const auto src_vid = it.get_vertex();
            if (edge_pred(triplet, src_vid, dst_vid, it.get_data_ptr())) {
              auto found = vertex_record_to_label_id.find(
                  execution::VertexRecord(triplet.src_label, src_vid));
              if (found != vertex_record_to_label_id.end()) {
                neighbor_labels[dst_vid].push_back(found->second);
              }
            }
          }
        }
      }
      for (vid_t dst_vid : vertices_by_label[dst_label]) {
        if (!neighbor_labels[dst_vid].empty()) {
          size_t most_frequent_label_id = 0;
          std::unordered_map<size_t, size_t> label_count;
          for (size_t label_id : neighbor_labels[dst_vid]) {
            ++label_count[label_id];
            if (label_count[label_id] > label_count[most_frequent_label_id]) {
              most_frequent_label_id = label_id;
            } else if (label_count[label_id] ==
                           label_count[most_frequent_label_id] &&
                       label_id < most_frequent_label_id) {
              most_frequent_label_id = label_id;
            }
          }
          if (most_frequent_label_id !=
              vertex_record_to_label_id[execution::VertexRecord(dst_label,
                                                                dst_vid)]) {
            vertex_record_to_label_id[execution::VertexRecord(
                dst_label, dst_vid)] = most_frequent_label_id;
            updated = true;
          }
        }
      }
    }
    if (!updated) {
      break;
    }
  }

  ctx.clear();
  execution::MSVertexColumnBuilder node_builder(*vertex_labels.begin());
  execution::ValueColumnBuilder<int64_t> label_builder;
  node_builder.reserve(next_label_id);
  label_builder.reserve(next_label_id);

  for (label_t label = 0; label < vertices_by_label.size(); ++label) {
    if (vertices_by_label[label].empty()) {
      continue;
    }
    node_builder.start_label(label);
    for (vid_t vid : vertices_by_label[label]) {
      node_builder.push_back_opt(vid);
      label_builder.push_back_opt(static_cast<int64_t>(
          vertex_record_to_label_id.at(execution::VertexRecord(label, vid))));
    }
  }

  ctx.set(0, node_builder.finish());
  ctx.set(1, label_builder.finish());
  ctx.tag_ids = {0, 1};
  return ctx;
}
}  // namespace gds
}  // namespace neug
