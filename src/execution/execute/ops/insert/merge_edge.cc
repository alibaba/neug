/** Copyright 2020 Alibaba Group Holding Limited.
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

#include "neug/execution/execute/ops/insert/merge_edge.h"

#include <glog/logging.h>
#include <algorithm>
#include <optional>
#include <set>
#include <utility>
#include <vector>

#include "neug/common/columns/edge_columns.h"
#include "neug/common/columns/vertex_columns.h"
#include "neug/common/types/graph_types.h"
#include "neug/common/types/value.h"
#include "neug/execution/common/context.h"
#include "neug/execution/execute/ops/edge_column_rebuild.h"
#include "neug/execution/expression/expr.h"
#include "neug/generated/proto/plan/cypher_dml.pb.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/result.h"

namespace neug {
namespace execution {
namespace ops {

namespace {

std::vector<std::pair<std::string, std::unique_ptr<ExprBase>>> parse_mappings(
    const google::protobuf::RepeatedPtrField<::physical::PropertyMapping>&
        mappings,
    const ContextMeta& ctx_meta) {
  std::vector<std::pair<std::string, std::unique_ptr<ExprBase>>> props;
  for (const auto& prop : mappings) {
    if (!prop.has_property()) {
      LOG(FATAL) << "PropertyMapping has no property: " << prop.DebugString();
    }
    if (!prop.has_data()) {
      LOG(FATAL) << "PropertyMapping has no data: " << prop.DebugString();
    }
    props.emplace_back(
        prop.property().key().name(),
        parse_expression(prop.data(), ctx_meta, VarType::kRecord));
  }
  return props;
}

std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>
merge_pattern_and_on_create(
    std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>
        pattern,
    std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>
        on_create) {
  flat_hash_map<std::string, size_t> pos;
  std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>> merged;
  merged.reserve(pattern.size() + on_create.size());
  for (auto& p : pattern) {
    pos[p.first] = merged.size();
    merged.push_back(std::move(p));
  }
  for (auto& o : on_create) {
    auto it = pos.find(o.first);
    if (it != pos.end()) {
      merged[it->second].second = std::move(o.second);
    } else {
      pos[o.first] = merged.size();
      merged.push_back(std::move(o));
    }
  }
  return merged;
}

struct EdgePropertyMutation {
  EdgeRecord record;
  std::pair<int32_t, int32_t> offsets;
  int property_id;
  Value value;
};

struct PreparedEdgeInsert {
  vid_t src;
  vid_t dst;
  std::vector<Value> properties;
};

PreparedEdgeInsert prepare_edge_insert(
    const StorageUpdateInterface& graph, DataChunk& chunk, size_t row,
    label_t src_label, label_t dst_label, label_t edge_label,
    const IVertexColumn& src_vertex_col, const IVertexColumn& dst_vertex_col,
    const std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>&
        properties) {
  const auto& schema = graph.schema();
  auto properties_name =
      schema.get_edge_property_names(src_label, dst_label, edge_label);
  auto properties_type =
      schema.get_edge_properties(src_label, dst_label, edge_label);
  const auto& default_values =
      schema.get_edge_default_property_values(src_label, dst_label, edge_label);
  if (properties.size() != properties_name.size()) {
    THROW_RUNTIME_ERROR("Provided properties size " +
                        std::to_string(properties.size()) +
                        " does not match schema size: " +
                        std::to_string(properties_name.size()));
  }
  auto v1 = src_vertex_col.get_vertex(row);
  if (v1.label_ != src_label) {
    THROW_RUNTIME_ERROR("Source vertex label mismatch: expected " +
                        std::to_string(src_label) + ", got " +
                        std::to_string(v1.label_));
  }
  auto v2 = dst_vertex_col.get_vertex(row);
  if (v2.label_ != dst_label) {
    THROW_RUNTIME_ERROR("Destination vertex label mismatch: expected " +
                        std::to_string(dst_label) + ", got " +
                        std::to_string(v2.label_));
  }
  std::vector<Value> property_values(properties.size());
  for (size_t j = 0; j < properties.size(); ++j) {
    const auto& [prop_name, prop_expr] = properties[j];
    Value value = prop_expr->Cast<RecordExprBase>().eval_record(chunk, row);
    auto it =
        std::find(properties_name.begin(), properties_name.end(), prop_name);
    if (it == properties_name.end()) {
      THROW_RUNTIME_ERROR(
          "Property " + prop_name + " not found in schema for edge (" +
          std::to_string(src_label) + "," + std::to_string(edge_label) + "," +
          std::to_string(dst_label) + ")");
    }
    size_t index = std::distance(properties_name.begin(), it);
    if (value.IsNull()) {
      property_values[index] = default_values[index];
    } else {
      if (properties_type[index] != value.type()) {
        THROW_RUNTIME_ERROR("Property type mismatch for property " + prop_name);
      }
      property_values[index] = value;
    }
  }
  return PreparedEdgeInsert{v1.vid_, v2.vid_, std::move(property_values)};
}

EdgeRecord apply_edge_insert(StorageUpdateInterface& graph,
                             const LabelTriplet& labels,
                             const PreparedEdgeInsert& insert) {
  const void* edge_prop = nullptr;
  auto add_status =
      graph.AddEdge(labels.src_label, insert.src, labels.dst_label, insert.dst,
                    labels.edge_label, insert.properties, edge_prop);
  if (!add_status.ok()) {
    THROW_RUNTIME_ERROR(
        "Failed to add edge (" + std::to_string(labels.src_label) + "," +
        std::to_string(labels.edge_label) + "," +
        std::to_string(labels.dst_label) + "): " + add_status.ToString());
  }
  return EdgeRecord{labels, insert.src, insert.dst, edge_prop};
}

struct EdgeEntryPlan {
  LabelTriplet labels;
  int32_t alias_id;
  std::pair<int32_t, int32_t> src_dst_tags;
  std::vector<std::pair<std::string, std::unique_ptr<ExprBase>>> pattern_props;
  std::vector<std::pair<std::string, std::unique_ptr<ExprBase>>>
      on_create_props;
  std::vector<std::pair<std::string, std::unique_ptr<ExprBase>>> on_match_props;
};

class MergeEdgeOpr : public IOperator {
 public:
  explicit MergeEdgeOpr(std::vector<EdgeEntryPlan>&& entries)
      : entries_(std::move(entries)) {}

  std::string get_operator_name() const override { return "MergeEdgeOpr"; }

  neug::result<Context> Eval(IStorageInterface& graph_interface,
                             const ParamsMap& params, Context&& ctx,
                             OprTimer* timer) override {
    (void) timer;
    auto& graph = dynamic_cast<StorageUpdateInterface&>(graph_interface);
    const StorageReadInterface* graph_read = nullptr;
    if (graph_interface.readable()) {
      graph_read = dynamic_cast<const StorageReadInterface*>(&graph_interface);
    }

    for (const auto& plan : entries_) {
      std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>
          pattern_binded;
      std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>
          on_create_binded;
      std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>
          on_match_binded;
      for (const auto& [n, e] : plan.pattern_props) {
        pattern_binded.emplace_back(n, e->bind(graph_read, params));
      }
      for (const auto& [n, e] : plan.on_create_props) {
        on_create_binded.emplace_back(n, e->bind(graph_read, params));
      }
      for (const auto& [n, e] : plan.on_match_props) {
        on_match_binded.emplace_back(n, e->bind(graph_read, params));
      }
      auto merged_binded = merge_pattern_and_on_create(
          std::move(pattern_binded), std::move(on_create_binded));

      const auto edge_schema = graph.schema().get_edge_schema(
          plan.labels.src_label, plan.labels.dst_label, plan.labels.edge_label);
      const bool bundled = edge_schema->is_bundled();
      bool has_unmatched_rows = false;
      if (bundled && on_match_binded.empty()) {
        for (auto& chunk : ctx.chunks()) {
          if (chunk.row_num() == 0) {
            continue;
          }
          auto edge_column =
              std::dynamic_pointer_cast<IEdgeColumn>(chunk.get(plan.alias_id));
          if (!edge_column) {
            continue;
          }
          for (size_t row = 0; row < edge_column->size(); ++row) {
            if (!edge_column->has_value(row) ||
                edge_column->get_edge(row).label != plan.labels) {
              has_unmatched_rows = true;
              break;
            }
          }
          if (has_unmatched_rows) {
            break;
          }
        }
      }
      std::set<LabelTriplet> affected_labels;
      if (bundled && (!on_match_binded.empty() || has_unmatched_rows)) {
        affected_labels.insert(plan.labels);
      }
      auto snapshots =
          CaptureEdgeColumnsForRefresh(graph, ctx, affected_labels);

      struct PendingRow {
        EdgeRecord record;
        std::optional<PreparedEdgeInsert> insert;
      };
      struct MatchChunk {
        ContextChunk* chunk;
        const EdgeColumnSnapshot* alias_snapshot;
        std::vector<PendingRow> rows;
      };
      std::vector<MatchChunk> matched_chunks;
      for (auto& chunk : ctx.chunks()) {
        const auto nrows = chunk.row_num();
        MatchChunk matched_chunk{&chunk, nullptr,
                                 std::vector<PendingRow>(nrows)};
        if (nrows == 0) {
          matched_chunks.push_back(std::move(matched_chunk));
          continue;
        }
        if (!chunk.exist(plan.alias_id)) {
          THROW_RUNTIME_ERROR(
              "MERGE edge requires the pattern edge alias in context "
              "(missing column for alias id " +
              std::to_string(plan.alias_id) + ")");
        }
        auto alias_column = chunk.get(plan.alias_id);
        if (!alias_column || alias_column->size() != nrows) {
          THROW_RUNTIME_ERROR(
              "MERGE edge alias column size does not match "
              "context row count");
        }
        auto edge_column = std::dynamic_pointer_cast<IEdgeColumn>(alias_column);
        if (!edge_column) {
          THROW_RUNTIME_ERROR(
              "MERGE edge pattern alias must refer to an edge column (alias "
              "id " +
              std::to_string(plan.alias_id) + ")");
        }
        matched_chunk.alias_snapshot = snapshots.Find(edge_column.get());
        const auto& src_vertex_col = dynamic_cast<const IVertexColumn&>(
            *chunk.get(plan.src_dst_tags.first).get());
        const auto& dst_vertex_col = dynamic_cast<const IVertexColumn&>(
            *chunk.get(plan.src_dst_tags.second).get());
        for (size_t row = 0; row < nrows; ++row) {
          auto& pending = matched_chunk.rows[row];
          bool matched = false;
          if (edge_column->has_value(row)) {
            pending.record = matched_chunk.alias_snapshot == nullptr
                                 ? edge_column->get_edge(row)
                                 : matched_chunk.alias_snapshot->records[row];
            matched = pending.record.label == plan.labels;
          }
          if (!matched) {
            pending.insert = prepare_edge_insert(
                graph, chunk.chunk(), row, plan.labels.src_label,
                plan.labels.dst_label, plan.labels.edge_label, src_vertex_col,
                dst_vertex_col, merged_binded);
          }
        }
        matched_chunks.push_back(std::move(matched_chunk));
      }

      for (const auto& [prop_name, expression] : on_match_binded) {
        const auto property_id = edge_schema->get_property_index(prop_name);
        if (property_id < 0) {
          THROW_RUNTIME_ERROR(
              "Property " + prop_name + " does not exist for edge label " +
              std::to_string(static_cast<int>(plan.labels.edge_label)));
        }
        std::vector<EdgePropertyMutation> mutations;
        for (auto& matched_chunk : matched_chunks) {
          auto& chunk = *matched_chunk.chunk;
          for (size_t row = 0; row < matched_chunk.rows.size(); ++row) {
            auto& pending = matched_chunk.rows[row];
            if (pending.insert) {
              continue;
            }
            std::pair<int32_t, int32_t> offsets;
            if (matched_chunk.alias_snapshot != nullptr &&
                matched_chunk.alias_snapshot->refresh_rows[row]) {
              pending.record = matched_chunk.alias_snapshot->records[row];
              offsets = matched_chunk.alias_snapshot->offsets[row];
            } else {
              offsets = ResolveEdgeOffsets(graph, pending.record);
            }
            auto value = expression->Cast<RecordExprBase>().eval_record(
                chunk.chunk(), row);
            if (edge_schema->properties[property_id] != value.type()) {
              THROW_RUNTIME_ERROR("Property type mismatch for property " +
                                  prop_name);
            }
            mutations.push_back(EdgePropertyMutation{
                pending.record, offsets, property_id, std::move(value)});
          }
        }
        for (const auto& mutation : mutations) {
          auto status = graph.UpdateEdgeProperty(
              mutation.record.label.src_label, mutation.record.src,
              mutation.record.label.dst_label, mutation.record.dst,
              mutation.record.label.edge_label, mutation.offsets.first,
              mutation.offsets.second, mutation.property_id, mutation.value);
          if (!status.ok()) {
            THROW_RUNTIME_ERROR(status.ToString());
          }
        }
        if (bundled && !mutations.empty()) {
          RefreshEdgeColumns(graph, snapshots);
        }
      }

      for (auto& matched_chunk : matched_chunks) {
        for (auto& pending : matched_chunk.rows) {
          if (!pending.insert) {
            continue;
          }
          pending.record =
              apply_edge_insert(graph, plan.labels, *pending.insert);
        }
      }

      RefreshEdgeColumns(graph, snapshots);

      for (auto& matched_chunk : matched_chunks) {
        auto& chunk = *matched_chunk.chunk;
        SDSLEdgeColumnBuilder builder(Direction::kOut, plan.labels);
        for (size_t row = 0; row < matched_chunk.rows.size(); ++row) {
          auto& pending = matched_chunk.rows[row];
          if (!pending.insert && matched_chunk.alias_snapshot != nullptr &&
              matched_chunk.alias_snapshot->refresh_rows[row]) {
            pending.record = matched_chunk.alias_snapshot->records[row];
          } else if (pending.insert) {
            RefreshEdgeRecord(graph, pending.record,
                              ResolveEdgeOffsets(graph, pending.record));
          }
          builder.push_back_opt(pending.record.src, pending.record.dst,
                                pending.record.prop);
        }
        if (chunk.exist(plan.alias_id)) {
          chunk.remove(plan.alias_id);
        }
        chunk.set(plan.alias_id, builder.finish());
      }
    }
    return std::move(ctx);
  }

 private:
  std::vector<EdgeEntryPlan> entries_;
};

}  // namespace

neug::result<OpBuildResultT> MergeEdgeOprBuilder::Build(
    const Schema& schema, const ContextMeta& ctx_meta,
    const physical::PhysicalPlan& plan, int op_idx) {
  ContextMeta ret_meta = ctx_meta;
  const auto& opr = plan.plan(op_idx).opr().merge_edge();
  std::vector<EdgeEntryPlan> entries;
  for (const auto& edge : opr.entries()) {
    if (edge.alias().item_case() != common::NameOrId::ItemCase::kId) {
      THROW_RUNTIME_ERROR(
          "MERGE edge physical plan entry must include edge pattern alias id");
    }
    EdgeEntryPlan e;
    e.labels.src_label = edge.edge_type().src_type_name().id();
    e.labels.dst_label = edge.edge_type().dst_type_name().id();
    e.labels.edge_label = edge.edge_type().type_name().id();
    e.alias_id = edge.alias().id();
    e.src_dst_tags = {edge.source_vertex_binding().id(),
                      edge.destination_vertex_binding().id()};
    ret_meta.set(e.alias_id, DataType::EDGE);
    e.pattern_props = parse_mappings(edge.property_mappings(), ctx_meta);
    e.on_create_props = parse_mappings(edge.on_create(), ctx_meta);
    e.on_match_props = parse_mappings(edge.on_match(), ctx_meta);
    entries.push_back(std::move(e));
  }
  return std::make_pair(std::make_unique<MergeEdgeOpr>(std::move(entries)),
                        ret_meta);
}

}  // namespace ops
}  // namespace execution
}  // namespace neug
