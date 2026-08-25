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

#include "neug/execution/common/operators/insert/create_edge.h"
#include "neug/common/columns/edge_columns.h"
#include "neug/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/execute/ops/edge_column_rebuild.h"
#include "neug/execution/expression/expr.h"
#include "neug/storages/graph/graph_interface.h"

namespace neug {
namespace execution {
namespace ops {
namespace {

struct CreatedEdgeColumn {
  size_t label_index;
  ContextChunk* chunk;
  std::vector<EdgeRecord> records;
  std::vector<std::pair<int32_t, int32_t>> offsets;
};

}  // namespace

neug::result<Context> CreateEdge::insert_edge(
    StorageInsertInterface& graph, Context&& ctx,
    std::vector<LabelTriplet> labels,
    const std::vector<std::pair<int32_t, int32_t>>& src_dst_tags,
    std::vector<
        std::vector<std::pair<std::string, std::unique_ptr<BindedExprBase>>>>&&
        props,
    const std::vector<int>& alias) {
  const auto& schema = graph.schema();
  auto* update_graph = dynamic_cast<StorageUpdateInterface*>(&graph);
  const std::set<LabelTriplet> affected_labels(labels.begin(), labels.end());
  auto snapshots =
      update_graph == nullptr
          ? EdgeColumnSnapshots{}
          : CaptureEdgeColumnsForRefresh(*update_graph, ctx, affected_labels);
  std::vector<CreatedEdgeColumn> created_columns;
  created_columns.reserve(labels.size() * ctx.chunk_num());

  for (auto& chunk : ctx.chunks()) {
    for (size_t i = 0; i < labels.size(); ++i) {
      label_t src_label = labels[i].src_label;
      label_t dst_label = labels[i].dst_label;
      label_t edge_label = labels[i].edge_label;
      auto& properties = props[i];
      auto properties_name =
          schema.get_edge_property_names(src_label, dst_label, edge_label);
      auto properties_type =
          schema.get_edge_properties(src_label, dst_label, edge_label);
      const auto& default_values = schema.get_edge_default_property_values(
          src_label, dst_label, edge_label);
      assert(properties_name.size() == properties_type.size() &&
             properties_name.size() == default_values.size());
      if (properties.size() != properties_name.size()) {
        THROW_RUNTIME_ERROR("Provided properties size " +
                            std::to_string(properties.size()) +
                            " does not match schema size: " +
                            std::to_string(properties_name.size()));
      }
      const auto& src_vertex_col = dynamic_cast<const IVertexColumn&>(
          *chunk.get(src_dst_tags[i].first).get());
      const auto& dst_vertex_col = dynamic_cast<const IVertexColumn&>(
          *chunk.get(src_dst_tags[i].second).get());
      CreatedEdgeColumn created{.label_index = i, .chunk = &chunk};
      created.records.reserve(chunk.row_num());
      if (update_graph != nullptr) {
        created.offsets.reserve(chunk.row_num());
      }
      for (size_t row = 0; row < chunk.row_num(); ++row) {
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
          Value value =
              prop_expr->Cast<RecordExprBase>().eval_record(chunk.chunk(), row);
          auto it = std::find(properties_name.begin(), properties_name.end(),
                              prop_name);
          if (it == properties_name.end()) {
            THROW_RUNTIME_ERROR(
                "Property " + prop_name + " not found in schema for edge (" +
                std::to_string(src_label) + "," + std::to_string(edge_label) +
                "," + std::to_string(dst_label) + ")");
          } else {
            size_t index = std::distance(properties_name.begin(), it);
            if (value.IsNull()) {
              property_values[index] = default_values[index];
            } else {
              if (properties_type[index] != value.type()) {
                THROW_RUNTIME_ERROR("Property type mismatch for property " +
                                    prop_name);
              }
              property_values[index] = value;
            }
          }
        }
        const void* edge_prop = nullptr;
        auto add_status = graph.AddEdge(src_label, v1.vid_, dst_label, v2.vid_,
                                        edge_label, property_values, edge_prop);
        if (!add_status.ok()) {
          THROW_RUNTIME_ERROR(
              "Failed to add edge (" + std::to_string(src_label) + "," +
              std::to_string(edge_label) + "," + std::to_string(dst_label) +
              "): " + add_status.ToString());
        }
        EdgeRecord record;
        record.label = labels[i];
        record.dir = Direction::kOut;
        record.src = v1.vid_;
        record.dst = v2.vid_;
        record.prop = edge_prop;
        if (update_graph != nullptr) {
          created.offsets.push_back(ResolveEdgeOffsets(*update_graph, record));
        }
        created.records.push_back(record);
      }
      created_columns.push_back(std::move(created));
    }
  }

  if (update_graph != nullptr) {
    RefreshEdgeColumns(*update_graph, snapshots);
  }
  for (auto& created : created_columns) {
    SDSLEdgeColumnBuilder builder(Direction::kOut, labels[created.label_index]);
    builder.reserve(created.records.size());
    for (size_t row = 0; row < created.records.size(); ++row) {
      auto& record = created.records[row];
      if (update_graph != nullptr) {
        RefreshEdgeRecord(*update_graph, record, created.offsets[row]);
      }
      builder.push_back_opt(record.src, record.dst, record.prop);
    }
    created.chunk->set(alias[created.label_index], builder.finish());
  }
  return ctx;
}
}  // namespace ops
}  // namespace execution
}  // namespace neug
