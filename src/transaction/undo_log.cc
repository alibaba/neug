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

#include "neug/transaction/undo_log.h"
#include "neug/storages/csr/generic_view_utils.h"

namespace neug {
void CreateVertexTypeUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  graph.DeleteVertexType(vertex_type);
};

void CreateEdgeTypeUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  graph.DeleteEdgeType(src_type, dst_type, edge_type);
};

void InsertVertexUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  assert(graph.schema().vertex_label_valid(v_label));
  graph.DeleteVertex(v_label, vid, ts);
};

void InsertEdgeUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  assert(graph.schema().exist(src_label, dst_label, edge_label));
  graph.DeleteEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                   oe_offset, ie_offset, ts);
};

void UpdateVertexPropUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  assert(graph.schema().vertex_label_valid(v_label));
  graph.UpdateVertexProperty(v_label, vid, col_id, value, ts);
};

void UpdateEdgePropUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  assert(graph.schema().exist(src_label, dst_label, edge_label));
  graph.UpdateEdgeProperty(src_label, src_lid, dst_label, dst_lid, edge_label,
                           oe_offset, ie_offset, col_id, value, ts);
};

void RemoveVertexUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  assert(graph.schema().vertex_label_valid(v_label));
  graph.get_vertex_table(v_label).RevertDeleteVertex(lid, ts);
  for (const auto& [edge_triplet_id, edge_vec] : related_edges) {
    auto [src_label, dst_label, edge_label] =
        graph.schema().parse_edge_label(edge_triplet_id);
    auto& edge_table = graph.get_edge_table(src_label, dst_label, edge_label);
    for (const auto& edge : edge_vec)  // <src, dst, oe_offset, ie_offset>
      edge_table.RevertDeleteEdge(std::get<0>(edge), std::get<1>(edge),
                                  std::get<2>(edge), std::get<3>(edge), ts);
  }
};

void RemoveEdgeUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  assert(graph.schema().exist(src_label, dst_label, edge_label));
  graph.get_edge_table(src_label, dst_label, edge_label)
      .RevertDeleteEdge(src_lid, dst_lid, oe_offset, ie_offset, ts);
};

void AddVertexPropUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  if (!graph.schema().vertex_label_valid(label)) {
    THROW_INTERNAL_EXCEPTION("Vertex label  not found in schema: " +
                             std::to_string(label));
  }
  auto label_name = graph.schema().get_vertex_label_name(label);
  auto status = graph.DeleteVertexProperties(label_name, prop_names);
  if (!status.ok()) {
    THROW_RUNTIME_ERROR("Failed to undo AddVertexProp for label " + label_name +
                        ": " + status.error_message());
  }
};

void AddEdgePropUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  if (!graph.schema().exist(src_label, dst_label, edge_label)) {
    THROW_INTERNAL_EXCEPTION(
        "Edge label not found in schema: " + std::to_string(src_label) + "->" +
        std::to_string(dst_label) + ":" + std::to_string(edge_label));
  }
  auto src_label_name = graph.schema().get_vertex_label_name(src_label);
  auto dst_label_name = graph.schema().get_vertex_label_name(dst_label);
  auto edge_label_name = graph.schema().get_edge_label_name(edge_label);
  auto status = graph.DeleteEdgeProperties(src_label_name, dst_label_name,
                                           edge_label_name, prop_names);
  if (!status.ok()) {
    THROW_RUNTIME_ERROR("Failed to undo AddEdgeProp for edge " +
                        src_label_name + "->" + dst_label_name + ":" +
                        edge_label_name + ": " + status.error_message());
  }
};

void RenameVertexPropUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  if (!graph.schema().vertex_label_valid(label)) {
    THROW_INTERNAL_EXCEPTION("Vertex label  not found in schema: " +
                             std::to_string(label));
  }
  auto label_name = graph.schema().get_vertex_label_name(label);
  std::vector<std::pair<std::string, std::string>> new_names_to_old_names;
  for (const auto& pair : old_names_to_new_names) {
    new_names_to_old_names.emplace_back(pair.second, pair.first);
  }
  auto status =
      graph.RenameVertexProperties(label_name, new_names_to_old_names);
  if (!status.ok()) {
    THROW_RUNTIME_ERROR("Failed to undo RenameVertexProp for label " +
                        label_name + ": " + status.error_message());
  }
};

void RenameEdgePropUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  if (!graph.schema().exist(src_label, dst_label, edge_label)) {
    THROW_INTERNAL_EXCEPTION(
        "Edge label not found in schema: " + std::to_string(src_label) + "->" +
        std::to_string(dst_label) + ":" + std::to_string(edge_label));
  }
  auto src_label_name = graph.schema().get_vertex_label_name(src_label);
  auto dst_label_name = graph.schema().get_vertex_label_name(dst_label);
  auto edge_label_name = graph.schema().get_edge_label_name(edge_label);
  std::vector<std::pair<std::string, std::string>> new_names_to_old_names;
  for (const auto& pair : old_names_to_new_names) {
    new_names_to_old_names.emplace_back(pair.second, pair.first);
  }
  auto status = graph.RenameEdgeProperties(
      src_label_name, dst_label_name, edge_label_name, new_names_to_old_names);
  if (!status.ok()) {
    THROW_RUNTIME_ERROR("Failed to undo RenameEdgeProp for edge " +
                        src_label_name + "->" + dst_label_name + ":" +
                        edge_label_name + ": " + status.error_message());
  }
};

void DeleteVertexPropUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  if (!graph.schema().vertex_label_valid(label)) {
    THROW_INTERNAL_EXCEPTION("Vertex label  not found in schema: " +
                             std::to_string(label));
  }
  auto label_name = graph.schema().get_vertex_label_name(label);
  graph.mutable_schema().RevertDeleteVertexProperties(label_name, prop_names);
};

void DeleteEdgePropUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  if (!graph.schema().exist(src_label, dst_label, edge_label)) {
    THROW_INTERNAL_EXCEPTION(
        "Edge label not found in schema: " + std::to_string(src_label) + "->" +
        std::to_string(dst_label) + ":" + std::to_string(edge_label));
  }
  auto src_label_name = graph.schema().get_vertex_label_name(src_label);
  auto dst_label_name = graph.schema().get_vertex_label_name(dst_label);
  auto edge_label_name = graph.schema().get_edge_label_name(edge_label);
  graph.mutable_schema().RevertDeleteEdgeProperties(
      src_label_name, dst_label_name, edge_label_name, prop_names);
};

void DeleteVertexTypeUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  if (graph.schema().contains_vertex_label(v_label)) {
    THROW_RUNTIME_ERROR("Cannot undo DeleteVertexType for vertex label " +
                        v_label + " since it does  exist.");
  }
  if (!graph.schema().IsVertexLabelSoftDeleted(v_label)) {
    THROW_RUNTIME_ERROR("Cannot undo DeleteVertexType for vertex label " +
                        v_label + " since it is not soft deleted.");
  }
  graph.mutable_schema().RevertDeleteVertexLabel(v_label);
  // No need to do with the vertex tables and edge tables, they are only marked
  // as deleted in txn.
};

void DeleteEdgeTypeUndo::Undo(PropertyGraph& graph, timestamp_t ts) const {
  if (graph.schema().exist(src_label, dst_label, edge_label)) {
    THROW_RUNTIME_ERROR("Cannot undo DeleteEdgeType for edge label " +
                        edge_label + " since it exists.");
  }
  if (!graph.schema().IsEdgeLabelSoftDeleted(src_label, dst_label,
                                             edge_label)) {
    THROW_RUNTIME_ERROR("Cannot undo DeleteEdgeType for edge label " +
                        edge_label + " since it is not soft deleted.");
  }
  graph.mutable_schema().RevertDeleteEdgeLabel(src_label, dst_label,
                                               edge_label);
  // No need to do with the vertex tables and edge tables, they are only marked
  // as deleted in txn.
};

}  // namespace neug