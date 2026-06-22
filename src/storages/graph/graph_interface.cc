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

#include "neug/storages/graph/graph_interface.h"

namespace neug {

Status StorageAPUpdateInterface::UpdateVertexProperty(
    label_t label, vid_t lid, int col_id, const execution::Value& value) {
  return graph_.UpdateVertexProperty(label, lid, col_id, value, timestamp_);
}

Status StorageAPUpdateInterface::UpdateEdgeProperty(
    label_t src_label, vid_t src, label_t dst_label, vid_t dst,
    label_t edge_label, int32_t oe_offset, int32_t ie_offset, int32_t col_id,
    const execution::Value& value) {
  return graph_.UpdateEdgeProperty(src_label, src, dst_label, dst, edge_label,
                                   oe_offset, ie_offset, col_id, value,
                                   neug::timestamp_t(0));
}

Status StorageAPUpdateInterface::AddVertex(
    label_t label, const execution::Value& id,
    const std::vector<execution::Value>& props, vid_t& vid) {
  const auto& vertex_table = graph_.get_vertex_table(label);
  if (vertex_table.Size() >= vertex_table.Capacity()) {
    auto new_cap = vertex_table.Size() < 4096
                       ? 4096
                       : vertex_table.Size() + vertex_table.Size() / 4;
    auto status = graph_.EnsureCapacity(label, new_cap);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to ensure space for vertex of label "
                 << graph_.schema().get_vertex_label_name(label) << ": "
                 << status.ToString();
      return status;
    }
  }

  auto status =
      graph_.AddVertex(label, id, props, vid, neug::timestamp_t(0), true);
  if (!status.ok()) {
    LOG(ERROR) << "AddVertex failed: " << status.ToString();
  }
  return status;
}

Status StorageAPUpdateInterface::AddEdge(
    label_t src_label, vid_t src, label_t dst_label, vid_t dst,
    label_t edge_label, const std::vector<execution::Value>& properties,
    const void*& prop) {
  const auto& edge_table =
      graph_.get_edge_table(src_label, dst_label, edge_label);
  if (edge_table.PropTableSize() >= edge_table.Capacity()) {
    size_t cur_size = edge_table.PropTableSize();
    auto new_cap = cur_size < 4096 ? 4096 : cur_size + cur_size / 4;
    auto status =
        graph_.EnsureCapacity(src_label, dst_label, edge_label, new_cap);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to ensure space for edge of label "
                 << graph_.schema().get_edge_label_name(edge_label) << ": "
                 << status.ToString();
      return status;
    }
  }
  int32_t oe_offset = 0;
  auto status =
      graph_.AddEdge(src_label, src, dst_label, dst, edge_label, properties,
                     neug::timestamp_t(0), alloc_, oe_offset, prop, true);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to add edge: " << status.ToString();
  }
  return status;
}

void StorageAPUpdateInterface::CreateCheckpoint() {
  graph_.Dump();
  // Dump(reopen=true) clears and re-opens the graph, replacing all vertex/edge
  // tables.  Rebuild the view so cached pointers stay valid.
  mut_view_.Rebuild(graph_);
}

Status StorageAPUpdateInterface::DeleteVertex(label_t label, vid_t lid) {
  return graph_.DeleteVertex(label, lid, timestamp_);
}

Status StorageAPUpdateInterface::DeleteEdge(label_t src_label, vid_t src,
                                            label_t dst_label, vid_t dst,
                                            label_t edge_label,
                                            int32_t oe_offset,
                                            int32_t ie_offset) {
  return graph_.DeleteEdge(src_label, src, dst_label, dst, edge_label,
                           oe_offset, ie_offset, timestamp_);
}

Status StorageAPUpdateInterface::DeleteEdges(label_t src_label, vid_t src,
                                             label_t dst_label, vid_t dst,
                                             label_t edge_label) {
  // AP mode: delegate to batch version with single pair
  std::vector<std::tuple<vid_t, vid_t>> edges = {{src, dst}};
  return graph_.BatchDeleteEdges(src_label, dst_label, edge_label, edges);
}

Status StorageAPUpdateInterface::BatchAddVertices(
    label_t v_label_id, std::shared_ptr<IDataChunkSupplier> supplier) {
  return graph_.BatchAddVertices(v_label_id, std::move(supplier));
}

Status StorageAPUpdateInterface::BatchAddEdges(
    label_t src_label, label_t dst_label, label_t edge_label,
    std::shared_ptr<IDataChunkSupplier> supplier) {
  return graph_.BatchAddEdges(src_label, dst_label, edge_label,
                              std::move(supplier));
}

Status StorageAPUpdateInterface::BatchDeleteVertices(
    label_t v_label_id, const std::vector<vid_t>& vids) {
  return graph_.BatchDeleteVertices(v_label_id, vids);
}

Status StorageAPUpdateInterface::BatchDeleteEdges(
    label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
    const std::vector<std::tuple<vid_t, vid_t>>& edges) {
  return graph_.BatchDeleteEdges(src_v_label_id, dst_v_label_id, edge_label_id,
                                 edges);
}

Status StorageAPUpdateInterface::BatchDeleteEdges(
    label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
    const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
    const std::vector<std::pair<vid_t, int32_t>>& ie_edges) {
  return graph_.BatchDeleteEdges(src_v_label_id, dst_v_label_id, edge_label_id,
                                 oe_edges, ie_edges);
}

Status StorageAPUpdateInterface::CreateVertexType(
    const CreateVertexTypeParam& config) {
  auto status = graph_.CreateVertexType(config);
  if (status.ok()) {
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::CreateEdgeType(
    const CreateEdgeTypeParam& config) {
  auto status = graph_.CreateEdgeType(config);
  if (status.ok()) {
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::AddVertexProperties(
    const AddVertexPropertiesParam& config) {
  auto status = graph_.AddVertexProperties(config);
  if (status.ok()) {
    // Adding columns replaces the table header/column list cached by
    // GraphView, so refresh the mutable view before subsequent reads.
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::AddEdgeProperties(
    const AddEdgePropertiesParam& config) {
  auto status = graph_.AddEdgeProperties(config);
  if (status.ok()) {
    // Adding edge properties may trigger a bundled→unbundled CSR rebuild
    // (dropAndCreateNewUnbundledCSR), which replaces the underlying CsrBase
    // objects.  The mutable view caches raw pointers to those objects, so we
    // must rebuild to pick up the new pointers.
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::RenameVertexProperties(
    const RenameVertexPropertiesParam& config) {
  return graph_.RenameVertexProperties(config);
}

Status StorageAPUpdateInterface::RenameEdgeProperties(
    const RenameEdgePropertiesParam& config) {
  return graph_.RenameEdgeProperties(config);
}

Status StorageAPUpdateInterface::DeleteVertexProperties(
    const DeleteVertexPropertiesParam& config) {
  auto status = graph_.DeleteVertexProperties(config);
  if (status.ok()) {
    // Deleting columns shifts the table column vector cached by GraphView.
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::DeleteEdgeProperties(
    const DeleteEdgePropertiesParam& config) {
  auto status = graph_.DeleteEdgeProperties(config);
  if (status.ok()) {
    // Deleting edge properties may trigger a CSR rebuild (unbundled→bundled or
    // unbundled→empty), which replaces the underlying CsrBase objects.  Rebuild
    // the mutable view so cached pointers stay valid.
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::DeleteVertexType(
    const std::string& vertex_type_name) {
  auto status = graph_.DeleteVertexType(vertex_type_name);
  if (status.ok()) {
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::DeleteEdgeType(const std::string& src_type,
                                                const std::string& dst_type,
                                                const std::string& edge_type) {
  auto status = graph_.DeleteEdgeType(src_type, dst_type, edge_type);
  if (status.ok()) {
    mut_view_.Rebuild(graph_);
  }
  return status;
}

}  // namespace neug
