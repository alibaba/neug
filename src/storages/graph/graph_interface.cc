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

void StorageAPUpdateInterface::UpdateVertexProperty(label_t label, vid_t lid,
                                                    int col_id,
                                                    const Property& value) {
  graph_.UpdateVertexProperty(label, lid, col_id, value, timestamp_);
}

void StorageAPUpdateInterface::UpdateEdgeProperty(
    label_t src_label, vid_t src, label_t dst_label, vid_t dst,
    label_t edge_label, int32_t oe_offset, int32_t ie_offset, int32_t col_id,
    const Property& value) {
  graph_.UpdateEdgeProperty(src_label, src, dst_label, dst, edge_label,
                            oe_offset, ie_offset, col_id, value,
                            neug::timestamp_t(0));
}

bool StorageAPUpdateInterface::AddVertex(label_t label, const Property& id,
                                         const std::vector<Property>& props,
                                         vid_t& vid) {
  const auto& table = graph_.get_vertex_table(label);
  if (table.LidNum() >= table.Capacity()) {
    graph_.Reserve(label, table.Capacity() * 2);
  }
  auto status =
      graph_.AddVertex(label, id, props, vid, neug::timestamp_t(0), true);
  if (!status.ok()) {
    LOG(ERROR) << "AddVertex failed: " << status.ToString();
  }
  return status.ok();
}

bool StorageAPUpdateInterface::AddEdge(
    label_t src_label, vid_t src, label_t dst_label, vid_t dst,
    label_t edge_label, const std::vector<Property>& properties) {
  graph_.AddEdge(src_label, src, dst_label, dst, edge_label, properties,
                 neug::timestamp_t(0), alloc_, true);
  return true;
}

void StorageAPUpdateInterface::CreateCheckpoint() { graph_.Dump(); }

Status StorageAPUpdateInterface::BatchAddVertices(
    label_t v_label_id, std::shared_ptr<IRecordBatchSupplier> supplier) {
  return graph_.BatchAddVertices(v_label_id, std::move(supplier));
}

Status StorageAPUpdateInterface::BatchAddEdges(
    label_t src_label, label_t dst_label, label_t edge_label,
    std::shared_ptr<IRecordBatchSupplier> supplier) {
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
    const std::string& name,
    const std::vector<std::tuple<DataType, std::string, Property>>& properties,
    const std::vector<std::string>& primary_key_names, bool error_on_conflict) {
  return graph_.CreateVertexType(name, properties, primary_key_names,
                                 error_on_conflict);
}

Status StorageAPUpdateInterface::CreateEdgeType(
    const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::tuple<DataType, std::string, Property>>& properties,
    bool error_on_conflict, EdgeStrategy oe_edge_strategy,
    EdgeStrategy ie_edge_strategy) {
  return graph_.CreateEdgeType(src_type, dst_type, edge_type, properties,
                               error_on_conflict, oe_edge_strategy,
                               ie_edge_strategy);
}

Status StorageAPUpdateInterface::AddVertexProperties(
    const std::string& vertex_type_name,
    const std::vector<std::tuple<DataType, std::string, Property>>&
        add_properties,
    bool error_on_conflict) {
  return graph_.AddVertexProperties(vertex_type_name, add_properties,
                                    error_on_conflict);
}

Status StorageAPUpdateInterface::AddEdgeProperties(
    const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::tuple<DataType, std::string, Property>>&
        add_properties,
    bool error_on_conflict) {
  return graph_.AddEdgeProperties(src_type, dst_type, edge_type, add_properties,
                                  error_on_conflict);
}

Status StorageAPUpdateInterface::RenameVertexProperties(
    const std::string& vertex_type_name,
    const std::vector<std::pair<std::string, std::string>>& rename_properties,
    bool error_on_conflict) {
  return graph_.RenameVertexProperties(vertex_type_name, rename_properties,
                                       error_on_conflict);
}

Status StorageAPUpdateInterface::RenameEdgeProperties(
    const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::pair<std::string, std::string>>& rename_properties,
    bool error_on_conflict) {
  return graph_.RenameEdgeProperties(src_type, dst_type, edge_type,
                                     rename_properties, error_on_conflict);
}

Status StorageAPUpdateInterface::DeleteVertexProperties(
    const std::string& vertex_type_name,
    const std::vector<std::string>& delete_properties, bool error_on_conflict) {
  return graph_.DeleteVertexProperties(vertex_type_name, delete_properties,
                                       error_on_conflict);
}

Status StorageAPUpdateInterface::DeleteEdgeProperties(
    const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::string>& delete_properties, bool error_on_conflict) {
  return graph_.DeleteEdgeProperties(src_type, dst_type, edge_type,
                                     delete_properties, error_on_conflict);
}

Status StorageAPUpdateInterface::DeleteVertexType(
    const std::string& vertex_type_name, bool error_on_conflict) {
  return graph_.DeleteVertexType(vertex_type_name, error_on_conflict);
}

Status StorageAPUpdateInterface::DeleteEdgeType(const std::string& src_type,
                                                const std::string& dst_type,
                                                const std::string& edge_type,
                                                bool error_on_conflict) {
  return graph_.DeleteEdgeType(src_type, dst_type, edge_type,
                               error_on_conflict);
}

}  // namespace neug