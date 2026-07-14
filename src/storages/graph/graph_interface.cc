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

#include "neug/storages/index/storage_index_manager.h"

namespace neug {

namespace {

// Drops every index whose metadata references the given vertex label/property.
//
// This is a schema-level cleanup helper, not a row-data maintenance helper. It
// is called before schema operations that invalidate an indexed property, such
// as deleting a vertex property, renaming a vertex property, or deleting a
// vertex type. The index lookup is based on IndexManager's own index metadata,
// but running the cleanup before the schema mutation keeps the graph from
// entering a state where the schema has already removed the property while
// IndexManager still owns indexes that refer to it.
//
// Index names are copied out before calling DropIndex because DropIndex mutates
// IndexManager's internal container. Keeping only the names avoids using Index*
// values after the manager has been modified.
static Status deleteVertexIndexSchema(PropertyGraph& graph, label_t label,
                                      const std::string& prop_name) {
  auto& index_manager = graph.mutable_index_manager();
  auto indexes = index_manager.GetIndex(label, prop_name);
  if (!indexes) {
    return indexes.error();
  }

  std::vector<std::string> index_names;
  index_names.reserve(indexes->size());
  for (auto* index : indexes.value()) {
    index_names.push_back(index->GetMeta().name);
  }
  for (const auto& index_name : index_names) {
    RETURN_IF_NOT_OK(index_manager.DropIndex(index_name));
  }
  return Status::OK();
}

// Appends index entries for one newly inserted vertex row.
//
// The caller has already inserted the vertex data into the vertex table and
// passes the inserted local id plus the property values that were written. This
// helper walks all active properties on the vertex label and appends the row to
// every index registered for each property. Properties that are soft-deleted or
// not present in the input payload are skipped because they do not have valid
// row values to add.
//
// The reader is passed through to Index::Append so index implementations can
// read any additional graph state they need while building the index entry. If
// a property has no index, IndexManager returns an empty list and no work is
// done for that property.
static Status addVertexIndexData(PropertyGraph& graph,
                                 const StorageReadInterface& reader,
                                 label_t label, vid_t lid,
                                 const std::vector<Value>& props) {
  const auto& v_schema = graph.schema().get_vertex_schema(label);
  auto& index_manager = graph.mutable_index_manager();
  for (size_t prop_idx = 0; prop_idx < v_schema->property_names.size();
       ++prop_idx) {
    if (v_schema->vprop_soft_deleted[prop_idx] || prop_idx >= props.size()) {
      continue;
    }
    auto indexes =
        index_manager.GetIndex(label, v_schema->property_names[prop_idx]);
    if (!indexes) {
      return indexes.error();
    }
    for (auto* index : indexes.value()) {
      RETURN_IF_NOT_OK(index->Append(lid, props[prop_idx], reader));
    }
  }
  return Status::OK();
}

// Appends index entries for a batch of newly inserted vertex rows.
//
// Batch insertion only returns the local ids that were created. Unlike
// addVertexIndexData, the original input property vector for each row is no
// longer available here, so this helper reads property values back from the
// vertex table columns. It scans each active property once, skips properties
// without indexes, and then appends every newly inserted vid to each matching
// index.
//
// Missing property columns are ignored. That keeps this helper tolerant of
// schema/table states where a property is known to the schema but the
// corresponding column has not been materialized for the AP table.
static Status batchAddVertexIndexData(PropertyGraph& graph,
                                      const StorageReadInterface& reader,
                                      label_t label,
                                      const std::vector<vid_t>& vids) {
  const auto& v_schema = graph.schema().get_vertex_schema(label);
  const auto& vtable = graph.get_vertex_table(label);
  auto& index_manager = graph.mutable_index_manager();

  for (size_t prop_idx = 0; prop_idx < v_schema->property_names.size();
       ++prop_idx) {
    if (v_schema->vprop_soft_deleted[prop_idx]) {
      continue;
    }
    auto indexes =
        index_manager.GetIndex(label, v_schema->property_names[prop_idx]);
    if (!indexes) {
      return indexes.error();
    }
    if (indexes->empty()) {
      continue;
    }
    auto col = vtable.GetPropertyColumn(static_cast<int32_t>(prop_idx));
    if (!col) {
      continue;
    }
    for (auto* index : indexes.value()) {
      for (vid_t vid : vids) {
        RETURN_IF_NOT_OK(index->Append(vid, col->get_any(vid), reader));
      }
    }
  }
  return Status::OK();
}

// Updates index entries for one changed vertex property.
//
// The graph property value has already been updated by the caller. The new
// value is passed explicitly so this helper can replace the corresponding index
// entry without relying on another table read for the changed column. For every
// index registered on the updated property, the old row entry is deleted first
// and the new value is appended afterwards.
//
// Invalid column ids and soft-deleted properties are treated as no-ops because
// no valid index should be maintained for them. The helper only touches indexes
// attached to the updated property; indexes on other properties of the same
// vertex label are unaffected.
static Status updateVertexIndexData(PropertyGraph& graph,
                                    const StorageReadInterface& reader,
                                    label_t label, vid_t lid, int32_t col_id,
                                    const Value& value) {
  const auto& v_schema = graph.schema().get_vertex_schema(label);
  if (col_id < 0 ||
      static_cast<size_t>(col_id) >= v_schema->property_names.size() ||
      v_schema->vprop_soft_deleted[col_id]) {
    return Status::OK();
  }

  auto& index_manager = graph.mutable_index_manager();
  auto indexes =
      index_manager.GetIndex(label, v_schema->property_names[col_id]);
  if (!indexes) {
    return indexes.error();
  }
  for (auto* index : indexes.value()) {
    RETURN_IF_NOT_OK(index->Delete(lid));
    RETURN_IF_NOT_OK(index->Append(lid, value, reader));
  }
  return Status::OK();
}

// Deletes index entries for one or more removed vertex rows.
//
// This helper is used by both single-row and batch vertex deletion paths. It
// scans all active properties on the vertex label, finds every index registered
// for each property, and removes each deleted local id from those indexes. It
// must run in the same logical update path as the vertex table deletion so
// index contents stay aligned with the visible vertex set.
//
// The helper does not read property values from the table because Index::Delete
// is keyed by local vertex id. This is important for delete paths where the row
// data may already have been marked deleted or may be unavailable after the
// table mutation.
static Status deleteVertexIndexData(PropertyGraph& graph, label_t label,
                                    const std::vector<vid_t>& vids) {
  const auto& v_schema = graph.schema().get_vertex_schema(label);
  auto& index_manager = graph.mutable_index_manager();
  for (size_t prop_idx = 0; prop_idx < v_schema->property_names.size();
       ++prop_idx) {
    if (v_schema->vprop_soft_deleted[prop_idx]) {
      continue;
    }
    auto indexes =
        index_manager.GetIndex(label, v_schema->property_names[prop_idx]);
    if (!indexes) {
      return indexes.error();
    }
    for (auto* index : indexes.value()) {
      for (vid_t vid : vids) {
        RETURN_IF_NOT_OK(index->Delete(vid));
      }
    }
  }
  return Status::OK();
}

}  // namespace

Status StorageAPUpdateInterface::UpdateVertexProperty(
    label_t label, vid_t lid, int col_id, const Value& value) {
  RETURN_IF_NOT_OK(
      graph_.UpdateVertexProperty(label, lid, col_id, value, timestamp_));
  return updateVertexIndexData(graph_, *this, label, lid, col_id, value);
}

Status StorageAPUpdateInterface::UpdateEdgeProperty(
    label_t src_label, vid_t src, label_t dst_label, vid_t dst,
    label_t edge_label, int32_t oe_offset, int32_t ie_offset, int32_t col_id,
    const Value& value) {
  return graph_.UpdateEdgeProperty(src_label, src, dst_label, dst, edge_label,
                                   oe_offset, ie_offset, col_id, value,
                                   neug::timestamp_t(0));
}

Status StorageAPUpdateInterface::AddVertex(label_t label, const Value& id,
                                           const std::vector<Value>& props,
                                           vid_t& vid) {
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
    return status;
  }

  RETURN_IF_NOT_OK(addVertexIndexData(graph_, *this, label, vid, props));
  return Status::OK();
}

Status StorageAPUpdateInterface::AddEdge(label_t src_label, vid_t src,
                                         label_t dst_label, vid_t dst,
                                         label_t edge_label,
                                         const std::vector<Value>& properties,
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
  RETURN_IF_NOT_OK(graph_.DeleteVertex(label, lid, timestamp_));
  return deleteVertexIndexData(graph_, label, {lid});
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
  auto new_vids = graph_.BatchAddVertices(v_label_id, std::move(supplier));
  if (!new_vids) {
    return new_vids.error();
  }

  if (new_vids->empty()) {
    return Status::OK();
  }

  return batchAddVertexIndexData(graph_, *this, v_label_id, new_vids.value());
}

Status StorageAPUpdateInterface::BatchAddEdges(
    label_t src_label, label_t dst_label, label_t edge_label,
    std::shared_ptr<IDataChunkSupplier> supplier) {
  return graph_.BatchAddEdges(src_label, dst_label, edge_label,
                              std::move(supplier));
}

Status StorageAPUpdateInterface::BatchDeleteVertices(
    label_t v_label_id, const std::vector<vid_t>& vids) {
  RETURN_IF_NOT_OK(deleteVertexIndexData(graph_, v_label_id, vids));
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
  const auto& vertex_type_name = config.GetVertexLabel();
  auto v_label = graph_.schema().get_vertex_label_id(vertex_type_name);
  for (const auto& [old_name, new_name] : config.GetRenameProperties()) {
    RETURN_IF_NOT_OK(deleteVertexIndexSchema(graph_, v_label, old_name));
  }
  return graph_.RenameVertexProperties(config);
}

Status StorageAPUpdateInterface::RenameEdgeProperties(
    const RenameEdgePropertiesParam& config) {
  return graph_.RenameEdgeProperties(config);
}

Status StorageAPUpdateInterface::DeleteVertexProperties(
    const DeleteVertexPropertiesParam& config) {
  const auto& vertex_type_name = config.GetVertexLabel();
  auto v_label = graph_.schema().get_vertex_label_id(vertex_type_name);
  for (const auto& prop_name : config.GetDeleteProperties()) {
    RETURN_IF_NOT_OK(deleteVertexIndexSchema(graph_, v_label, prop_name));
  }
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
  auto v_label = graph_.schema().get_vertex_label_id(vertex_type_name);
  const auto& v_schema = graph_.schema().get_vertex_schema(v_label);
  for (size_t prop_idx = 0; prop_idx < v_schema->property_names.size();
       ++prop_idx) {
    if (v_schema->vprop_soft_deleted[prop_idx])
      continue;
    RETURN_IF_NOT_OK(deleteVertexIndexSchema(
        graph_, v_label, v_schema->property_names[prop_idx]));
  }
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

neug::result<StorageIndex*> StorageAPUpdateInterface::CreateIndex(
    const std::string& name, std::unique_ptr<IndexMeta> meta) {
  return index_manager_.CreateIndex(name, std::move(meta));
}

Status StorageAPUpdateInterface::DropIndex(const std::string& name) {
  return index_manager_.DropIndex(name);
}

}  // namespace neug
