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

#include <cstring>
#include <limits>

#include "neug/storages/index/index_id_accessor.h"
#include "neug/storages/index/index_utils.h"
#include "neug/storages/index/storage_index_manager.h"
#include "neug/storages/module_descriptor.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/array_column.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/vec_column.h"
#include "neug/utils/result.h"

namespace neug {

namespace {

std::unique_ptr<ColumnBase> FromArrayColumn(const ArrayColumn& array,
                                            size_t vid_size,
                                            const Value& default_value,
                                            Checkpoint& ckp,
                                            MemoryLevel level) {
  if (vid_size > array.size()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "FromArrayColumn: vid size exceeds array column size");
  }
  if (vid_size != 0 && vid_size - 1 > std::numeric_limits<vid_t>::max()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "FromArrayColumn: vid size exceeds the VID range");
  }
  auto offset_accessor = std::make_unique<DefaultIndexIDAccessor>();
  offset_accessor->Open(ckp, ModuleDescriptor{}, level);
  for (size_t vid = 0; vid < vid_size; ++vid) {
    offset_accessor->UpsertVID(static_cast<vid_t>(vid));
  }

  const auto child_type = ArrayType::GetChildType(array.array_type()).id();
  switch (child_type) {
  case DataTypeId::kFloat:
    return std::make_unique<VecColumn>(
        array.shared_buffer<float>(), std::move(offset_accessor),
        array.array_type(), array.size(), default_value, ckp, level);
  default:
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "HNSW index supports only FLOAT array properties");
  }
}

std::unique_ptr<ArrayColumn> FromVecColumn(VecColumn& vec, size_t vid_size,
                                           size_t size,
                                           const Value& default_value,
                                           Checkpoint& ckp, MemoryLevel level) {
  if (vid_size > size) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "FromVecColumn: vid size exceeds array column size");
  }
  if (vid_size != 0 && vid_size - 1 > std::numeric_limits<vid_t>::max()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "FromVecColumn: vid size exceeds the VID range");
  }

  auto array_column = std::make_unique<ArrayColumn>(vec.array_type());
  array_column->Open(ckp, ModuleDescriptor{}, level);
  array_column->resize(size, default_value);

  const auto* src = static_cast<const float*>(vec.get_buffer_ptr());
  auto* dst =
      static_cast<float*>(array_column->shared_buffer<float>()->GetData());
  const auto array_size = vec.array_size();
  const auto* offset_accessor = vec.get_offset_accessor();
  for (size_t vid = 0; vid < vid_size; ++vid) {
    const auto offset =
        offset_accessor->GetIndexIDByVID(static_cast<vid_t>(vid));
    if (offset == INVALID_OFFSET) {
      continue;
    }
    if (offset >= vec.size()) {
      THROW_RUNTIME_ERROR("FromVecColumn: offset out of range");
    }
    std::memcpy(dst + vid * array_size, src + offset * array_size,
                array_size * sizeof(float));
  }
  return array_column;
}

// Drops every index whose metadata references the given vertex label/property.
static Status dropVertexIndex(PropertyGraph& graph, label_t label,
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

// Updates metadata for every index bound to a renamed vertex property.
static Status renameVertexIndex(PropertyGraph& graph, label_t label,
                                const std::string& old_name,
                                const std::string& new_name) {
  auto indexes = graph.mutable_index_manager().GetIndex(label, old_name);
  if (!indexes) {
    return indexes.error();
  }
  for (auto* index : indexes.value()) {
    index->RenameProperty(new_name);
  }
  return Status::OK();
}

// Appends index entries for one newly inserted vertex row.
static Status addVertexIndexData(PropertyGraph& graph, label_t label, vid_t lid,
                                 const Value& id,
                                 const std::vector<Value>& props) {
  const auto& v_schema = graph.schema().get_vertex_schema(label);
  auto& index_manager = graph.mutable_index_manager();

  // Primary keys are stored separately from property_names, so maintain their
  // indexes explicitly.
  const auto& pk_name = std::get<1>(v_schema->primary_keys[0]);
  auto pk_indexes = index_manager.GetIndex(label, pk_name);
  if (!pk_indexes) {
    return pk_indexes.error();
  }
  for (auto* index : pk_indexes.value()) {
    RETURN_IF_NOT_OK(index->Upsert(lid, id));
  }

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
      RETURN_IF_NOT_OK(index->Upsert(lid, props[prop_idx]));
    }
  }
  return Status::OK();
}

// Appends index entries for a batch of newly inserted vertex rows.
static Status batchAddVertexIndexData(PropertyGraph& graph, label_t label,
                                      const std::vector<vid_t>& vids) {
  const auto& v_schema = graph.schema().get_vertex_schema(label);
  const auto& vtable = graph.get_vertex_table(label);
  auto& index_manager = graph.mutable_index_manager();

  // Primary keys are stored outside property_names in the vertex indexer, so
  // read their column explicitly when maintaining indexes for a batch.
  const auto& pk_name = std::get<1>(v_schema->primary_keys[0]);
  auto pk_indexes = index_manager.GetIndex(label, pk_name);
  if (!pk_indexes) {
    return pk_indexes.error();
  }
  if (!pk_indexes->empty()) {
    auto pk_col = vtable.GetPropertyColumn(pk_name);
    if (!pk_col) {
      return Status::InternalError("Primary key column does not exist");
    }
    for (auto* index : pk_indexes.value()) {
      for (vid_t vid : vids) {
        RETURN_IF_NOT_OK(index->Upsert(vid, pk_col->get_any(vid)));
      }
    }
  }

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
        RETURN_IF_NOT_OK(index->Upsert(vid, col->get_any(vid)));
      }
    }
  }
  return Status::OK();
}

// Updates index entries for one changed vertex property. Primary keys are not
// handled here because PropertyGraph does not allow modifying the primary key
// of an existing vertex.
static Status updateVertexIndexData(PropertyGraph& graph, label_t label,
                                    vid_t lid, int32_t col_id,
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
    RETURN_IF_NOT_OK(index->Upsert(lid, value));
  }
  return Status::OK();
}

// Deletes index entries for one or more removed vertex rows.
static Status deleteVertexIndexData(PropertyGraph& graph, label_t label,
                                    const std::vector<vid_t>& vids) {
  const auto& v_schema = graph.schema().get_vertex_schema(label);
  auto& index_manager = graph.mutable_index_manager();

  // Primary keys are excluded from property_names, so delete their index
  // entries explicitly.
  const auto& pk_name = std::get<1>(v_schema->primary_keys[0]);
  auto pk_indexes = index_manager.GetIndex(label, pk_name);
  if (!pk_indexes) {
    return pk_indexes.error();
  }
  for (auto* index : pk_indexes.value()) {
    for (vid_t vid : vids) {
      RETURN_IF_NOT_OK(index->Delete(vid));
    }
  }

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

result<std::vector<SearchResult>> StorageReadInterface::IndexSearch(
    const std::string& unique_index_name,
    const IndexQueryParams& params) const {
  GS_AUTO(index, view_.GetIndexByName(unique_index_name));
  return index->Search(params);
}

Status StorageAPUpdateInterface::UpdateVertexPropertyImpl(label_t label,
                                                          vid_t lid, int col_id,
                                                          const Value& value) {
  RETURN_IF_NOT_OK(
      graph_.UpdateVertexProperty(label, lid, col_id, value, timestamp_));
  return updateVertexIndexData(graph_, label, lid, col_id, value);
}

Status StorageAPUpdateInterface::UpdateEdgePropertyImpl(
    label_t src_label, vid_t src, label_t dst_label, vid_t dst,
    label_t edge_label, int32_t oe_offset, int32_t ie_offset, int32_t col_id,
    const Value& value) {
  return graph_.UpdateEdgeProperty(src_label, src, dst_label, dst, edge_label,
                                   oe_offset, ie_offset, col_id, value,
                                   neug::timestamp_t(0));
}

Status StorageAPUpdateInterface::AddVertexImpl(label_t label, const Value& id,
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

  RETURN_IF_NOT_OK(addVertexIndexData(graph_, label, vid, id, props));
  return Status::OK();
}

Status StorageAPUpdateInterface::AddEdgeImpl(
    label_t src_label, vid_t src, label_t dst_label, vid_t dst,
    label_t edge_label, const std::vector<Value>& properties,
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
  if (!graph_.IsModified()) {
    return;
  }
  auto ckp = graph_.checkpoint_ptr();
  auto memory_level = graph_.memory_level();
  graph_.DumpAndClear(ckp);
  graph_.Open(ckp, memory_level);
  mut_view_.Rebuild(graph_);
  // Open rebuilds dirty bits to false; ClearAllDirty is redundant but keeps
  // the post-publish contract explicit for in-place dump paths.
  graph_.ClearAllDirty();
}

Status StorageAPUpdateInterface::DeleteVertexImpl(label_t label, vid_t lid) {
  RETURN_IF_NOT_OK(graph_.DeleteVertex(label, lid, timestamp_));
  return deleteVertexIndexData(graph_, label, {lid});
}

Status StorageAPUpdateInterface::DeleteEdgeImpl(label_t src_label, vid_t src,
                                                label_t dst_label, vid_t dst,
                                                label_t edge_label,
                                                int32_t oe_offset,
                                                int32_t ie_offset) {
  return graph_.DeleteEdge(src_label, src, dst_label, dst, edge_label,
                           oe_offset, ie_offset, timestamp_);
}

Status StorageAPUpdateInterface::DeleteEdgesImpl(label_t src_label, vid_t src,
                                                 label_t dst_label, vid_t dst,
                                                 label_t edge_label) {
  // AP mode: delegate to batch version with single pair
  std::vector<std::tuple<vid_t, vid_t>> edges = {{src, dst}};
  return graph_.BatchDeleteEdges(src_label, dst_label, edge_label, edges);
}

result<std::vector<vid_t>> StorageAPUpdateInterface::BatchAddVerticesImpl(
    label_t v_label_id, std::shared_ptr<IDataChunkSupplier> supplier) {
  auto new_vids = graph_.BatchAddVertices(v_label_id, std::move(supplier));
  if (!new_vids) {
    return tl::unexpected(new_vids.error());
  }

  if (new_vids->empty()) {
    return new_vids;
  }

  auto status = batchAddVertexIndexData(graph_, v_label_id, new_vids.value());
  if (!status.ok()) {
    return tl::unexpected(std::move(status));
  }
  return new_vids;
}

Status StorageAPUpdateInterface::BatchAddEdgesImpl(
    label_t src_label, label_t dst_label, label_t edge_label,
    std::shared_ptr<IDataChunkSupplier> supplier) {
  return graph_.BatchAddEdges(src_label, dst_label, edge_label,
                              std::move(supplier));
}

Status StorageAPUpdateInterface::BatchDeleteVerticesImpl(
    label_t v_label_id, const std::vector<vid_t>& vids) {
  RETURN_IF_NOT_OK(graph_.BatchDeleteVertices(v_label_id, vids));
  return deleteVertexIndexData(graph_, v_label_id, vids);
}

Status StorageAPUpdateInterface::BatchDeleteEdgesImpl(
    label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
    const std::vector<std::tuple<vid_t, vid_t>>& edges) {
  return graph_.BatchDeleteEdges(src_v_label_id, dst_v_label_id, edge_label_id,
                                 edges);
}

Status StorageAPUpdateInterface::BatchDeleteEdgesImpl(
    label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
    const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
    const std::vector<std::pair<vid_t, int32_t>>& ie_edges) {
  return graph_.BatchDeleteEdges(src_v_label_id, dst_v_label_id, edge_label_id,
                                 oe_edges, ie_edges);
}

Status StorageAPUpdateInterface::CreateVertexTypeImpl(
    const CreateVertexTypeParam& config) {
  auto status = graph_.CreateVertexType(config);
  if (status.ok()) {
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::CreateEdgeTypeImpl(
    const CreateEdgeTypeParam& config) {
  auto status = graph_.CreateEdgeType(config);
  if (status.ok()) {
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::AddVertexPropertiesImpl(
    label_t label, const AddVertexPropertiesParam& config) {
  auto status = graph_.AddVertexProperties(label, config);
  if (status.ok()) {
    // Adding columns replaces the table header/column list cached by
    // GraphView, so refresh the mutable view before subsequent reads.
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::AddEdgePropertiesImpl(
    label_t src, label_t dst, label_t edge,
    const AddEdgePropertiesParam& config) {
  auto status = graph_.AddEdgeProperties(src, dst, edge, config);
  if (status.ok()) {
    // Adding edge properties may trigger a bundled→unbundled CSR rebuild
    // (dropAndCreateNewUnbundledCSR), which replaces the underlying CsrBase
    // objects.  The mutable view caches raw pointers to those objects, so we
    // must rebuild to pick up the new pointers.
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::RenameVertexPropertiesImpl(
    label_t label, const RenameVertexPropertiesParam& config) {
  RETURN_IF_NOT_OK(graph_.RenameVertexProperties(label, config));
  for (const auto& [old_name, new_name] : config.GetRenameProperties()) {
    if (old_name == new_name)
      continue;
    RETURN_IF_NOT_OK(renameVertexIndex(graph_, label, old_name, new_name));
  }
  return Status::OK();
}

Status StorageAPUpdateInterface::RenameEdgePropertiesImpl(
    label_t src, label_t dst, label_t edge,
    const RenameEdgePropertiesParam& config) {
  return graph_.RenameEdgeProperties(src, dst, edge, config);
}

Status StorageAPUpdateInterface::DeleteVertexPropertiesImpl(
    label_t label, const DeleteVertexPropertiesParam& config) {
  RETURN_IF_NOT_OK(graph_.DeleteVertexProperties(label, config));
  for (const auto& prop_name : config.GetDeleteProperties()) {
    RETURN_IF_NOT_OK(dropVertexIndex(graph_, label, prop_name));
  }
  // Deleting columns shifts the table column vector cached by GraphView.
  mut_view_.Rebuild(graph_);
  return Status::OK();
}

Status StorageAPUpdateInterface::DeleteEdgePropertiesImpl(
    label_t src, label_t dst, label_t edge,
    const DeleteEdgePropertiesParam& config) {
  auto status = graph_.DeleteEdgeProperties(src, dst, edge, config);
  if (status.ok()) {
    // Deleting edge properties may trigger a CSR rebuild (unbundled→bundled or
    // unbundled→empty), which replaces the underlying CsrBase objects.  Rebuild
    // the mutable view so cached pointers stay valid.
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status StorageAPUpdateInterface::DeleteVertexTypeImpl(label_t label) {
  const auto& v_schema = graph_.schema().get_vertex_schema(label);
  std::vector<std::string> indexed_properties;
  indexed_properties.reserve(v_schema->property_names.size() + 1);
  // The primary key is stored separately from property_names but indexes bound
  // to it must be removed together with the vertex type.
  indexed_properties.push_back(std::get<1>(v_schema->primary_keys[0]));
  for (size_t prop_idx = 0; prop_idx < v_schema->property_names.size();
       ++prop_idx) {
    if (v_schema->vprop_soft_deleted[prop_idx])
      continue;
    indexed_properties.push_back(v_schema->property_names[prop_idx]);
  }
  RETURN_IF_NOT_OK(graph_.DeleteVertexType(label));
  for (const auto& property_name : indexed_properties) {
    RETURN_IF_NOT_OK(dropVertexIndex(graph_, label, property_name));
  }
  mut_view_.Rebuild(graph_);
  return Status::OK();
}

Status StorageAPUpdateInterface::DeleteEdgeTypeImpl(label_t src, label_t dst,
                                                    label_t edge) {
  auto status = graph_.DeleteEdgeType(src, dst, edge);
  if (status.ok()) {
    mut_view_.Rebuild(graph_);
  }
  return status;
}

/**
 * Creates an index for a vertex property.
 *
 * When creating an HNSW index, this method converts an ArrayColumn to a
 * VecColumn. The VecColumn reuses the ArrayColumn's underlying vector buffer
 * without copying its data. Subsequent incremental vector updates avoid
 * copy-on-write by letting the VecColumn maintain separate buffer versions.
 */
neug::result<StorageIndex*> StorageAPUpdateInterface::CreateIndex(
    std::unique_ptr<IndexMeta> meta) {
  if (!meta) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Cannot create index with null metadata");
  }
  auto label_id = meta->schema.label_id;
  if (!graph_.schema().is_vertex_label_valid(label_id)) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Index label id is out of range");
  }

  auto& vertex_table = graph_.get_vertex_table(label_id);
  const auto schema = vertex_table.get_vertex_schema_ptr();
  const auto& property_name = meta->schema.property_name;
  const bool is_primary_key =
      property_name == std::get<1>(schema->primary_keys[0]);
  const ColumnBase* column = vertex_table.GetPropertyColumnBase(property_name);
  if (!column) {
    RETURN_STATUS_ERROR(
        StatusCode::ERR_INVALID_ARGUMENT,
        "Indexed property column does not exist: " + property_name);
  }

  int32_t property_col = -1;
  std::unique_ptr<ColumnBase> vec_column;
  std::unique_ptr<IndexIDAccessor> index_id_accessor;

  if (IsHNSWIndex(*meta)) {
    GS_AUTO(existing_indexes, index_manager_.GetIndex(label_id, property_name));
    const bool has_non_hnsw = std::any_of(
        existing_indexes.begin(), existing_indexes.end(),
        [](StorageIndex* index) { return !IsHNSWIndex(index->GetMeta()); });
    if (has_non_hnsw) {
      RETURN_STATUS_ERROR(
          StatusCode::ERR_INVALID_ARGUMENT,
          "HNSW index cannot coexist with non-HNSW indexes on the same "
          "property");
    }
    if (is_primary_key) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "HNSW index cannot be created on a primary key");
    }
    property_col = schema->get_property_index(property_name);
    if (property_col < 0) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "Indexed property does not exist: " + property_name);
    }

    if (const auto* array = dynamic_cast<const ArrayColumn*>(column)) {
      const auto& default_value = schema->default_property_values[property_col];
      vec_column = FromArrayColumn(*array, vertex_table.Size(), default_value,
                                   graph_.checkpoint(), graph_.memory_level());
      column = vec_column.get();
    }

    auto* candidate_column =
        vec_column ? vec_column.get()
                   : vertex_table.get_table().get_column_by_id(property_col);
    if (auto* vec = dynamic_cast<VecColumn*>(candidate_column)) {
      index_id_accessor = std::make_unique<VecColumnBackedIndexIDAccessor>(
          *vec->get_offset_accessor());
    } else {
      RETURN_STATUS_ERROR(
          StatusCode::ERR_INVALID_ARGUMENT,
          "CreateIndex: HNSW index can only be created on VecColumn");
    }
  } else {
    if (dynamic_cast<const VecColumn*>(column)) {
      RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                          "Non-HNSW index cannot be created on VecColumn");
    }
    index_id_accessor = std::make_unique<DefaultIndexIDAccessor>();
  }

  GS_AUTO(index, index_manager_.CreateIndex(
                     std::move(meta), std::move(index_id_accessor), column,
                     graph_.GetVertexSet(label_id, timestamp_)));

  if (on_planning_changed_) {
    on_planning_changed_();
  }
  if (vec_column) {
    vertex_table.SetColumn(static_cast<size_t>(property_col),
                           std::move(vec_column));
    mut_view_.Rebuild(graph_);
  }
  return index;
}

/**
 * Drops an index.
 *
 * When dropping the last HNSW index on a VecColumn, this method converts the
 * VecColumn back to an ArrayColumn. It creates a new ArrayColumn and copies
 * vectors from the VecColumn by vertex ID. This is equivalent to compaction:
 * obsolete vector versions addressed by previous index IDs are discarded.
 */
Status StorageAPUpdateInterface::DropIndex(const std::string& name) {
  auto target = index_manager_.GetIndexByName(name);
  if (!target) {
    return target.error();
  }

  const auto meta = target.value()->GetMeta();
  std::unique_ptr<ArrayColumn> array_column;
  int32_t property_col = -1;

  if (IsHNSWIndex(meta)) {
    auto indexes = index_manager_.GetIndex(meta.schema.label_id,
                                           meta.schema.property_name);
    if (!indexes) {
      return indexes.error();
    }
    const bool has_other_hnsw =
        std::any_of(indexes->begin(), indexes->end(), [&](StorageIndex* index) {
          return index->GetMeta().name != name && IsHNSWIndex(index->GetMeta());
        });
    if (!has_other_hnsw) {
      auto& vertex_table = graph_.get_vertex_table(meta.schema.label_id);
      const auto schema = vertex_table.get_vertex_schema_ptr();
      property_col = schema->get_property_index(meta.schema.property_name);
      if (property_col < 0) {
        return Status(
            StatusCode::ERR_INVALID_ARGUMENT,
            "Indexed property does not exist: " + meta.schema.property_name);
      }

      const auto& default_value = schema->default_property_values[property_col];
      auto* column = vertex_table.get_table().get_column_by_id(property_col);
      if (auto* vec = dynamic_cast<VecColumn*>(column)) {
        array_column = FromVecColumn(
            *vec, vertex_table.Size(), vertex_table.Capacity(), default_value,
            graph_.checkpoint(), graph_.memory_level());
      } else {
        return Status(StatusCode::ERR_INVALID_ARGUMENT,
                      "DropIndex: HNSW index can only be created on VecColumn");
      }
    }
  }

  RETURN_IF_NOT_OK(index_manager_.DropIndex(name));
  if (on_planning_changed_) {
    on_planning_changed_();
  }
  if (array_column) {
    auto& vertex_table = graph_.get_vertex_table(meta.schema.label_id);
    vertex_table.SetColumn(static_cast<size_t>(property_col),
                           std::move(array_column));
    mut_view_.Rebuild(graph_);
  }
  return Status::OK();
}

}  // namespace neug
