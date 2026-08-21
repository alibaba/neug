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

#include "neug/transaction/cow_graph_storage_adapter.h"

#include <glog/logging.h>
#include <cstdint>

#include <algorithm>
#include <cstring>
#include <exception>
#include <filesystem>
#include <functional>
#include <limits>
#include <optional>
#include <ostream>
#include <string_view>

#include <flat_hash_map.hpp>
#include "neug/common/extra_type_info.h"
#include "neug/common/types/value.h"
#include "neug/storages/allocators.h"
#include "neug/storages/csr/csr_base.h"
#include "neug/storages/csr/csr_view_utils.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/index/index_id_accessor.h"
#include "neug/storages/index/index_utils.h"
#include "neug/storages/index/storage_index_manager.h"
#include "neug/storages/module_descriptor.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/io/file/file_utils.h"
#include "neug/utils/likely.h"
#include "neug/utils/property/array_column.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"
#include "neug/utils/property/vec_column.h"
#include "neug/utils/result.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

Status CowGraphStorageAdapter::AddGraphEntry(const std::string& name,
                                             const ProjectedGraphEntry& entry) {
  if (!workspace_.is_in_place()) {
    logical_redo_.LogAddGraphEntry(name, entry);
  }
  RETURN_IF_NOT_OK(graph_.mutable_schema().AddGraphEntry(name, entry));
  MarkSchemaDirty();
  return Status::OK();
}

Status CowGraphStorageAdapter::DropGraphEntry(const std::string& name) {
  if (!workspace_.is_in_place()) {
    logical_redo_.LogDropGraphEntry(name);
  }
  RETURN_IF_NOT_OK(graph_.mutable_schema().DropGraphEntry(name));
  MarkSchemaDirty();
  return Status::OK();
}

namespace {

// Converts an ArrayColumn into a VecColumn that reuses the array's underlying
// vector buffer without copying data. Subsequent incremental vector updates
// avoid copy-on-write by letting the VecColumn maintain separate buffer
// versions. Used when creating an HNSW index in-place.
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

// Converts a VecColumn back into an ArrayColumn by copying vectors by vertex
// ID. Equivalent to compaction: obsolete vector versions addressed by previous
// index IDs are discarded. Used when dropping the last HNSW index in-place.
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

}  // namespace

// Iterates the `primary` adjacency list of `lid`, cross-references the
// `secondary` list to find the corresponding offset, and returns
// (nbr_vid, primary_offset, secondary_offset) for each visible edge.
static std::vector<std::tuple<vid_t, int32_t, int32_t>> collect_nbr_offsets(
    const std::vector<DataType>& props, const CsrView& primary,
    const CsrView& secondary, vid_t lid, timestamp_t ts) {
  std::vector<std::tuple<vid_t, int32_t, int32_t>> offsets;
  NbrList nbr_list = primary.get_edges(lid);
  auto stride = nbr_list.cfg.stride;
  auto start_ptr = static_cast<const char*>(nbr_list.start_ptr);
  for (auto it = nbr_list.begin(); it != nbr_list.end(); ++it) {
    if (it.get_timestamp() > ts) {
      continue;
    }
    int32_t primary_offset = static_cast<int32_t>(
        (static_cast<const char*>(it.get_nbr_ptr()) - start_ptr) / stride);
    vid_t nbr = it.get_vertex();
    int32_t secondary_offset = neug::search_other_offset_with_cur_offset(
        primary, secondary, lid, nbr, primary_offset, props);
    offsets.emplace_back(nbr, primary_offset, secondary_offset);
  }
  return offsets;
}

std::vector<std::tuple<vid_t, vid_t, int32_t, int32_t>>
fetch_edges_related_to_vertex_from_view(const std::vector<DataType>& props,
                                        const CsrView& oe, const CsrView& ie,
                                        vid_t lid, bool is_src,
                                        timestamp_t ts) {
  std::vector<std::tuple<vid_t, vid_t, int32_t, int32_t>> result;
  if (is_src) {
    // lid is the source: iterate OE, find matching IE offset
    for (auto& [nbr, oe_off, ie_off] :
         collect_nbr_offsets(props, oe, ie, lid, ts)) {
      result.emplace_back(lid, nbr, oe_off, ie_off);
    }
  } else {
    // lid is the destination: iterate IE, find matching OE offset
    for (auto& [nbr, ie_off, oe_off] :
         collect_nbr_offsets(props, ie, oe, lid, ts)) {
      result.emplace_back(nbr, lid, oe_off, ie_off);
    }
  }
  return result;
}

std::unordered_map<uint32_t,
                   std::vector<std::tuple<vid_t, vid_t, int32_t, int32_t>>>
fetch_edges_related_to_vertex(const StorageReadInterface& graph,
                              const Schema& schema, label_t v_label, vid_t lid,
                              timestamp_t ts) {
  std::unordered_map<uint32_t,
                     std::vector<std::tuple<vid_t, vid_t, int32_t, int32_t>>>
      related_edges;  // edge_triplet_id -> <src, dst, oe_offset, ie_offset>

  auto v_label_num = schema.vertex_label_frontier();
  auto e_label_num = schema.edge_label_frontier();

  // Fetches edges for triplet (src_label, dst_label, e_label) in which lid
  // plays the role indicated by is_src, and appends them to related_edges.
  auto collect_triplet = [&](label_t src_label, label_t dst_label,
                             label_t e_label, bool is_src) {
    auto props = schema.get_edge_properties(src_label, dst_label, e_label);
    auto triplet_id = schema.generate_edge_label(src_label, dst_label, e_label);
    auto oe_view =
        graph.GetGenericOutgoingGraphView(src_label, dst_label, e_label);
    auto ie_view =
        graph.GetGenericIncomingGraphView(dst_label, src_label, e_label);
    auto edges = fetch_edges_related_to_vertex_from_view(
        props, oe_view, ie_view, lid, is_src, ts);
    auto& bucket = related_edges[triplet_id];
    bucket.insert(bucket.end(), edges.begin(), edges.end());
  };

  for (label_t other = 0; other < v_label_num; ++other) {
    if (!schema.is_vertex_label_valid(other)) {
      continue;
    }
    for (label_t e_label = 0; e_label < e_label_num; ++e_label) {
      if (!schema.is_edge_label_valid(e_label)) {
        continue;
      }
      if (other == v_label) {
        // Intra-label triplet: lid may be source or destination in the same
        // CSR, so both roles must be collected for COW to cover all adjlists.
        if (schema.is_edge_triplet_valid(v_label, v_label, e_label)) {
          collect_triplet(v_label, v_label, e_label, /*is_src=*/true);
          collect_triplet(v_label, v_label, e_label, /*is_src=*/false);
        }
      } else {
        // Inter-label triplets: the two directions are independent.
        if (schema.is_edge_triplet_valid(v_label, other, e_label)) {
          collect_triplet(v_label, other, e_label, /*is_src=*/true);
        }
        if (schema.is_edge_triplet_valid(other, v_label, e_label)) {
          collect_triplet(other, v_label, e_label, /*is_src=*/false);
        }
      }
    }
  }
  return related_edges;
}

Status CowGraphStorageAdapter::CreateVertexTypeImpl(
    const CreateVertexTypeParam& config) {
  if (workspace_.is_in_place()) {
    auto status = graph_.CreateVertexType(config);
    if (status.ok()) {
      mut_view_.Rebuild(graph_);
    }
    return status;
  }
  const auto& name = config.GetVertexLabel();
  if (graph_.schema().is_vertex_label_valid(name)) {
    LOG(ERROR) << "Vertex type " << name << " already exists.";
    return Status(StatusCode::ERR_SCHEMA_MISMATCH,
                  "Vertex type " + name + " already exists.");
  }
  logical_redo_.LogCreateVertexType(config);
  auto status = graph_.CreateVertexType(config);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to create vertex type " << name << ": "
               << status.ToString();
    return status;
  }

  // Expand detach_state for the newly created vertex type.
  label_t new_label = graph_.schema().get_vertex_label_id(name);
  if (new_label >= detach_state_.vertex_tables.size()) {
    detach_state_.vertex_tables.resize(new_label + 1);
  }
  auto& new_state = detach_state_.vertex_tables[new_label];
  new_state.indexer_detached = true;
  new_state.vertex_timestamp_detached = true;
  size_t col_count = config.GetProperties().size();
  new_state.columns_detached.assign(col_count, true);

  mut_view_.Rebuild(graph_);
  return status;
}

Status CowGraphStorageAdapter::CreateEdgeTypeImpl(
    const CreateEdgeTypeParam& config) {
  if (workspace_.is_in_place()) {
    auto status = graph_.CreateEdgeType(config);
    if (status.ok()) {
      mut_view_.Rebuild(graph_);
    }
    return status;
  }
  const auto& src_type = config.GetSrcLabel();
  const auto& dst_type = config.GetDstLabel();
  const auto& edge_type = config.GetEdgeLabel();
  if (graph_.schema().is_edge_triplet_valid(src_type, dst_type, edge_type)) {
    LOG(ERROR) << "Edge type " << edge_type << " already exists between "
               << src_type << " and " << dst_type << ".";
    return Status(StatusCode::ERR_SCHEMA_MISMATCH,
                  "Edge type " + edge_type + " already exists between " +
                      src_type + " and " + dst_type + ".");
  }
  logical_redo_.LogCreateEdgeType(config);
  auto status = graph_.CreateEdgeType(config);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to create edge type " << edge_type << " between "
               << src_type << " and " << dst_type << ": " << status.ToString();
    return status;
  }

  const auto& schema = graph_.schema();
  label_t src_label = schema.get_vertex_label_id(src_type);
  label_t dst_label = schema.get_vertex_label_id(dst_type);
  label_t edge_label = schema.get_edge_label_id(edge_type);
  uint32_t triplet_id =
      schema.generate_edge_label(src_label, dst_label, edge_label);
  EdgeTableDetachState new_edge_state;
  new_edge_state.out_csr_detached = true;
  new_edge_state.in_csr_detached = true;
  new_edge_state.columns_detached.assign(config.GetProperties().size(), true);
  detach_state_.edge_tables.emplace(triplet_id, std::move(new_edge_state));

  mut_view_.Rebuild(graph_);
  return status;
}

Status CowGraphStorageAdapter::AddVertexPropertiesImpl(
    label_t v_label, const AddVertexPropertiesParam& config) {
  if (workspace_.is_in_place()) {
    auto status = graph_.AddVertexProperties(v_label, config);
    if (status.ok()) {
      // Adding columns replaces the table header/column list cached by
      // GraphView, so refresh the mutable view before subsequent reads.
      mut_view_.Rebuild(graph_);
    }
    return status;
  }
  logical_redo_.LogAddVertexProperties(
      graph_.schema().get_vertex_label_name(v_label), config);
  auto status = graph_.AddVertexProperties(v_label, config);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to add properties to vertex type "
               << graph_.schema().get_vertex_label_name(v_label) << ": "
               << status.ToString();
    return status;
  }

  auto& vt_state = detach_state_.vertex_tables[v_label];
  size_t new_col_count =
      graph_.schema().get_vertex_schema(v_label)->property_names.size();
  vt_state.columns_detached.resize(new_col_count, true);

  mut_view_.Rebuild(graph_);
  return status;
}

Status CowGraphStorageAdapter::AddEdgePropertiesImpl(
    label_t src_label_id, label_t dst_label_id, label_t edge_label_id,
    const AddEdgePropertiesParam& config) {
  if (workspace_.is_in_place()) {
    auto status = graph_.AddEdgeProperties(src_label_id, dst_label_id,
                                           edge_label_id, config);
    if (status.ok()) {
      // Adding edge properties may trigger a bundled↔unbundled CSR rebuild
      // (dropAndCreateNewUnbundledCSR), which replaces the underlying
      // CsrBase objects.  The mutable view caches raw pointers to those
      // objects, so we must rebuild to pick up the new pointers.
      mut_view_.Rebuild(graph_);
    }
    return status;
  }
  const auto& schema = graph_.schema();
  logical_redo_.LogAddEdgeProperties(schema.get_vertex_label_name(src_label_id),
                                     schema.get_vertex_label_name(dst_label_id),
                                     schema.get_edge_label_name(edge_label_id),
                                     config);
  auto status = graph_.AddEdgeProperties(src_label_id, dst_label_id,
                                         edge_label_id, config);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to add properties to edge type "
               << schema.get_edge_label_name(edge_label_id) << " between "
               << schema.get_vertex_label_name(src_label_id) << " and "
               << schema.get_vertex_label_name(dst_label_id) << ": "
               << status.ToString();
    return status;
  }

  uint32_t triplet_id =
      schema.generate_edge_label(src_label_id, dst_label_id, edge_label_id);
  auto& et_state = detach_state_.edge_tables[triplet_id];
  auto edge_schema =
      schema.get_edge_schema(src_label_id, dst_label_id, edge_label_id);
  et_state.columns_detached.resize(edge_schema->property_names.size(), true);

  mut_view_.Rebuild(graph_);
  return status;
}

Status CowGraphStorageAdapter::RenameVertexPropertiesImpl(
    label_t v_label, const RenameVertexPropertiesParam& config) {
  if (workspace_.is_in_place()) {
    RETURN_IF_NOT_OK(graph_.RenameVertexProperties(v_label, config));
    // Schema metadata is already changed even if a bound index rename fails.
    MarkSchemaDirty();
    for (const auto& [old_name, new_name] : config.GetRenameProperties()) {
      if (old_name == new_name) {
        continue;
      }
      RETURN_IF_NOT_OK(renameVertexIndex(graph_, v_label, old_name, new_name));
    }
    return Status::OK();
  }
  const auto vertex_type_name = graph_.schema().get_vertex_label_name(v_label);
  logical_redo_.LogRenameVertexProperties(vertex_type_name, config);
  auto status = graph_.RenameVertexProperties(v_label, config);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to rename properties of vertex type "
               << graph_.schema().get_vertex_label_name(v_label) << ": "
               << status.ToString();
    return status;
  }
  for (const auto& [old_name, new_name] : config.GetRenameProperties()) {
    if (old_name == new_name) {
      continue;
    }
    RETURN_IF_NOT_OK(renameVertexIndex(graph_, v_label, old_name, new_name));
  }
  return status;
}

Status CowGraphStorageAdapter::RenameEdgePropertiesImpl(
    label_t src_label_id, label_t dst_label_id, label_t edge_label_id,
    const RenameEdgePropertiesParam& config) {
  if (workspace_.is_in_place()) {
    return graph_.RenameEdgeProperties(src_label_id, dst_label_id,
                                       edge_label_id, config);
  }
  const auto& schema = graph_.schema();
  logical_redo_.LogRenameEdgeProperties(
      schema.get_vertex_label_name(src_label_id),
      schema.get_vertex_label_name(dst_label_id),
      schema.get_edge_label_name(edge_label_id), config);
  auto status = graph_.RenameEdgeProperties(src_label_id, dst_label_id,
                                            edge_label_id, config);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to rename properties of edge type "
               << schema.get_edge_label_name(edge_label_id) << " between "
               << schema.get_vertex_label_name(src_label_id) << " and "
               << schema.get_vertex_label_name(dst_label_id) << ": "
               << status.ToString();
    return status;
  }
  return status;
}

Status CowGraphStorageAdapter::DeleteVertexPropertiesImpl(
    label_t v_label, const DeleteVertexPropertiesParam& config) {
  if (workspace_.is_in_place()) {
    RETURN_IF_NOT_OK(graph_.DeleteVertexProperties(v_label, config));
    // Column removal precedes bound-index cleanup and cannot be rolled back.
    MarkSchemaDirty();
    for (const auto& prop_name : config.GetDeleteProperties()) {
      RETURN_IF_NOT_OK(dropVertexIndex(graph_, v_label, prop_name));
    }
    // Deleting columns shifts the table column vector cached by GraphView.
    mut_view_.Rebuild(graph_);
    return Status::OK();
  }
  const auto& properties = config.GetDeleteProperties();
  const auto& vertex_type_name = graph_.schema().get_vertex_label_name(v_label);
  for (const auto& prop_name : properties) {
    if (!graph_.schema().vertex_has_property(v_label, prop_name)) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Property [" + prop_name + "] does not exist in vertex [" +
                        vertex_type_name + "].");
    }
  }

  std::vector<int> del_col_ids;
  auto& v_table = graph_.get_vertex_table(v_label);
  for (const auto& prop_name : properties) {
    int col_id = v_table.get_table().get_column_id_by_name(prop_name);
    if (col_id >= 0) {
      del_col_ids.push_back(col_id);
    }
  }

  logical_redo_.LogDeleteVertexProperties(vertex_type_name, config);
  auto status = graph_.DeleteVertexProperties(v_label, config);
  if (!status.ok()) {
    return status;
  }
  std::sort(del_col_ids.rbegin(), del_col_ids.rend());
  auto& columns_detached =
      detach_state_.vertex_tables[v_label].columns_detached;
  for (int col_id : del_col_ids) {
    if (static_cast<size_t>(col_id) < columns_detached.size()) {
      columns_detached.erase(columns_detached.begin() + col_id);
    }
  }
  for (const auto& prop_name : config.GetDeleteProperties()) {
    RETURN_IF_NOT_OK(
        dropVertexIndex(graph_, v_label, prop_name, &detach_state_));
  }
  mut_view_.Rebuild(graph_);
  return status;
}

Status CowGraphStorageAdapter::DeleteEdgePropertiesImpl(
    label_t src_label_id, label_t dst_label_id, label_t edge_label_id,
    const DeleteEdgePropertiesParam& config) {
  if (workspace_.is_in_place()) {
    auto status = graph_.DeleteEdgeProperties(src_label_id, dst_label_id,
                                              edge_label_id, config);
    if (status.ok()) {
      // Deleting edge properties may trigger a CSR rebuild
      // (unbundled→bundled or unbundled→empty), which replaces the underlying
      // CsrBase objects.  Rebuild the mutable view so cached pointers stay
      // valid.
      mut_view_.Rebuild(graph_);
    }
    return status;
  }
  const auto& schema = graph_.schema();
  const auto& src_type = schema.get_vertex_label_name(src_label_id);
  const auto& dst_type = schema.get_vertex_label_name(dst_label_id);
  const auto& edge_type = schema.get_edge_label_name(edge_label_id);
  for (const auto& prop_name : config.GetDeleteProperties()) {
    if (!schema.edge_has_property(src_label_id, dst_label_id, edge_label_id,
                                  prop_name)) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Property [" + prop_name + "] does not exist in edge [" +
                        edge_type + "] between [" + src_type + "] and [" +
                        dst_type + "].");
    }
  }

  // Collect pre-deletion state so we can update detach_state_ correctly.
  // EdgeTable::DeleteProperties may recreate the table entirely
  // (bundled↔unbundled conversion) in which case we must reset the
  // entire COW state rather than erasing individual entries.
  uint32_t triplet_id =
      schema.generate_edge_label(src_label_id, dst_label_id, edge_label_id);
  auto& edge_table = graph_.get_edge_table_by_index(triplet_id);
  bool was_bundled = edge_table.get_edge_schema_ptr()->is_bundled();
  size_t old_col_count =
      detach_state_.edge_tables.count(triplet_id)
          ? detach_state_.edge_tables.at(triplet_id).columns_detached.size()
          : 0;
  std::vector<int> del_col_ids;
  if (!was_bundled) {
    auto* table = edge_table.table();
    if (table) {
      for (const auto& prop_name : config.GetDeleteProperties()) {
        int col_id = table->get_column_id_by_name(prop_name);
        if (col_id >= 0) {
          del_col_ids.push_back(col_id);
        }
      }
    }
  }

  logical_redo_.LogDeleteEdgeProperties(src_type, dst_type, edge_type, config);
  auto status = graph_.DeleteEdgeProperties(src_label_id, dst_label_id,
                                            edge_label_id, config);
  if (status.ok()) {
    auto& state = detach_state_.edge_tables[triplet_id];
    auto edge_schema = graph_.schema().get_edge_schema(
        src_label_id, dst_label_id, edge_label_id);
    size_t new_col_count = edge_schema ? edge_schema->property_names.size() : 0;
    bool is_now_bundled = edge_schema && edge_schema->is_bundled();

    if (was_bundled != is_now_bundled ||
        new_col_count != old_col_count - del_col_ids.size()) {
      state.out_csr_detached = true;
      state.in_csr_detached = true;
      state.columns_detached.assign(new_col_count, true);
      state.out_adjlists_detached.clear();
      state.in_adjlists_detached.clear();
    } else {
      std::sort(del_col_ids.rbegin(), del_col_ids.rend());
      for (int col_id : del_col_ids) {
        if (static_cast<size_t>(col_id) < state.columns_detached.size()) {
          state.columns_detached.erase(state.columns_detached.begin() + col_id);
        }
      }
    }
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status CowGraphStorageAdapter::DeleteVertexTypeImpl(label_t v_label) {
  if (workspace_.is_in_place()) {
    const auto& v_schema = graph_.schema().get_vertex_schema(v_label);
    std::vector<std::string> indexed_properties;
    indexed_properties.reserve(v_schema->property_names.size() + 1);
    // The primary key is stored separately from property_names but indexes
    // bound to it must be removed together with the vertex type.
    indexed_properties.push_back(std::get<1>(v_schema->primary_keys[0]));
    for (size_t prop_idx = 0; prop_idx < v_schema->property_names.size();
         ++prop_idx) {
      if (v_schema->vprop_soft_deleted[prop_idx]) {
        continue;
      }
      indexed_properties.push_back(v_schema->property_names[prop_idx]);
    }
    RETURN_IF_NOT_OK(graph_.DeleteVertexType(v_label));
    for (const auto& property_name : indexed_properties) {
      RETURN_IF_NOT_OK(dropVertexIndex(graph_, v_label, property_name));
    }
    mut_view_.Rebuild(graph_);
    return Status::OK();
  }
  // Collect related edge triplet IDs before deletion.
  // PropertyGraph::DeleteVertexType removes them from edge_tables_, so
  // we must capture them while the schema is still intact.
  std::vector<uint32_t> related_edge_ids;
  const auto& schema = graph_.schema();
  const auto& vertex_type_name = schema.get_vertex_label_name(v_label);
  auto v_label_count = schema.vertex_label_frontier();
  auto e_label_count = schema.edge_label_frontier();
  for (label_t i = 0; i < v_label_count; ++i) {
    if (!schema.is_vertex_label_valid(i)) {
      continue;
    }
    for (label_t e = 0; e < e_label_count; ++e) {
      if (schema.is_edge_triplet_valid(v_label, i, e)) {
        related_edge_ids.push_back(schema.generate_edge_label(v_label, i, e));
      }
      if (v_label != i && schema.is_edge_triplet_valid(i, v_label, e)) {
        related_edge_ids.push_back(schema.generate_edge_label(i, v_label, e));
      }
    }
  }

  const auto& v_schema = graph_.schema().get_vertex_schema(v_label);
  std::vector<std::string> indexed_properties;
  indexed_properties.reserve(v_schema->property_names.size() + 1);
  // The primary key is stored separately from property_names but indexes bound
  // to it must be removed together with the vertex type.
  indexed_properties.push_back(std::get<1>(v_schema->primary_keys[0]));
  for (size_t prop_idx = 0; prop_idx < v_schema->property_names.size();
       ++prop_idx) {
    if (v_schema->vprop_soft_deleted[prop_idx]) {
      continue;
    }
    indexed_properties.push_back(v_schema->property_names[prop_idx]);
  }
  logical_redo_.LogDeleteVertexType(vertex_type_name);
  auto status = graph_.DeleteVertexType(v_label);
  if (!status.ok()) {
    return status;
  }
  detach_state_.vertex_tables[v_label] = VertexTableDetachState();
  for (uint32_t edge_id : related_edge_ids) {
    detach_state_.edge_tables.erase(edge_id);
  }
  for (const auto& property_name : indexed_properties) {
    RETURN_IF_NOT_OK(
        dropVertexIndex(graph_, v_label, property_name, &detach_state_));
  }
  mut_view_.Rebuild(graph_);
  return status;
}

Status CowGraphStorageAdapter::DeleteEdgeTypeImpl(label_t src_label_id,
                                                  label_t dst_label_id,
                                                  label_t edge_label_id) {
  if (workspace_.is_in_place()) {
    auto status =
        graph_.DeleteEdgeType(src_label_id, dst_label_id, edge_label_id);
    if (status.ok()) {
      mut_view_.Rebuild(graph_);
    }
    return status;
  }
  const auto& schema = graph_.schema();
  const auto& src_type = schema.get_vertex_label_name(src_label_id);
  const auto& dst_type = schema.get_vertex_label_name(dst_label_id);
  const auto& edge_type = schema.get_edge_label_name(edge_label_id);
  uint32_t triplet_id =
      schema.generate_edge_label(src_label_id, dst_label_id, edge_label_id);

  logical_redo_.LogDeleteEdgeType(src_type, dst_type, edge_type);
  auto status =
      graph_.DeleteEdgeType(src_label_id, dst_label_id, edge_label_id);
  if (status.ok()) {
    detach_state_.edge_tables.erase(triplet_id);
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status CowGraphStorageAdapter::AddVertexImpl(label_t label, const Value& oid,
                                             const std::vector<Value>& props,
                                             vid_t& vid) {
  if (workspace_.is_in_place()) {
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
        graph_.AddVertex(label, oid, props, vid, neug::timestamp_t(0), true);
    if (!status.ok()) {
      LOG(ERROR) << "AddVertex failed: " << status.ToString();
      return status;
    }

    // The graph row is already visible even if index maintenance fails.
    MarkVertexTableDirty(label);
    RETURN_IF_NOT_OK(addVertexIndexData(graph_, label, vid, oid, props));
    return Status::OK();
  }
  std::vector<DataType> types = graph_.schema().get_vertex_properties(label);
  if (types.size() != props.size()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Property count mismatch for vertex of label " +
                      graph_.schema().get_vertex_label_name(label));
  }
  int col_num = types.size();
  for (int col_i = 0; col_i != col_num; ++col_i) {
    if (props[col_i].type() != types[col_i]) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Property type mismatch at column " +
                        std::to_string(col_i) + " for vertex of label " +
                        graph_.schema().get_vertex_label_name(label));
    }
  }

  const auto& v_table = graph_.get_vertex_table(label);
  if (v_table.Size() >= v_table.Capacity()) {
    size_t new_capacity =
        v_table.Size() < 4096 ? 4096 : v_table.Size() + v_table.Size() / 4;
    RETURN_IF_NOT_OK(detachForResize(label, new_capacity));
    auto status = graph_.EnsureCapacity(label, new_capacity);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to ensure space for vertex of label "
                 << graph_.schema().get_vertex_label_name(label) << ": "
                 << status.ToString();
      return status;
    }
  }

  RETURN_IF_NOT_OK(detachVertexTableForInsert(label));
  logical_redo_.LogInsertVertex(label, oid, props);
  auto status = graph_.AddVertex(label, oid, props, vid, write_ts_, true);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to add vertex of label "
               << graph_.schema().get_vertex_label_name(label) << ": "
               << status.ToString();
    return status;
  }
  RETURN_IF_NOT_OK(addVertexIndexData(
      graph_, label, vid, oid, props,
      [this](StorageIndex& index) { return detachIndex(index); }));
  return Status::OK();
}

Status CowGraphStorageAdapter::DeleteVertexImpl(label_t label, vid_t lid) {
  if (workspace_.is_in_place()) {
    RETURN_IF_NOT_OK(graph_.DeleteVertex(label, lid, write_ts_));
    MarkVertexTableDirty(label);
    markIncidentEdgeTablesDirty(label);
    return deleteVertexIndexData(graph_, label, {lid});
  }
  if (!graph_.IsValidLid(label, lid, read_ts_)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Vertex id is out of range or already deleted");
  }
  auto oid = graph_.GetOid(label, lid, read_ts_);

  RETURN_IF_NOT_OK(prepareVertexDelete(label, {lid}));
  logical_redo_.LogRemoveVertex(label, oid);
  RETURN_IF_NOT_OK(graph_.DeleteVertex(label, lid, write_ts_));
  return deleteVertexIndexData(
      graph_, label, {lid},
      [this](StorageIndex& index) { return detachIndex(index); });
}

Status CowGraphStorageAdapter::AddEdgeImpl(label_t src_label, vid_t src_lid,
                                           label_t dst_label, vid_t dst_lid,
                                           label_t edge_label,
                                           const std::vector<Value>& properties,
                                           const void*& prop) {
  if (workspace_.is_in_place()) {
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
    auto status = graph_.AddEdge(src_label, src_lid, dst_label, dst_lid,
                                 edge_label, properties, neug::timestamp_t(0),
                                 alloc_, oe_offset, prop, true);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to add edge: " << status.ToString();
    }
    return status;
  }
  const auto& edge_table =
      graph_.get_edge_table(src_label, dst_label, edge_label);
  if (edge_table.PropTableSize() >= edge_table.Capacity()) {
    auto new_capacity =
        edge_table.PropTableSize() < 4096
            ? 4096
            : edge_table.PropTableSize() + edge_table.PropTableSize() / 4;
    RETURN_IF_NOT_OK(
        detachForResize(src_label, dst_label, edge_label, new_capacity));
    auto status =
        graph_.EnsureCapacity(src_label, dst_label, edge_label, new_capacity);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to ensure space before insert edge: "
                 << status.ToString();
      return status;
    }
  }
  uint32_t edge_idx =
      graph_.schema().generate_edge_label(src_label, dst_label, edge_label);
  RETURN_IF_NOT_OK(detachEdgeTableForInsert(edge_idx));
  RETURN_IF_NOT_OK(detachAdjlists(edge_idx, src_lid, dst_lid, alloc_));
  logical_redo_.LogInsertEdge(src_label, GetVertexId(src_label, src_lid),
                              dst_label, GetVertexId(dst_label, dst_lid),
                              edge_label, properties);
  int32_t oe_offset = 0;
  return graph_.AddEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                        properties, write_ts_, alloc_, oe_offset, prop, true);
}

Status CowGraphStorageAdapter::DeleteEdgesImpl(label_t src_label, vid_t src_lid,
                                               label_t dst_label, vid_t dst_lid,
                                               label_t edge_label) {
  if (workspace_.is_in_place()) {
    // Bulk mode: delegate to the batch version with a single pair.
    std::vector<std::tuple<vid_t, vid_t>> edges = {{src_lid, dst_lid}};
    return graph_.BatchDeleteEdges(src_label, dst_label, edge_label, edges);
  }
  if (!graph_.IsValidLid(src_label, src_lid, read_ts_) ||
      !graph_.IsValidLid(dst_label, dst_lid, read_ts_)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Source or destination vertex id is out of range or "
                  "already deleted");
  }

  uint32_t edge_idx =
      graph_.schema().generate_edge_label(src_label, dst_label, edge_label);
  auto search_edge_prop_type = determine_search_prop_type(
      graph_.schema().get_edge_properties(src_label, dst_label, edge_label));
  std::vector<std::pair<int32_t, int32_t>> matched_offsets;
  {
    auto oe_edges =
        GetGenericOutgoingGraphView(src_label, dst_label, edge_label)
            .get_edges(src_lid);
    auto ie_edges =
        GetGenericIncomingGraphView(dst_label, src_label, edge_label)
            .get_edges(dst_lid);
    int32_t oe_offset = 0;
    for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
      if (it.get_vertex() == dst_lid) {
        auto ie_offset = fuzzy_search_offset_from_nbr_list(
            ie_edges, src_lid, it.get_data_ptr(), search_edge_prop_type);
        matched_offsets.emplace_back(oe_offset, ie_offset);
      }
      ++oe_offset;
    }
  }

  if (matched_offsets.empty()) {
    return Status::OK();
  }

  const auto src_id = GetVertexId(src_label, src_lid);
  const auto dst_id = GetVertexId(dst_label, dst_lid);
  RETURN_IF_NOT_OK(detachEdgeTableForDelete(edge_idx));
  RETURN_IF_NOT_OK(detachAdjlists(edge_idx, src_lid, dst_lid, alloc_));

  for (const auto& [oe_offset, ie_offset] : matched_offsets) {
    logical_redo_.LogRemoveEdge(src_label, src_id, dst_label, dst_id,
                                edge_label, oe_offset, ie_offset);
    auto status =
        graph_.DeleteEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                          oe_offset, ie_offset, write_ts_);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to delete edge: " << status.ToString();
      return status;
    }
  }

  return Status::OK();
}

Status CowGraphStorageAdapter::DeleteEdgeImpl(label_t src_label, vid_t src_lid,
                                              label_t dst_label, vid_t dst_lid,
                                              label_t edge_label,
                                              int32_t oe_offset,
                                              int32_t ie_offset) {
  if (workspace_.is_in_place()) {
    return graph_.DeleteEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                             oe_offset, ie_offset, write_ts_);
  }
  if (!graph_.IsValidLid(src_label, src_lid, read_ts_) ||
      !graph_.IsValidLid(dst_label, dst_lid, read_ts_)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Source or destination vertex id is out of range or "
                  "already deleted");
  }

  uint32_t edge_idx =
      graph_.schema().generate_edge_label(src_label, dst_label, edge_label);
  RETURN_IF_NOT_OK(detachEdgeTableForDelete(edge_idx));
  RETURN_IF_NOT_OK(detachAdjlists(edge_idx, src_lid, dst_lid, alloc_));

  logical_redo_.LogRemoveEdge(src_label, GetVertexId(src_label, src_lid),
                              dst_label, GetVertexId(dst_label, dst_lid),
                              edge_label, oe_offset, ie_offset);

  return graph_.DeleteEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                           oe_offset, ie_offset, write_ts_);
}

Status CowGraphStorageAdapter::UpdateVertexPropertyImpl(label_t label,
                                                        vid_t lid, int col_id,
                                                        const Value& value) {
  if (workspace_.is_in_place()) {
    RETURN_IF_NOT_OK(
        graph_.UpdateVertexProperty(label, lid, col_id, value, write_ts_));
    MarkVertexTableDirty(label);
    return updateVertexIndexData(graph_, label, lid, col_id, value);
  }
  if (!graph_.IsValidLid(label, lid, read_ts_)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Vertex lid " + std::to_string(lid) + " of label " +
                      graph_.schema().get_vertex_label_name(label) +
                      " is not valid in this transaction.");
  }
  std::vector<DataType> types = graph_.schema().get_vertex_properties(label);
  if (static_cast<size_t>(col_id) >= types.size()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Column id " + std::to_string(col_id) + " is out of range.");
  }
  if (types[col_id] != value.type()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Type mismatch for column " + std::to_string(col_id) + ".");
  }
  RETURN_IF_NOT_OK(detachVertexColumn(label, col_id));
  logical_redo_.LogUpdateVertexProp(label, GetVertexId(label, lid), col_id,
                                    value);

  RETURN_IF_NOT_OK(
      graph_.UpdateVertexProperty(label, lid, col_id, value, write_ts_));

  return updateVertexIndexData(
      graph_, label, lid, col_id, value,
      [this](StorageIndex& index) { return detachIndex(index); });
}

Status CowGraphStorageAdapter::UpdateEdgePropertyImpl(
    label_t src_label, vid_t src, label_t dst_label, vid_t dst,
    label_t edge_label, int32_t oe_offset, int32_t ie_offset, int32_t col_id,
    const Value& value) {
  if (workspace_.is_in_place()) {
    return graph_.UpdateEdgeProperty(src_label, src, dst_label, dst, edge_label,
                                     oe_offset, ie_offset, col_id, value,
                                     neug::timestamp_t(0));
  }
  if (!graph_.IsValidLid(src_label, src, read_ts_) ||
      !graph_.IsValidLid(dst_label, dst, read_ts_)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Source or destination vertex id is out of range or "
                  "already deleted");
  }
  uint32_t edge_idx =
      graph_.schema().generate_edge_label(src_label, dst_label, edge_label);
  if (!graph_.HasEdgeTable(edge_idx)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Edge table for edge label triplet not found");
  }
  if (graph_.get_edge_table_by_index(edge_idx)
          .get_edge_schema_ptr()
          ->is_bundled()) {
    // Bundled properties live inside the adjacency-list records, so updating
    // them requires the CSR directory and both touched adjlists.
    RETURN_IF_NOT_OK(detachEdgeTableForDelete(edge_idx));
    RETURN_IF_NOT_OK(detachAdjlists(edge_idx, src, dst, alloc_));
  } else {
    // Unbundled properties live in the property table; the update does not
    // touch any CSR structure.
    RETURN_IF_NOT_OK(detachEdgeColumn(edge_idx, col_id));
  }
  logical_redo_.LogUpdateEdgeProp(src_label, GetVertexId(src_label, src),
                                  dst_label, GetVertexId(dst_label, dst),
                                  edge_label, oe_offset, ie_offset, col_id,
                                  value);
  return graph_.UpdateEdgeProperty(src_label, src, dst_label, dst, edge_label,
                                   oe_offset, ie_offset, col_id, value,
                                   write_ts_);
}

Status CowGraphStorageAdapter::detachVertexTableForInsert(label_t label) {
  auto& state = detach_state_.vertex_tables[label];
  auto& vertex_table = graph_.get_vertex_table(label);
  bool did_detach = false;
  if (!state.indexer_detached) {
    vertex_table.DetachIndexer();
    state.indexer_detached = true;
    did_detach = true;
  }
  if (!state.vertex_timestamp_detached) {
    vertex_table.DetachVertexTimestamp();
    state.vertex_timestamp_detached = true;
    did_detach = true;
  }
  for (size_t i = 0; i < state.columns_detached.size(); ++i) {
    if (!state.columns_detached[i]) {
      vertex_table.get_table().DetachColumn(i, ckp_, graph_.memory_level());
      state.columns_detached[i] = true;
      did_detach = true;
    }
  }
  if (did_detach) {
    mut_view_.Rebuild(graph_);
  }
  return Status::OK();
}

Status CowGraphStorageAdapter::detachVertexTableForDelete(label_t label) {
  auto& state = detach_state_.vertex_tables[label];
  auto& vertex_table = graph_.get_vertex_table(label);
  bool did_detach = false;
  if (!state.vertex_timestamp_detached) {
    vertex_table.DetachVertexTimestamp();
    state.vertex_timestamp_detached = true;
    did_detach = true;
  }
  for (size_t i = 0; i < state.columns_detached.size(); ++i) {
    if (!state.columns_detached[i] &&
        dynamic_cast<VecColumn*>(
            vertex_table.get_table().get_column_by_id(i)) != nullptr) {
      vertex_table.get_table().DetachColumn(i, ckp_, graph_.memory_level());
      state.columns_detached[i] = true;
      did_detach = true;
    }
  }
  if (did_detach) {
    mut_view_.Rebuild(graph_);
  }
  return Status::OK();
}

Status CowGraphStorageAdapter::detachVertexColumn(label_t label,
                                                  int32_t col_id) {
  auto& state = detach_state_.vertex_tables[label];
  if (col_id >= 0 &&
      static_cast<size_t>(col_id) < state.columns_detached.size() &&
      !state.columns_detached[col_id]) {
    auto& vertex_table = graph_.get_vertex_table(label);
    vertex_table.get_table().DetachColumn(col_id, ckp_, graph_.memory_level());
    state.columns_detached[col_id] = true;
    mut_view_.Rebuild(graph_);
  }
  return Status::OK();
}

Status CowGraphStorageAdapter::detachEdgeTableForInsert(
    uint32_t edge_triplet_id) {
  if (!graph_.HasEdgeTable(edge_triplet_id)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Edge table for edge label triplet not found");
  }
  auto& state = detach_state_.edge_tables[edge_triplet_id];
  auto& edge_table = graph_.get_edge_table_by_index(edge_triplet_id);
  bool did_detach = false;
  if (!state.out_csr_detached) {
    edge_table.DetachOutCsr();
    state.out_csr_detached = true;
    did_detach = true;
  }
  if (!state.in_csr_detached) {
    edge_table.DetachInCsr();
    state.in_csr_detached = true;
    did_detach = true;
  }
  if (edge_table.table()) {
    for (size_t i = 0; i < state.columns_detached.size(); ++i) {
      if (!state.columns_detached[i]) {
        edge_table.table()->DetachColumn(i, ckp_, graph_.memory_level());
        state.columns_detached[i] = true;
        did_detach = true;
      }
    }
  }
  if (did_detach) {
    mut_view_.Rebuild(graph_);
  }
  return Status::OK();
}

Status CowGraphStorageAdapter::detachEdgeTableForDelete(
    uint32_t edge_triplet_id) {
  if (!graph_.HasEdgeTable(edge_triplet_id)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Edge table for edge label triplet not found");
  }
  auto& state = detach_state_.edge_tables[edge_triplet_id];
  auto& edge_table = graph_.get_edge_table_by_index(edge_triplet_id);
  bool did_detach = false;
  if (!state.out_csr_detached) {
    edge_table.DetachOutCsr();
    state.out_csr_detached = true;
    did_detach = true;
  }
  if (!state.in_csr_detached) {
    edge_table.DetachInCsr();
    state.in_csr_detached = true;
    did_detach = true;
  }
  if (did_detach) {
    mut_view_.Rebuild(graph_);
  }
  return Status::OK();
}

Status CowGraphStorageAdapter::detachEdgeColumn(uint32_t edge_triplet_id,
                                                int32_t col_id) {
  // Edge property updates never detach CSRs: bundled properties live in the
  // adjlists detached by the caller, and unbundled properties live in the
  // property table column detached here.
  if (!graph_.HasEdgeTable(edge_triplet_id)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Edge table for edge label triplet not found");
  }
  auto& state = detach_state_.edge_tables[edge_triplet_id];
  auto& edge_table = graph_.get_edge_table_by_index(edge_triplet_id);
  if (!edge_table.get_edge_schema_ptr()->is_bundled() && edge_table.table() &&
      col_id >= 0 &&
      static_cast<size_t>(col_id) < state.columns_detached.size() &&
      !state.columns_detached[col_id]) {
    edge_table.table()->DetachColumn(col_id, ckp_, graph_.memory_level());
    state.columns_detached[col_id] = true;
    mut_view_.Rebuild(graph_);
  }
  return Status::OK();
}

Status CowGraphStorageAdapter::detachAdjlists(uint32_t edge_triplet_id,
                                              vid_t src_lid, vid_t dst_lid,
                                              Allocator& alloc) {
  auto& state = detach_state_.edge_tables[edge_triplet_id];
  auto& edge_table = graph_.get_edge_table_by_index(edge_triplet_id);
  if (state.out_adjlists_detached.find(src_lid) ==
      state.out_adjlists_detached.end()) {
    edge_table.DetachOutAdjlist(src_lid, alloc);
    state.out_adjlists_detached.insert(src_lid);
  }
  if (state.in_adjlists_detached.find(dst_lid) ==
      state.in_adjlists_detached.end()) {
    edge_table.DetachInAdjlist(dst_lid, alloc);
    state.in_adjlists_detached.insert(dst_lid);
  }
  return Status::OK();
}

Status CowGraphStorageAdapter::detachForResize(label_t label, size_t capacity) {
  const auto& vertex_table = graph_.get_vertex_table(label);
  if (capacity <= vertex_table.Capacity()) {
    return Status::OK();
  }
  RETURN_IF_NOT_OK(detachVertexTableForInsert(label));
  const auto& schema = graph_.schema();
  auto vertex_label_count = schema.vertex_label_frontier();
  auto edge_label_count = schema.edge_label_frontier();
  for (label_t dst = 0; dst < vertex_label_count; ++dst) {
    if (!schema.is_vertex_label_valid(dst)) {
      continue;
    }
    for (label_t e = 0; e < edge_label_count; ++e) {
      if (schema.is_edge_triplet_valid(label, dst, e)) {
        uint32_t idx = schema.generate_edge_label(label, dst, e);
        RETURN_IF_NOT_OK(detachEdgeTableForDelete(idx));
      }
      if (label != dst && schema.is_edge_triplet_valid(dst, label, e)) {
        uint32_t idx = schema.generate_edge_label(dst, label, e);
        RETURN_IF_NOT_OK(detachEdgeTableForDelete(idx));
      }
    }
  }
  return Status::OK();
}

Status CowGraphStorageAdapter::detachForResize(label_t src_label,
                                               label_t dst_label,
                                               label_t edge_label,
                                               size_t capacity) {
  uint32_t idx =
      graph_.schema().generate_edge_label(src_label, dst_label, edge_label);
  return detachEdgeTableForInsert(idx);
}

Status CowGraphStorageAdapter::prepareVertexDelete(
    label_t label, const std::vector<vid_t>& lids) {
  // Detach the validity/timestamp module, then only the triplets that actually
  // hold an incident edge of a deleted vertex. Per-triplet detachment covers
  // the CSR directory arrays (required before any adjlist buffer can be
  // redirected) and the touched adjacency lists themselves, so the write
  // footprint stays proportional to the real delete set instead of the schema
  // breadth.
  RETURN_IF_NOT_OK(detachVertexTableForDelete(label));

  const auto& schema = graph_.schema();
  for (vid_t lid : lids) {
    if (!graph_.IsValidLid(label, lid, read_ts_)) {
      continue;
    }
    auto related_edges =
        fetch_edges_related_to_vertex(*this, schema, label, lid, read_ts_);
    for (auto& [edge_triplet_id, edges] : related_edges) {
      RETURN_IF_NOT_OK(detachEdgeTableForDelete(edge_triplet_id));
      for (auto& [src, dst, oe_off, ie_off] : edges) {
        RETURN_IF_NOT_OK(detachAdjlists(edge_triplet_id, src, dst, alloc_));
      }
    }
  }
  return Status::OK();
}

result<std::vector<vid_t>> CowGraphStorageAdapter::BatchAddVerticesImpl(
    label_t v_label_id, std::shared_ptr<IDataChunkSupplier> supplier) {
  if (workspace_.is_in_place()) {
    auto new_vids = graph_.BatchAddVertices(v_label_id, std::move(supplier));
    if (!new_vids) {
      return tl::unexpected(new_vids.error());
    }
    if (new_vids->empty()) {
      return new_vids;
    }
    auto status = AddBatchVertexIndexData(graph_, v_label_id, new_vids.value());
    if (!status.ok()) {
      return tl::unexpected(std::move(status));
    }
    return new_vids;
  }
  LOG(ERROR) << "BatchAddVertices is not supported in TP mode currently.";
  RETURN_STATUS_ERROR(
      StatusCode::ERR_NOT_SUPPORTED,
      "BatchAddVertices is not supported in TP mode currently.");
}

Status CowGraphStorageAdapter::BatchAddEdgesImpl(
    label_t src_label, label_t dst_label, label_t edge_label,
    std::shared_ptr<IDataChunkSupplier> supplier) {
  if (workspace_.is_in_place()) {
    return graph_.BatchAddEdges(src_label, dst_label, edge_label,
                                std::move(supplier));
  }
  LOG(ERROR) << "BatchAddEdges is not supported in TP mode currently.";
  return Status(StatusCode::ERR_NOT_SUPPORTED,
                "BatchAddEdges is not supported in TP mode currently.");
}

Status CowGraphStorageAdapter::BatchDeleteVerticesImpl(
    label_t v_label_id, const std::vector<vid_t>& vids) {
  if (workspace_.is_in_place()) {
    RETURN_IF_NOT_OK(graph_.BatchDeleteVertices(v_label_id, vids));
    MarkVertexTableDirty(v_label_id);
    markIncidentEdgeTablesDirty(v_label_id);
    return deleteVertexIndexData(graph_, v_label_id, vids);
  }
  const auto initial_op_num = logical_redo_.op_num();
  for (vid_t lid : vids) {
    if (!DeleteVertex(v_label_id, lid)) {
      LOG(ERROR) << "Failed to delete vertex " << lid << " of label "
                 << v_label_id << " in batch request.";
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Failed to delete vertex " + std::to_string(lid));
    }
  }
  if (logical_redo_.op_num() != initial_op_num) {
    workspace_.MarkBatchMutation();
  }
  return Status::OK();
}

Status CowGraphStorageAdapter::BatchDeleteEdgesImpl(
    label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
    const std::vector<std::tuple<vid_t, vid_t>>& edges) {
  if (workspace_.is_in_place()) {
    return graph_.BatchDeleteEdges(src_v_label_id, dst_v_label_id,
                                   edge_label_id, edges);
  }
  const auto initial_op_num = logical_redo_.op_num();
  for (const auto& edge : edges) {
    vid_t src_lid = std::get<0>(edge);
    vid_t dst_lid = std::get<1>(edge);
    if (!DeleteEdges(src_v_label_id, src_lid, dst_v_label_id, dst_lid,
                     edge_label_id)) {
      LOG(ERROR) << "Failed to delete edge from vertex " << src_lid
                 << " to vertex " << dst_lid << " in batch request.";
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Failed to delete edge from vertex " +
                        std::to_string(src_lid) + " to vertex " +
                        std::to_string(dst_lid));
    }
  }
  if (logical_redo_.op_num() != initial_op_num) {
    workspace_.MarkBatchMutation();
  }
  return Status::OK();
}

Status CowGraphStorageAdapter::BatchDeleteEdgesImpl(
    label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
    const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
    const std::vector<std::pair<vid_t, int32_t>>& ie_edges) {
  if (workspace_.is_in_place()) {
    return graph_.BatchDeleteEdges(src_v_label_id, dst_v_label_id,
                                   edge_label_id, oe_edges, ie_edges);
  }
  assert(oe_edges.size() == ie_edges.size());
  const auto initial_op_num = logical_redo_.op_num();
  for (size_t i = 0; i < oe_edges.size(); ++i) {
    vid_t src_lid = oe_edges[i].first;
    vid_t dst_lid = ie_edges[i].first;
    int32_t oe_offset = oe_edges[i].second;
    int32_t ie_offset = ie_edges[i].second;
    if (!DeleteEdge(src_v_label_id, src_lid, dst_v_label_id, dst_lid,
                    edge_label_id, oe_offset, ie_offset)) {
      LOG(ERROR) << "Failed to delete edge from vertex " << src_lid
                 << " to vertex " << dst_lid << " in batch request.";
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Failed to delete edge from vertex " +
                        std::to_string(src_lid) + " to vertex " +
                        std::to_string(dst_lid));
    }
  }
  if (logical_redo_.op_num() != initial_op_num) {
    workspace_.MarkBatchMutation();
  }
  return Status::OK();
}

Status CowGraphStorageAdapter::detachIndex(StorageIndex& index) {
  const auto& name = index.GetMeta().name;
  auto it = detach_state_.index_detached.find(name);
  if (it != detach_state_.index_detached.end() && it->second) {
    return Status::OK();
  }
  index.Detach(ckp_, graph_.memory_level());
  detach_state_.index_detached[name] = true;
  return Status::OK();
}

// Index DDL below is only meaningful for in-place (bulk/index) workspaces:
// private-COW transactions do not manage indexes yet.

/**
 * Creates an index for a vertex property.
 *
 * When creating an HNSW index, this method converts an ArrayColumn to a
 * VecColumn. The VecColumn reuses the ArrayColumn's underlying vector buffer
 * without copying its data. Subsequent incremental vector updates avoid
 * copy-on-write by letting the VecColumn maintain separate buffer versions.
 */
neug::result<StorageIndex*> CowGraphStorageAdapter::CreateIndex(
    std::unique_ptr<IndexMeta> meta) {
  if (!workspace_.is_in_place()) {
    RETURN_STATUS_ERROR(
        StatusCode::ERR_NOT_SUPPORTED,
        "Index DDL is not supported by private-COW transactions");
  }
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
    GS_AUTO(existing_indexes,
            graph_.mutable_index_manager().GetIndex(label_id, property_name));
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

  GS_AUTO(index, graph_.mutable_index_manager().CreateIndex(
                     std::move(meta), std::move(index_id_accessor), column,
                     graph_.GetVertexSet(label_id, write_ts_)));

  workspace_.MarkPlanningChanged();
  if (vec_column) {
    vertex_table.SetColumn(static_cast<size_t>(property_col),
                           std::move(vec_column));
    MarkVertexTableDirty(label_id);
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
Status CowGraphStorageAdapter::DropIndex(const std::string& name) {
  if (!workspace_.is_in_place()) {
    return Status(StatusCode::ERR_NOT_SUPPORTED,
                  "Index DDL is not supported by private-COW transactions");
  }
  auto& index_manager = graph_.mutable_index_manager();
  auto pending = index_manager.GetPendingIndexByName(name);
  const bool is_pending = pending.has_value();
  IndexMeta meta;
  if (is_pending) {
    meta = pending.value()->meta;
  } else {
    auto target = index_manager.GetIndexByName(name);
    if (!target) {
      return target.error();
    }
    meta = target.value()->GetMeta();
  }
  std::unique_ptr<ArrayColumn> array_column;
  int32_t property_col = -1;

  if (IsHNSWIndex(meta)) {
    bool has_other_hnsw = false;
    auto pending_indexes = index_manager.GetPendingIndex(
        meta.schema.label_id, meta.schema.property_name);
    if (!pending_indexes) {
      return pending_indexes.error();
    }
    has_other_hnsw = std::any_of(
        pending_indexes->begin(), pending_indexes->end(),
        [&](const StorageIndexManager::PendingIndex* index) {
          return index->meta.name != name && IsHNSWIndex(index->meta);
        });
    if (!has_other_hnsw) {
      auto indexes = index_manager.GetAllIndexes();
      if (!indexes) {
        return indexes.error();
      }
      has_other_hnsw = std::any_of(
          indexes->begin(), indexes->end(), [&](StorageIndex* index) {
            const auto& other_meta = index->GetMeta();
            return other_meta.name != name &&
                   other_meta.schema.label_id == meta.schema.label_id &&
                   other_meta.schema.property_name ==
                       meta.schema.property_name &&
                   IsHNSWIndex(other_meta);
          });
    }
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

  RETURN_IF_NOT_OK(index_manager.DropIndex(name));
  workspace_.MarkPlanningChanged();
  if (array_column) {
    auto& vertex_table = graph_.get_vertex_table(meta.schema.label_id);
    vertex_table.SetColumn(static_cast<size_t>(property_col),
                           std::move(array_column));
    MarkVertexTableDirty(meta.schema.label_id);
    mut_view_.Rebuild(graph_);
  }
  return Status::OK();
}

Status CowGraphStorageAdapter::ActivateIndexes() {
  if (!workspace_.is_in_place()) {
    return Status(StatusCode::ERR_NOT_SUPPORTED,
                  "Index DDL is not supported by private-COW transactions");
  }
  auto activated = graph_.ActivateIndexes();
  if (!activated) {
    return activated.error();
  }
  if (activated.value() == 0) {
    return Status::OK();
  }
  mut_view_.Rebuild(graph_);
  workspace_.MarkPlanningChanged();
  return Status::OK();
}

}  // namespace neug
