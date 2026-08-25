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

#include "neug/transaction/cow_graph_storage.h"

#include <glog/logging.h>
#include <cstdint>

#include <algorithm>
#include <exception>
#include <filesystem>
#include <functional>
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

Status CowGraphStorage::AddGraphEntry(const std::string& name,
                                      const ProjectedGraphEntry& entry) {
  if (graph_.schema().references_temporary_schema(entry)) {
    workspace_.MarkTransientMutation();
  } else {
    logical_redo_.LogAddGraphEntry(name, entry);
  }
  RETURN_IF_NOT_OK(graph_.mutable_schema().AddGraphEntry(name, entry));
  MarkSchemaDirty();
  return Status::OK();
}

Status CowGraphStorage::DropGraphEntry(const std::string& name) {
  auto entry = graph_.schema().GetGraphEntry(name);
  if (!entry) {
    return entry.error();
  }
  if (graph_.schema().references_temporary_schema(*entry.value())) {
    workspace_.MarkTransientMutation();
  } else {
    logical_redo_.LogDropGraphEntry(name);
  }
  RETURN_IF_NOT_OK(graph_.mutable_schema().DropGraphEntry(name));
  MarkSchemaDirty();
  return Status::OK();
}

result<CreatedIndex> CowGraphStorage::CreateIndex(
    std::unique_ptr<IndexMeta> meta) {
  if (!meta) {
    RETURN_STATUS_ERROR(StatusCode::ERR_INVALID_ARGUMENT,
                        "Cannot create index with null metadata");
  }
  const auto label = meta->schema.label_id;
  if (graph_.schema().is_vertex_label_temporary(label)) {
    RETURN_STATUS_ERROR(StatusCode::ERR_NOT_SUPPORTED,
                        "Indexes on temporary vertex types are not supported");
  }
  auto& index_manager = graph_.mutable_index_manager();
  if (index_manager.GetPendingIndexByName(meta->name).has_value() ||
      index_manager.GetIndexByName(meta->name).has_value()) {
    RETURN_STATUS_ERROR(StatusCode::ERR_ILLEGAL_OPERATION,
                        "Index already exists: " + meta->name);
  }
  const IndexMeta redo_meta = *meta;
  auto created =
      CreateStorageIndex(graph_, mut_view_, read_ts_, std::move(meta));
  if (!created) {
    return tl::unexpected(created.error());
  }
  logical_redo_.LogCreateIndex(redo_meta);
  return created;
}

Status CowGraphStorage::DropIndex(const std::string& name) {
  auto& index_manager = graph_.mutable_index_manager();
  if (!index_manager.GetPendingIndexByName(name).has_value() &&
      !index_manager.GetIndexByName(name).has_value()) {
    return Status(StatusCode::ERR_NOT_FOUND, "Index not found: " + name);
  }
  RETURN_IF_NOT_OK(DropStorageIndex(graph_, mut_view_, name));
  logical_redo_.LogDropIndex(name);
  return Status::OK();
}

result<size_t> CowGraphStorage::ActivateIndexes() {
  auto activated = ActivateStorageIndexes(graph_, mut_view_);
  if (!activated) {
    return tl::unexpected(activated.error());
  }
  if (activated.value() > 0) {
    logical_redo_.LogActivateIndexes();
  }
  return activated.value();
}

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
    if (edges.empty()) {
      return;
    }
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

Status CowGraphStorage::CreateVertexTypeImpl(
    const CreateVertexTypeParam& config) {
  return applyCreateVertexType(config, PersistentSchemaCommitMode::kWal);
}

Status CowGraphStorage::applyCreateVertexType(
    const CreateVertexTypeParam& config,
    PersistentSchemaCommitMode commit_mode) {
  const auto& name = config.GetVertexLabel();
  if (graph_.schema().is_vertex_label_valid(name)) {
    LOG(ERROR) << "Vertex type " << name << " already exists.";
    return Status(StatusCode::ERR_SCHEMA_MISMATCH,
                  "Vertex type " + name + " already exists.");
  }
  if (config.IsTemporary()) {
    workspace_.MarkTransientMutation();
  } else {
    switch (commit_mode) {
    case PersistentSchemaCommitMode::kWal:
      logical_redo_.LogCreateVertexType(config);
      break;
    case PersistentSchemaCommitMode::kCheckpoint:
      workspace_.MarkBulkMutation();
      break;
    }
  }
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

Status CowGraphStorage::CreateEdgeTypeImpl(const CreateEdgeTypeParam& config) {
  return applyCreateEdgeType(config, PersistentSchemaCommitMode::kWal);
}

Status CowGraphStorage::applyCreateEdgeType(
    const CreateEdgeTypeParam& config, PersistentSchemaCommitMode commit_mode) {
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
  const auto& current_schema = graph_.schema();
  const bool references_temporary_vertex =
      (current_schema.is_vertex_label_valid(src_type) &&
       current_schema.is_vertex_label_temporary(
           current_schema.get_vertex_label_id(src_type))) ||
      (current_schema.is_vertex_label_valid(dst_type) &&
       current_schema.is_vertex_label_temporary(
           current_schema.get_vertex_label_id(dst_type)));
  if (config.IsTemporary() || references_temporary_vertex) {
    workspace_.MarkTransientMutation();
  } else {
    switch (commit_mode) {
    case PersistentSchemaCommitMode::kWal:
      logical_redo_.LogCreateEdgeType(config);
      break;
    case PersistentSchemaCommitMode::kCheckpoint:
      workspace_.MarkBulkMutation();
      break;
    }
  }
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

Status CowGraphStorage::AddVertexPropertiesImpl(
    label_t v_label, const AddVertexPropertiesParam& config) {
  const auto& schema = graph_.schema();
  if (schema.is_vertex_label_temporary(v_label)) {
    workspace_.MarkTransientMutation();
  } else {
    logical_redo_.LogAddVertexProperties(schema.get_vertex_label_name(v_label),
                                         config);
  }
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

Status CowGraphStorage::AddEdgePropertiesImpl(
    label_t src_label_id, label_t dst_label_id, label_t edge_label_id,
    const AddEdgePropertiesParam& config) {
  const auto& schema = graph_.schema();
  if (schema.is_edge_triplet_temporary(src_label_id, dst_label_id,
                                       edge_label_id)) {
    workspace_.MarkTransientMutation();
  } else {
    logical_redo_.LogAddEdgeProperties(
        schema.get_vertex_label_name(src_label_id),
        schema.get_vertex_label_name(dst_label_id),
        schema.get_edge_label_name(edge_label_id), config);
  }
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

Status CowGraphStorage::RenameVertexPropertiesImpl(
    label_t v_label, const RenameVertexPropertiesParam& config) {
  const auto& schema = graph_.schema();
  const auto vertex_type_name = schema.get_vertex_label_name(v_label);
  if (schema.is_vertex_label_temporary(v_label)) {
    workspace_.MarkTransientMutation();
  } else {
    logical_redo_.LogRenameVertexProperties(vertex_type_name, config);
  }
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

Status CowGraphStorage::RenameEdgePropertiesImpl(
    label_t src_label_id, label_t dst_label_id, label_t edge_label_id,
    const RenameEdgePropertiesParam& config) {
  const auto& schema = graph_.schema();
  if (schema.is_edge_triplet_temporary(src_label_id, dst_label_id,
                                       edge_label_id)) {
    workspace_.MarkTransientMutation();
  } else {
    logical_redo_.LogRenameEdgeProperties(
        schema.get_vertex_label_name(src_label_id),
        schema.get_vertex_label_name(dst_label_id),
        schema.get_edge_label_name(edge_label_id), config);
  }
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

Status CowGraphStorage::DeleteVertexPropertiesImpl(
    label_t v_label, const DeleteVertexPropertiesParam& config) {
  const auto& properties = config.GetDeleteProperties();
  const auto& vertex_type_name = graph_.schema().get_vertex_label_name(v_label);
  for (const auto& prop_name : properties) {
    if (!graph_.schema().vertex_has_property(v_label, prop_name)) {
      return Status(StatusCode::ERR_SCHEMA_MISMATCH,
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

  if (graph_.schema().is_vertex_label_temporary(v_label)) {
    workspace_.MarkTransientMutation();
  } else {
    logical_redo_.LogDeleteVertexProperties(vertex_type_name, config);
  }
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

Status CowGraphStorage::DeleteEdgePropertiesImpl(
    label_t src_label_id, label_t dst_label_id, label_t edge_label_id,
    const DeleteEdgePropertiesParam& config) {
  const auto& schema = graph_.schema();
  const auto& src_type = schema.get_vertex_label_name(src_label_id);
  const auto& dst_type = schema.get_vertex_label_name(dst_label_id);
  const auto& edge_type = schema.get_edge_label_name(edge_label_id);
  for (const auto& prop_name : config.GetDeleteProperties()) {
    if (!schema.edge_has_property(src_label_id, dst_label_id, edge_label_id,
                                  prop_name)) {
      return Status(StatusCode::ERR_SCHEMA_MISMATCH,
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

  if (schema.is_edge_triplet_temporary(src_label_id, dst_label_id,
                                       edge_label_id)) {
    workspace_.MarkTransientMutation();
  } else {
    logical_redo_.LogDeleteEdgeProperties(src_type, dst_type, edge_type,
                                          config);
  }
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

Status CowGraphStorage::DeleteVertexTypeImpl(label_t v_label) {
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
  const bool is_temporary = graph_.schema().is_vertex_label_temporary(v_label);
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
  if (is_temporary) {
    workspace_.MarkTransientMutation();
  } else {
    logical_redo_.LogDeleteVertexType(vertex_type_name);
  }
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

Status CowGraphStorage::DeleteEdgeTypeImpl(label_t src_label_id,
                                           label_t dst_label_id,
                                           label_t edge_label_id) {
  const auto& schema = graph_.schema();
  const auto& src_type = schema.get_vertex_label_name(src_label_id);
  const auto& dst_type = schema.get_vertex_label_name(dst_label_id);
  const auto& edge_type = schema.get_edge_label_name(edge_label_id);
  uint32_t triplet_id =
      schema.generate_edge_label(src_label_id, dst_label_id, edge_label_id);
  const bool is_temporary = schema.is_edge_triplet_temporary(
      src_label_id, dst_label_id, edge_label_id);

  if (is_temporary) {
    workspace_.MarkTransientMutation();
  } else {
    logical_redo_.LogDeleteEdgeType(src_type, dst_type, edge_type);
  }
  auto status =
      graph_.DeleteEdgeType(src_label_id, dst_label_id, edge_label_id);
  if (status.ok()) {
    detach_state_.edge_tables.erase(triplet_id);
    mut_view_.Rebuild(graph_);
  }
  return status;
}

Status CowGraphStorage::AddVertexImpl(label_t label, const Value& oid,
                                      const std::vector<Value>& props,
                                      vid_t& vid) {
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
  const auto& schema = graph_.schema();
  const auto& vertex_type = schema.get_vertex_label_name(label);
  if (schema.is_vertex_label_temporary(label)) {
    workspace_.MarkTransientMutation();
  } else {
    logical_redo_.LogInsertVertex(vertex_type, oid, props);
  }
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

Status CowGraphStorage::DeleteVertexImpl(label_t label, vid_t lid) {
  if (!graph_.IsValidLid(label, lid, read_ts_)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Vertex id is out of range or already deleted");
  }
  auto oid = graph_.GetOid(label, lid, read_ts_);

  std::vector<uint32_t> touched_edge_triplets;
  RETURN_IF_NOT_OK(prepareVertexDelete(label, lid, touched_edge_triplets));
  const auto& schema = graph_.schema();
  if (schema.is_vertex_label_temporary(label)) {
    workspace_.MarkTransientMutation();
  } else {
    logical_redo_.LogRemoveVertex(schema.get_vertex_label_name(label), oid);
  }
  RETURN_IF_NOT_OK(graph_.DeleteVertex(label, lid, write_ts_));
  // Exact edge-table dirtiness is only known while preparing this deletion;
  // mark it here instead of conservatively marking every incident triplet in
  // StorageUpdateInterface.
  graph_.MarkVertexTableDirty(label);
  for (uint32_t edge_triplet_id : touched_edge_triplets) {
    auto [src_label, dst_label, edge_label] =
        schema.parse_edge_label(edge_triplet_id);
    graph_.MarkEdgeTableDirty(src_label, dst_label, edge_label);
  }
  return deleteVertexIndexData(
      graph_, label, {lid},
      [this](StorageIndex& index) { return detachIndex(index); });
}

Status CowGraphStorage::AddEdgeImpl(label_t src_label, vid_t src_lid,
                                    label_t dst_label, vid_t dst_lid,
                                    label_t edge_label,
                                    const std::vector<Value>& properties,
                                    const void*& prop) {
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
  const auto& schema = graph_.schema();
  if (schema.is_edge_triplet_temporary(src_label, dst_label, edge_label)) {
    workspace_.MarkTransientMutation();
  } else {
    logical_redo_.LogInsertEdge(schema.get_vertex_label_name(src_label),
                                GetVertexId(src_label, src_lid),
                                schema.get_vertex_label_name(dst_label),
                                GetVertexId(dst_label, dst_lid),
                                schema.get_edge_label_name(edge_label),
                                properties);
  }
  int32_t oe_offset = 0;
  return graph_.AddEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                        properties, write_ts_, alloc_, oe_offset, prop, true);
}

Status CowGraphStorage::DeleteEdgesImpl(label_t src_label, vid_t src_lid,
                                        label_t dst_label, vid_t dst_lid,
                                        label_t edge_label) {
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

  const auto& schema = graph_.schema();
  const bool is_temporary =
      schema.is_edge_triplet_temporary(src_label, dst_label, edge_label);
  if (is_temporary) {
    workspace_.MarkTransientMutation();
  }
  for (const auto& [oe_offset, ie_offset] : matched_offsets) {
    if (!is_temporary) {
      logical_redo_.LogRemoveEdge(
          schema.get_vertex_label_name(src_label), src_id,
          schema.get_vertex_label_name(dst_label), dst_id,
          schema.get_edge_label_name(edge_label), oe_offset, ie_offset);
    }
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

Status CowGraphStorage::DeleteEdgeImpl(label_t src_label, vid_t src_lid,
                                       label_t dst_label, vid_t dst_lid,
                                       label_t edge_label, int32_t oe_offset,
                                       int32_t ie_offset) {
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

  const auto& schema = graph_.schema();
  if (schema.is_edge_triplet_temporary(src_label, dst_label, edge_label)) {
    workspace_.MarkTransientMutation();
  } else {
    logical_redo_.LogRemoveEdge(schema.get_vertex_label_name(src_label),
                                GetVertexId(src_label, src_lid),
                                schema.get_vertex_label_name(dst_label),
                                GetVertexId(dst_label, dst_lid),
                                schema.get_edge_label_name(edge_label),
                                oe_offset, ie_offset);
  }

  return graph_.DeleteEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                           oe_offset, ie_offset, write_ts_);
}

Status CowGraphStorage::UpdateVertexPropertyImpl(label_t label, vid_t lid,
                                                 int col_id,
                                                 const Value& value) {
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
  const auto& schema = graph_.schema();
  if (schema.is_vertex_label_temporary(label)) {
    workspace_.MarkTransientMutation();
  } else {
    logical_redo_.LogUpdateVertexProp(schema.get_vertex_label_name(label),
                                      GetVertexId(label, lid), col_id, value);
  }

  RETURN_IF_NOT_OK(
      graph_.UpdateVertexProperty(label, lid, col_id, value, write_ts_));

  return updateVertexIndexData(
      graph_, label, lid, col_id, value,
      [this](StorageIndex& index) { return detachIndex(index); });
}

Status CowGraphStorage::UpdateEdgePropertyImpl(
    label_t src_label, vid_t src, label_t dst_label, vid_t dst,
    label_t edge_label, int32_t oe_offset, int32_t ie_offset, int32_t col_id,
    const Value& value) {
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
  const auto& schema = graph_.schema();
  if (schema.is_edge_triplet_temporary(src_label, dst_label, edge_label)) {
    workspace_.MarkTransientMutation();
  } else {
    logical_redo_.LogUpdateEdgeProp(
        schema.get_vertex_label_name(src_label), GetVertexId(src_label, src),
        schema.get_vertex_label_name(dst_label), GetVertexId(dst_label, dst),
        schema.get_edge_label_name(edge_label), oe_offset, ie_offset, col_id,
        value);
  }
  return graph_.UpdateEdgeProperty(src_label, src, dst_label, dst, edge_label,
                                   oe_offset, ie_offset, col_id, value,
                                   write_ts_);
}

Status CowGraphStorage::detachVertexTableForInsert(label_t label) {
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

Status CowGraphStorage::detachVertexTableForDelete(label_t label) {
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

Status CowGraphStorage::detachVertexColumn(label_t label, int32_t col_id) {
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

Status CowGraphStorage::detachEdgeTableForInsert(uint32_t edge_triplet_id) {
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

Status CowGraphStorage::detachEdgeTableForDelete(uint32_t edge_triplet_id) {
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

Status CowGraphStorage::detachEdgeColumn(uint32_t edge_triplet_id,
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

Status CowGraphStorage::detachAdjlists(uint32_t edge_triplet_id, vid_t src_lid,
                                       vid_t dst_lid, Allocator& alloc) {
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

Status CowGraphStorage::detachForResize(label_t label, size_t capacity) {
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

Status CowGraphStorage::detachForResize(label_t src_label, label_t dst_label,
                                        label_t edge_label, size_t capacity) {
  uint32_t idx =
      graph_.schema().generate_edge_label(src_label, dst_label, edge_label);
  return detachEdgeTableForInsert(idx);
}

Status CowGraphStorage::prepareVertexDelete(
    label_t label, vid_t lid, std::vector<uint32_t>& touched_edge_triplets) {
  // Detach the validity/timestamp module, then only the triplets that actually
  // hold an incident edge of a deleted vertex. Per-triplet detachment covers
  // the CSR directory arrays (required before any adjlist buffer can be
  // redirected) and the touched adjacency lists themselves, so the write
  // footprint stays proportional to the real delete set instead of the schema
  // breadth.
  RETURN_IF_NOT_OK(detachVertexTableForDelete(label));
  touched_edge_triplets.clear();

  const auto& schema = graph_.schema();
  if (!graph_.IsValidLid(label, lid, read_ts_)) {
    return Status::OK();
  }
  auto related_edges =
      fetch_edges_related_to_vertex(*this, schema, label, lid, read_ts_);
  for (auto& [edge_triplet_id, edges] : related_edges) {
    touched_edge_triplets.push_back(edge_triplet_id);
    RETURN_IF_NOT_OK(detachEdgeTableForDelete(edge_triplet_id));
    for (auto& [src, dst, oe_off, ie_off] : edges) {
      RETURN_IF_NOT_OK(detachAdjlists(edge_triplet_id, src, dst, alloc_));
    }
  }
  return Status::OK();
}

result<std::vector<vid_t>> CowGraphStorage::BatchAddVerticesImpl(
    label_t, std::shared_ptr<IDataChunkSupplier>) {
  RETURN_STATUS_ERROR(StatusCode::ERR_NOT_SUPPORTED,
                      "BatchAddVertices requires BulkCowGraphStorage");
}

Status CowGraphStorage::BatchAddEdgesImpl(label_t, label_t, label_t,
                                          std::shared_ptr<IDataChunkSupplier>) {
  return Status(StatusCode::ERR_NOT_SUPPORTED,
                "BatchAddEdges requires BulkCowGraphStorage");
}

Status BulkCowGraphStorage::CreateVertexTypeImpl(
    const CreateVertexTypeParam& config) {
  return applyCreateVertexType(config, PersistentSchemaCommitMode::kCheckpoint);
}

Status BulkCowGraphStorage::CreateEdgeTypeImpl(
    const CreateEdgeTypeParam& config) {
  return applyCreateEdgeType(config, PersistentSchemaCommitMode::kCheckpoint);
}

result<std::vector<vid_t>> BulkCowGraphStorage::BatchAddVerticesImpl(
    label_t v_label_id, std::shared_ptr<IDataChunkSupplier> supplier) {
  RETURN_STATUS_ERROR_IF_NOT_OK(detachVertexTableForInsert(v_label_id));
  GS_AUTO(indexes, graph_.mutable_index_manager().GetAllIndexes());
  for (auto* index : indexes) {
    if (index->GetMeta().schema.label_id == v_label_id) {
      RETURN_STATUS_ERROR_IF_NOT_OK(detachIndex(*index));
    }
  }
  auto new_vids = graph_.BatchAddVertices(v_label_id, std::move(supplier));
  if (!new_vids || new_vids->empty()) {
    return new_vids;
  }
  auto status = AddBatchVertexIndexData(graph_, v_label_id, new_vids.value());
  if (!status.ok()) {
    return tl::unexpected(std::move(status));
  }
  if (graph_.schema().is_vertex_label_temporary(v_label_id)) {
    workspace_.MarkTransientMutation();
  } else {
    // Persistent COPY finalizes this detached target immediately before the
    // checkpoint consumes the private graph.
    workspace_.MarkBulkVertexTableForCheckpoint(v_label_id);
  }
  return new_vids;
}

Status BulkCowGraphStorage::BatchAddEdgesImpl(
    label_t src_label, label_t dst_label, label_t edge_label,
    std::shared_ptr<IDataChunkSupplier> supplier) {
  const uint32_t edge_triplet_id =
      graph_.schema().generate_edge_label(src_label, dst_label, edge_label);
  RETURN_IF_NOT_OK(detachEdgeTableForInsert(edge_triplet_id));
  RETURN_IF_NOT_OK(graph_.BatchAddEdges(src_label, dst_label, edge_label,
                                        std::move(supplier)));
  if (graph_.schema().is_edge_triplet_temporary(src_label, dst_label,
                                                edge_label)) {
    workspace_.MarkTransientMutation();
  } else {
    workspace_.MarkBulkEdgeTableForCheckpoint(edge_triplet_id);
  }
  return Status::OK();
}

Status CowGraphStorage::BatchDeleteVerticesImpl(
    label_t v_label_id, const std::vector<vid_t>& vids) {
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
    workspace_.MarkBulkMutation();
  }
  return Status::OK();
}

Status CowGraphStorage::BatchDeleteEdgesImpl(
    label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
    const std::vector<std::tuple<vid_t, vid_t>>& edges) {
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
    workspace_.MarkBulkMutation();
  }
  return Status::OK();
}

Status CowGraphStorage::BatchDeleteEdgesImpl(
    label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
    const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
    const std::vector<std::pair<vid_t, int32_t>>& ie_edges) {
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
    workspace_.MarkBulkMutation();
  }
  return Status::OK();
}

Status CowGraphStorage::detachIndex(StorageIndex& index) {
  const auto& name = index.GetMeta().name;
  auto it = detach_state_.index_detached.find(name);
  if (it != detach_state_.index_detached.end() && it->second) {
    return Status::OK();
  }
  index.Detach(ckp_, graph_.memory_level());
  detach_state_.index_detached[name] = true;
  return Status::OK();
}

}  // namespace neug
