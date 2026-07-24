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

#include "neug/transaction/update_transaction.h"

#include <glog/logging.h>
#include <cstdint>

#include <algorithm>
#include <filesystem>
#include <functional>
#include <limits>
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
#include "neug/storages/index/storage_index_manager.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/io/file/file_utils.h"
#include "neug/utils/likely.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"
#include "neug/utils/result.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

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

using IndexDetachFn = std::function<Status(StorageIndex&)>;

// Drops every index whose metadata references the given vertex label/property.
// cow_state is provided so the COW detach bookkeeping can be cleared for
// indexes that no longer exist.
static Status dropVertexIndex(PropertyGraph& graph, label_t label,
                              const std::string& prop_name,
                              PropertyGraphCowState* cow_state = nullptr) {
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
    if (cow_state) {
      cow_state->index_detached.erase(index_name);
    }
  }
  return Status::OK();
}

// Appends index entries for one newly inserted vertex row.
// When detach_index is set, it is invoked before mutating each index so TP
// transactions get a private COW copy.
static Status addVertexIndexData(PropertyGraph& graph, label_t label, vid_t lid,
                                 const Value& id,
                                 const std::vector<Value>& props,
                                 const IndexDetachFn& detach_index = nullptr) {
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
    if (detach_index) {
      RETURN_IF_NOT_OK(detach_index(*index));
    }
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
      if (detach_index) {
        RETURN_IF_NOT_OK(detach_index(*index));
      }
      RETURN_IF_NOT_OK(index->Upsert(lid, props[prop_idx]));
    }
  }
  return Status::OK();
}

// Updates index entries for one changed vertex property. Primary keys are not
// handled here because PropertyGraph does not allow modifying the primary key
// of an existing vertex.
// When detach_index is set, it is invoked before mutating each index so TP
// transactions get a private COW copy.
static Status updateVertexIndexData(
    PropertyGraph& graph, label_t label, vid_t lid, int32_t col_id,
    const Value& value, const IndexDetachFn& detach_index = nullptr) {
  const auto& v_schema = graph.schema().get_vertex_schema(label);
  if (col_id < 0 ||
      static_cast<size_t>(col_id) >= v_schema->property_names.size() ||
      v_schema->vprop_soft_deleted[col_id]) {
    return Status::OK();
  }

  auto indexes = graph.mutable_index_manager().GetIndex(
      label, v_schema->property_names[col_id]);
  if (!indexes) {
    return indexes.error();
  }
  for (auto* index : indexes.value()) {
    if (detach_index) {
      RETURN_IF_NOT_OK(detach_index(*index));
    }
    RETURN_IF_NOT_OK(index->Upsert(lid, value));
  }
  return Status::OK();
}

// Deletes index entries for one or more removed vertex rows.
// When detach_index is set, it is invoked before mutating each index so TP
// transactions get a private COW copy.
static Status deleteVertexIndexData(
    PropertyGraph& graph, label_t label, const std::vector<vid_t>& vids,
    const IndexDetachFn& detach_index = nullptr) {
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
    if (detach_index) {
      RETURN_IF_NOT_OK(detach_index(*index));
    }
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
      if (detach_index) {
        RETURN_IF_NOT_OK(detach_index(*index));
      }
      for (vid_t vid : vids) {
        RETURN_IF_NOT_OK(index->Delete(vid));
      }
    }
  }
  return Status::OK();
}

// =============================================================================
// UpdateTransaction — lifecycle methods only
// =============================================================================

UpdateTransaction::UpdateTransaction(std::shared_ptr<PropertyGraph> cow_graph,
                                     Allocator& alloc, IWalWriter& logger,
                                     IVersionManager& vm,
                                     GraphSnapshotStore& snapshot_store,
                                     execution::LocalQueryCache& cache,
                                     timestamp_t timestamp)
    : cow_graph_(std::move(cow_graph)),
      cow_state_(PropertyGraphCowState::FromSchema(cow_graph_->schema())),
      view_(*cow_graph_),
      alloc_(alloc),
      logger_(logger),
      vm_(vm),
      snapshot_store_(snapshot_store),
      pipeline_cache_(cache),
      timestamp_(timestamp),
      ckp_(cow_graph_->checkpoint_ptr()) {}

UpdateTransaction::~UpdateTransaction() { Abort(); }

timestamp_t UpdateTransaction::timestamp() const { return timestamp_; }

bool UpdateTransaction::Commit() {
  if (timestamp_ == INVALID_TIMESTAMP) {
    return true;
  }
  if (wal_builder_.op_num() == 0 && wal_builder_.content_size() == 0) {
    release();
    return true;
  }

  if (!snapshot_store_.HasFreeSlot()) {
    LOG(ERROR) << "GraphSnapshotStore slot exhausted; refusing to commit";
    Abort();
    return false;
  }

  wal_builder_.finalize(timestamp_);
  if (!logger_.append(wal_builder_.data(), wal_builder_.size())) {
    LOG(ERROR) << "Failed to append wal log";
    Abort();
    return false;
  }

  vm_.begin_update_commit(timestamp_);

  if (wal_builder_.schema_changed()) {
    pipeline_cache_.clearGlobalCache();
  }

  // PublishSnapshot MUST happen BEFORE release() which calls
  // release_update_timestamp (advancing read_ts_). This ordering guarantees
  // that any new reader observing the advanced read_ts will also see the new
  // slot.
  auto status = snapshot_store_.PublishSnapshot(cow_graph_);
  if (!status.ok()) {
    // We should never fail to publish the snapshot.
    LOG(FATAL) << "Failed to publish snapshot: " << status.ToString();
  }

  // The COW PG is now owned by the new slot. Drop our reference so this
  // transaction does not appear to still hold the snapshot. view_ wraps
  // cow_graph_, so reset it too to keep pg_ from dangling.
  view_ = GraphView();
  release();
  return true;
}

void UpdateTransaction::Abort() { release(); }

void UpdateTransaction::release() {
  if (timestamp_ != INVALID_TIMESTAMP) {
    wal_builder_.clear();
    vm_.release_update_timestamp(timestamp_);
    timestamp_ = INVALID_TIMESTAMP;
  }
  cow_graph_.reset();
}

Status StorageTPUpdateInterface::CreateVertexTypeImpl(
    const CreateVertexTypeParam& config) {
  const auto& name = config.GetVertexLabel();
  if (cow_graph_->schema().is_vertex_label_valid(name)) {
    LOG(ERROR) << "Vertex type " << name << " already exists.";
    return Status(StatusCode::ERR_SCHEMA_MISMATCH,
                  "Vertex type " + name + " already exists.");
  }
  wal_.LogCreateVertexType(config);
  auto status = cow_graph_->CreateVertexType(config);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to create vertex type " << name << ": "
               << status.ToString();
    return status;
  }

  // Expand cow_state for the newly created vertex type.
  label_t new_label = cow_graph_->schema().get_vertex_label_id(name);
  if (new_label >= cow_state_.vertex_tables.size()) {
    cow_state_.vertex_tables.resize(new_label + 1);
  }
  auto& new_state = cow_state_.vertex_tables[new_label];
  new_state.indexer_detached = true;
  new_state.vertex_timestamp_detached = true;
  size_t col_count = config.GetProperties().size();
  new_state.columns_detached.assign(col_count, true);

  mut_view_.Rebuild(*cow_graph_);
  return status;
}

Status StorageTPUpdateInterface::CreateEdgeTypeImpl(
    const CreateEdgeTypeParam& config) {
  const auto& src_type = config.GetSrcLabel();
  const auto& dst_type = config.GetDstLabel();
  const auto& edge_type = config.GetEdgeLabel();
  if (cow_graph_->schema().is_edge_triplet_valid(src_type, dst_type,
                                                 edge_type)) {
    LOG(ERROR) << "Edge type " << edge_type << " already exists between "
               << src_type << " and " << dst_type << ".";
    return Status(StatusCode::ERR_SCHEMA_MISMATCH,
                  "Edge type " + edge_type + " already exists between " +
                      src_type + " and " + dst_type + ".");
  }
  wal_.LogCreateEdgeType(config);
  auto status = cow_graph_->CreateEdgeType(config);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to create edge type " << edge_type << " between "
               << src_type << " and " << dst_type << ": " << status.ToString();
    return status;
  }

  const auto& schema = cow_graph_->schema();
  label_t src_label = schema.get_vertex_label_id(src_type);
  label_t dst_label = schema.get_vertex_label_id(dst_type);
  label_t edge_label = schema.get_edge_label_id(edge_type);
  uint32_t triplet_id =
      schema.generate_edge_label(src_label, dst_label, edge_label);
  EdgeTableCowState new_edge_state;
  new_edge_state.out_csr_detached = true;
  new_edge_state.in_csr_detached = true;
  new_edge_state.columns_detached.assign(config.GetProperties().size(), true);
  cow_state_.edge_tables.emplace(triplet_id, std::move(new_edge_state));

  mut_view_.Rebuild(*cow_graph_);
  return status;
}

Status StorageTPUpdateInterface::AddVertexPropertiesImpl(
    label_t v_label, const AddVertexPropertiesParam& config) {
  wal_.LogAddVertexProperties(
      cow_graph_->schema().get_vertex_label_name(v_label), config);
  auto status = cow_graph_->AddVertexProperties(v_label, config);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to add properties to vertex type "
               << cow_graph_->schema().get_vertex_label_name(v_label) << ": "
               << status.ToString();
    return status;
  }

  auto& vt_state = cow_state_.vertex_tables[v_label];
  size_t new_col_count =
      cow_graph_->schema().get_vertex_schema(v_label)->property_names.size();
  vt_state.columns_detached.resize(new_col_count, true);

  mut_view_.Rebuild(*cow_graph_);
  return status;
}

Status StorageTPUpdateInterface::AddEdgePropertiesImpl(
    label_t src_label_id, label_t dst_label_id, label_t edge_label_id,
    const AddEdgePropertiesParam& config) {
  const auto& schema = cow_graph_->schema();
  wal_.LogAddEdgeProperties(schema.get_vertex_label_name(src_label_id),
                            schema.get_vertex_label_name(dst_label_id),
                            schema.get_edge_label_name(edge_label_id), config);
  auto status = cow_graph_->AddEdgeProperties(src_label_id, dst_label_id,
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
  auto& et_state = cow_state_.edge_tables[triplet_id];
  auto edge_schema =
      schema.get_edge_schema(src_label_id, dst_label_id, edge_label_id);
  et_state.columns_detached.resize(edge_schema->property_names.size(), true);

  mut_view_.Rebuild(*cow_graph_);
  return status;
}

Status StorageTPUpdateInterface::RenameVertexPropertiesImpl(
    label_t v_label, const RenameVertexPropertiesParam& config) {
  const auto vertex_type_name =
      cow_graph_->schema().get_vertex_label_name(v_label);
  wal_.LogRenameVertexProperties(vertex_type_name, config);
  auto status = cow_graph_->RenameVertexProperties(v_label, config);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to rename properties of vertex type "
               << cow_graph_->schema().get_vertex_label_name(v_label) << ": "
               << status.ToString();
    return status;
  }
  for (const auto& [old_name, new_name] : config.GetRenameProperties()) {
    if (old_name == new_name)
      continue;
    RETURN_IF_NOT_OK(
        dropVertexIndex(*cow_graph_, v_label, old_name, &cow_state_));
  }
  return status;
}

Status StorageTPUpdateInterface::RenameEdgePropertiesImpl(
    label_t src_label_id, label_t dst_label_id, label_t edge_label_id,
    const RenameEdgePropertiesParam& config) {
  const auto& schema = cow_graph_->schema();
  wal_.LogRenameEdgeProperties(schema.get_vertex_label_name(src_label_id),
                               schema.get_vertex_label_name(dst_label_id),
                               schema.get_edge_label_name(edge_label_id),
                               config);
  auto status = cow_graph_->RenameEdgeProperties(src_label_id, dst_label_id,
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

Status StorageTPUpdateInterface::DeleteVertexPropertiesImpl(
    label_t v_label, const DeleteVertexPropertiesParam& config) {
  const auto& properties = config.GetDeleteProperties();
  const auto& vertex_type_name =
      cow_graph_->schema().get_vertex_label_name(v_label);
  for (const auto& prop_name : properties) {
    if (!cow_graph_->schema().vertex_has_property(v_label, prop_name)) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Property [" + prop_name + "] does not exist in vertex [" +
                        vertex_type_name + "].");
    }
  }

  std::vector<int> del_col_ids;
  auto& v_table = cow_graph_->get_vertex_table(v_label);
  for (const auto& prop_name : properties) {
    int col_id = v_table.get_table().get_column_id_by_name(prop_name);
    if (col_id >= 0) {
      del_col_ids.push_back(col_id);
    }
  }

  wal_.LogDeleteVertexProperties(vertex_type_name, config);
  auto status = cow_graph_->DeleteVertexProperties(v_label, config);
  if (!status.ok()) {
    return status;
  }
  std::sort(del_col_ids.rbegin(), del_col_ids.rend());
  auto& columns_detached = cow_state_.vertex_tables[v_label].columns_detached;
  for (int col_id : del_col_ids) {
    if (static_cast<size_t>(col_id) < columns_detached.size()) {
      columns_detached.erase(columns_detached.begin() + col_id);
    }
  }
  for (const auto& prop_name : config.GetDeleteProperties()) {
    RETURN_IF_NOT_OK(
        dropVertexIndex(*cow_graph_, v_label, prop_name, &cow_state_));
  }
  mut_view_.Rebuild(*cow_graph_);
  return status;
}

Status StorageTPUpdateInterface::DeleteEdgePropertiesImpl(
    label_t src_label_id, label_t dst_label_id, label_t edge_label_id,
    const DeleteEdgePropertiesParam& config) {
  const auto& schema = cow_graph_->schema();
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

  // Collect pre-deletion state so we can update cow_state_ correctly.
  // EdgeTable::DeleteProperties may recreate the table entirely
  // (bundled↔unbundled conversion) in which case we must reset the
  // entire COW state rather than erasing individual entries.
  uint32_t triplet_id =
      schema.generate_edge_label(src_label_id, dst_label_id, edge_label_id);
  auto& edge_table = cow_graph_->get_edge_table_by_index(triplet_id);
  bool was_bundled = edge_table.get_edge_schema_ptr()->is_bundled();
  size_t old_col_count =
      cow_state_.edge_tables.count(triplet_id)
          ? cow_state_.edge_tables.at(triplet_id).columns_detached.size()
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

  wal_.LogDeleteEdgeProperties(src_type, dst_type, edge_type, config);
  auto status = cow_graph_->DeleteEdgeProperties(src_label_id, dst_label_id,
                                                 edge_label_id, config);
  if (status.ok()) {
    auto& state = cow_state_.edge_tables[triplet_id];
    auto edge_schema = cow_graph_->schema().get_edge_schema(
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
    mut_view_.Rebuild(*cow_graph_);
  }
  return status;
}

Status StorageTPUpdateInterface::DeleteVertexTypeImpl(label_t v_label) {
  // Collect related edge triplet IDs before deletion.
  // PropertyGraph::DeleteVertexType removes them from edge_tables_, so
  // we must capture them while the schema is still intact.
  std::vector<uint32_t> related_edge_ids;
  const auto& schema = cow_graph_->schema();
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

  const auto& v_schema = cow_graph_->schema().get_vertex_schema(v_label);
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
  wal_.LogDeleteVertexType(vertex_type_name);
  auto status = cow_graph_->DeleteVertexType(v_label);
  if (!status.ok()) {
    return status;
  }
  cow_state_.vertex_tables[v_label] = VertexTableCowState();
  for (uint32_t edge_id : related_edge_ids) {
    cow_state_.edge_tables.erase(edge_id);
  }
  for (const auto& property_name : indexed_properties) {
    RETURN_IF_NOT_OK(
        dropVertexIndex(*cow_graph_, v_label, property_name, &cow_state_));
  }
  mut_view_.Rebuild(*cow_graph_);
  return status;
}

Status StorageTPUpdateInterface::DeleteEdgeTypeImpl(label_t src_label_id,
                                                    label_t dst_label_id,
                                                    label_t edge_label_id) {
  const auto& schema = cow_graph_->schema();
  const auto& src_type = schema.get_vertex_label_name(src_label_id);
  const auto& dst_type = schema.get_vertex_label_name(dst_label_id);
  const auto& edge_type = schema.get_edge_label_name(edge_label_id);
  uint32_t triplet_id =
      schema.generate_edge_label(src_label_id, dst_label_id, edge_label_id);

  wal_.LogDeleteEdgeType(src_type, dst_type, edge_type);
  auto status =
      cow_graph_->DeleteEdgeType(src_label_id, dst_label_id, edge_label_id);
  if (status.ok()) {
    cow_state_.edge_tables.erase(triplet_id);
    mut_view_.Rebuild(*cow_graph_);
  }
  return status;
}

neug::result<StorageIndex*> StorageTPUpdateInterface::CreateIndex(
    std::unique_ptr<IndexMeta>) {
  RETURN_STATUS_ERROR(StatusCode::ERR_NOT_SUPPORTED,
                      "CreateIndex is not supported in TP mode currently.");
}

Status StorageTPUpdateInterface::DropIndex(const std::string&) {
  return Status(StatusCode::ERR_NOT_SUPPORTED,
                "DropIndex is not supported in TP mode currently.");
}

Status StorageTPUpdateInterface::AddVertexImpl(label_t label, const Value& oid,
                                               const std::vector<Value>& props,
                                               vid_t& vid) {
  std::vector<DataType> types =
      cow_graph_->schema().get_vertex_properties(label);
  if (types.size() != props.size()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Property count mismatch for vertex of label " +
                      cow_graph_->schema().get_vertex_label_name(label));
  }
  int col_num = types.size();
  for (int col_i = 0; col_i != col_num; ++col_i) {
    if (props[col_i].type() != types[col_i]) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Property type mismatch at column " +
                        std::to_string(col_i) + " for vertex of label " +
                        cow_graph_->schema().get_vertex_label_name(label));
    }
  }

  const auto& v_table = cow_graph_->get_vertex_table(label);
  if (v_table.Size() >= v_table.Capacity()) {
    size_t new_capacity =
        v_table.Size() < 4096 ? 4096 : v_table.Size() + v_table.Size() / 4;
    RETURN_IF_NOT_OK(detachForResize(label, new_capacity));
    auto status = cow_graph_->EnsureCapacity(label, new_capacity);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to ensure space for vertex of label "
                 << cow_graph_->schema().get_vertex_label_name(label) << ": "
                 << status.ToString();
      return status;
    }
  }

  RETURN_IF_NOT_OK(detachVertexTableForInsert(label));
  wal_.LogInsertVertex(label, oid, props);
  auto status = cow_graph_->AddVertex(label, oid, props, vid, read_ts_, true);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to add vertex of label "
               << cow_graph_->schema().get_vertex_label_name(label) << ": "
               << status.ToString();
    return status;
  }
  RETURN_IF_NOT_OK(addVertexIndexData(
      *cow_graph_, label, vid, oid, props,
      [this](StorageIndex& index) { return detachIndex(index); }));
  return Status::OK();
}

Status StorageTPUpdateInterface::DeleteVertexImpl(label_t label, vid_t lid) {
  if (!cow_graph_->IsValidLid(label, lid, read_ts_)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Vertex id is out of range or already deleted");
  }
  auto oid = cow_graph_->GetOid(label, lid, read_ts_);

  prepareVertexDelete(label, {lid});
  wal_.LogRemoveVertex(label, oid);
  RETURN_IF_NOT_OK(cow_graph_->DeleteVertex(label, lid, read_ts_));
  return deleteVertexIndexData(
      *cow_graph_, label, {lid},
      [this](StorageIndex& index) { return detachIndex(index); });
}

Status StorageTPUpdateInterface::AddEdgeImpl(
    label_t src_label, vid_t src_lid, label_t dst_label, vid_t dst_lid,
    label_t edge_label, const std::vector<Value>& properties,
    const void*& prop) {
  const auto& edge_table =
      cow_graph_->get_edge_table(src_label, dst_label, edge_label);
  if (edge_table.PropTableSize() >= edge_table.Capacity()) {
    auto new_capacity =
        edge_table.PropTableSize() < 4096
            ? 4096
            : edge_table.PropTableSize() + edge_table.PropTableSize() / 4;
    RETURN_IF_NOT_OK(
        detachForResize(src_label, dst_label, edge_label, new_capacity));
    auto status = cow_graph_->EnsureCapacity(src_label, dst_label, edge_label,
                                             new_capacity);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to ensure space before insert edge: "
                 << status.ToString();
      return status;
    }
  }
  uint32_t edge_idx = cow_graph_->schema().generate_edge_label(
      src_label, dst_label, edge_label);
  RETURN_IF_NOT_OK(detachEdgeTableForInsert(edge_idx));
  RETURN_IF_NOT_OK(detachAdjlists(edge_idx, src_lid, dst_lid, alloc_));
  wal_.LogInsertEdge(src_label, GetVertexId(src_label, src_lid), dst_label,
                     GetVertexId(dst_label, dst_lid), edge_label, properties);
  int32_t oe_offset = 0;
  return cow_graph_->AddEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                             properties, read_ts_, alloc_, oe_offset, prop,
                             true);
}

Status StorageTPUpdateInterface::DeleteEdgesImpl(label_t src_label,
                                                 vid_t src_lid,
                                                 label_t dst_label,
                                                 vid_t dst_lid,
                                                 label_t edge_label) {
  if (!cow_graph_->IsValidLid(src_label, src_lid, read_ts_) ||
      !cow_graph_->IsValidLid(dst_label, dst_lid, read_ts_)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Source or destination vertex id is out of range or "
                  "already deleted");
  }

  uint32_t edge_idx = cow_graph_->schema().generate_edge_label(
      src_label, dst_label, edge_label);
  RETURN_IF_NOT_OK(detachEdgeTableForDelete(edge_idx));
  RETURN_IF_NOT_OK(detachAdjlists(edge_idx, src_lid, dst_lid, alloc_));

  auto oe_edges = GetGenericOutgoingGraphView(src_label, dst_label, edge_label)
                      .get_edges(src_lid);
  auto ie_edges = GetGenericIncomingGraphView(dst_label, src_label, edge_label)
                      .get_edges(dst_lid);
  auto search_edge_prop_type =
      determine_search_prop_type(cow_graph_->schema().get_edge_properties(
          src_label, dst_label, edge_label));
  int32_t oe_offset = 0;
  for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
    if (it.get_vertex() == dst_lid) {
      auto ie_offset = fuzzy_search_offset_from_nbr_list(
          ie_edges, src_lid, it.get_data_ptr(), search_edge_prop_type);
      wal_.LogRemoveEdge(src_label, GetVertexId(src_label, src_lid), dst_label,
                         GetVertexId(dst_label, dst_lid), edge_label, oe_offset,
                         ie_offset);
      auto status =
          cow_graph_->DeleteEdge(src_label, src_lid, dst_label, dst_lid,
                                 edge_label, oe_offset, ie_offset, read_ts_);
      if (!status.ok()) {
        LOG(ERROR) << "Failed to delete edge: " << status.ToString();
        return status;
      }
    }
    oe_offset++;
  }

  return Status::OK();
}

Status StorageTPUpdateInterface::DeleteEdgeImpl(
    label_t src_label, vid_t src_lid, label_t dst_label, vid_t dst_lid,
    label_t edge_label, int32_t oe_offset, int32_t ie_offset) {
  if (!cow_graph_->IsValidLid(src_label, src_lid, read_ts_) ||
      !cow_graph_->IsValidLid(dst_label, dst_lid, read_ts_)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Source or destination vertex id is out of range or "
                  "already deleted");
  }

  uint32_t edge_idx = cow_graph_->schema().generate_edge_label(
      src_label, dst_label, edge_label);
  RETURN_IF_NOT_OK(detachEdgeTableForDelete(edge_idx));
  RETURN_IF_NOT_OK(detachAdjlists(edge_idx, src_lid, dst_lid, alloc_));

  wal_.LogRemoveEdge(src_label, GetVertexId(src_label, src_lid), dst_label,
                     GetVertexId(dst_label, dst_lid), edge_label, oe_offset,
                     ie_offset);

  return cow_graph_->DeleteEdge(src_label, src_lid, dst_label, dst_lid,
                                edge_label, oe_offset, ie_offset, read_ts_);
}

Value UpdateTransaction::GetVertexProperty(label_t label, vid_t lid,
                                           int col_id) const {
  auto col = cow_graph_->GetVertexPropertyColumn(label, col_id);
  if (!cow_graph_->IsValidLid(label, lid, timestamp_)) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Vertex lid is not valid in this transaction");
  }
  if (col == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Fail to find property column");
  }
  return col->get_any(lid);
}

Value UpdateTransaction::GetVertexId(label_t label, vid_t lid) const {
  return cow_graph_->GetOid(label, lid, timestamp_);
}

bool UpdateTransaction::GetVertexIndex(label_t label, const Value& id,
                                       vid_t& index) const {
  return cow_graph_->get_lid(label, id, index, timestamp_);
}

Status StorageTPUpdateInterface::UpdateVertexPropertyImpl(label_t label,
                                                          vid_t lid, int col_id,
                                                          const Value& value) {
  if (!cow_graph_->IsValidLid(label, lid, read_ts_)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Vertex lid " + std::to_string(lid) + " of label " +
                      cow_graph_->schema().get_vertex_label_name(label) +
                      " is not valid in this transaction.");
  }
  std::vector<DataType> types =
      cow_graph_->schema().get_vertex_properties(label);
  if (static_cast<size_t>(col_id) >= types.size()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Column id " + std::to_string(col_id) + " is out of range.");
  }
  if (types[col_id] != value.type()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Type mismatch for column " + std::to_string(col_id) + ".");
  }
  RETURN_IF_NOT_OK(detachVertexColumn(label, col_id));
  wal_.LogUpdateVertexProp(label, GetVertexId(label, lid), col_id, value);

  RETURN_IF_NOT_OK(
      cow_graph_->UpdateVertexProperty(label, lid, col_id, value, read_ts_));

  return updateVertexIndexData(
      *cow_graph_, label, lid, col_id, value,
      [this](StorageIndex& index) { return detachIndex(index); });
}

Status StorageTPUpdateInterface::UpdateEdgePropertyImpl(
    label_t src_label, vid_t src, label_t dst_label, vid_t dst,
    label_t edge_label, int32_t oe_offset, int32_t ie_offset, int32_t col_id,
    const Value& value) {
  if (!cow_graph_->IsValidLid(src_label, src, read_ts_) ||
      !cow_graph_->IsValidLid(dst_label, dst, read_ts_)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Source or destination vertex id is out of range or "
                  "already deleted");
  }
  uint32_t edge_idx = cow_graph_->schema().generate_edge_label(
      src_label, dst_label, edge_label);
  RETURN_IF_NOT_OK(detachEdgeColumn(edge_idx, col_id));
  RETURN_IF_NOT_OK(detachAdjlists(edge_idx, src, dst, alloc_));
  wal_.LogUpdateEdgeProp(src_label, GetVertexId(src_label, src), dst_label,
                         GetVertexId(dst_label, dst), edge_label, oe_offset,
                         ie_offset, col_id, value);
  return cow_graph_->UpdateEdgeProperty(src_label, src, dst_label, dst,
                                        edge_label, oe_offset, ie_offset,
                                        col_id, value, read_ts_);
}

void UpdateTransaction::IngestWal(PropertyGraph& graph, uint32_t timestamp,
                                  char* data, size_t length, Allocator& alloc) {
  OutArchive arc;
  arc.SetSlice(data, length);
  while (!arc.Empty()) {
    OpType op_type;
    arc >> op_type;
    if (op_type == OpType::kCreateVertexType) {
      CreateVertexTypeParam redo = CreateVertexTypeRedo::Deserialize(arc);
      graph.MarkSchemaDirty();
      auto ret = graph.CreateVertexType(redo);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to create vertex type in redo: ",
                                     ret);
    } else if (op_type == OpType::kCreateEdgeType) {
      const auto& redo = CreateEdgeTypeRedo::Deserialize(arc);
      graph.MarkSchemaDirty();
      auto ret = graph.CreateEdgeType(redo);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to create edge type in redo: ",
                                     ret);
    } else if (op_type == OpType::kInsertVertex) {
      InsertVertexRedo redo;
      arc >> redo;
      vid_t vid;
      bool inserted = false;
      auto& v_table = graph.get_vertex_table(redo.label);
      if (!graph.get_lid(redo.label, redo.oid, vid, timestamp) ||
          !graph.IsValidLid(redo.label, vid, timestamp)) {
        if (v_table.Size() >= v_table.Capacity()) {
          auto new_capacity = v_table.Size() < 4096
                                  ? 4096
                                  : v_table.Size() + v_table.Size() / 4;
          graph.EnsureCapacity(redo.label, new_capacity);
        }
        graph.MarkVertexTableDirty(redo.label);
        auto ret = graph.AddVertex(redo.label, redo.oid, redo.props, vid,
                                   timestamp, true);
        THROW_STORAGE_EXCEPTION_STATUS("Failed to add vertex in redo: ", ret);
        inserted = true;
      }
      if (inserted) {
        auto ret =
            addVertexIndexData(graph, redo.label, vid, redo.oid, redo.props);
        THROW_STORAGE_EXCEPTION_STATUS(
            "Failed to append vertex indexes in redo: ", ret);
      }
    } else if (op_type == OpType::kInsertEdge) {
      InsertEdgeRedo redo;
      arc >> redo;
      vid_t src_vid, dst_vid;
      CHECK(graph.get_lid(redo.src_label, redo.src, src_vid, timestamp));
      CHECK(graph.get_lid(redo.dst_label, redo.dst, dst_vid, timestamp));
      int32_t oe_offset_unused = 0;
      const void* prop_unused = nullptr;
      graph.MarkEdgeTableDirty(redo.src_label, redo.dst_label, redo.edge_label);
      auto ret = graph.AddEdge(redo.src_label, src_vid, redo.dst_label, dst_vid,
                               redo.edge_label, redo.properties, timestamp,
                               alloc, oe_offset_unused, prop_unused, true);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to add edge in redo: ", ret);
    } else if (op_type == OpType::kUpdateVertexProp) {
      UpdateVertexPropRedo redo;
      arc >> redo;
      vid_t vid;
      CHECK(graph.get_lid(redo.label, redo.oid, vid, timestamp));
      graph.MarkVertexTableDirty(redo.label);
      auto ret = graph.UpdateVertexProperty(redo.label, vid, redo.prop_id,
                                            redo.value, timestamp);
      THROW_STORAGE_EXCEPTION_STATUS(
          "Failed to update vertex property in redo: ", ret);
      ret = updateVertexIndexData(graph, redo.label, vid, redo.prop_id,
                                  redo.value);
      THROW_STORAGE_EXCEPTION_STATUS(
          "Failed to update vertex property indexes in redo: ", ret);
    } else if (op_type == OpType::kUpdateEdgeProp) {
      UpdateEdgePropRedo redo;
      arc >> redo;
      vid_t src_vid, dst_vid;
      CHECK(graph.get_lid(redo.src_label, redo.src, src_vid, timestamp));
      CHECK(graph.get_lid(redo.dst_label, redo.dst, dst_vid, timestamp));
      graph.MarkEdgeTableDirty(redo.src_label, redo.dst_label, redo.edge_label);
      auto ret = graph.UpdateEdgeProperty(
          redo.src_label, src_vid, redo.dst_label, dst_vid, redo.edge_label,
          redo.oe_offset, redo.ie_offset, redo.prop_id, redo.value, timestamp);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to update edge property in redo: ",
                                     ret);
    } else if (op_type == OpType::kRemoveVertex) {
      RemoveVertexRedo redo;
      arc >> redo;
      vid_t vid;
      CHECK(graph.get_lid(redo.label, redo.oid, vid, timestamp));
      graph.MarkVertexTableDirty(redo.label);
      // Cascade: DeleteVertex physically writes incident edge tables.
      for (const auto& [_, es] : graph.schema().get_all_edge_schemas()) {
        if (es->src_label_id == redo.label || es->dst_label_id == redo.label) {
          graph.MarkEdgeTableDirty(es->src_label_id, es->dst_label_id,
                                   es->edge_label_id);
        }
      }
      auto ret = graph.DeleteVertex(redo.label, vid, timestamp);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to delete vertex in redo: ", ret);
      ret = deleteVertexIndexData(graph, redo.label, {vid});
      THROW_STORAGE_EXCEPTION_STATUS(
          "Failed to delete vertex indexes in redo: ", ret);
    } else if (op_type == OpType::kRemoveEdge) {
      RemoveEdgeRedo redo;
      arc >> redo;
      vid_t src_vid, dst_vid;
      CHECK(graph.get_lid(redo.src_label, redo.src, src_vid, timestamp));
      CHECK(graph.get_lid(redo.dst_label, redo.dst, dst_vid, timestamp));
      graph.MarkEdgeTableDirty(redo.src_label, redo.dst_label, redo.edge_label);
      auto ret = graph.DeleteEdge(redo.src_label, src_vid, redo.dst_label,
                                  dst_vid, redo.edge_label, redo.oe_offset,
                                  redo.ie_offset, timestamp);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to delete edge in redo: ", ret);
    } else if (op_type == OpType::kAddVertexProp) {
      auto redo = AddVertexPropertiesRedo::Deserialize(arc);
      label_t label = graph.schema().get_vertex_label_id(redo.vertex_type);
      graph.MarkSchemaDirty();
      graph.MarkVertexTableDirty(label);
      auto ret = graph.AddVertexProperties(label, redo.config);
      THROW_STORAGE_EXCEPTION_STATUS(
          "Failed to add vertex properties in redo: ", ret);
    } else if (op_type == OpType::kAddEdgeProp) {
      auto redo = AddEdgePropertiesRedo::Deserialize(arc);
      const auto& schema = graph.schema();
      label_t src = schema.get_vertex_label_id(redo.src_type);
      label_t dst = schema.get_vertex_label_id(redo.dst_type);
      label_t edge = schema.get_edge_label_id(redo.edge_type);
      graph.MarkSchemaDirty();
      graph.MarkEdgeTableDirty(src, dst, edge);
      auto ret = graph.AddEdgeProperties(src, dst, edge, redo.config);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to add edge properties in redo: ",
                                     ret);
    } else if (op_type == OpType::kRenameVertexProp) {
      auto redo = RenameVertexPropertiesRedo::Deserialize(arc);
      label_t label = graph.schema().get_vertex_label_id(redo.vertex_type);
      graph.MarkSchemaDirty();
      graph.MarkVertexTableDirty(label);
      auto ret = graph.RenameVertexProperties(label, redo.config);
      THROW_STORAGE_EXCEPTION_STATUS(
          "Failed to rename vertex properties in redo: ", ret);
      for (const auto& [old_name, new_name] :
           redo.config.GetRenameProperties()) {
        if (old_name == new_name)
          continue;
        ret = dropVertexIndex(graph, label, old_name);
        THROW_STORAGE_EXCEPTION_STATUS(
            "Failed to drop renamed-property indexes in redo: ", ret);
      }
    } else if (op_type == OpType::kRenameEdgeProp) {
      auto redo = RenameEdgePropertiesRedo::Deserialize(arc);
      const auto& schema = graph.schema();
      label_t src = schema.get_vertex_label_id(redo.src_type);
      label_t dst = schema.get_vertex_label_id(redo.dst_type);
      label_t edge = schema.get_edge_label_id(redo.edge_type);
      graph.MarkSchemaDirty();
      graph.MarkEdgeTableDirty(src, dst, edge);
      auto ret = graph.RenameEdgeProperties(src, dst, edge, redo.config);
      THROW_STORAGE_EXCEPTION_STATUS(
          "Failed to rename edge properties in redo: ", ret);
    } else if (op_type == OpType::kDeleteVertexProp) {
      auto redo = DeleteVertexPropertiesRedo::Deserialize(arc);
      label_t label = graph.schema().get_vertex_label_id(redo.vertex_type);
      graph.MarkSchemaDirty();
      graph.MarkVertexTableDirty(label);
      auto ret = graph.DeleteVertexProperties(label, redo.config);
      THROW_STORAGE_EXCEPTION_STATUS(
          "Failed to delete vertex properties in redo: ", ret);
      for (const auto& prop_name : redo.config.GetDeleteProperties()) {
        ret = dropVertexIndex(graph, label, prop_name);
        THROW_STORAGE_EXCEPTION_STATUS(
            "Failed to drop deleted-property indexes in redo: ", ret);
      }
    } else if (op_type == OpType::kDeleteEdgeProp) {
      auto redo = DeleteEdgePropertiesRedo::Deserialize(arc);
      const auto& schema = graph.schema();
      label_t src = schema.get_vertex_label_id(redo.src_type);
      label_t dst = schema.get_vertex_label_id(redo.dst_type);
      label_t edge = schema.get_edge_label_id(redo.edge_type);
      graph.MarkSchemaDirty();
      graph.MarkEdgeTableDirty(src, dst, edge);
      auto ret = graph.DeleteEdgeProperties(src, dst, edge, redo.config);
      THROW_STORAGE_EXCEPTION_STATUS(
          "Failed to delete edge properties in redo: ", ret);
    } else if (op_type == OpType::kDeleteVertexType) {
      DeleteVertexTypeRedo redo;
      arc >> redo;
      graph.MarkSchemaDirty();
      auto v_label = graph.schema().get_vertex_label_id(redo.vertex_type);
      const auto& v_schema = graph.schema().get_vertex_schema(v_label);
      std::vector<std::string> indexed_properties;
      indexed_properties.reserve(v_schema->property_names.size() + 1);
      indexed_properties.push_back(std::get<1>(v_schema->primary_keys[0]));
      for (size_t prop_idx = 0; prop_idx < v_schema->property_names.size();
           ++prop_idx) {
        if (v_schema->vprop_soft_deleted[prop_idx]) {
          continue;
        }
        indexed_properties.push_back(v_schema->property_names[prop_idx]);
      }
      auto ret = graph.DeleteVertexType(redo.vertex_type);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to delete vertex type in redo: ",
                                     ret);
      for (const auto& property_name : indexed_properties) {
        ret = dropVertexIndex(graph, v_label, property_name);
        THROW_STORAGE_EXCEPTION_STATUS(
            "Failed to drop deleted-vertex-type indexes in redo: ", ret);
      }
    } else if (op_type == OpType::kDeleteEdgeType) {
      DeleteEdgeTypeRedo redo;
      arc >> redo;
      graph.MarkSchemaDirty();
      auto ret =
          graph.DeleteEdgeType(redo.src_type, redo.dst_type, redo.edge_type);
      THROW_STORAGE_EXCEPTION_STATUS("Failed to delete edge type in redo: ",
                                     ret);
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION("Unexpected op_type: " +
                                    std::to_string(static_cast<int>(op_type)));
    }
  }
}

Status StorageTPUpdateInterface::detachVertexTableForInsert(label_t label) {
  auto& state = cow_state_.vertex_tables[label];
  auto& vertex_table = cow_graph_->get_vertex_table(label);
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
      vertex_table.get_table().DetachColumn(i, *ckp_,
                                            cow_graph_->memory_level());
      state.columns_detached[i] = true;
      did_detach = true;
    }
  }
  if (did_detach) {
    mut_view_.Rebuild(*cow_graph_);
  }
  return Status::OK();
}

Status StorageTPUpdateInterface::detachVertexTableForDelete(label_t label) {
  auto& state = cow_state_.vertex_tables[label];
  if (!state.vertex_timestamp_detached) {
    cow_graph_->get_vertex_table(label).DetachVertexTimestamp();
    state.vertex_timestamp_detached = true;
    mut_view_.Rebuild(*cow_graph_);
  }
  return Status::OK();
}

Status StorageTPUpdateInterface::detachVertexColumn(label_t label,
                                                    int32_t col_id) {
  auto& state = cow_state_.vertex_tables[label];
  if (col_id >= 0 &&
      static_cast<size_t>(col_id) < state.columns_detached.size() &&
      !state.columns_detached[col_id]) {
    auto& vertex_table = cow_graph_->get_vertex_table(label);
    vertex_table.get_table().DetachColumn(col_id, *ckp_,
                                          cow_graph_->memory_level());
    state.columns_detached[col_id] = true;
    mut_view_.Rebuild(*cow_graph_);
  }
  return Status::OK();
}

Status StorageTPUpdateInterface::detachEdgeTableForInsert(
    uint32_t edge_triplet_id) {
  if (!cow_graph_->HasEdgeTable(edge_triplet_id)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Edge table for edge label triplet not found");
  }
  auto& state = cow_state_.edge_tables[edge_triplet_id];
  auto& edge_table = cow_graph_->get_edge_table_by_index(edge_triplet_id);
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
        edge_table.table()->DetachColumn(i, *ckp_, cow_graph_->memory_level());
        state.columns_detached[i] = true;
        did_detach = true;
      }
    }
  }
  if (did_detach) {
    mut_view_.Rebuild(*cow_graph_);
  }
  return Status::OK();
}

Status StorageTPUpdateInterface::detachEdgeTableForDelete(
    uint32_t edge_triplet_id) {
  if (!cow_graph_->HasEdgeTable(edge_triplet_id)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Edge table for edge label triplet not found");
  }
  auto& state = cow_state_.edge_tables[edge_triplet_id];
  auto& edge_table = cow_graph_->get_edge_table_by_index(edge_triplet_id);
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
    mut_view_.Rebuild(*cow_graph_);
  }
  return Status::OK();
}

Status StorageTPUpdateInterface::detachEdgeColumn(uint32_t edge_triplet_id,
                                                  int32_t col_id) {
  RETURN_IF_NOT_OK(detachEdgeTableForDelete(edge_triplet_id));
  auto& state = cow_state_.edge_tables[edge_triplet_id];
  auto& edge_table = cow_graph_->get_edge_table_by_index(edge_triplet_id);
  if (!edge_table.get_edge_schema_ptr()->is_bundled() && edge_table.table() &&
      col_id >= 0 &&
      static_cast<size_t>(col_id) < state.columns_detached.size() &&
      !state.columns_detached[col_id]) {
    edge_table.table()->DetachColumn(col_id, *ckp_, cow_graph_->memory_level());
    state.columns_detached[col_id] = true;
    mut_view_.Rebuild(*cow_graph_);
  }
  return Status::OK();
}

Status StorageTPUpdateInterface::detachAdjlists(uint32_t edge_triplet_id,
                                                vid_t src_lid, vid_t dst_lid,
                                                Allocator& alloc) {
  auto& state = cow_state_.edge_tables[edge_triplet_id];
  auto& edge_table = cow_graph_->get_edge_table_by_index(edge_triplet_id);
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

Status StorageTPUpdateInterface::detachForResize(label_t label,
                                                 size_t capacity) {
  const auto& vertex_table = cow_graph_->get_vertex_table(label);
  if (capacity <= vertex_table.Capacity()) {
    return Status::OK();
  }
  RETURN_IF_NOT_OK(detachVertexTableForInsert(label));
  const auto& schema = cow_graph_->schema();
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

Status StorageTPUpdateInterface::detachForResize(label_t src_label,
                                                 label_t dst_label,
                                                 label_t edge_label,
                                                 size_t capacity) {
  uint32_t idx = cow_graph_->schema().generate_edge_label(src_label, dst_label,
                                                          edge_label);
  return detachEdgeTableForInsert(idx);
}

Status StorageTPUpdateInterface::prepareVertexDelete(
    label_t label, const std::vector<vid_t>& lids) {
  // Step 1: detach vertex timestamp for delete.
  RETURN_IF_NOT_OK(detachVertexTableForDelete(label));

  // Step 2: detach all related edge tables' CSRs (structure-level COW).
  const auto& schema = cow_graph_->schema();
  auto vertex_label_count = schema.vertex_label_frontier();
  auto edge_label_count = schema.edge_label_frontier();
  for (label_t i = 0; i < vertex_label_count; ++i) {
    if (!schema.is_vertex_label_valid(i))
      continue;
    for (label_t e = 0; e < edge_label_count; ++e) {
      if (schema.is_edge_triplet_valid(i, label, e)) {
        uint32_t idx = schema.generate_edge_label(i, label, e);
        RETURN_IF_NOT_OK(detachEdgeTableForDelete(idx));
      }
      if (schema.is_edge_triplet_valid(label, i, e)) {
        uint32_t idx = schema.generate_edge_label(label, i, e);
        RETURN_IF_NOT_OK(detachEdgeTableForDelete(idx));
      }
    }
  }

  // Step 3: Adjlist-level COW — ensure each touched adjacency list is
  // writable before the storage layer mutates them.
  for (vid_t lid : lids) {
    if (!cow_graph_->IsValidLid(label, lid, read_ts_)) {
      continue;
    }
    auto related_edges =
        fetch_edges_related_to_vertex(*this, schema, label, lid, read_ts_);
    for (auto& [edge_triplet_id, edges] : related_edges) {
      for (auto& [src, dst, oe_off, ie_off] : edges) {
        RETURN_IF_NOT_OK(detachAdjlists(edge_triplet_id, src, dst, alloc_));
      }
    }
  }
  return Status::OK();
}

void StorageTPUpdateInterface::CreateCheckpoint() {
  if (wal_.op_num() != 0) {
    THROW_INTERNAL_EXCEPTION(
        "Checkpoint should be created in a update "
        "transaction without any updates");
  }
  if (!cow_graph_->IsModified()) {
    wal_.LogCheckpoint();
    return;
  }
  auto ckp = cow_graph_->checkpoint_ptr();
  auto memory_level = cow_graph_->memory_level();
  cow_graph_->DumpAndClear(ckp);
  cow_graph_->Open(ckp, memory_level);
  mut_view_.Rebuild(*cow_graph_);
  cow_graph_->ClearAllDirty();
  wal_.LogCheckpoint();
}

Status StorageTPUpdateInterface::BatchAddVerticesImpl(
    label_t v_label_id, std::shared_ptr<IDataChunkSupplier> supplier) {
  LOG(ERROR) << "BatchAddVertices is not supported in TP mode currently.";
  return Status(StatusCode::ERR_NOT_SUPPORTED,
                "BatchAddVertices is not supported in TP mode currently.");
}

Status StorageTPUpdateInterface::BatchAddEdgesImpl(
    label_t src_label, label_t dst_label, label_t edge_label,
    std::shared_ptr<IDataChunkSupplier> supplier) {
  LOG(ERROR) << "BatchAddEdges is not supported in TP mode currently.";
  return Status(StatusCode::ERR_NOT_SUPPORTED,
                "BatchAddEdges is not supported in TP mode currently.");
}

Status StorageTPUpdateInterface::BatchDeleteVerticesImpl(
    label_t v_label_id, const std::vector<vid_t>& vids) {
  for (vid_t lid : vids) {
    if (!DeleteVertex(v_label_id, lid)) {
      LOG(ERROR) << "Failed to delete vertex " << lid << " of label "
                 << v_label_id << " in batch request.";
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Failed to delete vertex " + std::to_string(lid));
    }
  }
  return Status::OK();
}

Status StorageTPUpdateInterface::BatchDeleteEdgesImpl(
    label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
    const std::vector<std::tuple<vid_t, vid_t>>& edges) {
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
  return Status::OK();
}

Status StorageTPUpdateInterface::BatchDeleteEdgesImpl(
    label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
    const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
    const std::vector<std::pair<vid_t, int32_t>>& ie_edges) {
  assert(oe_edges.size() == ie_edges.size());
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
  return Status::OK();
}

Status StorageTPUpdateInterface::detachIndex(StorageIndex& index) {
  const auto& name = index.GetMeta().name;
  auto it = cow_state_.index_detached.find(name);
  if (it != cow_state_.index_detached.end() && it->second) {
    return Status::OK();
  }
  index.Detach(*ckp_, cow_graph_->memory_level());
  cow_state_.index_detached[name] = true;
  return Status::OK();
}

}  // namespace neug
