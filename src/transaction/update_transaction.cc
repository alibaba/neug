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
#include <limits>
#include <ostream>
#include <string_view>

#include <flat_hash_map.hpp>
#include "neug/common/extra_type_info.h"
#include "neug/storages/csr/csr_base.h"
#include "neug/storages/csr/generic_view_utils.h"
#include "neug/storages/file_names.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/transaction/transaction_utils.h"
#include "neug/transaction/version_manager.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/allocators.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/id_indexer.h"
#include "neug/utils/likely.h"
#include "neug/utils/property/column.h"
#include "neug/utils/property/table.h"
#include "neug/utils/property/types.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {

std::vector<std::tuple<vid_t, vid_t, int32_t, int32_t>>
fetch_edges_related_to_vertex_from_view(const std::vector<DataType>& props,
                                        const GenericView& oe,
                                        const GenericView& ie, vid_t lid,
                                        bool is_src, timestamp_t ts) {
  std::vector<std::tuple<vid_t, vid_t, int32_t, int32_t>> related_edges;
  if (is_src) {
    NbrList nbr_list = oe.get_edges(lid);
    auto stride = nbr_list.cfg.stride;
    auto start_ptr = static_cast<const char*>(nbr_list.start_ptr);
    for (auto it = nbr_list.begin(); it != nbr_list.end(); ++it) {
      if (it.get_timestamp() > ts) {
        continue;
      }
      auto oe_offset =
          (static_cast<const char*>(it.get_nbr_ptr()) - start_ptr) / stride;
      vid_t dst_lid = it.get_vertex();
      int32_t ie_offset = neug::search_other_offset_with_cur_offset(
          oe, ie, lid, dst_lid, oe_offset, props);
      related_edges.emplace_back(lid, dst_lid, oe_offset, ie_offset);
    }
  } else {
    NbrList nbr_list = ie.get_edges(lid);
    auto stride = nbr_list.cfg.stride;
    auto start_ptr = static_cast<const char*>(nbr_list.start_ptr);
    for (auto it = nbr_list.begin(); it != nbr_list.end(); ++it) {
      if (it.get_timestamp() > ts) {
        continue;
      }
      auto ie_offset =
          (static_cast<const char*>(it.get_nbr_ptr()) - start_ptr) / stride;
      vid_t src_lid = it.get_vertex();
      int32_t oe_offset = neug::search_other_offset_with_cur_offset(
          ie, oe, lid, src_lid, ie_offset, props);
      related_edges.emplace_back(src_lid, lid, oe_offset, ie_offset);
    }
  }
  return related_edges;
}

std::unordered_map<uint32_t,
                   std::vector<std::tuple<vid_t, vid_t, int32_t, int32_t>>>
fetch_edges_related_to_vertex(UpdateTransaction& txn, label_t v_label,
                              vid_t lid, timestamp_t ts) {
  std::unordered_map<uint32_t,
                     std::vector<std::tuple<vid_t, vid_t, int32_t, int32_t>>>
      related_edges;  // edge_triplet_id: <src, dst, oe_offset, ie_offset>
  auto v_label_num = txn.schema().vertex_label_frontier();
  auto e_label_num = txn.schema().edge_label_frontier();
  auto& schema = txn.schema();
  for (auto other_label_id = 0; other_label_id < v_label_num;
       ++other_label_id) {
    if (!schema.vertex_label_valid(other_label_id)) {
      continue;
    }
    for (auto e_label_id = 0; e_label_id < e_label_num; ++e_label_id) {
      if (!schema.edge_label_valid(e_label_id)) {
        continue;
      }
      if (schema.exist(v_label, other_label_id, e_label_id)) {
        auto props =
            schema.get_edge_properties(v_label, other_label_id, e_label_id);
        auto edge_triplet_id =
            schema.generate_edge_label(v_label, other_label_id, e_label_id);
        auto oe_view = txn.GetGenericOutgoingGraphView(v_label, other_label_id,
                                                       e_label_id);
        auto ie_view = txn.GetGenericIncomingGraphView(other_label_id, v_label,
                                                       e_label_id);

        related_edges[edge_triplet_id] =
            fetch_edges_related_to_vertex_from_view(props, oe_view, ie_view,
                                                    lid, true, ts);
      }
      if (other_label_id != v_label &&
          schema.exist(other_label_id, v_label, e_label_id)) {
        auto props =
            schema.get_edge_properties(other_label_id, v_label, e_label_id);
        auto edge_triplet_id =
            schema.generate_edge_label(other_label_id, v_label, e_label_id);
        auto oe_view = txn.GetGenericOutgoingGraphView(other_label_id, v_label,
                                                       e_label_id);
        auto ie_view = txn.GetGenericIncomingGraphView(v_label, other_label_id,
                                                       e_label_id);
        related_edges[edge_triplet_id] =
            fetch_edges_related_to_vertex_from_view(props, oe_view, ie_view,
                                                    lid, false, ts);
      }
    }
  }
  return related_edges;
}

UpdateTransaction::UpdateTransaction(PropertyGraph& graph, Allocator& alloc,
                                     IWalWriter& logger, IVersionManager& vm,
                                     execution::LocalQueryCache& cache,
                                     timestamp_t timestamp)
    : graph_(graph),
      alloc_(alloc),
      logger_(logger),
      vm_(vm),
      pipeline_cache_(cache),
      timestamp_(timestamp),
      op_num_(0) {
  arc_.Resize(sizeof(WalHeader));
}

UpdateTransaction::~UpdateTransaction() { Abort(); }

timestamp_t UpdateTransaction::timestamp() const { return timestamp_; }

bool UpdateTransaction::Commit() {
  if (timestamp_ == INVALID_TIMESTAMP) {
    return true;
  }
  if (op_num_ == 0) {
    release();
    return true;
  }

  auto* header = reinterpret_cast<WalHeader*>(arc_.GetBuffer());
  header->length = arc_.GetSize() - sizeof(WalHeader);
  header->type = 1;
  header->timestamp = timestamp_;
  if (!logger_.append(arc_.GetBuffer(), arc_.GetSize())) {
    LOG(ERROR) << "Failed to append wal log";
    Abort();
    return false;
  }

  // Should delete edge types before vertex types
  applyEdgeTypeDeletions();
  applyVertexTypeDeletions();
  // Apply properties deletions after type deletions
  applyVertexPropDeletion();
  applyEdgePropDeletion();
  invalidate_query_cache_if_needed();
  release();
  return true;
}

void UpdateTransaction::Abort() {
  revert_changes();
  release();
}

void UpdateTransaction::revert_changes() {
  while (!undo_logs_.empty()) {
    auto& log = undo_logs_.top();
    log->Undo(graph_, timestamp_);
    undo_logs_.pop();
  }
}

Status UpdateTransaction::CreateVertexType(
    const std::string& name,
    const std::vector<std::tuple<DataType, std::string, Property>>& properties,
    const std::vector<std::string>& primary_key_names, bool error_on_conflict) {
  if (graph_.schema().contains_vertex_label(name)) {
    LOG(ERROR) << "Vertex type " << name << " already exists.";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Vertex type " + name + " already exists.");
    } else {
      return Status::OK();
    }
  }
  {
    CreateVertexTypeRedo::Serialize(arc_, name, properties, primary_key_names);
    op_num_ += 1;
  }
  { undo_logs_.push(std::make_unique<CreateVertexTypeUndo>(name)); }
  auto status = graph_.CreateVertexType(name, properties, primary_key_names,
                                        error_on_conflict);

  if (!status.ok()) {
    LOG(ERROR) << "Failed to create vertex type " << name << ": "
               << status.ToString();
    undo_logs_.pop();
    return status;
  }
  auto label_id = graph_.schema().get_vertex_label_id(name);
  if (deleted_vertex_labels_.find(label_id) != deleted_vertex_labels_.end()) {
    deleted_vertex_labels_.erase(label_id);
  }
  schema_changed_ = true;
  return status;
}

Status UpdateTransaction::CreateEdgeType(
    const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::tuple<DataType, std::string, Property>>& properties,
    bool error_on_conflict, EdgeStrategy oe_edge_strategy,
    EdgeStrategy ie_edge_strategy) {
  if (graph_.schema().exist(src_type, dst_type, edge_type)) {
    LOG(ERROR) << "Edge type " << edge_type << " already exists between "
               << src_type << " and " << dst_type << ".";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Edge type " + edge_type + " already exists between " +
                        src_type + " and " + dst_type + ".");
    } else {
      return Status::OK();
    }
  }
  {
    CreateEdgeTypeRedo::Serialize(arc_, src_type, dst_type, edge_type,
                                  properties, oe_edge_strategy,
                                  ie_edge_strategy);
    op_num_ += 1;
  }
  {
    undo_logs_.push(
        std::make_unique<CreateEdgeTypeUndo>(src_type, dst_type, edge_type));
  }
  auto status = graph_.CreateEdgeType(src_type, dst_type, edge_type, properties,
                                      true, oe_edge_strategy, ie_edge_strategy);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to create edge type " << edge_type << " between "
               << src_type << " and " << dst_type << ": " << status.ToString();
    undo_logs_.pop();
    return status;
  }
  auto src_label_id = graph_.schema().get_vertex_label_id(src_type);
  auto dst_label_id = graph_.schema().get_vertex_label_id(dst_type);
  auto edge_label_id = graph_.schema().get_edge_label_id(edge_type);
  if (deleted_edge_labels_.find(
          std::make_tuple(src_label_id, dst_label_id, edge_label_id)) !=
      deleted_edge_labels_.end()) {
    deleted_edge_labels_.erase(
        std::make_tuple(src_label_id, dst_label_id, edge_label_id));
  }
  schema_changed_ = true;
  return status;
}

Status UpdateTransaction::AddVertexProperties(
    const std::string& vertex_type_name,
    const std::vector<std::tuple<DataType, std::string, Property>>&
        add_properties,
    bool error_on_conflict) {
  if (!graph_.schema().contains_vertex_label(vertex_type_name)) {
    LOG(ERROR) << "Vertex type " << vertex_type_name << " does not exist.";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Vertex type " + vertex_type_name + " does not exist.");
    } else {
      return Status::OK();
    }
  }
  label_t v_label = graph_.schema().get_vertex_label_id(vertex_type_name);
  {
    AddVertexPropertiesRedo::Serialize(arc_, vertex_type_name, add_properties);
    op_num_ += 1;
  }
  {
    std::vector<std::string> add_property_names;
    for (const auto& prop : add_properties) {
      add_property_names.push_back(std::get<1>(prop));
    }
    undo_logs_.push(
        std::make_unique<AddVertexPropUndo>(v_label, add_property_names));
  }
  auto status = graph_.AddVertexProperties(vertex_type_name, add_properties,
                                           error_on_conflict);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to add properties to vertex type " << vertex_type_name
               << ": " << status.ToString();
    undo_logs_.pop();
    return status;
  }
  if (deleted_vertex_properties_.size() > v_label) {
    for (const auto& prop : add_properties) {
      if (deleted_vertex_properties_[v_label].find(std::get<1>(prop)) !=
          deleted_vertex_properties_[v_label].end()) {
        deleted_vertex_properties_[v_label].erase(std::get<1>(prop));
      }
    }
  }
  schema_changed_ = true;
  return status;
}

Status UpdateTransaction::AddEdgeProperties(
    const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::tuple<DataType, std::string, Property>>&
        add_properties,
    bool error_on_conflict) {
  if (!graph_.schema().exist(src_type, dst_type, edge_type)) {
    LOG(ERROR) << "Edge type " << edge_type << " does not exist between "
               << src_type << " and " << dst_type << ".";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Edge type " + edge_type + " does not exist between " +
                        src_type + " and " + dst_type + ".");
    } else {
      return Status::OK();
    }
  }
  auto src_label_id = graph_.schema().get_vertex_label_id(src_type);
  auto dst_label_id = graph_.schema().get_vertex_label_id(dst_type);
  auto edge_label_id = graph_.schema().get_edge_label_id(edge_type);
  {
    AddEdgePropertiesRedo::Serialize(arc_, src_type, dst_type, edge_type,
                                     add_properties);
    op_num_ += 1;
  }
  {
    std::vector<std::string> add_property_names;
    for (const auto& prop : add_properties) {
      add_property_names.push_back(std::get<1>(prop));
    }
    undo_logs_.push(std::make_unique<AddEdgePropUndo>(
        src_label_id, dst_label_id, edge_label_id, add_property_names));
  }
  auto status = graph_.AddEdgeProperties(src_type, dst_type, edge_type,
                                         add_properties, error_on_conflict);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to add properties to edge type " << edge_type
               << " between " << src_type << " and " << dst_type << ": "
               << status.ToString();
    undo_logs_.pop();
    return status;
  }
  auto index = graph_.schema().generate_edge_label(src_label_id, dst_label_id,
                                                   edge_label_id);
  if (deleted_edge_properties_.find(index) != deleted_edge_properties_.end()) {
    for (const auto& prop : add_properties) {
      if (deleted_edge_properties_[index].find(std::get<1>(prop)) !=
          deleted_edge_properties_[index].end()) {
        deleted_edge_properties_[index].erase(std::get<1>(prop));
      }
    }
  }
  schema_changed_ = true;
  return status;
}

Status UpdateTransaction::RenameVertexProperties(
    const std::string& vertex_type_name,
    const std::vector<std::pair<std::string, std::string>>& rename_properties,
    bool error_on_conflict) {
  if (!graph_.schema().contains_vertex_label(vertex_type_name)) {
    LOG(ERROR) << "Vertex type " << vertex_type_name << " does not exist.";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Vertex type " + vertex_type_name + " does not exist.");
    } else {
      return Status::OK();
    }
  }
  label_t v_label = graph_.schema().get_vertex_label_id(vertex_type_name);
  ENSURE_VERTEX_LABEL_NOT_DELETED(v_label);
  for (const auto& [old_name, _] : rename_properties) {
    ENSURE_VERTEX_PROPERTY_NOT_DELETED(v_label, old_name);
  }
  {
    RenameVertexPropertiesRedo::Serialize(arc_, vertex_type_name,
                                          rename_properties);
    op_num_ += 1;
  }
  {
    undo_logs_.push(
        std::make_unique<RenameVertexPropUndo>(v_label, rename_properties));
  }
  auto status = graph_.RenameVertexProperties(
      vertex_type_name, rename_properties, error_on_conflict);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to rename properties of vertex type "
               << vertex_type_name << ": " << status.ToString();
    undo_logs_.pop();
  }
  schema_changed_ = true;
  return status;
}

Status UpdateTransaction::RenameEdgeProperties(
    const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::pair<std::string, std::string>>& rename_properties,
    bool error_on_conflict) {
  if (!graph_.schema().exist(src_type, dst_type, edge_type)) {
    LOG(ERROR) << "Edge type " << edge_type << " does not exist between "
               << src_type << " and " << dst_type << ".";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Edge type " + edge_type + " does not exist between " +
                        src_type + " and " + dst_type + ".");
    } else {
      return Status::OK();
    }
  }
  auto src_label_id = graph_.schema().get_vertex_label_id(src_type);
  auto dst_label_id = graph_.schema().get_vertex_label_id(dst_type);
  auto edge_label_id = graph_.schema().get_edge_label_id(edge_type);
  ENSURE_VERTEX_LABEL_NOT_DELETED(src_label_id);
  ENSURE_VERTEX_LABEL_NOT_DELETED(dst_label_id);
  ENSURE_EDGE_LABEL_NOT_DELETED(src_label_id, dst_label_id, edge_label_id);
  for (const auto& [old_name, _] : rename_properties) {
    auto index = graph_.schema().generate_edge_label(src_label_id, dst_label_id,
                                                     edge_label_id);
    ENSURE_EDGE_PROPERTY_NOT_DELETED(index, old_name);
  }
  {
    RenameEdgePropertiesRedo::Serialize(arc_, src_type, dst_type, edge_type,
                                        rename_properties);
    op_num_ += 1;
  }
  {
    undo_logs_.push(std::make_unique<RenameEdgePropUndo>(
        src_label_id, dst_label_id, edge_label_id, rename_properties));
  }
  auto status = graph_.RenameEdgeProperties(
      src_type, dst_type, edge_type, rename_properties, error_on_conflict);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to rename properties of edge type " << edge_type
               << " between " << src_type << " and " << dst_type << ": "
               << status.ToString();
    undo_logs_.pop();
  }
  schema_changed_ = true;
  return status;
}

Status UpdateTransaction::DeleteVertexProperties(
    const std::string& vertex_type_name,
    const std::vector<std::string>& delete_properties, bool error_on_conflict) {
  if (!graph_.schema().contains_vertex_label(vertex_type_name)) {
    LOG(ERROR) << "Vertex type " << vertex_type_name << " does not exist.";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Vertex type " + vertex_type_name + " does not exist.");
    } else {
      return Status::OK();
    }
  }
  label_t v_label = graph_.schema().get_vertex_label_id(vertex_type_name);
  for (auto& prop_name : delete_properties) {
    ENSURE_VERTEX_PROPERTY_NOT_DELETED(v_label, prop_name);
    if (!graph_.schema().vertex_has_property(vertex_type_name, prop_name)) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Property [" + prop_name + "] does not exist in vertex [" +
                        vertex_type_name + "].");
    }
  }
  {
    DeleteVertexPropertiesRedo::Serialize(arc_, vertex_type_name,
                                          delete_properties);
    op_num_ += 1;
  }
  {
    undo_logs_.push(
        std::make_unique<DeleteVertexPropUndo>(v_label, delete_properties));
  }
  graph_.mutable_schema().DeleteVertexProperties(vertex_type_name,
                                                 delete_properties, true);
  if (deleted_vertex_properties_.size() <= v_label) {
    deleted_vertex_properties_.resize(v_label + 1);
  }
  for (const auto& prop_name : delete_properties) {
    deleted_vertex_properties_[v_label].emplace(prop_name);
  }
  schema_changed_ = true;
  return Status::OK();
}

Status UpdateTransaction::DeleteEdgeProperties(
    const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::string>& delete_properties, bool error_on_conflict) {
  if (!graph_.schema().exist(src_type, dst_type, edge_type)) {
    LOG(ERROR) << "Edge type " << edge_type << " does not exist between "
               << src_type << " and " << dst_type << ".";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Edge type " + edge_type + " does not exist between " +
                        src_type + " and " + dst_type + ".");
    } else {
      return Status::OK();
    }
  }
  auto src_label_id = graph_.schema().get_vertex_label_id(src_type);
  auto dst_label_id = graph_.schema().get_vertex_label_id(dst_type);
  auto edge_label_id = graph_.schema().get_edge_label_id(edge_type);
  for (auto& prop_name : delete_properties) {
    auto index = graph_.schema().generate_edge_label(src_label_id, dst_label_id,
                                                     edge_label_id);
    ENSURE_EDGE_PROPERTY_NOT_DELETED(index, prop_name);
    if (!graph_.schema().edge_has_property(src_type, dst_type, edge_type,
                                           prop_name)) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Property [" + prop_name + "] does not exist in edge [" +
                        edge_type + "] between [" + src_type + "] and [" +
                        dst_type + "].");
    }
  }
  {
    DeleteEdgePropertiesRedo::Serialize(arc_, src_type, dst_type, edge_type,
                                        delete_properties);
    op_num_ += 1;
  }
  {
    undo_logs_.push(std::make_unique<DeleteEdgePropUndo>(
        src_label_id, dst_label_id, edge_label_id, delete_properties));
  }
  graph_.mutable_schema().DeleteEdgeProperties(src_type, dst_type, edge_type,
                                               delete_properties, true);
  auto index = graph_.schema().generate_edge_label(src_label_id, dst_label_id,
                                                   edge_label_id);
  if (deleted_edge_properties_.find(index) == deleted_edge_properties_.end()) {
    deleted_edge_properties_[index] = std::unordered_set<std::string>();
  }
  for (const auto& prop_name : delete_properties) {
    deleted_edge_properties_[index].emplace(prop_name);
  }
  schema_changed_ = true;
  return Status::OK();
}

Status UpdateTransaction::DeleteVertexType(const std::string& vertex_type_name,
                                           bool error_on_conflict) {
  if (!graph_.schema().contains_vertex_label(vertex_type_name)) {
    LOG(ERROR) << "Vertex type " << vertex_type_name << " does not exist.";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Vertex type " + vertex_type_name + " does not exist.");
    } else {
      return Status::OK();
    }
  }
  label_t v_label = graph_.schema().get_vertex_label_id(vertex_type_name);
  {
    DeleteVertexTypeRedo::Serialize(arc_, vertex_type_name);
    op_num_ += 1;
  }
  { undo_logs_.push(std::make_unique<DeleteVertexTypeUndo>(vertex_type_name)); }
  if (graph_.schema().IsVertexLabelSoftDeleted(v_label)) {
    LOG(ERROR) << "Vertex type " << vertex_type_name
               << " is already deleted (soft delete).";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Vertex type " + vertex_type_name +
                        " is already deleted (soft delete).");
    } else {
      return Status::OK();
    }
  }
  graph_.mutable_schema().DeleteVertexLabel(vertex_type_name, true);
  // Mark the vertex table as deleted in this transaction
  deleted_vertex_labels_.emplace(v_label);
  auto vertex_label_num = graph_.schema().vertex_label_frontier();
  auto edge_label_num = graph_.schema().edge_label_frontier();
  for (label_t dst_label = 0; dst_label < vertex_label_num; ++dst_label) {
    if (!graph_.schema().vertex_label_valid(dst_label)) {
      continue;
    }
    for (label_t edge_label = 0; edge_label < edge_label_num; ++edge_label) {
      if (graph_.schema().exist(v_label, dst_label, edge_label)) {
        deleted_edge_labels_.emplace(
            std::make_tuple(v_label, dst_label, edge_label));
      }
      if (graph_.schema().exist(dst_label, v_label, edge_label)) {
        deleted_edge_labels_.emplace(
            std::make_tuple(dst_label, v_label, edge_label));
      }
    }
  }
  schema_changed_ = true;
  return Status::OK();
}

Status UpdateTransaction::DeleteEdgeType(const std::string& src_type,
                                         const std::string& dst_type,
                                         const std::string& edge_type,
                                         bool error_on_conflict) {
  if (!graph_.schema().exist(src_type, dst_type, edge_type)) {
    LOG(ERROR) << "Edge type " << edge_type << " does not exist between "
               << src_type << " and " << dst_type << ".";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Edge type " + edge_type + " does not exist between " +
                        src_type + " and " + dst_type + ".");
    } else {
      return Status::OK();
    }
  }
  label_t src_label_id = graph_.schema().get_vertex_label_id(src_type);
  label_t dst_label_id = graph_.schema().get_vertex_label_id(dst_type);
  label_t edge_label_id = graph_.schema().get_edge_label_id(edge_type);
  {
    DeleteEdgeTypeRedo::Serialize(arc_, src_type, dst_type, edge_type);
    op_num_ += 1;
  }
  {
    undo_logs_.push(
        std::make_unique<DeleteEdgeTypeUndo>(src_type, dst_type, edge_type));
  }
  if (graph_.schema().IsEdgeLabelSoftDeleted(src_label_id, dst_label_id,
                                             edge_label_id)) {
    LOG(ERROR) << "Edge type " << edge_type << " between " << src_type
               << " and " << dst_type << " is already deleted (soft delete).";
    if (error_on_conflict) {
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Edge type " + edge_type + " between " + src_type +
                        " and " + dst_type +
                        " is already deleted (soft delete).");
    } else {
      return Status::OK();
    }
  }
  graph_.mutable_schema().DeleteEdgeLabel(src_label_id, dst_label_id,
                                          edge_label_id, true);
  // Mark the edge table as deleted in this transaction
  deleted_edge_labels_.emplace(
      std::make_tuple(src_label_id, dst_label_id, edge_label_id));
  schema_changed_ = true;
  return Status::OK();
}

bool UpdateTransaction::AddVertex(label_t label, const Property& oid,
                                  const std::vector<Property>& props,
                                  vid_t& vid) {
  ENSURE_VERTEX_LABEL_NOT_DELETED(label);
  std::vector<DataType> types = graph_.schema().get_vertex_properties(label);
  if (types.size() != props.size()) {
    return false;
  }
  int col_num = types.size();
  for (int col_i = 0; col_i != col_num; ++col_i) {
    if (props[col_i].type() != types[col_i].id()) {
      return false;
    }
  }

  InsertVertexRedo::Serialize(arc_, label, oid, props);
  op_num_ += 1;
  auto status = graph_.AddVertex(label, oid, props, vid, timestamp_, true);
  if (!status.ok()) {
    // The most possible reason is that the space is not enough.
    graph_.Reserve(label, std::max((vid_t) 1024, graph_.LidNum(label) * 2));
    status = graph_.AddVertex(label, oid, props, vid, timestamp_, true);
  }
  if (!status.ok()) {
    LOG(ERROR) << "Failed to add vertex of label "
               << graph_.schema().get_vertex_label_name(label) << ": "
               << status.ToString();
    return false;
  }
  undo_logs_.push(std::make_unique<InsertVertexUndo>(label, vid));
  return true;
}

bool UpdateTransaction::DeleteVertex(label_t label, vid_t lid) {
  ENSURE_VERTEX_LABEL_NOT_DELETED(label);
  if (!graph_.IsValidLid(label, lid, timestamp_)) {
    THROW_RUNTIME_ERROR("Vertex id is out of range or already deleted");
  }
  auto oid = graph_.GetOid(label, lid, timestamp_);
  RemoveVertexRedo::Serialize(arc_, label, oid);
  op_num_ += 1;
  // edge_triplet_id: < src, dst, oe_offset, ie_offset>
  std::unordered_map<uint32_t,
                     std::vector<std::tuple<vid_t, vid_t, int32_t, int32_t>>>
      related_edges =
          fetch_edges_related_to_vertex(*this, label, lid, timestamp_);
  undo_logs_.push(
      std::make_unique<RemoveVertexUndo>(label, lid, related_edges));
  auto status = graph_.DeleteVertex(label, lid, timestamp_);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to delete vertex of label "
               << graph_.schema().get_vertex_label_name(label) << ": "
               << status.ToString();
    undo_logs_.pop();
    return false;
  }
  return true;
}

// TODO(zhanglei): Return NbrIterator when refactoring the GraphInterface.
bool UpdateTransaction::AddEdge(label_t src_label, vid_t src_lid,
                                label_t dst_label, vid_t dst_lid,
                                label_t edge_label,
                                const std::vector<Property>& properties) {
  ENSURE_VERTEX_LABEL_NOT_DELETED(src_label);
  ENSURE_VERTEX_LABEL_NOT_DELETED(dst_label);
  ENSURE_EDGE_LABEL_NOT_DELETED(src_label, dst_label, edge_label);

  InsertEdgeRedo::Serialize(arc_, src_label, GetVertexId(src_label, src_lid),
                            dst_label, GetVertexId(dst_label, dst_lid),
                            edge_label, properties);
  op_num_ += 1;
  auto oe_offset =
      graph_.AddEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                     properties, timestamp_, alloc_, true);
  auto ie_offset = search_other_offset_with_cur_offset(
      graph_.GetGenericOutgoingGraphView(src_label, dst_label, edge_label),
      graph_.GetGenericIncomingGraphView(dst_label, src_label, edge_label),
      src_lid, dst_lid, oe_offset,
      graph_.schema()
          .get_edge_schema(src_label, dst_label, edge_label)
          ->properties);
  undo_logs_.push(std::make_unique<InsertEdgeUndo>(src_label, dst_label,
                                                   edge_label, src_lid, dst_lid,
                                                   oe_offset, ie_offset));
  return true;
}

bool UpdateTransaction::DeleteEdges(label_t src_label, vid_t src_lid,
                                    label_t dst_label, vid_t dst_lid,
                                    label_t edge_label) {
  ENSURE_VERTEX_LABEL_NOT_DELETED(src_label);
  ENSURE_VERTEX_LABEL_NOT_DELETED(dst_label);
  ENSURE_EDGE_LABEL_NOT_DELETED(src_label, dst_label, edge_label);
  if (!graph_.IsValidLid(src_label, src_lid, timestamp_) ||
      !graph_.IsValidLid(dst_label, dst_lid, timestamp_)) {
    THROW_RUNTIME_ERROR(
        "Source or destination vertex id is out of range or "
        "already deleted");
  }

  auto oe_edges = GetGenericOutgoingGraphView(src_label, dst_label, edge_label)
                      .get_edges(src_lid);
  auto ie_edges = GetGenericIncomingGraphView(dst_label, src_label, edge_label)
                      .get_edges(dst_lid);
  auto search_edge_prop_type = determine_search_prop_type(
      graph_.schema().get_edge_properties(src_label, dst_label, edge_label));
  int32_t oe_offset = 0;
  for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
    if (it.get_vertex() == dst_lid) {
      auto ie_offset = fuzzy_search_offset_from_nbr_list(
          ie_edges, src_lid, it.get_data_ptr(), search_edge_prop_type);
      {
        RemoveEdgeRedo::Serialize(
            arc_, src_label, GetVertexId(src_label, src_lid), dst_label,
            GetVertexId(dst_label, dst_lid), edge_label, oe_offset, ie_offset);
        op_num_ += 1;
      }
      auto status =
          graph_.DeleteEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                            oe_offset, ie_offset, timestamp_);
      if (!status.ok()) {
        LOG(ERROR) << "Failed to delete edge: " << status.ToString();
        return false;
      }
      undo_logs_.push(std::make_unique<RemoveEdgeUndo>(
          src_label, dst_label, edge_label, src_lid, dst_lid, oe_offset,
          ie_offset));
    }
    oe_offset++;
  }

  return true;
}

bool UpdateTransaction::DeleteEdge(label_t src_label, vid_t src_lid,
                                   label_t dst_label, vid_t dst_lid,
                                   label_t edge_label, int32_t oe_offset,
                                   int32_t ie_offset) {
  ENSURE_VERTEX_LABEL_NOT_DELETED(src_label);
  ENSURE_VERTEX_LABEL_NOT_DELETED(dst_label);
  ENSURE_EDGE_LABEL_NOT_DELETED(src_label, dst_label, edge_label);
  if (!graph_.IsValidLid(src_label, src_lid, timestamp_) ||
      !graph_.IsValidLid(dst_label, dst_lid, timestamp_)) {
    THROW_RUNTIME_ERROR(
        "Source or destination vertex id is out of range or "
        "already deleted");
  }

  RemoveEdgeRedo::Serialize(arc_, src_label, GetVertexId(src_label, src_lid),
                            dst_label, GetVertexId(dst_label, dst_lid),
                            edge_label, oe_offset, ie_offset);
  op_num_ += 1;

  auto status = graph_.DeleteEdge(src_label, src_lid, dst_label, dst_lid,
                                  edge_label, oe_offset, ie_offset, timestamp_);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to delete edge: " << status.ToString();
    return false;
  }
  undo_logs_.push(std::make_unique<RemoveEdgeUndo>(src_label, dst_label,
                                                   edge_label, src_lid, dst_lid,
                                                   oe_offset, ie_offset));
  return true;
}

Property UpdateTransaction::GetVertexProperty(label_t label, vid_t lid,
                                              int col_id) const {
  ENSURE_VERTEX_LABEL_NOT_DELETED(label);
  auto prop_name =
      graph_.schema().get_vertex_schema(label)->get_property_name(col_id);
  ENSURE_VERTEX_PROPERTY_NOT_DELETED(label, prop_name);
  auto col = graph_.GetVertexPropertyColumn(label, col_id);
  if (!graph_.IsValidLid(label, lid, timestamp_)) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Vertex lid is not valid in this transaction");
  }
  if (col == nullptr) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Fail to find property column");
  }
  return col->get_prop(lid);
}

Property UpdateTransaction::GetVertexId(label_t label, vid_t lid) const {
  ENSURE_VERTEX_LABEL_NOT_DELETED(label);
  return graph_.GetOid(label, lid, timestamp_);
}

bool UpdateTransaction::GetVertexIndex(label_t label, const Property& id,
                                       vid_t& index) const {
  ENSURE_VERTEX_LABEL_NOT_DELETED(label);
  return graph_.get_lid(label, id, index, timestamp_);
}

bool UpdateTransaction::UpdateVertexProperty(label_t label, vid_t lid,
                                             int col_id,
                                             const Property& value) {
  ENSURE_VERTEX_LABEL_NOT_DELETED(label);
  auto prop_name =
      graph_.schema().get_vertex_schema(label)->get_property_name(col_id);
  ENSURE_VERTEX_PROPERTY_NOT_DELETED(label, prop_name);
  if (!graph_.IsValidLid(label, lid, timestamp_)) {
    LOG(ERROR) << "Vertex lid " << lid << " of label "
               << graph_.schema().get_vertex_label_name(label)
               << " is not valid in this transaction.";
    return false;
  }
  std::vector<DataType> types = graph_.schema().get_vertex_properties(label);
  if (static_cast<size_t>(col_id) >= types.size()) {
    return false;
  }
  if (types[col_id].id() != value.type()) {
    return false;
  }
  UpdateVertexPropRedo::Serialize(arc_, label, GetVertexId(label, lid), col_id,
                                  value);

  auto old_prop = GetVertexProperty(label, lid, col_id);
  auto status =
      graph_.UpdateVertexProperty(label, lid, col_id, value, timestamp_);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to update vertex property: " << status.ToString();
    return false;
  }
  undo_logs_.push(
      std::make_unique<UpdateVertexPropUndo>(label, lid, col_id, old_prop));
  op_num_ += 1;
  return true;
}

bool UpdateTransaction::UpdateEdgeProperty(label_t src_label, vid_t src,
                                           label_t dst_label, vid_t dst,
                                           label_t edge_label, int32_t col_id,
                                           const Property& value) {
  ENSURE_VERTEX_LABEL_NOT_DELETED(src_label);
  ENSURE_VERTEX_LABEL_NOT_DELETED(dst_label);
  ENSURE_EDGE_LABEL_NOT_DELETED(src_label, dst_label, edge_label);
  auto index =
      graph_.schema().generate_edge_label(src_label, dst_label, edge_label);
  auto prop_name = graph_.schema()
                       .get_edge_schema(src_label, dst_label, edge_label)
                       ->get_property_name(col_id);
  ENSURE_EDGE_PROPERTY_NOT_DELETED(index, prop_name);
  if (!graph_.IsValidLid(src_label, src, timestamp_) ||
      !graph_.IsValidLid(dst_label, dst, timestamp_)) {
    THROW_RUNTIME_ERROR(
        "Source or destination vertex id is out of range or "
        "already deleted");
  }
  auto prop_getter =
      graph_.GetEdgeDataAccessor(src_label, dst_label, edge_label, col_id);
  auto oe_edges = GetGenericOutgoingGraphView(src_label, dst_label, edge_label)
                      .get_edges(src);
  auto ie_edges = GetGenericIncomingGraphView(dst_label, src_label, edge_label)
                      .get_edges(dst);
  auto edge_prop_types =
      graph_.schema().get_edge_properties(src_label, dst_label, edge_label);
  assert(col_id >= 0 && static_cast<size_t>(col_id) < edge_prop_types.size());
  auto search_prop_type = determine_search_prop_type(edge_prop_types);
  int32_t oe_offset = 0;
  for (auto it = oe_edges.begin(); it != oe_edges.end(); ++it) {
    if (it.get_vertex() == dst) {
      auto ie_offset = fuzzy_search_offset_from_nbr_list(
          ie_edges, src, it.get_data_ptr(), search_prop_type);
      auto old_prop = prop_getter.get_data(it);
      {
        UpdateEdgePropRedo::Serialize(arc_, src_label,
                                      GetVertexId(src_label, src), dst_label,
                                      GetVertexId(dst_label, dst), edge_label,
                                      oe_offset, ie_offset, col_id, value);
        op_num_ += 1;
      }
      auto status = graph_.UpdateEdgeProperty(src_label, src, dst_label, dst,
                                              edge_label, oe_offset, ie_offset,
                                              col_id, value, timestamp_);
      if (!status.ok()) {
        LOG(ERROR) << "Failed to update edge property: " << status.ToString();
        return false;
      }
      undo_logs_.push(std::make_unique<UpdateEdgePropUndo>(
          src_label, dst_label, edge_label, src, dst, oe_offset, ie_offset,
          col_id, old_prop));
    }
    oe_offset += 1;
  }

  return true;
}

bool UpdateTransaction::UpdateEdgeProperty(label_t src_label, vid_t src,
                                           label_t dst_label, vid_t dst,
                                           label_t edge_label,
                                           int32_t oe_offset, int32_t ie_offset,
                                           int32_t col_id,
                                           const Property& value) {
  ENSURE_VERTEX_LABEL_NOT_DELETED(src_label);
  ENSURE_VERTEX_LABEL_NOT_DELETED(dst_label);
  ENSURE_EDGE_LABEL_NOT_DELETED(src_label, dst_label, edge_label);
  auto index =
      graph_.schema().generate_edge_label(src_label, dst_label, edge_label);
  auto prop_name = graph_.schema()
                       .get_edge_schema(src_label, dst_label, edge_label)
                       ->get_property_name(col_id);
  ENSURE_EDGE_PROPERTY_NOT_DELETED(index, prop_name);
  if (!graph_.IsValidLid(src_label, src, timestamp_) ||
      !graph_.IsValidLid(dst_label, dst, timestamp_)) {
    THROW_RUNTIME_ERROR(
        "Source or destination vertex id is out of range or "
        "already deleted");
  }
  {
    UpdateEdgePropRedo::Serialize(arc_, src_label, GetVertexId(src_label, src),
                                  dst_label, GetVertexId(dst_label, dst),
                                  edge_label, oe_offset, ie_offset, col_id,
                                  value);
    op_num_ += 1;
  }
  auto prop_accessor =
      graph_.GetEdgeDataAccessor(src_label, dst_label, edge_label, col_id);
  auto oe_edges =
      graph_.GetGenericOutgoingGraphView(src_label, dst_label, edge_label)
          .get_edges(src);
  auto oe_iter = oe_edges.begin();
  for (int32_t i = 0; i < oe_offset; ++i) {
    ++oe_iter;
  }
  assert(oe_iter != oe_edges.end());
  auto old_prop = prop_accessor.get_data(oe_iter);
  auto status = graph_.UpdateEdgeProperty(src_label, src, dst_label, dst,
                                          edge_label, oe_offset, ie_offset,
                                          col_id, value, timestamp_);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to update edge property: " << status.ToString();
    return false;
  }
  undo_logs_.push(std::make_unique<UpdateEdgePropUndo>(
      src_label, dst_label, edge_label, src, dst, oe_offset, ie_offset, col_id,
      old_prop));
  return true;
}

void UpdateTransaction::IngestWal(PropertyGraph& graph,
                                  const std::string& work_dir,
                                  uint32_t timestamp, char* data, size_t length,
                                  Allocator& alloc) {
  OutArchive arc;
  arc.SetSlice(data, length);
  while (!arc.Empty()) {
    OpType op_type;
    arc >> op_type;
    if (op_type == OpType::kCreateVertexType) {
      CreateVertexTypeRedo redo;
      arc >> redo;
      graph.CreateVertexType(redo.vertex_type, redo.properties,
                             redo.primary_key_names, true);
    } else if (op_type == OpType::kCreateEdgeType) {
      CreateEdgeTypeRedo redo;
      arc >> redo;
      graph.CreateEdgeType(redo.src_type, redo.dst_type, redo.edge_type,
                           redo.properties, true, redo.oe_edge_strategy,
                           redo.ie_edge_strategy);
    } else if (op_type == OpType::kInsertVertex) {
      InsertVertexRedo redo;
      arc >> redo;
      vid_t vid;
      auto& v_table = graph.get_vertex_table(redo.label);
      if (!graph.get_lid(redo.label, redo.oid, vid, timestamp) ||
          !graph.IsValidLid(redo.label, vid, timestamp)) {
        if (v_table.Capacity() < v_table.LidNum() + 1) {
          graph.Reserve(redo.label, v_table.Capacity() * 2);
        }
        auto ret = graph.AddVertex(redo.label, redo.oid, redo.props, vid,
                                   timestamp, true);
        if (!ret.ok()) {
          THROW_STORAGE_EXCEPTION(
              "Failed to add vertex during WAL ingestion: " + ret.ToString());
        }
      }
    } else if (op_type == OpType::kInsertEdge) {
      InsertEdgeRedo redo;
      arc >> redo;
      vid_t src_vid, dst_vid;
      CHECK(graph.get_lid(redo.src_label, redo.src, src_vid, timestamp));
      CHECK(graph.get_lid(redo.dst_label, redo.dst, dst_vid, timestamp));
      graph.AddEdge(redo.src_label, src_vid, redo.dst_label, dst_vid,
                    redo.edge_label, redo.properties, timestamp, alloc, true);
    } else if (op_type == OpType::kUpdateVertexProp) {
      UpdateVertexPropRedo redo;
      arc >> redo;
      vid_t vid;
      CHECK(graph.get_lid(redo.label, redo.oid, vid, timestamp));
      graph.get_vertex_table(redo.label)
          .UpdateProperty(vid, redo.prop_id, redo.value, timestamp);
    } else if (op_type == OpType::kUpdateEdgeProp) {
      UpdateEdgePropRedo redo;
      arc >> redo;
      vid_t src_vid, dst_vid;
      CHECK(graph.get_lid(redo.src_label, redo.src, src_vid, timestamp));
      CHECK(graph.get_lid(redo.dst_label, redo.dst, dst_vid, timestamp));
      graph.UpdateEdgeProperty(redo.src_label, src_vid, redo.dst_label, dst_vid,
                               redo.edge_label, redo.oe_offset, redo.ie_offset,
                               redo.prop_id, redo.value, timestamp);
    } else if (op_type == OpType::kRemoveVertex) {
      RemoveVertexRedo redo;
      arc >> redo;
      vid_t vid;
      CHECK(graph.get_lid(redo.label, redo.oid, vid, timestamp));
      graph.DeleteVertex(redo.label, vid, timestamp);
    } else if (op_type == OpType::kRemoveEdge) {
      RemoveEdgeRedo redo;
      arc >> redo;
      vid_t src_vid, dst_vid;
      CHECK(graph.get_lid(redo.src_label, redo.src, src_vid, timestamp));
      CHECK(graph.get_lid(redo.dst_label, redo.dst, dst_vid, timestamp));
      graph.DeleteEdge(redo.src_label, src_vid, redo.dst_label, dst_vid,
                       redo.edge_label, redo.oe_offset, redo.ie_offset,
                       timestamp);
    } else if (op_type == OpType::kAddVertexProp) {
      AddVertexPropertiesRedo redo;
      arc >> redo;
      graph.AddVertexProperties(redo.vertex_type, redo.properties);
    } else if (op_type == OpType::kAddEdgeProp) {
      AddEdgePropertiesRedo redo;
      arc >> redo;
      graph.AddEdgeProperties(redo.src_type, redo.dst_type, redo.edge_type,
                              redo.properties);
    } else if (op_type == OpType::kRenameVertexProp) {
      RenameVertexPropertiesRedo redo;
      arc >> redo;
      graph.RenameVertexProperties(redo.vertex_type, redo.update_properties);
    } else if (op_type == OpType::kRenameEdgeProp) {
      RenameEdgePropertiesRedo redo;
      arc >> redo;
      graph.RenameEdgeProperties(redo.src_type, redo.dst_type, redo.edge_type,
                                 redo.update_properties);
    } else if (op_type == OpType::kDeleteVertexProp) {
      DeleteVertexPropertiesRedo redo;
      arc >> redo;
      graph.DeleteVertexProperties(redo.vertex_type, redo.delete_properties);
    } else if (op_type == OpType::kDeleteEdgeProp) {
      DeleteEdgePropertiesRedo redo;
      arc >> redo;
      graph.DeleteEdgeProperties(redo.src_type, redo.dst_type, redo.edge_type,
                                 redo.delete_properties);
    } else if (op_type == OpType::kDeleteVertexType) {
      DeleteVertexTypeRedo redo;
      arc >> redo;
      graph.DeleteVertexType(redo.vertex_type);
    } else if (op_type == OpType::kDeleteEdgeType) {
      DeleteEdgeTypeRedo redo;
      arc >> redo;
      graph.DeleteEdgeType(redo.src_type, redo.dst_type, redo.edge_type);
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION("Unexpected op_type: " +
                                    std::to_string(static_cast<int>(op_type)));
    }
  }
}

void UpdateTransaction::CreateCheckpoint() {
  // Create a checkpoint for the current graph. Expect no changes are made to
  // this transaction
  if (op_num_ != 0) {
    THROW_INTERNAL_EXCEPTION(
        "Checkpoint should be created in a update "
        "transaction without any updates");
  }
  graph_.Dump();
}

bool UpdateTransaction::IsValidLid(label_t label, vid_t lid) const {
  ENSURE_VERTEX_LABEL_NOT_DELETED(label);
  return graph_.IsValidLid(label, lid, timestamp_);
}

void UpdateTransaction::release() {
  if (timestamp_ != INVALID_TIMESTAMP) {
    arc_.Clear();
    vm_.release_update_timestamp(timestamp_);
    timestamp_ = INVALID_TIMESTAMP;

    op_num_ = 0;

    deleted_vertex_labels_.clear();
    deleted_edge_labels_.clear();
    while (!undo_logs_.empty()) {
      undo_logs_.pop();
    }
  }
}

void UpdateTransaction::applyVertexTypeDeletions() {
  for (auto label : deleted_vertex_labels_) {
    auto status = graph_.DeleteVertexType(label, true);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to delete vertex type " << (int32_t) label << ": "
                 << status.ToString();
    }
  }
  deleted_vertex_labels_.clear();
}

void UpdateTransaction::applyEdgeTypeDeletions() {
  for (auto& triple : deleted_edge_labels_) {
    auto status = graph_.DeleteEdgeType(
        std::get<0>(triple), std::get<1>(triple), std::get<2>(triple));
    if (!status.ok()) {
      LOG(ERROR) << "Failed to delete edge type (" << std::get<0>(triple)
                 << ", " << std::get<1>(triple) << ", " << std::get<2>(triple)
                 << "): " << status.ToString();
    }
  }
  deleted_edge_labels_.clear();
}

void UpdateTransaction::applyVertexPropDeletion() {
  for (label_t v_label = 0; v_label < deleted_vertex_properties_.size();
       ++v_label) {
    if (!graph_.schema().vertex_label_valid(v_label)) {
      continue;
    }
    auto v_label_name = graph_.schema().get_vertex_label_name(v_label);
    std::vector<std::string> prop_names;
    for (const auto& prop_name : deleted_vertex_properties_[v_label]) {
      prop_names.push_back(prop_name);
    }
    graph_.DeleteVertexProperties(v_label_name, prop_names);
  }
  deleted_vertex_properties_.clear();
}

void UpdateTransaction::applyEdgePropDeletion() {
  for (auto iter : deleted_edge_properties_) {
    uint32_t index = iter.first;
    auto& prop_names_set = iter.second;
    if (prop_names_set.empty()) {
      continue;
    }
    label_t src_label, dst_label, edge_label;
    std::tie(src_label, dst_label, edge_label) =
        graph_.schema().parse_edge_label(index);
    if (!graph_.schema().edge_triplet_valid(src_label, dst_label, edge_label)) {
      continue;
    }

    auto src_label_name = graph_.schema().get_vertex_label_name(src_label);
    auto dst_label_name = graph_.schema().get_vertex_label_name(dst_label);
    auto edge_label_name = graph_.schema().get_edge_label_name(edge_label);
    std::vector<std::string> prop_names;
    for (const auto& prop_name : prop_names_set) {
      prop_names.push_back(prop_name);
    }
    graph_.DeleteEdgeProperties(src_label_name, dst_label_name, edge_label_name,
                                prop_names);
  }
  deleted_edge_properties_.clear();
}

void UpdateTransaction::invalidate_query_cache_if_needed() {
  if (schema_changed_) {
    pipeline_cache_.clearGlobalCache(graph_.schema().to_yaml().value());
  }
}

void StorageTPUpdateInterface::CreateCheckpoint() { txn_.CreateCheckpoint(); }

Status StorageTPUpdateInterface::BatchAddVertices(
    label_t v_label_id, std::shared_ptr<IRecordBatchSupplier> supplier) {
  LOG(ERROR) << "BatchAddVertices is not supported in TP mode currently.";
  return Status(StatusCode::ERR_NOT_SUPPORTED,
                "BatchAddVertices is not supported in TP mode currently.");
}

Status StorageTPUpdateInterface::BatchAddEdges(
    label_t src_label, label_t dst_label, label_t edge_label,
    std::shared_ptr<IRecordBatchSupplier> supplier) {
  LOG(ERROR) << "BatchAddEdges is not supported in TP mode currently.";
  return Status(StatusCode::ERR_NOT_SUPPORTED,
                "BatchAddEdges is not supported in TP mode currently.");
}

Status StorageTPUpdateInterface::BatchDeleteVertices(
    label_t v_label_id, const std::vector<vid_t>& vids) {
  for (vid_t lid : vids) {
    if (!txn_.DeleteVertex(v_label_id, lid)) {
      LOG(ERROR) << "Failed to delete vertex " << lid << " of label "
                 << v_label_id << " in batch request.";
      return Status(StatusCode::ERR_INVALID_ARGUMENT,
                    "Failed to delete vertex " + std::to_string(lid));
    }
  }
  return Status::OK();
}

Status StorageTPUpdateInterface::BatchDeleteEdges(
    label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
    const std::vector<std::tuple<vid_t, vid_t>>& edges) {
  for (const auto& edge : edges) {
    vid_t src_lid = std::get<0>(edge);
    vid_t dst_lid = std::get<1>(edge);
    if (!txn_.DeleteEdges(src_v_label_id, src_lid, dst_v_label_id, dst_lid,
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

Status StorageTPUpdateInterface::BatchDeleteEdges(
    label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
    const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
    const std::vector<std::pair<vid_t, int32_t>>& ie_edges) {
  assert(oe_edges.size() == ie_edges.size());
  for (size_t i = 0; i < oe_edges.size(); ++i) {
    vid_t src_lid = oe_edges[i].first;
    vid_t dst_lid = ie_edges[i].first;
    int32_t oe_offset = oe_edges[i].second;
    int32_t ie_offset = ie_edges[i].second;
    if (!txn_.DeleteEdge(src_v_label_id, src_lid, dst_v_label_id, dst_lid,
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

Status StorageTPUpdateInterface::CreateVertexType(
    const std::string& name,
    const std::vector<std::tuple<DataType, std::string, Property>>& properties,
    const std::vector<std::string>& primary_key_names, bool error_on_conflict) {
  return txn_.CreateVertexType(name, properties, primary_key_names,
                               error_on_conflict);
}

Status StorageTPUpdateInterface::CreateEdgeType(
    const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::tuple<DataType, std::string, Property>>& properties,
    bool error_on_conflict, EdgeStrategy oe_edge_strategy,
    EdgeStrategy ie_edge_strategy) {
  return txn_.CreateEdgeType(src_type, dst_type, edge_type, properties,
                             error_on_conflict, oe_edge_strategy,
                             ie_edge_strategy);
}

Status StorageTPUpdateInterface::AddVertexProperties(
    const std::string& vertex_type_name,
    const std::vector<std::tuple<DataType, std::string, Property>>&
        add_properties,
    bool error_on_conflict) {
  return txn_.AddVertexProperties(vertex_type_name, add_properties,
                                  error_on_conflict);
}

Status StorageTPUpdateInterface::AddEdgeProperties(
    const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::tuple<DataType, std::string, Property>>&
        add_properties,
    bool error_on_conflict) {
  return txn_.AddEdgeProperties(src_type, dst_type, edge_type, add_properties,
                                error_on_conflict);
}

Status StorageTPUpdateInterface::RenameVertexProperties(
    const std::string& vertex_type_name,
    const std::vector<std::pair<std::string, std::string>>& rename_properties,
    bool error_on_conflict) {
  return txn_.RenameVertexProperties(vertex_type_name, rename_properties,
                                     error_on_conflict);
}
Status StorageTPUpdateInterface::RenameEdgeProperties(
    const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::pair<std::string, std::string>>& rename_properties,
    bool error_on_conflict) {
  return txn_.RenameEdgeProperties(src_type, dst_type, edge_type,
                                   rename_properties, error_on_conflict);
}

Status StorageTPUpdateInterface::DeleteVertexProperties(
    const std::string& vertex_type_name,
    const std::vector<std::string>& delete_properties, bool error_on_conflict) {
  return txn_.DeleteVertexProperties(vertex_type_name, delete_properties,
                                     error_on_conflict);
}

Status StorageTPUpdateInterface::DeleteEdgeProperties(
    const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::string>& delete_properties, bool error_on_conflict) {
  return txn_.DeleteEdgeProperties(src_type, dst_type, edge_type,
                                   delete_properties, error_on_conflict);
}

Status StorageTPUpdateInterface::DeleteVertexType(
    const std::string& vertex_type_name, bool error_on_conflict) {
  return txn_.DeleteVertexType(vertex_type_name, error_on_conflict);
}

Status StorageTPUpdateInterface::DeleteEdgeType(const std::string& src_type,
                                                const std::string& dst_type,
                                                const std::string& edge_type,
                                                bool error_on_conflict) {
  return txn_.DeleteEdgeType(src_type, dst_type, edge_type, error_on_conflict);
}

}  // namespace neug
