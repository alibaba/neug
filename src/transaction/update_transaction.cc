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
#include "libgrape-lite/grape/serialization/in_archive.h"
#include "libgrape-lite/grape/serialization/out_archive.h"
#include "neug/storages/csr/csr_base.h"
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

namespace gs {

UpdateTransaction::UpdateTransaction(PropertyGraph& graph, Allocator& alloc,
                                     const std::string& work_dir,
                                     IWalWriter& logger, IVersionManager& vm,
                                     timestamp_t timestamp)
    : graph_(graph),
      alloc_(alloc),
      logger_(logger),
      vm_(vm),
      timestamp_(timestamp),
      op_num_(0) {
  arc_.Resize(sizeof(WalHeader));

  vertex_label_num_ = graph_.schema().vertex_label_num();
  edge_label_num_ = graph_.schema().edge_label_num();

  std::string txn_work_dir = update_txn_dir(work_dir, 0);
  if (std::filesystem::exists(txn_work_dir)) {
    remove_directory(txn_work_dir);
  }
  std::filesystem::create_directories(txn_work_dir);

  added_vertices_base_.resize(vertex_label_num_);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    added_vertices_base_[i] = graph_.LidNum(i);
  }

  size_t csr_num = 2 * vertex_label_num_ * vertex_label_num_ * edge_label_num_;
  added_edges_.resize(csr_num);
  updated_edge_data_.resize(csr_num);
}

UpdateTransaction::~UpdateTransaction() { release(); }

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

  applyEdgesUpdates();
  // Should delete edge types before vertex types
  applyEdgeTypeDeletions();
  applyVertexTypeDeletions();
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

bool UpdateTransaction::CreateVertexType(
    const std::string& name,
    const std::vector<std::tuple<PropertyType, std::string, Property>>&
        properties,
    const std::vector<std::string>& primary_key_names, bool error_on_conflict) {
  if (graph_.schema().contains_vertex_label(name)) {
    LOG(ERROR) << "Vertex type " << name << " already exists.";
    return !error_on_conflict;
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
    return false;
  }
  return true;
}

bool UpdateTransaction::CreateEdgeType(
    const std::string& src_type, const std::string& dst_type,
    const std::string& edge_type,
    const std::vector<std::tuple<PropertyType, std::string, Property>>&
        properties,
    bool error_on_conflict, EdgeStrategy oe_edge_strategy,
    EdgeStrategy ie_edge_strategy) {
  if (graph_.schema().exist(src_type, dst_type, edge_type)) {
    LOG(ERROR) << "Edge type " << edge_type << " already exists between "
               << src_type << " and " << dst_type << ".";
    return !error_on_conflict;
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
    return false;
  }
  return true;
}

bool UpdateTransaction::DeleteVertexType(const std::string& vertex_type_name,
                                         bool error_on_conflict) {
  if (!graph_.schema().contains_vertex_label(vertex_type_name)) {
    LOG(ERROR) << "Vertex type " << vertex_type_name << " does not exist.";
    return !error_on_conflict;
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
    return !error_on_conflict;
  }
  graph_.mutable_schema().DeleteVertexLabel(vertex_type_name, true);
  // Mark the vertex table as deleted in this transaction
  deleted_vertex_labels_.emplace(v_label);
  for (label_t dst_label = 0; dst_label < vertex_label_num_; ++dst_label) {
    for (label_t edge_label = 0; edge_label < edge_label_num_; ++edge_label) {
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
  return true;
}

bool UpdateTransaction::DeleteEdgeType(const std::string& src_type,
                                       const std::string& dst_type,
                                       const std::string& edge_type,
                                       bool error_on_conflict) {
  if (!graph_.schema().exist(src_type, dst_type, edge_type)) {
    LOG(ERROR) << "Edge type " << edge_type << " does not exist between "
               << src_type << " and " << dst_type << ".";
    return !error_on_conflict;
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
    return !error_on_conflict;
  }
  graph_.mutable_schema().DeleteEdgeLabel(src_label_id, dst_label_id,
                                          edge_label_id, true);
  // Mark the edge table as deleted in this transaction
  deleted_edge_labels_.emplace(
      std::make_tuple(src_label_id, dst_label_id, edge_label_id));
  return true;
}

bool UpdateTransaction::AddVertex(label_t label, const Property& oid,
                                  const std::vector<Property>& props) {
  ENSURE_VERTEX_LABEL_NOT_DELETED(label);
  vid_t vid;
  return AddVertex(label, oid, props, vid);
}

bool UpdateTransaction::AddVertex(label_t label, const Property& oid,
                                  const std::vector<Property>& props,
                                  vid_t& vid) {
  ENSURE_VERTEX_LABEL_NOT_DELETED(label);
  std::vector<PropertyType> types =
      graph_.schema().get_vertex_properties(label);
  if (types.size() != props.size()) {
    return false;
  }
  int col_num = types.size();
  for (int col_i = 0; col_i != col_num; ++col_i) {
    if (props[col_i].type() != types[col_i]) {
      if (props[col_i].type().type_enum ==
          impl::PropertyTypeImpl::kStringView) {
        continue;
      }
      return false;
    }
  }

  InsertVertexRedo::Serialize(arc_, label, oid, props);
  op_num_ += 1;
  auto status = graph_.AddVertex(label, oid, props, vid, timestamp_);
  if (!status.ok()) {
    // The most possible reason is that the space is not enough.
    graph_.Reserve(label, std::max((vid_t) 1024, graph_.LidNum(label) * 2));
    status = graph_.AddVertex(label, oid, props, vid, timestamp_);
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
  undo_logs_.push(std::make_unique<RemoveVertexUndo>(label, lid));
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

static size_t get_offset(NbrIterator base, NbrIterator end, vid_t target) {
  size_t offset = 0;
  while (base != end) {
    if (base.get_vertex() == target) {
      return offset;
    }
    offset++;
    ++base;
  }
  return std::numeric_limits<size_t>::max();
}

// Currently multiple edges between two vertices are not supported, could not be
// added
bool UpdateTransaction::AddEdge(label_t src_label, vid_t src_lid,
                                label_t dst_label, vid_t dst_lid,
                                label_t edge_label,
                                const std::vector<Property>& properties) {
  ENSURE_EDGE_LABEL_NOT_DELETED(src_label, dst_label, edge_label);
  static constexpr size_t sentinel = std::numeric_limits<size_t>::max();
  size_t offset_out = sentinel, offset_in = sentinel;

  if (!graph_.IsValidLid(src_label, src_lid, timestamp_) ||
      !graph_.IsValidLid(dst_label, dst_lid, timestamp_)) {
    THROW_RUNTIME_ERROR("Source or destination vertex id is out of range");
  }
  // To check whether the src/dst is inserted in this transaction or not.
  auto src_v_num = added_vertices_base_[src_label];
  if (src_lid < src_v_num &&
      graph_.IsValidLid(src_label, src_lid, timestamp_)) {
    auto view = graph_.GetGenericOutgoingGraphView(src_label, dst_label,
                                                   edge_label, timestamp_);
    auto es = view.get_edges(src_lid);
    offset_out = get_offset(es.begin(), es.end(), dst_lid);
  }
  auto dst_v_num = added_vertices_base_[dst_label];
  if (dst_lid < dst_v_num &&
      graph_.IsValidLid(dst_label, dst_lid, timestamp_)) {
    auto view = graph_.GetGenericIncomingGraphView(dst_label, src_label,
                                                   edge_label, timestamp_);
    auto es = view.get_edges(dst_lid);
    offset_in = get_offset(es.begin(), es.end(), src_lid);
  }
  auto types =
      graph_.schema().get_edge_properties(src_label, dst_label, edge_label);
  if (types.size() != properties.size()) {
    LOG(ERROR) << "Edge property size not match, expected " << types.size()
               << ", got " << properties.size();
    return false;
  }
  std::vector<Property> dup_properties;
  for (size_t i = 0; i < properties.size(); ++i) {
    if (properties[i].type() != types[i]) {
      std::string label_name = graph_.schema().get_edge_label_name(edge_label);
      LOG(ERROR) << "Edge property " << label_name
                 << " type not match, expected " << types[i] << ", got "
                 << properties[i].type().ToString();
      return false;
    }
    dup_properties.push_back(own_property_memory(properties[i]));
  }

  size_t in_csr_index = get_in_csr_index(src_label, dst_label, edge_label);
  size_t out_csr_index = get_out_csr_index(src_label, dst_label, edge_label);
  if (offset_in == sentinel) {
    added_edges_[in_csr_index][dst_lid].push_back(src_lid);
  }
  if (offset_out == sentinel) {
    added_edges_[out_csr_index][src_lid].push_back(dst_lid);
  }

  InsertEdgeRedo::Serialize(arc_, src_label, GetVertexId(src_label, src_lid),
                            dst_label, GetVertexId(dst_label, dst_lid),
                            edge_label, dup_properties);
  op_num_ += 1;

  if (!updated_edge_data_[in_csr_index][dst_lid].count(src_lid)) {
    updated_edge_data_[in_csr_index][dst_lid].emplace(
        src_lid, std::vector<std::tuple<Property, int32_t, size_t>>{});
  }
  if (!updated_edge_data_[in_csr_index][src_lid].count(dst_lid)) {
    updated_edge_data_[in_csr_index][src_lid].emplace(
        dst_lid, std::vector<std::tuple<Property, int32_t, size_t>>{});
  }
  for (size_t col_id = 0; col_id < dup_properties.size(); ++col_id) {
    updated_edge_data_[in_csr_index][dst_lid][src_lid].emplace_back(
        std::tuple<Property, int32_t, size_t>{
            dup_properties[col_id], static_cast<int32_t>(col_id), offset_in});
    updated_edge_data_[out_csr_index][src_lid][dst_lid].emplace_back(
        std::tuple<Property, int32_t, size_t>{
            dup_properties[col_id], static_cast<int32_t>(col_id), offset_out});
  }
  if (dup_properties.size() == 0) {
    updated_edge_data_[in_csr_index][dst_lid][src_lid].emplace_back(
        std::tuple<Property, int32_t, size_t>{Property::empty(), 0, offset_in});
    updated_edge_data_[out_csr_index][src_lid][dst_lid].emplace_back(
        std::tuple<Property, int32_t, size_t>{Property::empty(), 0,
                                              offset_out});
  }

  return true;
}

bool UpdateTransaction::AddEdge(label_t src_label, const Property& src,
                                label_t dst_label, const Property& dst,
                                label_t edge_label,
                                const std::vector<Property>& properties) {
  ENSURE_EDGE_LABEL_NOT_DELETED(src_label, dst_label, edge_label);
  vid_t src_lid, dst_lid;
  if (!(graph_.get_lid(src_label, src, src_lid, timestamp_) &&
        graph_.get_lid(dst_label, dst, dst_lid, timestamp_))) {
    return false;
  }
  return this->AddEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                       properties);
}

UpdateTransaction::edge_iterator::edge_iterator(
    bool dir, label_t label, vid_t v, label_t neighbor_label,
    label_t edge_label, const vid_t* aeb, const vid_t* aee,
    const NbrList& nbr_list, const EdgeDataAccessor& ed_accessor,
    int32_t prop_id, UpdateTransaction* txn)
    : dir_(dir),
      label_(label),
      v_(v),
      neighbor_label_(neighbor_label),
      edge_label_(edge_label),
      added_edges_cur_(aeb),
      added_edges_end_(aee),
      init_iter_(nbr_list.begin()),
      init_iter_end_(nbr_list.end()),
      ed_accessor_(ed_accessor),
      prop_id_(prop_id),
      txn_(txn),
      offset_(0) {}
UpdateTransaction::edge_iterator::~edge_iterator() = default;

Property UpdateTransaction::edge_iterator::GetData() const {
  if (init_iter_ != init_iter_end_) {
    vid_t cur = init_iter_.get_vertex();
    Property ret;
    if (txn_->GetUpdatedEdgeData(dir_, label_, v_, neighbor_label_, cur,
                                 edge_label_, prop_id_, ret)) {
      return ret;
    } else {
      return ed_accessor_.get_data(init_iter_);
    }
  } else {
    vid_t cur = *added_edges_cur_;
    Property ret;
    CHECK(txn_->GetUpdatedEdgeData(dir_, label_, v_, neighbor_label_, cur,
                                   edge_label_, prop_id_, ret));
    return ret;
  }
}

void UpdateTransaction::edge_iterator::SetData(const Property& value) {
  if (init_iter_ != init_iter_end_) {
    vid_t cur = init_iter_.get_vertex();
    txn_->set_edge_data_with_offset(dir_, label_, v_, neighbor_label_, cur,
                                    edge_label_, value, offset_);
  } else {
    vid_t cur = *added_edges_cur_;
    txn_->set_edge_data_with_offset(dir_, label_, v_, neighbor_label_, cur,
                                    edge_label_, value,
                                    std::numeric_limits<size_t>::max());
  }
}

bool UpdateTransaction::edge_iterator::IsValid() const {
  return (init_iter_ != init_iter_end_) ||
         (added_edges_cur_ != added_edges_end_);
}

void UpdateTransaction::edge_iterator::Next() {
  if (init_iter_ != init_iter_end_) {
    ++init_iter_;
    if (init_iter_ != init_iter_end_) {
      ++offset_;
    } else {
      offset_ = std::numeric_limits<size_t>::max();
    }
  } else {
    offset_ = std::numeric_limits<size_t>::max();
    ++added_edges_cur_;
  }
}

void UpdateTransaction::edge_iterator::Forward(size_t offset) {
  for (size_t i = 0; i < offset; ++i) {
    Next();
    if (!IsValid()) {
      break;
    }
  }
}

vid_t UpdateTransaction::edge_iterator::GetNeighbor() const {
  if (init_iter_ != init_iter_end_) {
    return init_iter_.get_vertex();
  } else {
    return *added_edges_cur_;
  }
}

label_t UpdateTransaction::edge_iterator::GetNeighborLabel() const {
  return neighbor_label_;
}

label_t UpdateTransaction::edge_iterator::GetEdgeLabel() const {
  return edge_label_;
}

UpdateTransaction::edge_iterator UpdateTransaction::GetOutEdgeIterator(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label,
    int prop_id) {
  ENSURE_EDGE_LABEL_NOT_DELETED(label, neighbor_label, edge_label);
  size_t csr_index = get_out_csr_index(label, neighbor_label, edge_label);
  const vid_t* begin = nullptr;
  const vid_t* end = nullptr;
  auto iter = added_edges_[csr_index].find(u);
  if (iter != added_edges_[csr_index].end()) {
    begin = iter->second.data();
    end = begin + iter->second.size();
  }
  auto view = graph_.GetGenericOutgoingGraphView(label, neighbor_label,
                                                 edge_label, timestamp_);
  auto ed_accessor =
      GetEdgeDataAccessor(label, neighbor_label, edge_label, prop_id);
  return {true,        label,   u,   neighbor_label,
          edge_label,  begin,   end, view.get_edges(u),
          ed_accessor, prop_id, this};
}

UpdateTransaction::edge_iterator UpdateTransaction::GetInEdgeIterator(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label,
    int prop_id) {
  ENSURE_EDGE_LABEL_NOT_DELETED(neighbor_label, label, edge_label);
  size_t csr_index = get_in_csr_index(label, neighbor_label, edge_label);
  const vid_t* begin = nullptr;
  const vid_t* end = nullptr;
  auto iter = added_edges_[csr_index].find(u);
  if (iter != added_edges_[csr_index].end()) {
    begin = iter->second.data();
    end = begin + iter->second.size();
  }
  auto view = graph_.GetGenericIncomingGraphView(label, neighbor_label,
                                                 edge_label, timestamp_);
  auto ed_accessor =
      GetEdgeDataAccessor(neighbor_label, label, edge_label, prop_id);
  return {false,       label,   u,   neighbor_label,
          edge_label,  begin,   end, view.get_edges(u),
          ed_accessor, prop_id, this};
}

Property UpdateTransaction::GetVertexProperty(label_t label, vid_t lid,
                                              int col_id) const {
  ENSURE_VERTEX_LABEL_NOT_DELETED(label);
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
  if (!graph_.IsValidLid(label, lid, timestamp_)) {
    LOG(ERROR) << "Vertex lid " << lid << " of label "
               << graph_.schema().get_vertex_label_name(label)
               << " is not valid in this transaction.";
    return false;
  }
  std::vector<PropertyType> types =
      graph_.schema().get_vertex_properties(label);
  if (static_cast<size_t>(col_id) >= types.size()) {
    return false;
  }
  if (types[col_id] != value.type()) {
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

void UpdateTransaction::SetEdgeData(bool dir, label_t label, vid_t v,
                                    label_t neighbor_label, vid_t nbr,
                                    label_t edge_label, const Property& value,
                                    int32_t col_id) {
  ENSURE_EDGE_LABEL_NOT_DELETED(dir ? label : neighbor_label,
                                dir ? neighbor_label : label, edge_label);
  auto view = dir ? graph_.GetGenericOutgoingGraphView(label, neighbor_label,
                                                       edge_label, timestamp_)
                  : graph_.GetGenericIncomingGraphView(label, neighbor_label,
                                                       edge_label, timestamp_);
  auto es = view.get_edges(v);
  size_t offset = get_offset(es.begin(), es.end(), nbr);
  set_edge_data_with_offset(dir, label, v, neighbor_label, nbr, edge_label,
                            value, offset, col_id);
}

void UpdateTransaction::set_edge_data_with_offset(
    bool dir, label_t label, vid_t v, label_t neighbor_label, vid_t nbr,
    label_t edge_label, const Property& value, size_t offset, int32_t col_id) {
  size_t csr_index = dir ? get_out_csr_index(label, neighbor_label, edge_label)
                         : get_in_csr_index(neighbor_label, label, edge_label);
  auto edge_prop_types = dir ? graph_.schema().get_edge_properties(
                                   label, neighbor_label, edge_label)
                             : graph_.schema().get_edge_properties(
                                   neighbor_label, label, edge_label);
  if (col_id >= 0 && static_cast<size_t>(col_id) >= edge_prop_types.size()) {
    THROW_RUNTIME_ERROR("Column id out of range for edge properties");
  }
  if (col_id >= 0 && value.type() != edge_prop_types[col_id]) {
    THROW_RUNTIME_ERROR("Edge property type does not match the schema");
  }
  Property dup_value = own_property_memory(value);
  if (!updated_edge_data_[csr_index].count(v)) {
    updated_edge_data_[csr_index].emplace(
        v, ska::flat_hash_map<
               vid_t, std::vector<std::tuple<Property, int32_t, size_t>>>{});
  }
  if (!updated_edge_data_[csr_index][v].count(nbr)) {
    updated_edge_data_[csr_index][v].emplace(
        nbr, std::vector<std::tuple<Property, int32_t, size_t>>{});
  }
  updated_edge_data_[csr_index][v][nbr].emplace_back(
      std::tuple<Property, int32_t, size_t>{dup_value, col_id, offset});

  UpdateEdgePropRedo::Serialize(
      arc_, dir, label, GetVertexId(label, v), neighbor_label,
      GetVertexId(neighbor_label, nbr), edge_label, col_id, dup_value);
  op_num_ += 1;

  // Modify the edge data in place, since in this transaction the updated edge
  // data should be visible right now.
  auto view = dir ? graph_.GetGenericOutgoingGraphView(label, neighbor_label,
                                                       edge_label, timestamp_)
                  : graph_.GetGenericIncomingGraphView(label, neighbor_label,
                                                       edge_label, timestamp_);
  auto es = view.get_edges(v);
  auto ed_accessor =
      GetEdgeDataAccessor(dir ? label : neighbor_label,
                          dir ? neighbor_label : label, edge_label, col_id);
  if (offset != std::numeric_limits<size_t>::max()) {
    auto it = es.begin();
    for (size_t i = 0; i < offset; ++i) {
      ++it;
    }
    ed_accessor.set_data(it, dup_value, timestamp_);
  }
}

bool UpdateTransaction::GetUpdatedEdgeData(bool dir, label_t label, vid_t v,
                                           label_t neighbor_label, vid_t nbr,
                                           label_t edge_label, int32_t prop_id,
                                           Property& ret) const {
  ENSURE_EDGE_LABEL_NOT_DELETED(dir ? label : neighbor_label,
                                dir ? neighbor_label : label, edge_label);
  size_t csr_index = dir ? get_out_csr_index(label, neighbor_label, edge_label)
                         : get_in_csr_index(label, neighbor_label, edge_label);
  auto map_iter = updated_edge_data_[csr_index].find(v);
  if (map_iter == updated_edge_data_[csr_index].end()) {
    return false;
  } else {
    auto& updates = map_iter->second;
    auto iter = updates.find(nbr);
    if (iter == updates.end()) {
      return false;
    } else {
      const auto& vec = iter->second;
      for (const auto& tup : vec) {
        if (std::get<1>(tup) == prop_id) {
          ret = std::get<0>(tup);
          return true;
        }
      }
    }
    return false;
  }
}

void UpdateTransaction::IngestWal(PropertyGraph& graph,
                                  const std::string& work_dir,
                                  uint32_t timestamp, char* data, size_t length,
                                  Allocator& alloc) {
  grape::OutArchive arc;
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
        auto ret =
            graph.AddVertex(redo.label, redo.oid, redo.props, vid, timestamp);
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
                    redo.edge_label, redo.properties, timestamp, alloc);
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
      auto edge_prop_types = graph.schema().get_edge_properties(
          redo.dir == 0 ? redo.src_label : redo.dst_label,
          redo.dir == 0 ? redo.dst_label : redo.src_label, redo.edge_label);
      vid_t v_lid, nbr_lid;
      CHECK(graph.get_lid(redo.src_label, redo.src, v_lid, timestamp));
      CHECK(graph.get_lid(redo.dst_label, redo.dst, nbr_lid, timestamp));

      auto view =
          redo.dir
              ? graph.GetGenericOutgoingGraphView(
                    redo.src_label, redo.dst_label, redo.edge_label, timestamp)
              : graph.GetGenericIncomingGraphView(
                    redo.dst_label, redo.src_label, redo.edge_label, timestamp);
      auto es = view.get_edges(v_lid);
      auto it = es.begin();
      while (it != es.end()) {
        if (it.get_vertex() == nbr_lid) {
          if (redo.prop_id >= 0 &&
              static_cast<size_t>(redo.prop_id) >= edge_prop_types.size()) {
            THROW_RUNTIME_ERROR("Column id out of range for edge properties");
          }
          if (redo.prop_id >= 0 &&
              redo.value.type() != edge_prop_types[redo.prop_id]) {
            THROW_RUNTIME_ERROR("Edge property type does not match the schema");
          }
          auto ed_accessor = graph.GetEdgeDataAccessor(
              redo.dir ? redo.src_label : redo.dst_label,
              redo.dir ? redo.dst_label : redo.src_label, redo.edge_label,
              redo.prop_id);
          ed_accessor.set_data(it, redo.value, timestamp);
          break;
        }
        ++it;
      }
    } else if (op_type == OpType::kRemoveVertex) {
      RemoveVertexRedo redo;
      arc >> redo;
      vid_t vid;
      CHECK(graph.get_lid(redo.label, redo.oid, vid, timestamp));
      graph.DeleteVertex(redo.label, vid, timestamp);
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

size_t UpdateTransaction::get_in_csr_index(label_t src_label, label_t dst_label,
                                           label_t edge_label) const {
  return src_label * vertex_label_num_ * edge_label_num_ +
         dst_label * edge_label_num_ + edge_label;
}

size_t UpdateTransaction::get_out_csr_index(label_t src_label,
                                            label_t dst_label,
                                            label_t edge_label) const {
  return src_label * vertex_label_num_ * edge_label_num_ +
         dst_label * edge_label_num_ + edge_label +
         vertex_label_num_ * vertex_label_num_ * edge_label_num_;
}

VertexSet UpdateTransaction::GetVertexSet(label_t label) const {
  return graph_.GetVertexSet(label, timestamp_);
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

    added_edges_.clear();
    updated_edge_data_.clear();
    sv_vec_.clear();
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

void UpdateTransaction::applyEdgesUpdates() {
  for (label_t src_label = 0; src_label < vertex_label_num_; ++src_label) {
    for (label_t dst_label = 0; dst_label < vertex_label_num_; ++dst_label) {
      for (label_t edge_label = 0; edge_label < edge_label_num_; ++edge_label) {
        size_t oe_csr_index =
            get_out_csr_index(src_label, dst_label, edge_label);

        for (auto& pair : added_edges_[oe_csr_index]) {
          vid_t v = pair.first;
          auto& add_list = pair.second;

          if (add_list.empty()) {
            continue;
          }
          std::sort(add_list.begin(), add_list.end());
          auto& edge_data = updated_edge_data_[oe_csr_index];
          for (size_t idx = 0; idx < add_list.size(); ++idx) {
            // TODO(zhanglei): multiple edges are ignored here.
            if (idx && add_list[idx] == add_list[idx - 1])
              continue;
            auto u = add_list[idx];
            if (edge_data.count(v) && edge_data[v].count(u)) {
              auto& tuples = edge_data[v].at(u);
              std::vector<Property> props;
              props.resize(tuples.size());
              for (size_t i = 0; i < tuples.size(); ++i) {
                assert(props[std::get<1>(tuples[i])] == Property::empty());
                props[std::get<1>(tuples[i])] = std::get<0>(tuples[i]);
              }
              graph_.AddEdge(src_label, v, dst_label, u, edge_label, props,
                             timestamp_, alloc_);
            }
          }
        }
      }
    }
  }

  added_edges_.clear();
  updated_edge_data_.clear();
}

Property UpdateTransaction::own_property_memory(const Property& prop) {
  if (prop.type().type_enum == impl::PropertyTypeImpl::kString) {
    sv_vec_.emplace_back(prop.as_string_view());
    return Property::from_string_view(
        std::string_view(sv_vec_.back().data(), sv_vec_.back().size()));
  }
  return prop;
}

}  // namespace gs
