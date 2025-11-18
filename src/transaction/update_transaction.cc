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
    : insert_vertex_with_resize_(false),
      graph_(graph),
      alloc_(alloc),
      logger_(logger),
      vm_(vm),
      timestamp_(timestamp),
      op_num_(0) {
  arc_.Resize(sizeof(WalHeader));

  vertex_label_num_ = graph_.schema().vertex_label_num();
  edge_label_num_ = graph_.schema().edge_label_num();
  for (label_t idx = 0; idx < vertex_label_num_; ++idx) {
    auto type = graph_.get_vertex_table(idx).get_indexer().get_type();
    if (type == PropertyType::kInt64) {
      added_vertices_.emplace_back(
          std::make_shared<IdIndexer<int64_t, vid_t>>());
    } else if (type == PropertyType::kUInt64) {
      added_vertices_.emplace_back(
          std::make_shared<IdIndexer<uint64_t, vid_t>>());
    } else if (type == PropertyType::kInt32) {
      added_vertices_.emplace_back(
          std::make_shared<IdIndexer<int32_t, vid_t>>());
    } else if (type == PropertyType::kUInt32) {
      added_vertices_.emplace_back(
          std::make_shared<IdIndexer<uint32_t, vid_t>>());
    } else if (type == PropertyType::kStringView) {
      added_vertices_.emplace_back(
          std::make_shared<IdIndexer<std::string_view, vid_t>>());
    } else {
      THROW_NOT_SUPPORTED_EXCEPTION(
          "Only (u)int64/32 and string_view types for pk are supported, but "
          "got: " +
          type.ToString());
    }
  }

  added_vertices_base_.resize(vertex_label_num_);
  vertex_nums_.resize(vertex_label_num_);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    added_vertices_base_[i] = vertex_nums_[i] = graph_.LidNum(i);
  }
  vertex_offsets_.resize(vertex_label_num_);
  extra_vertex_properties_.resize(vertex_label_num_);
  std::string txn_work_dir = update_txn_dir(work_dir, 0);
  if (std::filesystem::exists(txn_work_dir)) {
    remove_directory(txn_work_dir);
  }
  std::filesystem::create_directories(txn_work_dir);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    const Table& table = graph_.get_vertex_table(i).get_properties_table();
    std::string v_label = graph_.schema().get_vertex_label_name(i);
    std::string table_prefix = vertex_table_prefix(v_label);
    extra_vertex_properties_[i].init(table_prefix, txn_work_dir,
                                     table.column_names(), table.column_types(),
                                     {});
    extra_vertex_properties_[i].resize(4096);
  }

  size_t csr_num = 2 * vertex_label_num_ * vertex_label_num_ * edge_label_num_;
  added_edges_.resize(csr_num);
  updated_edge_data_.resize(csr_num);
}

UpdateTransaction::~UpdateTransaction() { release(); }

timestamp_t UpdateTransaction::timestamp() const { return timestamp_; }

void UpdateTransaction::set_insert_vertex_with_resize(
    bool insert_vertex_with_resize) {
  insert_vertex_with_resize_ = insert_vertex_with_resize;
}

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

  applyVerticesUpdates();
  applyEdgesUpdates();
  release();
  return true;
}

void UpdateTransaction::Abort() { release(); }

bool UpdateTransaction::AddVertex(label_t label, const Property& oid,
                                  const std::vector<Property>& props) {
  vid_t vid;
  return AddVertex(label, oid, props, vid);
}

bool UpdateTransaction::AddVertex(label_t label, const Property& oid,
                                  const std::vector<Property>& props,
                                  vid_t& vid) {
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
  Property dup_oid = own_property_memory(oid);

  if (!oid_to_lid(label, dup_oid, vid)) {
    added_vertices_[label]->_add(dup_oid);
    vid = vertex_nums_[label]++;
  }

  std::vector<Property> dup_props;
  for (size_t i = 0; i < props.size(); ++i) {
    dup_props.push_back(own_property_memory(props[i]));
  }

  InsertVertexRedo::Serialize(arc_, label, dup_oid, dup_props);
  op_num_ += 1;
  vid_t row_num = vertex_offsets_[label].size();
  vertex_offsets_[label].emplace(vid, row_num);
  extra_vertex_properties_[label].insert(row_num, dup_props);
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
  static constexpr size_t sentinel = std::numeric_limits<size_t>::max();
  size_t offset_out = sentinel, offset_in = sentinel;
  // TODO(lineng): refactor this part after delete vertex is supported.
  if (src_lid >= vertex_nums_[src_label] ||
      dst_lid >= vertex_nums_[dst_label]) {
    THROW_RUNTIME_ERROR("Source or destination vertex id is out of range");
  }
  // To check whether the src/dst is inserted in this transaction or not.
  auto src_v_num = graph_.LidNum(src_label);
  if (src_lid <= src_v_num &&
      graph_.IsValidLid(src_label, src_lid, timestamp_)) {
    auto view = graph_.GetGenericOutgoingGraphView(src_label, dst_label,
                                                   edge_label, timestamp_);
    auto es = view.get_edges(src_lid);
    offset_out = get_offset(es.begin(), es.end(), dst_lid);
  }
  auto dst_v_num = graph_.LidNum(dst_label);
  if (dst_lid <= dst_v_num &&
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

  InsertEdgeRedo::Serialize(arc_, src_label, lid_to_oid(src_label, src_lid),
                            dst_label, lid_to_oid(dst_label, dst_lid),
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
  vid_t src_lid, dst_lid;
  if (graph_.get_lid(src_label, src, src_lid, timestamp_) &&
      graph_.get_lid(dst_label, dst, dst_lid, timestamp_)) {
  } else {
    if (!oid_to_lid(src_label, src, src_lid) ||
        !oid_to_lid(dst_label, dst, dst_lid)) {
      return false;
    }
  }
  return this->AddEdge(src_label, src_lid, dst_label, dst_lid, edge_label,
                       properties);
}

UpdateTransaction::vertex_iterator::vertex_iterator(label_t label, vid_t cur,
                                                    vid_t& num, timestamp_t ts,

                                                    UpdateTransaction* txn)
    : label_(label), cur_(cur), num_(num), txn_(txn) {
  while (cur_ < num_ && !txn_->IsValidLid(label_, cur_)) {
    ++cur_;
  }
}
UpdateTransaction::vertex_iterator::~vertex_iterator() = default;
bool UpdateTransaction::vertex_iterator::IsValid() const { return cur_ < num_; }
void UpdateTransaction::vertex_iterator::Next() {
  while (++cur_ < num_ && !txn_->IsValidLid(label_, cur_)) {}
}
void UpdateTransaction::vertex_iterator::Goto(vid_t target) {
  if (std::min(target, num_) < num_ && !txn_->IsValidLid(label_, target)) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Target vertex is not valid");
  }
  cur_ = std::min(target, num_);
}

Property UpdateTransaction::vertex_iterator::GetId() const {
  return txn_->lid_to_oid(label_, cur_);
}

vid_t UpdateTransaction::vertex_iterator::GetIndex() const { return cur_; }

Property UpdateTransaction::vertex_iterator::GetField(int col_id) const {
  return txn_->GetVertexProperty(label_, cur_, col_id);
}

bool UpdateTransaction::vertex_iterator::SetField(int col_id,
                                                  const Property& value) {
  return txn_->UpdateVertexProperty(label_, cur_, col_id, value);
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

UpdateTransaction::vertex_iterator UpdateTransaction::GetVertexIterator(
    label_t label) {
  return {label, 0, vertex_nums_[label], timestamp_, this};
}

vid_t UpdateTransaction::GetVertexNum(label_t label) const {
  return vertex_nums_[label];
}

UpdateTransaction::edge_iterator UpdateTransaction::GetOutEdgeIterator(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label,
    int prop_id) {
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
  auto& vertex_offset = vertex_offsets_[label];
  auto iter = vertex_offset.find(lid);
  if (iter == vertex_offset.end()) {
    return graph_.get_vertex_table(label)
        .get_properties_table()
        .get_column_by_id(col_id)
        ->get_prop(lid);
  } else {
    return extra_vertex_properties_[label].get_column_by_id(col_id)->get_prop(
        iter->second);
  }
}

Property UpdateTransaction::GetVertexId(label_t label, vid_t lid) const {
  return lid_to_oid(label, lid);
}

bool UpdateTransaction::GetVertexIndex(label_t label, const Property& id,
                                       vid_t& index) const {
  return oid_to_lid(label, id, index);
}

bool UpdateTransaction::UpdateVertexProperty(label_t label, vid_t lid,
                                             int col_id,
                                             const Property& value) {
  auto& vertex_offset = vertex_offsets_[label];
  auto iter = vertex_offset.find(lid);
  auto& extra_table = extra_vertex_properties_[label];
  std::vector<PropertyType> types =
      graph_.schema().get_vertex_properties(label);
  if (static_cast<size_t>(col_id) >= types.size()) {
    return false;
  }
  if (types[col_id] != value.type()) {
    return false;
  }
  Property dup_value = own_property_memory(value);
  if (iter == vertex_offset.end()) {
    auto& table = graph_.get_vertex_table(label).get_properties_table();
    if (table.col_num() <= static_cast<size_t>(col_id)) {
      return false;
    }
    if (graph_.LidNum(label) <= lid) {
      return false;
    }
    vid_t new_offset = vertex_offset.size();
    vertex_offset.emplace(lid, new_offset);
    size_t col_num = table.col_num();
    for (size_t i = 0; i < col_num; ++i) {
      extra_table.get_column_by_id(i)->set_any(
          new_offset, table.get_column_by_id(i)->get_prop(lid));
    }
    extra_table.get_column_by_id(col_id)->set_any(new_offset, dup_value);
  } else {
    if (extra_table.col_num() <= static_cast<size_t>(col_id)) {
      return false;
    }
    extra_table.get_column_by_id(col_id)->set_any(iter->second, dup_value);
  }
  UpdateVertexPropRedo::Serialize(arc_, label, lid_to_oid(label, lid), col_id,
                                  dup_value);
  op_num_ += 1;
  return true;
}

void UpdateTransaction::SetEdgeData(bool dir, label_t label, vid_t v,
                                    label_t neighbor_label, vid_t nbr,
                                    label_t edge_label, const Property& value,
                                    int32_t col_id) {
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

  UpdateEdgePropRedo::Serialize(arc_, dir, label, lid_to_oid(label, v),
                                neighbor_label, lid_to_oid(neighbor_label, nbr),
                                edge_label, col_id, dup_value);
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
    if (op_type == OpType::kInsertVertex) {
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
          .get_properties_table()
          .get_column_by_id(redo.prop_id)
          ->set_any(vid, redo.value);
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

bool UpdateTransaction::oid_to_lid(label_t label, const Property& oid,
                                   vid_t& lid) const {
  if (graph_.get_lid(label, oid, lid, timestamp_)) {
    return true;
  } else {
    if (added_vertices_[label]->get_index(oid, lid)) {
      lid += added_vertices_base_[label];
      return true;
    }
  }
  return false;
}

bool UpdateTransaction::HasVertex(label_t label, const Property& oid) const {
  vid_t lid;
  if (graph_.get_lid(label, oid, lid, timestamp_)) {
    return true;
  } else {
    return added_vertices_[label]->get_index(oid, lid);
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

Property UpdateTransaction::lid_to_oid(label_t label, vid_t lid) const {
  if (graph_.LidNum(label) > lid) {
    return graph_.GetOid(label, lid, timestamp_);
  } else {
    Property ret;
    CHECK(added_vertices_[label]->get_key(lid - added_vertices_base_[label],
                                          ret));
    return ret;
  }
}

bool UpdateTransaction::IsValidLid(label_t label, vid_t lid) const {
  if (graph_.LidNum(label) > lid) {
    return graph_.IsValidLid(label, lid, timestamp_);
  } else {
    return lid - added_vertices_base_[label] < added_vertices_[label]->size();
  }
}

void UpdateTransaction::release() {
  if (timestamp_ != INVALID_TIMESTAMP) {
    arc_.Clear();
    vm_.release_update_timestamp(timestamp_);
    timestamp_ = INVALID_TIMESTAMP;

    op_num_ = 0;

    added_vertices_.clear();
    added_vertices_base_.clear();
    vertex_offsets_.clear();
    extra_vertex_properties_.clear();
    added_edges_.clear();
    updated_edge_data_.clear();
  }
}

void UpdateTransaction::applyVerticesUpdates() {
  for (label_t label = 0; label < vertex_label_num_; ++label) {
    std::vector<std::pair<vid_t, Property>> added_vertices;
    vid_t added_vertices_num = added_vertices_[label]->size();
    for (vid_t v = 0; v < added_vertices_num; ++v) {
      vid_t lid = v + added_vertices_base_[label];
      Property oid;
      CHECK(added_vertices_[label]->get_key(v, oid));
      added_vertices.emplace_back(lid, oid);
    }
    std::sort(added_vertices.begin(), added_vertices.end(),
              [](const std::pair<vid_t, Property>& lhs,
                 const std::pair<vid_t, Property>& rhs) {
                return lhs.first < rhs.first;
              });

    auto& graph_vertex_table = graph_.get_vertex_table(label);
    if (graph_vertex_table.Capacity() < vertex_nums_[label]) {
      graph_.Reserve(label, vertex_nums_[label]);
    }

    auto& table = extra_vertex_properties_[label];
    auto& vertex_offset = vertex_offsets_[label];
    for (auto& pair : added_vertices) {
      vid_t offset = vertex_offset.at(pair.first);
      vid_t lid;
      if (!graph_.get_vertex_table(label).AddVertex(
              pair.second, table.get_row(offset), lid, timestamp_)) {
        THROW_STORAGE_EXCEPTION(
            "Failed to add vertex during applying vertex updates.");
      }
      CHECK_EQ(lid, pair.first);
      vertex_offset.erase(pair.first);
    }

    for (auto& pair : vertex_offset) {
      vid_t lid = pair.first;
      vid_t offset = pair.second;
      auto vals = table.get_row(offset);
      if (insert_vertex_with_resize_) {
        graph_.get_vertex_table(label)
            .get_properties_table()
            .insert_with_resize(lid, table.get_row(offset));
      } else {
        graph_.get_vertex_table(label).get_properties_table().insert(
            lid, table.get_row(offset));
      }
    }

    CHECK_EQ(graph_.LidNum(label), vertex_nums_[label]);
  }

  added_vertices_.clear();
  vertex_nums_.clear();
  vertex_offsets_.clear();
  extra_vertex_properties_.clear();
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
