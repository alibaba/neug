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

#include "neug/storages/graph/graph_view.h"

#include "neug/storages/csr/csr_base.h"
#include "neug/storages/graph/property_graph.h"
#include "neug/utils/likely.h"

namespace neug {

// ── TableView ──

TableView::TableView(const Table& table) : col_id_map_(&table.col_id_map_) {
  columns_.reserve(table.col_num());
  for (int i = 0; i < table.col_num(); i++) {
    columns_.push_back(const_cast<ColumnBase*>(table.get_column_by_id(i)));
  }
}

std::shared_ptr<RefColumnBase> TableView::get_column(int col_id) const {
  if (col_id < 0 || col_id >= columns_.size()) {
    return nullptr;
  }
  if (columns_[col_id]) {
    return CreateRefColumn(*columns_[col_id]);
  }
  return nullptr;
}

std::shared_ptr<RefColumnBase> TableView::get_column(
    const std::string& name) const {
  auto it = col_id_map_->find(name);
  if (it == col_id_map_->end()) {
    return nullptr;
  }
  int col_id = it->second;
  return get_column(col_id);
}

ColumnBase* TableView::get_raw_column(int col_id) const {
  assert(col_id >= 0 && col_id < columns_.size());
  return columns_[col_id];
}

void TableView::insert(size_t index,
                       const std::vector<execution::Value>& values,
                       bool insert_safe) {
  assert(!insert_safe);
  assert(values.size() == columns_.size());
  for (size_t i = 0; i < values.size(); i++) {
    columns_[i]->set_any(index, values[i], false);
  }
}

// ── VertexTableView ──

VertexTableView::VertexTableView(VertexTable& table)
    : pk_name_(std::get<1>(table.get_vertex_schema_ptr()->primary_keys[0])),
      indexer_(&table.get_indexer()),
      v_ts_(table.v_ts_.get()),
      view_(*table.table_) {}

bool VertexTableView::get_lid(const execution::Value& oid, vid_t& lid,
                              timestamp_t ts) const {
  auto res = indexer_->get_index(oid, lid);
  if (NEUG_UNLIKELY(res && !v_ts_->IsVertexValid(lid, ts))) {
    return false;
  }
  return res;
}

vid_t VertexTableView::LidNum() const { return indexer_->size(); }

bool VertexTableView::IsValidLid(vid_t lid, timestamp_t ts) const {
  return lid < indexer_->size() && v_ts_->IsVertexValid(lid, ts);
}

execution::Value VertexTableView::GetOid(vid_t lid, timestamp_t ts) const {
  if (NEUG_UNLIKELY(lid >= indexer_->size())) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Lid " + std::to_string(lid) +
                                     " is out of range.");
  }
  if (NEUG_UNLIKELY(!v_ts_->IsVertexValid(lid, ts))) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Lid " + std::to_string(lid) +
                                     " has been deleted.");
  }
  return indexer_->get_key(lid);
}

VertexSet VertexTableView::GetVertexSet(timestamp_t ts) const {
  return VertexSet(indexer_->size(), *v_ts_, ts);
}

std::shared_ptr<RefColumnBase> VertexTableView::GetPropertyColumn(
    int col_id) const {
  return view_.get_column(col_id);
}

std::shared_ptr<RefColumnBase> VertexTableView::GetPropertyColumn(
    const std::string& prop) const {
  if (prop == pk_name_) {
    return CreateRefColumn(indexer_->get_keys());
  }
  return view_.get_column(prop);
}

bool VertexTableView::AddVertex(const execution::Value& id,
                                const std::vector<execution::Value>& props,
                                vid_t& ret, timestamp_t ts, bool insert_safe) {
  assert(!insert_safe);  // insert_safe should be false
  if (indexer_->capacity() <= indexer_->size()) {
    return false;
  }
  ret = internal::insert_vertex_pk_internal(*indexer_, *v_ts_, id, ts,
                                            insert_safe);
  view_.insert(ret, props, insert_safe);
  return true;
}

// ── EdgeTableView ──

EdgeTableView::EdgeTableView(EdgeTable& table)
    : meta_(table.get_edge_schema_ptr()),
      out_csr_(table.out_csr_.get()),
      in_csr_(table.in_csr_.get()),
      table_idx_(&table.table_idx_),
      view_(*table.table()) {}

CsrView EdgeTableView::GetOutgoingView(timestamp_t ts) const {
  return out_csr_->get_generic_view(ts);
}

CsrView EdgeTableView::GetIncomingView(timestamp_t ts) const {
  return in_csr_->get_generic_view(ts);
}

EdgeDataAccessor EdgeTableView::GetDataAccessor(int prop_id) const {
  if (prop_id < 0 || static_cast<size_t>(prop_id) >= meta_->properties.size()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Edge property column id out of range: " + std::to_string(prop_id) +
        " (edge has " + std::to_string(meta_->properties.size()) +
        " properties)");
  }
  if (!meta_->is_bundled()) {
    return EdgeDataAccessor(
        meta_->properties[prop_id].id(),
        const_cast<ColumnBase*>(view_.get_raw_column(prop_id)));
  } else {
    if (prop_id != 0) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Bundled edges store a single inline property; expected col_id 0 "
          "but got " +
          std::to_string(prop_id));
    }
    return EdgeDataAccessor(meta_->properties[0].id(), nullptr);
  }
}

EdgeDataAccessor EdgeTableView::GetDataAccessor(
    const std::string& prop_name) const {
  auto prop_ind = meta_->get_property_index(prop_name);
  if (prop_ind == -1) {
    THROW_INVALID_ARGUMENT_EXCEPTION("property " + prop_name +
                                     " not found in edge table, or deleted");
  }
  return GetDataAccessor(static_cast<int>(prop_ind));
}

std::pair<int32_t, const void*> EdgeTableView::AddEdge(
    vid_t src_lid, vid_t dst_lid,
    const std::vector<execution::Value>& properties, timestamp_t ts,
    Allocator& alloc, bool insert_safe) {
  return internal::insert_edge_into_csr_internal(
      *out_csr_, *in_csr_, view_, *table_idx_, *meta_, src_lid, dst_lid,
      properties, ts, alloc, insert_safe);
}

// ── GraphView ──

GraphView::GraphView(PropertyGraph& storage) : schema_(&storage.schema()) {
  Rebuild(storage);
}

void GraphView::Rebuild(PropertyGraph& pg) {
  schema_ = &pg.schema();
  vertex_views_.clear();
  edge_views_.clear();
  // Use vertex_label_frontier() (total label-id space) instead of
  // vertex_label_num() (only live labels) so that vertex_views_ is indexed
  // by label-id.  Deleted (tombstoned) labels get a default-constructed
  // (empty) VertexTableView placeholder.
  size_t v_frontier = schema_->vertex_label_frontier();
  vertex_views_.resize(v_frontier);
  for (size_t i = 0; i < v_frontier; ++i) {
    if (!schema_->is_vertex_label_valid(static_cast<label_t>(i))) {
      continue;  // keep the default-constructed empty view
    }
    vertex_views_[i] =
        VertexTableView(pg.get_vertex_table(static_cast<label_t>(i)));
  }

  for (auto& [key, edge_table] : pg.edge_tables_) {
    edge_views_.emplace(key, EdgeTableView(edge_table));
  }
}

VertexSet GraphView::GetVertexSet(label_t label, timestamp_t ts) const {
  return vertex_views_[label].GetVertexSet(ts);
}

execution::Value GraphView::GetOid(label_t label, vid_t lid,
                                   timestamp_t ts) const {
  return vertex_views_[label].GetOid(lid, ts);
}

CsrView GraphView::GetGenericOutgoingView(label_t src_label, label_t dst_label,
                                          label_t edge_label,
                                          timestamp_t ts) const {
  uint32_t index =
      schema_->generate_edge_label(src_label, dst_label, edge_label);
  auto it = edge_views_.find(index);
  if (it == edge_views_.end()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Edge table for edge label triplet not found");
  }
  return it->second.GetOutgoingView(ts);
}

CsrView GraphView::GetGenericIncomingView(label_t src_label, label_t dst_label,
                                          label_t edge_label,
                                          timestamp_t ts) const {
  uint32_t index =
      schema_->generate_edge_label(src_label, dst_label, edge_label);
  auto it = edge_views_.find(index);
  if (it == edge_views_.end()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Edge table for edge label triplet not found");
  }
  return it->second.GetIncomingView(ts);
}

EdgeDataAccessor GraphView::GetEdgeDataAccessor(label_t src_label,
                                                label_t dst_label,
                                                label_t edge_label,
                                                int prop_id) const {
  uint32_t index =
      schema_->generate_edge_label(src_label, dst_label, edge_label);
  auto it = edge_views_.find(index);
  if (it == edge_views_.end()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Edge table for edge label triplet not found");
  }
  return it->second.GetDataAccessor(prop_id);
}

EdgeDataAccessor GraphView::GetEdgeDataAccessor(
    label_t src_label, label_t dst_label, label_t edge_label,
    const std::string& prop_name) const {
  uint32_t index =
      schema_->generate_edge_label(src_label, dst_label, edge_label);
  auto it = edge_views_.find(index);
  if (it == edge_views_.end()) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Edge table for edge label triplet not found");
  }
  return it->second.GetDataAccessor(prop_name);
}

Status GraphView::AddVertex(label_t label, const execution::Value& id,
                            const std::vector<execution::Value>& props,
                            vid_t& vid, timestamp_t ts) {
  if (!vertex_views_[label].AddVertex(id, props, vid, ts, false)) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT, "Fail to add vertex.");
  }
  return Status::OK();
}

Status GraphView::AddEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                          vid_t dst_lid, label_t edge_label,
                          const std::vector<execution::Value>& properties,
                          timestamp_t ts, Allocator& alloc, int32_t& oe_offset,
                          const void*& prop) {
  uint32_t index =
      schema_->generate_edge_label(src_label, dst_label, edge_label);
  auto it = edge_views_.find(index);
  if (it == edge_views_.end()) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  "Edge table does not exist for label <" +
                      std::to_string(src_label) + ", " +
                      std::to_string(dst_label) + ", " +
                      std::to_string(edge_label) + ">");
  }
  try {
    auto ret =
        it->second.AddEdge(src_lid, dst_lid, properties, ts, alloc, false);
    oe_offset = ret.first;
    prop = ret.second;
  } catch (const std::exception& e) {
    return Status(StatusCode::ERR_INVALID_ARGUMENT,
                  std::string("Failed to add edge: ") + e.what());
  }
  return Status::OK();
}

}  // namespace neug
