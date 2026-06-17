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
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "neug/storages/allocators.h"
#include "neug/storages/csr/csr_view.h"
#include "neug/storages/graph/edge_table.h"
#include "neug/storages/graph/schema.h"
#include "neug/storages/graph/vertex_table.h"
#include "neug/utils/property/column.h"
#include "neug/utils/result.h"

namespace neug {

class PropertyGraph;

class TableView {
 public:
  TableView() = default;
  explicit TableView(const Table& table);

  std::shared_ptr<RefColumnBase> get_column(int col_id) const;
  std::shared_ptr<RefColumnBase> get_column(const std::string& name) const;
  ColumnBase* get_raw_column(int col_id) const;

  // Note: insert_safe is kept for compatibility with the old interface.
  // It must be false for TableView.
  void insert(size_t index, const std::vector<execution::Value>& values,
              bool insert_safe);

 private:
  const std::unordered_map<std::string, int>* col_id_map_{nullptr};
  std::vector<ColumnBase*> columns_;
};

class VertexTableView {
 public:
  VertexTableView() = default;
  explicit VertexTableView(VertexTable& table);

  bool get_lid(const execution::Value& oid, vid_t& lid, timestamp_t ts) const;
  vid_t LidNum() const;
  bool IsValidLid(vid_t lid, timestamp_t ts) const;
  execution::Value GetOid(vid_t lid, timestamp_t ts) const;
  VertexSet GetVertexSet(timestamp_t ts) const;
  std::shared_ptr<RefColumnBase> GetPropertyColumn(int col_id) const;
  std::shared_ptr<RefColumnBase> GetPropertyColumn(
      const std::string& prop) const;

  bool AddVertex(const execution::Value& id,
                 const std::vector<execution::Value>& props, vid_t& ret,
                 timestamp_t ts, bool insert_safe);

 private:
  std::string pk_name_;
  IndexerType* indexer_{nullptr};
  VertexTimestamp* v_ts_{nullptr};

  TableView view_;
};

class EdgeTableView {
 public:
  EdgeTableView() = default;
  explicit EdgeTableView(EdgeTable& table);

  CsrView GetOutgoingView(timestamp_t ts) const;
  CsrView GetIncomingView(timestamp_t ts) const;
  EdgeDataAccessor GetDataAccessor(int prop_id) const;
  EdgeDataAccessor GetDataAccessor(const std::string& prop_name) const;

  std::pair<int32_t, const void*> AddEdge(
      vid_t src_lid, vid_t dst_lid,
      const std::vector<execution::Value>& properties, timestamp_t ts,
      Allocator& alloc, bool insert_safe);

 private:
  std::shared_ptr<const EdgeSchema> meta_;
  CsrBase* out_csr_{nullptr};
  CsrBase* in_csr_{nullptr};
  std::atomic<uint64_t>* table_idx_{nullptr};

  TableView view_;
};

class GraphView {
 public:
  explicit GraphView(PropertyGraph& storage);

  GraphView() = default;
  ~GraphView() = default;

  GraphView(const GraphView&) = default;
  GraphView(GraphView&&) = default;
  GraphView& operator=(const GraphView&) = default;
  GraphView& operator=(GraphView&&) = default;

  const Schema& schema() const { return *schema_; }

  inline bool get_lid(label_t label, const execution::Value& oid, vid_t& lid,
                      timestamp_t ts) const {
    return vertex_views_[label].get_lid(oid, lid, ts);
  }
  inline vid_t LidNum(label_t label) const {
    return vertex_views_[label].LidNum();
  }
  inline bool IsValidLid(label_t label, vid_t lid, timestamp_t ts) const {
    return vertex_views_[label].IsValidLid(lid, ts);
  }
  inline std::shared_ptr<RefColumnBase> GetVertexPropertyColumn(
      label_t label, const std::string& prop) const {
    return vertex_views_[label].GetPropertyColumn(prop);
  }
  inline std::shared_ptr<RefColumnBase> GetVertexPropertyColumn(
      label_t label, int col_id) const {
    return vertex_views_[label].GetPropertyColumn(col_id);
  }

  VertexSet GetVertexSet(label_t label, timestamp_t ts) const;
  execution::Value GetOid(label_t label, vid_t lid, timestamp_t ts) const;

  CsrView GetGenericOutgoingView(label_t src_label, label_t dst_label,
                                 label_t edge_label, timestamp_t ts) const;
  CsrView GetGenericIncomingView(label_t src_label, label_t dst_label,
                                 label_t edge_label, timestamp_t ts) const;
  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const;
  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label,
                                       const std::string& prop_name) const;

  Status AddVertex(label_t label, const execution::Value& id,
                   const std::vector<execution::Value>& props, vid_t& vid,
                   timestamp_t ts);

  Status AddEdge(label_t src_label, vid_t src_lid, label_t dst_label,
                 vid_t dst_lid, label_t edge_label,
                 const std::vector<execution::Value>& properties,
                 timestamp_t ts, Allocator& alloc, int32_t& oe_offset,
                 const void*& prop);

  void Rebuild(PropertyGraph& pg);

 private:
  // needed by api schema().
  const Schema* schema_{nullptr};
  std::vector<VertexTableView> vertex_views_;
  std::unordered_map<uint32_t, EdgeTableView> edge_views_;
};

}  // namespace neug
