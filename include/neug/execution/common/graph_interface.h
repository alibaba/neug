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

#ifndef INCLUDE_NEUG_EXECUTION_COMMON_GRAPH_INTERFACE_H_
#define INCLUDE_NEUG_EXECUTION_COMMON_GRAPH_INTERFACE_H_

#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "neug/transaction/insert_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/update_transaction.h"
#include "neug/utils/property/types.h"

namespace gs {

namespace runtime {

namespace graph_interface_impl {

using gs::label_t;
using gs::timestamp_t;
using gs::vid_t;

template <typename T>
class VertexArray {
 public:
  VertexArray() : data_() {}
  VertexArray(const VertexSet& keys, const T& val) : data_(keys.size(), val) {}
  ~VertexArray() {}

  inline void Init(const VertexSet& keys, const T& val) {
    data_.resize(keys.size(), val);
  }

  inline typename std::vector<T>::reference operator[](vid_t v) {
    return data_[v];
  }
  inline typename std::vector<T>::const_reference operator[](vid_t v) const {
    return data_[v];
  }

 private:
  std::vector<T> data_;
};

}  // namespace graph_interface_impl

class IStorageInterface {
 public:
  virtual ~IStorageInterface() {}
  virtual bool readable() const = 0;
  virtual bool writable() const = 0;
  virtual const Schema& schema() const = 0;
  virtual bool GetVertexIndex(label_t label, const Property& id,
                              vid_t& index) const = 0;
};
class StorageReadInterface : public IStorageInterface {
 public:
  template <typename PROP_T>
  using vertex_column_t = TypedRefColumn<PROP_T>;

  using vertex_set_t = gs::VertexSet;

  template <typename T>
  using vertex_array_t = graph_interface_impl::VertexArray<T>;

  static constexpr vid_t kInvalidVid = std::numeric_limits<vid_t>::max();

  explicit StorageReadInterface(const PropertyGraph& graph, timestamp_t read_ts)
      : graph_(graph), read_ts_(read_ts) {}
  ~StorageReadInterface() {}
  bool readable() const override { return true; }
  bool writable() const override { return false; }

  template <typename PROP_T>
  inline std::shared_ptr<vertex_column_t<PROP_T>> GetVertexPropColumn(
      label_t label, const std::string& prop_name) const {
    return std::dynamic_pointer_cast<vertex_column_t<PROP_T>>(
        graph_.GetVertexPropertyColumn(label, prop_name));
  }

  VertexSet GetVertexSet(label_t label) const {
    return graph_.GetVertexSet(label, read_ts_);
  }

  bool GetVertexIndex(label_t label, const Property& id,
                      vid_t& index) const override {
    return graph_.get_lid(label, id, index, read_ts_);
  }

  inline bool IsValidVertex(label_t label, vid_t index) const {
    return graph_.IsValidLid(label, index, read_ts_);
  }

  inline Property GetVertexId(label_t label, vid_t index) const {
    return graph_.GetOid(label, index, read_ts_);
  }

  inline Property GetVertexProperty(label_t label, vid_t index,
                                    int prop_id) const {
    return graph_.get_vertex_table(label)
        .get_property_column(prop_id)
        ->get_prop(index);
  }

  GenericView GetGenericOutgoingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    return graph_.GetGenericOutgoingGraphView(v_label, neighbor_label,
                                              edge_label, read_ts_);
  }

  GenericView GetGenericIncomingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    return graph_.GetGenericIncomingGraphView(v_label, neighbor_label,
                                              edge_label, read_ts_);
  }

  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const {
    return graph_.GetEdgeDataAccessor(src_label, dst_label, edge_label,
                                      prop_id);
  }

  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label,
                                       const std::string& prop_name) const {
    return graph_.GetEdgeDataAccessor(src_label, dst_label, edge_label,
                                      prop_name);
  }

  const Schema& schema() const override { return graph_.schema(); }

 private:
  const PropertyGraph& graph_;
  timestamp_t read_ts_;
};

class StorageInsertInterface : public IStorageInterface {
 public:
  bool readable() const override { return false; }
  bool writable() const override { return true; }

  explicit StorageInsertInterface(gs::InsertTransaction& txn) : txn_(txn) {}
  ~StorageInsertInterface() {}

  inline bool AddVertex(label_t label, const Property& id,
                        const std::vector<Property>& props, vid_t& vid) {
    return txn_.AddVertex(label, id, props, vid);
  }

  inline bool AddEdge(label_t src_label, vid_t src, label_t dst_label,
                      vid_t dst, label_t edge_label,
                      const std::vector<Property>& properties) {
    return txn_.AddEdge(src_label, src, dst_label, dst, edge_label, properties);
  }

  inline const Schema& schema() const { return txn_.schema(); }

  bool GetVertexIndex(label_t label, const Property& id,
                      vid_t& index) const override {
    return txn_.GetVertexIndex(label, id, index);
  }

 private:
  gs::InsertTransaction& txn_;
};

class StorageUpdateInterface : public IStorageInterface {
 public:
  template <typename PROP_T>
  using vertex_column_t = TypedRefColumn<PROP_T>;

  bool readable() const override { return true; }
  bool writable() const override { return true; }

  template <typename PROP_T>
  inline std::shared_ptr<vertex_column_t<PROP_T>> GetVertexPropColumn(
      label_t label, const std::string& prop_name) const {
    return std::dynamic_pointer_cast<vertex_column_t<PROP_T>>(
        txn_.get_vertex_property_column(label, prop_name));
  }

  explicit StorageUpdateInterface(gs::UpdateTransaction& txn) : txn_(txn) {}
  ~StorageUpdateInterface() {}

  inline void UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                                   const Property& value) {
    txn_.UpdateVertexProperty(label, lid, col_id, value);
  }

  inline VertexSet GetVertexSet(label_t label) const {
    return txn_.GetVertexSet(label);
  }

  inline Property GetVertexProperty(label_t label, vid_t index,
                                    int prop_id) const {
    return txn_.GetGraph()
        .get_vertex_table(label)
        .get_property_column(prop_id)
        ->get_prop(index);
  }

  inline void SetEdgeData(bool dir, label_t label, vid_t v,
                          label_t neighbor_label, vid_t nbr, label_t edge_label,
                          const Property& value, int32_t col_id = 0) {
    txn_.SetEdgeData(dir, label, v, neighbor_label, nbr, edge_label, value,
                     col_id);
  }

  inline bool GetUpdatedEdgeData(bool dir, label_t label, vid_t v,
                                 label_t neighbor_label, vid_t nbr,
                                 label_t edge_label, int32_t prop_id,
                                 Property& ret) const {
    return txn_.GetUpdatedEdgeData(dir, label, v, neighbor_label, nbr,
                                   edge_label, prop_id, ret);
  }

  inline bool GetVertexIndex(label_t label, const Property& id,
                             vid_t& index) const {
    return txn_.GetVertexIndex(label, id, index);
  }

  inline bool AddVertex(label_t label, const Property& id,
                        const std::vector<Property>& props, vid_t& vid) {
    return txn_.AddVertex(label, id, props, vid);
  }

  inline bool AddEdge(label_t src_label, vid_t src, label_t dst_label,
                      vid_t dst, label_t edge_label,
                      const std::vector<Property>& properties) {
    return txn_.AddEdge(src_label, src, dst_label, dst, edge_label, properties);
  }

  inline const Schema& schema() const { return txn_.schema(); }

  inline Property GetVertexId(label_t label, vid_t index) const {
    return txn_.GetVertexId(label, index);
  }

  inline auto GetOutEdgeIterator(label_t label, vid_t src,
                                 label_t neighbor_label, label_t edge_label,
                                 int prop_id) const {
    return txn_.GetOutEdgeIterator(label, src, neighbor_label, edge_label,
                                   prop_id);
  }

  inline auto GetInEdgeIterator(label_t label, vid_t src,
                                label_t neighbor_label, label_t edge_label,
                                int prop_id) const {
    return txn_.GetInEdgeIterator(label, src, neighbor_label, edge_label,
                                  prop_id);
  }

  inline void CreateCheckpoint() { txn_.CreateCheckpoint(); }

  gs::UpdateTransaction& GetTransaction() { return txn_; }

  GenericView GetGenericOutgoingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    return txn_.GetGraph().GetGenericOutgoingGraphView(
        v_label, neighbor_label, edge_label, txn_.timestamp());
  }

  GenericView GetGenericIncomingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    return txn_.GetGraph().GetGenericIncomingGraphView(
        v_label, neighbor_label, edge_label, txn_.timestamp());
  }

  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const {
    return txn_.GetGraph().GetEdgeDataAccessor(src_label, dst_label, edge_label,
                                               prop_id);
  }

  inline std::string work_dir() const { return txn_.GetGraph().work_dir(); }

  inline Status BatchAddVertices(
      label_t v_label_id, std::shared_ptr<IRecordBatchSupplier> supplier) {
    return txn_.BatchAddVertices(v_label_id, std::move(supplier));
  }

  inline Status BatchAddEdges(label_t src_label, label_t dst_label,
                              label_t edge_label,
                              std::shared_ptr<IRecordBatchSupplier> supplier) {
    return txn_.BatchAddEdges(src_label, dst_label, edge_label,
                              std::move(supplier));
  }

  inline Status BatchDeleteVertices(label_t v_label_id,
                                    const std::vector<vid_t>& vids) {
    return txn_.BatchDeleteVertices(v_label_id, vids);
  }

  inline Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::tuple<vid_t, vid_t>>& edges) {
    return txn_.BatchDeleteEdges(src_v_label_id, dst_v_label_id, edge_label_id,
                                 edges);
  }

 private:
  gs::UpdateTransaction& txn_;
};

}  // namespace runtime

}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_COMMON_GRAPH_INTERFACE_H_
