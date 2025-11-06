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

template <typename PROP_T>
class VertexColumn {
 public:
  explicit VertexColumn(std::shared_ptr<TypedRefColumn<PROP_T>> column) {
    if (column == nullptr) {
      column_ = nullptr;
    } else {
      column_ = column;
    }
  }
  VertexColumn() : column_(nullptr) {}

  inline PROP_T get_view(vid_t v) const { return column_->get_view(v); }

  inline bool is_null() const { return column_ == nullptr; }

 private:
  std::shared_ptr<TypedRefColumn<PROP_T>> column_;
};

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

namespace graph_update_interface_impl {
template <typename PROP_T>
class VertexColumn {
 public:
  enum class ColState { kInvalidColId = -2, kPrimaryKeyColId = -1 };
  VertexColumn()
      : txn_(nullptr),
        label_(0),
        col_id(static_cast<int>(ColState::kInvalidColId)) {}
  VertexColumn(UpdateTransaction* txn, label_t label, int col_id)
      : txn_(txn), label_(label), col_id(col_id) {}
  inline PROP_T get_view(vid_t v) const {
    // col_id == -1 means the primary key column
    if (col_id == static_cast<int>(ColState::kPrimaryKeyColId)) {
      return PropUtils<PROP_T>::to_typed(txn_->GetVertexId(label_, v));
    } else if (col_id == static_cast<int>(ColState::kInvalidColId)) {
      return PROP_T();
    } else {
      return PropUtils<PROP_T>::to_typed(
          txn_->GetVertexProperty(label_, v, col_id));
    }
  }

  // when the column is not found, the col_id is -2
  inline bool is_null() const {
    return col_id == static_cast<int>(ColState::kInvalidColId);
  }

 private:
  UpdateTransaction* txn_;
  label_t label_;
  int col_id;
};
}  // namespace graph_update_interface_impl

class GraphReadInterface {
 public:
  template <typename PROP_T>
  using vertex_column_t = graph_interface_impl::VertexColumn<PROP_T>;

  using vertex_set_t = gs::VertexSet;

  template <typename T>
  using vertex_array_t = graph_interface_impl::VertexArray<T>;

  static constexpr vid_t kInvalidVid = std::numeric_limits<vid_t>::max();

  explicit GraphReadInterface(const gs::ReadTransaction& txn) : txn_(txn) {}
  ~GraphReadInterface() {}

  template <typename PROP_T>
  inline vertex_column_t<PROP_T> GetVertexColumn(
      label_t label, const std::string& prop_name) const {
    return vertex_column_t<PROP_T>(
        txn_.get_vertex_ref_property_column<PROP_T>(label, prop_name));
  }

  inline vertex_set_t GetVertexSet(label_t label) const {
    return txn_.GetVertexSet(label);
  }

  inline bool GetVertexIndex(label_t label, const Property& id,
                             vid_t& index) const {
    return txn_.GetVertexIndex(label, id, index);
  }

  inline bool IsValidVertex(label_t label, vid_t index) const {
    return txn_.IsValidVertex(label, index);
  }

  inline Property GetVertexId(label_t label, vid_t index) const {
    return txn_.GetVertexId(label, index);
  }

  inline Property GetVertexProperty(label_t label, vid_t index,
                                    int prop_id) const {
    return txn_.graph().get_vertex_table(label).get_properties_table().at(
        index, prop_id);
  }

  GenericView GetGenericOutgoingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    return txn_.graph().GetGenericOutgoingGraphView(
        v_label, neighbor_label, edge_label, txn_.timestamp());
  }

  GenericView GetGenericIncomingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    return txn_.graph().GetGenericIncomingGraphView(
        v_label, neighbor_label, edge_label, txn_.timestamp());
  }

  EdgeDataAccessor GetEdgeDataAccessor(label_t src_label, label_t dst_label,
                                       label_t edge_label, int prop_id) const {
    return txn_.graph().GetEdgeDataAccessor(src_label, dst_label, edge_label,
                                            prop_id);
  }

  inline const Schema& schema() const { return txn_.schema(); }

 private:
  const gs::ReadTransaction& txn_;
};

class GraphInsertInterface {
 public:
  explicit GraphInsertInterface(gs::InsertTransaction& txn) : txn_(txn) {}
  ~GraphInsertInterface() {}

  inline bool AddVertex(label_t label, const Property& id,
                        const std::vector<Property>& props) {
    return txn_.AddVertex(label, id, props);
  }

  inline bool AddEdge(label_t src_label, const Property& src, label_t dst_label,
                      const Property& dst, label_t edge_label,
                      const std::vector<Property>& properties) {
    return txn_.AddEdge(src_label, src, dst_label, dst, edge_label, properties);
  }

  inline bool Commit() { return txn_.Commit(); }

  inline void Abort() { txn_.Abort(); }

  inline const Schema& schema() const { return txn_.schema(); }

 private:
  gs::InsertTransaction& txn_;
};

class GraphUpdateInterface {
 public:
  template <typename PROP_T>
  using vertex_column_t = graph_update_interface_impl::VertexColumn<PROP_T>;

  template <typename PROP_T>
  inline vertex_column_t<PROP_T> GetVertexColumn(
      label_t label, const std::string& prop_name) const {
    auto prop_names = txn_.schema().get_vertex_property_names(label);
    for (size_t i = 0; i < prop_names.size(); i++) {
      if (prop_names[i] == prop_name) {
        return vertex_column_t<PROP_T>(&txn_, label, i);
      }
    }
    const auto& pk = txn_.schema().get_vertex_primary_key(label);
    CHECK_EQ(pk.size(), 1);

    if (std::get<1>(pk[0]) == prop_name) {
      return vertex_column_t<PROP_T>(
          &txn_, label,
          static_cast<int>(
              vertex_column_t<PROP_T>::ColState::kPrimaryKeyColId));
    }
    // null column
    return vertex_column_t<PROP_T>(
        &txn_, label,
        static_cast<int>(vertex_column_t<PROP_T>::ColState::kInvalidColId));
  }

  explicit GraphUpdateInterface(gs::UpdateTransaction& txn) : txn_(txn) {}
  ~GraphUpdateInterface() {}

  inline void UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                                   const Property& value) {
    txn_.UpdateVertexProperty(label, lid, col_id, value);
  }

  inline Property GetVertexProperty(label_t label, vid_t index,
                                    int prop_id) const {
    return txn_.GetGraph().get_vertex_table(label).get_properties_table().at(
        index, prop_id);
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

  inline bool AddVertex(label_t label, const Property& id,
                        const std::vector<Property>& props) {
    return txn_.AddVertex(label, id, props);
  }

  inline bool GetVertexIndex(label_t label, const Property& id,
                             vid_t& index) const {
    return txn_.GetVertexIndex(label, id, index);
  }

  inline bool AddVertex(label_t label, const Property& id,
                        const std::vector<Property>& props, vid_t& vid) {
    return txn_.AddVertex(label, id, props, vid);
  }

  inline bool AddEdge(label_t src_label, const Property& src, label_t dst_label,
                      const Property& dst, label_t edge_label,
                      const std::vector<Property>& properties) {
    return txn_.AddEdge(src_label, src, dst_label, dst, edge_label, properties);
  }

  inline bool AddEdge(label_t src_label, vid_t src, label_t dst_label,
                      vid_t dst, label_t edge_label,
                      const std::vector<Property>& properties) {
    return txn_.AddEdge(src_label, src, dst_label, dst, edge_label, properties);
  }

  inline bool Commit() { return txn_.Commit(); }

  inline void Abort() { txn_.Abort(); }

  inline const Schema& schema() const { return txn_.schema(); }

  inline Property GetVertexId(label_t label, vid_t index) const {
    return txn_.GetVertexId(label, index);
  }

  inline vid_t GetVertexNum(label_t label) const {
    return txn_.GetVertexNum(label);
  }

  inline bool HasVertex(label_t label, const Property& oid) const {
    return txn_.HasVertex(label, oid);
  }

  inline auto GetVertexIterator(label_t label) const {
    return txn_.GetVertexIterator(label);
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
