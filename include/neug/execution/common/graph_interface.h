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
class StorageReadInterface : virtual public IStorageInterface {
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

class StorageInsertInterface : virtual public IStorageInterface {
 public:
  bool readable() const { return false; }
  bool writable() const { return true; }
  explicit StorageInsertInterface() {}
  ~StorageInsertInterface() {}

  virtual bool AddVertex(label_t label, const Property& id,
                         const std::vector<Property>& props, vid_t& vid) = 0;

  virtual bool AddEdge(label_t src_label, vid_t src, label_t dst_label,
                       vid_t dst, label_t edge_label,
                       const std::vector<Property>& properties) = 0;
  virtual Status BatchAddVertices(
      label_t v_label_id, std::shared_ptr<IRecordBatchSupplier> supplier) = 0;

  virtual Status BatchAddEdges(
      label_t src_label, label_t dst_label, label_t edge_label,
      std::shared_ptr<IRecordBatchSupplier> supplier) = 0;
};
class StorageTPInsertInterface : public StorageInsertInterface {
 public:
  explicit StorageTPInsertInterface(gs::InsertTransaction& txn) : txn_(txn) {}
  ~StorageTPInsertInterface() {}

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

  inline Status BatchAddVertices(
      label_t v_label_id, std::shared_ptr<IRecordBatchSupplier> supplier) {
    LOG(FATAL) << "BatchAddVertices is not supported in TP mode currently.";
    return Status::OK();
  }

  inline Status BatchAddEdges(label_t src_label, label_t dst_label,
                              label_t edge_label,
                              std::shared_ptr<IRecordBatchSupplier> supplier) {
    LOG(FATAL) << "BatchAddEdges is not supported in TP mode currently.";
    return Status::OK();
  }

 private:
  gs::InsertTransaction& txn_;
};

class StorageUpdateInterface : public StorageReadInterface,
                               public StorageInsertInterface {
 public:
  bool readable() const override { return true; }
  bool writable() const override { return true; }

  explicit StorageUpdateInterface(const gs::PropertyGraph& graph,
                                  timestamp_t read_ts)
      : StorageReadInterface(graph, read_ts), StorageInsertInterface() {}
  ~StorageUpdateInterface() {}

  virtual void UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                                    const Property& value) = 0;

  virtual void UpdateEdgeProperty(label_t src_label, vid_t src,
                                  label_t dst_label, vid_t dst,
                                  label_t edge_label, int32_t oe_offset,
                                  int32_t ie_offset, int32_t col_id,
                                  const Property& value) = 0;

  virtual bool AddVertex(label_t label, const Property& id,
                         const std::vector<Property>& props, vid_t& vid) = 0;
  virtual bool AddEdge(label_t src_label, vid_t src, label_t dst_label,
                       vid_t dst, label_t edge_label,
                       const std::vector<Property>& properties) = 0;

  virtual Status BatchDeleteVertices(label_t v_label_id,
                                     const std::vector<vid_t>& vids) = 0;

  virtual Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::tuple<vid_t, vid_t>>& edges) = 0;
  virtual Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
      const std::vector<std::pair<vid_t, int32_t>>& ie_edges) = 0;

  virtual bool CreateVertexType(
      const std::string& name,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          properties,
      const std::vector<std::string>& primary_key_names,
      bool error_on_conflict) = 0;

  virtual bool CreateEdgeType(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          properties,
      bool error_on_conflict, EdgeStrategy oe_edge_strategy,
      EdgeStrategy ie_edge_strategy) = 0;

  virtual bool AddVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          add_properties,
      bool error_on_conflict) = 0;

  virtual bool AddEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          add_properties,
      bool error_on_conflict) = 0;

  virtual bool RenameVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict) = 0;

  virtual bool RenameEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict) = 0;

  virtual bool DeleteVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::string>& delete_properties,
      bool error_on_conflict) = 0;

  virtual bool DeleteEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::string>& delete_properties,
      bool error_on_conflict) = 0;

  virtual bool DeleteVertexType(const std::string& vertex_type_name,
                                bool error_on_conflict = true) = 0;

  virtual bool DeleteEdgeType(const std::string& src_type,
                              const std::string& dst_type,
                              const std::string& edge_type,
                              bool error_on_conflict) = 0;

  virtual void CreateCheckpoint() = 0;
};
class StorageTPUpdateInterface : public StorageUpdateInterface {
 public:
  explicit StorageTPUpdateInterface(gs::UpdateTransaction& txn)
      : StorageUpdateInterface(txn.graph(), txn.timestamp()),

        txn_(txn) {}
  ~StorageTPUpdateInterface() {}

  inline void UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                                   const Property& value) {
    txn_.UpdateVertexProperty(label, lid, col_id, value);
  }

  inline void UpdateEdgeProperty(label_t src_label, vid_t src,
                                 label_t dst_label, vid_t dst,
                                 label_t edge_label, int32_t oe_offset,
                                 int32_t ie_offset, int32_t col_id,
                                 const Property& value) {
    txn_.UpdateEdgeProperty(src_label, src, dst_label, dst, edge_label,
                            oe_offset, ie_offset, col_id, value);
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

  inline void CreateCheckpoint() { txn_.CreateCheckpoint(); }

  gs::UpdateTransaction& GetTransaction() { return txn_; }

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

  inline Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
      const std::vector<std::pair<vid_t, int32_t>>& ie_edges) {
    return txn_.BatchDeleteEdges(src_v_label_id, dst_v_label_id, edge_label_id,
                                 oe_edges, ie_edges);
  }

  bool CreateVertexType(
      const std::string& name,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          properties,
      const std::vector<std::string>& primary_key_names,
      bool error_on_conflict) override {
    return txn_.CreateVertexType(name, properties, primary_key_names,
                                 error_on_conflict);
  }

  bool CreateEdgeType(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          properties,
      bool error_on_conflict, EdgeStrategy oe_edge_strategy,
      EdgeStrategy ie_edge_strategy) override {
    return txn_.CreateEdgeType(src_type, dst_type, edge_type, properties,
                               error_on_conflict, oe_edge_strategy,
                               ie_edge_strategy);
  }

  bool AddVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          add_properties,
      bool error_on_conflict) override {
    return txn_.AddVertexProperties(vertex_type_name, add_properties,
                                    error_on_conflict);
  }

  bool AddEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          add_properties,
      bool error_on_conflict) override {
    return txn_.AddEdgeProperties(src_type, dst_type, edge_type, add_properties,
                                  error_on_conflict);
  }

  bool RenameVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict) override {
    return txn_.RenameVertexProperties(vertex_type_name, rename_properties,
                                       error_on_conflict);
  }
  bool RenameEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict) override {
    return txn_.RenameEdgeProperties(src_type, dst_type, edge_type,
                                     rename_properties, error_on_conflict);
  }

  bool DeleteVertexProperties(const std::string& vertex_type_name,
                              const std::vector<std::string>& delete_properties,
                              bool error_on_conflict) override {
    return txn_.DeleteVertexProperties(vertex_type_name, delete_properties,
                                       error_on_conflict);
  }

  bool DeleteEdgeProperties(const std::string& src_type,
                            const std::string& dst_type,
                            const std::string& edge_type,
                            const std::vector<std::string>& delete_properties,
                            bool error_on_conflict) override {
    return txn_.DeleteEdgeProperties(src_type, dst_type, edge_type,
                                     delete_properties, error_on_conflict);
  }

  bool DeleteVertexType(const std::string& vertex_type_name,
                        bool error_on_conflict = true) override {
    return txn_.DeleteVertexType(vertex_type_name, error_on_conflict);
  }

  bool DeleteEdgeType(const std::string& src_type, const std::string& dst_type,
                      const std::string& edge_type,
                      bool error_on_conflict) override {
    return txn_.DeleteEdgeType(src_type, dst_type, edge_type,
                               error_on_conflict);
  }

 private:
  gs::UpdateTransaction& txn_;
};

class StorageAPUpdateInterface : public StorageUpdateInterface {
 public:
  explicit StorageAPUpdateInterface(PropertyGraph& graph, timestamp_t timestamp,
                                    gs::Allocator& alloc)
      : StorageUpdateInterface(graph, timestamp),
        graph_(graph),
        alloc_(alloc),
        timestamp_(timestamp) {}
  ~StorageAPUpdateInterface() {}

  inline void UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                                   const Property& value) override {
    graph_.UpdateVertexProperty(label, lid, col_id, value, timestamp_);
  }

  inline void UpdateEdgeProperty(label_t src_label, vid_t src,
                                 label_t dst_label, vid_t dst,
                                 label_t edge_label, int32_t oe_offset,
                                 int32_t ie_offset, int32_t col_id,
                                 const Property& value) override {
    graph_.UpdateEdgeProperty(src_label, src, dst_label, dst, edge_label,
                              oe_offset, ie_offset, col_id, value,
                              gs::timestamp_t(0));
  }

  inline bool AddVertex(label_t label, const Property& id,
                        const std::vector<Property>& props, vid_t& vid) {
    auto status = graph_.AddVertex(label, id, props, vid, gs::timestamp_t(0));
    if (!status.ok()) {
      LOG(ERROR) << "AddVertex failed: " << status.ToString();
    }
    return status.ok();
  }

  inline bool AddEdge(label_t src_label, vid_t src, label_t dst_label,
                      vid_t dst, label_t edge_label,
                      const std::vector<Property>& properties) {
    graph_.AddEdge(src_label, src, dst_label, dst, edge_label, properties,
                   gs::timestamp_t(0), alloc_);
    return true;
  }

  inline void CreateCheckpoint() { graph_.Dump(); }

  inline Status BatchAddVertices(
      label_t v_label_id, std::shared_ptr<IRecordBatchSupplier> supplier) {
    return graph_.BatchAddVertices(v_label_id, std::move(supplier));
  }

  inline Status BatchAddEdges(label_t src_label, label_t dst_label,
                              label_t edge_label,
                              std::shared_ptr<IRecordBatchSupplier> supplier) {
    return graph_.BatchAddEdges(src_label, dst_label, edge_label,
                                std::move(supplier));
  }

  inline Status BatchDeleteVertices(label_t v_label_id,
                                    const std::vector<vid_t>& vids) {
    return graph_.BatchDeleteVertices(v_label_id, vids);
  }

  inline Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::tuple<vid_t, vid_t>>& edges) {
    return graph_.BatchDeleteEdges(src_v_label_id, dst_v_label_id,
                                   edge_label_id, edges);
  }

  inline Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
      const std::vector<std::pair<vid_t, int32_t>>& ie_edges) {
    return graph_.BatchDeleteEdges(src_v_label_id, dst_v_label_id,
                                   edge_label_id, oe_edges, ie_edges);
  }

  bool CreateVertexType(
      const std::string& name,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          properties,
      const std::vector<std::string>& primary_key_names,
      bool error_on_conflict) override {
    auto status = graph_.CreateVertexType(name, properties, primary_key_names,
                                          error_on_conflict);
    if (!status.ok()) {
      LOG(ERROR) << "CreateVertexType failed: " << status.ToString();
    }
    return status.ok();
  }

  bool CreateEdgeType(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          properties,
      bool error_on_conflict, EdgeStrategy oe_edge_strategy,
      EdgeStrategy ie_edge_strategy) override {
    auto status = graph_.CreateEdgeType(src_type, dst_type, edge_type,
                                        properties, error_on_conflict,
                                        oe_edge_strategy, ie_edge_strategy);
    if (!status.ok()) {
      LOG(ERROR) << "CreateEdgeType failed: " << status.ToString();
    }
    return status.ok();
  }

  bool AddVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          add_properties,
      bool error_on_conflict) override {
    auto status = graph_.AddVertexProperties(vertex_type_name, add_properties,
                                             error_on_conflict);
    if (!status.ok()) {
      LOG(ERROR) << "AddVertexProperties failed: " << status.ToString();
    }
    return status.ok();
  }

  bool AddEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<PropertyType, std::string, Property>>&
          add_properties,
      bool error_on_conflict) override {
    auto status = graph_.AddEdgeProperties(src_type, dst_type, edge_type,
                                           add_properties, error_on_conflict);
    if (!status.ok()) {
      LOG(ERROR) << "AddEdgeProperties failed: " << status.ToString();
    }
    return status.ok();
  }

  bool RenameVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict) override {
    auto status = graph_.RenameVertexProperties(
        vertex_type_name, rename_properties, error_on_conflict);
    if (!status.ok()) {
      LOG(ERROR) << "RenameVertexProperties failed: " << status.ToString();
    }
    return status.ok();
  }
  bool RenameEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict) override {
    auto status = graph_.RenameEdgeProperties(
        src_type, dst_type, edge_type, rename_properties, error_on_conflict);
    if (!status.ok()) {
      LOG(ERROR) << "RenameEdgeProperties failed: " << status.ToString();
    }
    return status.ok();
  }

  bool DeleteVertexProperties(const std::string& vertex_type_name,
                              const std::vector<std::string>& delete_properties,
                              bool error_on_conflict) override {
    auto status = graph_.DeleteVertexProperties(
        vertex_type_name, delete_properties, error_on_conflict);
    if (!status.ok()) {
      LOG(ERROR) << "DeleteVertexProperties failed: " << status.ToString();
    }
    return status.ok();
  }

  bool DeleteEdgeProperties(const std::string& src_type,
                            const std::string& dst_type,
                            const std::string& edge_type,
                            const std::vector<std::string>& delete_properties,
                            bool error_on_conflict) override {
    auto status = graph_.DeleteEdgeProperties(
        src_type, dst_type, edge_type, delete_properties, error_on_conflict);
    if (!status.ok()) {
      LOG(ERROR) << "DeleteEdgeProperties failed: " << status.ToString();
    }
    return status.ok();
  }

  bool DeleteVertexType(const std::string& vertex_type_name,
                        bool error_on_conflict = true) override {
    auto status = graph_.DeleteVertexType(vertex_type_name, error_on_conflict);
    if (!status.ok()) {
      LOG(ERROR) << "DeleteVertexType failed: " << status.ToString();
    }
    return status.ok();
  }

  bool DeleteEdgeType(const std::string& src_type, const std::string& dst_type,
                      const std::string& edge_type,
                      bool error_on_conflict) override {
    auto status =
        graph_.DeleteEdgeType(src_type, dst_type, edge_type, error_on_conflict);
    if (!status.ok()) {
      LOG(ERROR) << "DeleteEdgeType failed: " << status.ToString();
    }
    return status.ok();
  }

 private:
  PropertyGraph& graph_;
  gs::Allocator& alloc_;
  timestamp_t timestamp_;
};

}  // namespace runtime

}  // namespace gs

#endif  // INCLUDE_NEUG_EXECUTION_COMMON_GRAPH_INTERFACE_H_
