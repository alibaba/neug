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

#include "neug/storages/graph/property_graph.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/property/types.h"

namespace gs {

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
  explicit StorageInsertInterface() {}
  virtual ~StorageInsertInterface() {}

  bool readable() const override { return false; }
  bool writable() const override { return true; }

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

class StorageUpdateInterface : public StorageReadInterface,
                               public StorageInsertInterface {
 public:
  explicit StorageUpdateInterface(const gs::PropertyGraph& graph,
                                  timestamp_t read_ts)
      : StorageReadInterface(graph, read_ts), StorageInsertInterface() {}
  virtual ~StorageUpdateInterface() {}

  bool readable() const override { return true; }
  bool writable() const override { return true; }

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

  virtual Status CreateVertexType(
      const std::string& name,
      const std::vector<std::tuple<DataTypeId, std::string, Property>>&
          properties,
      const std::vector<std::string>& primary_key_names,
      bool error_on_conflict) = 0;

  virtual Status CreateEdgeType(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<DataTypeId, std::string, Property>>&
          properties,
      bool error_on_conflict, EdgeStrategy oe_edge_strategy,
      EdgeStrategy ie_edge_strategy) = 0;

  virtual Status AddVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<DataTypeId, std::string, Property>>&
          add_properties,
      bool error_on_conflict) = 0;

  virtual Status AddEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<DataTypeId, std::string, Property>>&
          add_properties,
      bool error_on_conflict) = 0;

  virtual Status RenameVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict) = 0;

  virtual Status RenameEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict) = 0;

  virtual Status DeleteVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::string>& delete_properties,
      bool error_on_conflict) = 0;

  virtual Status DeleteEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::string>& delete_properties,
      bool error_on_conflict) = 0;

  virtual Status DeleteVertexType(const std::string& vertex_type_name,
                                  bool error_on_conflict = true) = 0;

  virtual Status DeleteEdgeType(const std::string& src_type,
                                const std::string& dst_type,
                                const std::string& edge_type,
                                bool error_on_conflict) = 0;

  virtual void CreateCheckpoint() = 0;
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

  void UpdateVertexProperty(label_t label, vid_t lid, int col_id,
                            const Property& value) override;
  void UpdateEdgeProperty(label_t src_label, vid_t src, label_t dst_label,
                          vid_t dst, label_t edge_label, int32_t oe_offset,
                          int32_t ie_offset, int32_t col_id,
                          const Property& value) override;
  bool AddVertex(label_t label, const Property& id,
                 const std::vector<Property>& props, vid_t& vid) override;
  bool AddEdge(label_t src_label, vid_t src, label_t dst_label, vid_t dst,
               label_t edge_label,
               const std::vector<Property>& properties) override;
  void CreateCheckpoint() override;
  Status BatchAddVertices(
      label_t v_label_id,
      std::shared_ptr<IRecordBatchSupplier> supplier) override;
  Status BatchAddEdges(label_t src_label, label_t dst_label, label_t edge_label,
                       std::shared_ptr<IRecordBatchSupplier> supplier) override;
  Status BatchDeleteVertices(label_t v_label_id,
                             const std::vector<vid_t>& vids) override;
  Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::tuple<vid_t, vid_t>>& edges) override;
  Status BatchDeleteEdges(
      label_t src_v_label_id, label_t dst_v_label_id, label_t edge_label_id,
      const std::vector<std::pair<vid_t, int32_t>>& oe_edges,
      const std::vector<std::pair<vid_t, int32_t>>& ie_edges) override;
  Status CreateVertexType(
      const std::string& name,
      const std::vector<std::tuple<DataTypeId, std::string, Property>>&
          properties,
      const std::vector<std::string>& primary_key_names,
      bool error_on_conflict) override;
  Status CreateEdgeType(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<DataTypeId, std::string, Property>>&
          properties,
      bool error_on_conflict, EdgeStrategy oe_edge_strategy,
      EdgeStrategy ie_edge_strategy) override;
  Status AddVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::tuple<DataTypeId, std::string, Property>>&
          add_properties,
      bool error_on_conflict) override;
  Status AddEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::tuple<DataTypeId, std::string, Property>>&
          add_properties,
      bool error_on_conflict) override;
  Status RenameVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict) override;
  Status RenameEdgeProperties(
      const std::string& src_type, const std::string& dst_type,
      const std::string& edge_type,
      const std::vector<std::pair<std::string, std::string>>& rename_properties,
      bool error_on_conflict) override;
  Status DeleteVertexProperties(
      const std::string& vertex_type_name,
      const std::vector<std::string>& delete_properties,
      bool error_on_conflict) override;
  Status DeleteEdgeProperties(const std::string& src_type,
                              const std::string& dst_type,
                              const std::string& edge_type,
                              const std::vector<std::string>& delete_properties,
                              bool error_on_conflict) override;
  Status DeleteVertexType(const std::string& vertex_type_name,
                          bool error_on_conflict = true) override;
  Status DeleteEdgeType(const std::string& src_type,
                        const std::string& dst_type,
                        const std::string& edge_type,
                        bool error_on_conflict) override;

 private:
  PropertyGraph& graph_;
  gs::Allocator& alloc_;
  timestamp_t timestamp_;
};

}  // namespace gs
