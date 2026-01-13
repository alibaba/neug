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

#include <assert.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <vector>

#include "neug/execution/common/columns/arrow_context_column.h"
#include "neug/execution/common/columns/edge_columns.h"
#include "neug/execution/common/columns/path_columns.h"
#include "neug/execution/common/columns/value_columns.h"
#include "neug/execution/common/columns/vertex_columns.h"
#include "neug/execution/common/context.h"
#include "neug/execution/common/types.h"
#include "neug/storages/graph/graph_interface.h"
#include "neug/transaction/read_transaction.h"
#include "neug/utils/property/types.h"
#include "neug/utils/runtime/rt_any.h"

namespace gs {

namespace runtime {
class IContextColumnBuilder;
template <typename T>
class IValueColumn;

class IAccessor {
 public:
  virtual ~IAccessor() = default;
  virtual RTAny eval_path(size_t idx) const {
    LOG(FATAL) << "Not implemented";
    return RTAny();
  }
  virtual RTAny eval_vertex(label_t label, vid_t v) const {
    LOG(FATAL) << "Not implemented";
    return RTAny();
  }
  virtual RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                          const void* data_ptr) const {
    LOG(FATAL) << "Not implemented";
    return RTAny();
  }

  virtual bool is_optional() const { return false; }

  virtual std::string name() const { return "unknown"; }
};

class VertexPathAccessor : public IAccessor {
 public:
  using elem_t = VertexRecord;

  VertexPathAccessor(const Context& ctx, int tag)
      : vertex_col_(*std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag))) {}

  bool is_optional() const override { return vertex_col_.is_optional(); }

  elem_t typed_eval_path(size_t idx) const {
    return vertex_col_.get_vertex(idx);
  }

  RTAny eval_path(size_t idx) const override {
    if (!vertex_col_.has_value(idx)) {
      return RTAny(DataType(DataTypeId::kNull));
    }
    return RTAny::from_vertex(typed_eval_path(idx));
  }

 private:
  const IVertexColumn& vertex_col_;
};

class VertexGIdPathAccessor : public IAccessor {
 public:
  using elem_t = int64_t;
  VertexGIdPathAccessor(const Context& ctx, int tag)
      : vertex_col_(*std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag))) {}

  bool is_optional() const override { return vertex_col_.is_optional(); }

  elem_t typed_eval_path(size_t idx) const {
    const auto& v = vertex_col_.get_vertex(idx);
    return encode_unique_vertex_id(v.label_, v.vid_);
  }

  RTAny eval_path(size_t idx) const override {
    if (!vertex_col_.has_value(idx)) {
      return RTAny(DataType(DataTypeId::kNull));
    }
    return RTAny::from_int64(typed_eval_path(idx));
  }

 private:
  const IVertexColumn& vertex_col_;
};

template <typename T>
class VertexPropertyPathAccessor : public IAccessor {
 public:
  using elem_t = T;
  VertexPropertyPathAccessor(const StorageReadInterface& graph,
                             const Context& ctx, int tag,
                             const std::string& prop_name);

  bool is_optional() const override {
    for (auto col : property_columns_) {
      if (col == nullptr) {
        return true;
      }
    }
    return false;
  }

  elem_t typed_eval_path(size_t idx) const {
    const auto& v = vertex_col_.get_vertex(idx);
    auto& col = property_columns_[v.label_];
    if (!(col == nullptr)) {
      return col->get_view(v.vid_);
    } else {
      return elem_t();
    }
  }

  RTAny eval_path(size_t idx) const override {
    if (!vertex_col_.has_value(idx)) {
      return RTAny(DataType(DataTypeId::kNull));
    }
    const auto& v = vertex_col_.get_vertex(idx);
    auto col = property_columns_[v.label_];
    if (!(col == nullptr)) {
      return TypedConverter<T>::from_typed(col->get_view(v.vid_));
    } else {
      return RTAny(DataType(DataTypeId::kNull));
    }
  }

 private:
  bool is_optional_;
  const IVertexColumn& vertex_col_;
  using vertex_column_t =
      typename StorageReadInterface::template vertex_column_t<elem_t>;
  std::vector<std::shared_ptr<vertex_column_t>> property_columns_;
};

class VertexLabelPathAccessor : public IAccessor {
 public:
  using elem_t = std::string_view;
  VertexLabelPathAccessor(const Schema& schema, const Context& ctx, int tag)
      : vertex_col_(*std::dynamic_pointer_cast<IVertexColumn>(ctx.get(tag))),
        schema_(schema) {}

  elem_t typed_eval_path(size_t idx) const {
    auto label_id = vertex_col_.get_vertex(idx).label_;
    return schema_.get_vertex_label_name(label_id);
  }

  RTAny eval_path(size_t idx) const override {
    if (!vertex_col_.has_value(idx)) {
      return RTAny(DataType(DataTypeId::kNull));
    }
    auto label_id = vertex_col_.get_vertex(idx).label_;
    return RTAny::from_string(schema_.get_vertex_label_name(label_id));
  }

 private:
  const IVertexColumn& vertex_col_;
  const Schema& schema_;
};

class VertexLabelVertexAccessor : public IAccessor {
 public:
  using elem_t = std::string_view;
  explicit VertexLabelVertexAccessor(const Schema& schema) : schema_(schema) {}

  elem_t typed_eval_vertex(label_t label, vid_t v, size_t idx) const {
    return schema_.get_vertex_label_name(label);
  }

  RTAny eval_vertex(label_t label, vid_t v) const override {
    return RTAny::from_string(schema_.get_vertex_label_name(label));
  }

 private:
  const Schema& schema_;
};
template <typename T>
class ContextValueAccessor : public IAccessor {
 public:
  using elem_t = T;
  ContextValueAccessor(const Context& ctx, int tag)
      : col_(*std::dynamic_pointer_cast<IValueColumn<elem_t>>(ctx.get(tag))) {
    assert(std::dynamic_pointer_cast<IValueColumn<elem_t>>(ctx.get(tag)) !=
           nullptr);
  }

  elem_t typed_eval_path(size_t idx) const { return col_.get_value(idx); }

  RTAny eval_path(size_t idx) const override {
    if (!col_.has_value(idx)) {
      return RTAny(DataType(DataTypeId::kNull));
    }
    return col_.get_elem(idx);
  }

  bool is_optional() const override { return col_.is_optional(); }

 private:
  const IValueColumn<elem_t>& col_;
};

class ArrowArrayAccessor : public IAccessor {
 public:
  ArrowArrayAccessor(const Context& ctx, int tag)
      : col_(
            *std::dynamic_pointer_cast<ArrowArrayContextColumn>(ctx.get(tag))) {
    assert(std::dynamic_pointer_cast<ArrowArrayContextColumn>(ctx.get(tag)) !=
           nullptr);
  }

  RTAny eval_path(size_t idx) const override {
    if (!col_.has_value(idx)) {
      return RTAny(DataType(DataTypeId::kNull));
    }
    return col_.get_elem(idx);
  }

  bool is_optional() const override { return col_.is_optional(); }

 private:
  const ArrowArrayContextColumn& col_;
};

class VertexIdVertexAccessor : public IAccessor {
 public:
  using elem_t = VertexRecord;
  VertexIdVertexAccessor() {}

  elem_t typed_eval_vertex(label_t label, vid_t v) const {
    return VertexRecord{label, v};
  }

  RTAny eval_vertex(label_t label, vid_t v) const override {
    if (v == std::numeric_limits<vid_t>::max()) {
      return RTAny(DataType(DataTypeId::kNull));
    }
    return RTAny::from_vertex(typed_eval_vertex(label, v));
  }
};

class VertexGIdVertexAccessor : public IAccessor {
 public:
  using elem_t = int64_t;
  VertexGIdVertexAccessor() {}

  elem_t typed_eval_vertex(label_t label, vid_t v) const {
    return encode_unique_vertex_id(label, v);
  }

  RTAny eval_vertex(label_t label, vid_t v) const override {
    if (label == std::numeric_limits<label_t>::max() ||
        v == std::numeric_limits<vid_t>::max()) {
      return RTAny(DataType(DataTypeId::kNull));
    }
    auto ret = typed_eval_vertex(label, v);
    return RTAny::from_int64(ret);
  }
};

template <typename T>
class VertexPropertyVertexAccessor : public IAccessor {
 public:
  using elem_t = T;

  VertexPropertyVertexAccessor(const StorageReadInterface& graph,
                               const std::string& prop_name) {
    int label_num = graph.schema().vertex_label_num();
    for (int i = 0; i < label_num; ++i) {
      property_columns_.emplace_back(graph.template GetVertexPropColumn<T>(
          static_cast<label_t>(i), prop_name));
    }
  }

  elem_t typed_eval_vertex(label_t label, vid_t v) const {
    if (property_columns_[label] == nullptr) {
      return elem_t();
    }
    return property_columns_[label]->get_view(v);
  }

  RTAny eval_vertex(label_t label, vid_t v) const override {
    if (property_columns_[label] == nullptr) {
      return RTAny(DataType(DataTypeId::kNull));
    }
    return TypedConverter<T>::from_typed(property_columns_[label]->get_view(v));
  }

  bool is_optional() const override {
    for (auto col : property_columns_) {
      if (col == nullptr) {
        return true;
      }
    }
    return false;
  }

 private:
  using vertex_column_t =
      typename StorageReadInterface::template vertex_column_t<elem_t>;
  std::vector<std::shared_ptr<vertex_column_t>> property_columns_;
};

class EdgeIdPathAccessor : public IAccessor {
 public:
  using elem_t = EdgeRecord;
  EdgeIdPathAccessor(const Context& ctx, int tag)
      : edge_col_(*std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(tag))) {}

  elem_t typed_eval_path(size_t idx) const { return edge_col_.get_edge(idx); }

  RTAny eval_path(size_t idx) const override {
    auto e = edge_col_.get_edge(idx);
    if (e.src == std::numeric_limits<vid_t>::max() ||
        e.dst == std::numeric_limits<vid_t>::max()) {
      return RTAny(DataType(DataTypeId::kNull));
    }
    return RTAny::from_edge(e);
  }

  bool is_optional() const override { return edge_col_.is_optional(); }

 private:
  const IEdgeColumn& edge_col_;
};

class EdgeGIdPathAccessor : public IAccessor {
 public:
  using elem_t = int64_t;
  EdgeGIdPathAccessor(const Context& ctx, int tag)
      : edge_col_(*std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(tag))) {}

  elem_t typed_eval_path(size_t idx) const {
    auto edge_record = edge_col_.get_edge(idx);
    auto edge_label = generate_edge_label_id(edge_record.label.src_label,
                                             edge_record.label.dst_label,
                                             edge_record.label.edge_label);
    return encode_unique_edge_id(edge_label, edge_record.src, edge_record.dst);
  }

  RTAny eval_path(size_t idx) const override {
    if (!edge_col_.has_value(idx)) {
      return RTAny(DataType(DataTypeId::kNull));
    }
    return RTAny::from_int64(typed_eval_path(idx));
  }

  bool is_optional() const override { return edge_col_.is_optional(); }

 private:
  const IEdgeColumn& edge_col_;
};

template <typename T>
class SLEdgePropertyPathAccessor : public IAccessor {
 public:
  using elem_t = T;
  SLEdgePropertyPathAccessor(const StorageReadInterface& graph,
                             const std::string& prop_name, const Context& ctx,
                             int tag)
      : col_(*std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(tag))) {
    assert(col_.get_labels().size() == 1);
    auto label = col_.get_labels()[0];
    int prop_id = 0;
    for (auto& name : graph.schema().get_edge_property_names(
             label.src_label, label.dst_label, label.edge_label)) {
      if (name == prop_name) {
        break;
      }
      ++prop_id;
    }
    ed_accessor_ = graph.GetEdgeDataAccessor(label.src_label, label.dst_label,
                                             label.edge_label, prop_id);
  }

  RTAny eval_path(size_t idx) const override {
    auto e = col_.get_edge(idx);
    if (e.src == std::numeric_limits<vid_t>::max() ||
        e.dst == std::numeric_limits<vid_t>::max()) {
      return RTAny(DataType(DataTypeId::kNull));
    }
    T elem = ed_accessor_.get_typed_data_from_ptr<T>(e.prop);
    return TypedConverter<T>::from_typed(elem);
  }

  elem_t typed_eval_path(size_t idx) const {
    const auto& e = col_.get_edge(idx);
    return ed_accessor_.get_typed_data_from_ptr<T>(e.prop);
  }

  bool is_optional() const override { return col_.is_optional(); }

 private:
  const IEdgeColumn& col_;
  EdgeDataAccessor ed_accessor_;
};

template <typename T>
class EdgePropertyPathAccessor : public IAccessor {
 public:
  using elem_t = T;
  EdgePropertyPathAccessor(const StorageReadInterface& graph,
                           const std::string& prop_name, const Context& ctx,
                           int tag)
      : col_(*std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(tag))) {
    for (auto& label : col_.get_labels()) {
      int prop_id = 0;
      for (auto& name : graph.schema().get_edge_property_names(
               label.src_label, label.dst_label, label.edge_label)) {
        if (name == prop_name) {
          break;
        }
        ++prop_id;
      }
      ed_accessor_[label] = graph.GetEdgeDataAccessor(
          label.src_label, label.dst_label, label.edge_label, prop_id);
    }
  }

  RTAny eval_path(size_t idx) const override {
    auto e = col_.get_edge(idx);
    if (e.src == std::numeric_limits<vid_t>::max() ||
        e.dst == std::numeric_limits<vid_t>::max()) {
      return RTAny(DataType(DataTypeId::kNull));
    }
    auto it = ed_accessor_.find(e.label);
    if (it == ed_accessor_.end()) {
      return RTAny(DataType(DataTypeId::kNull));
    } else {
      T elem = it->second.template get_typed_data_from_ptr<T>(e.prop);
      return TypedConverter<T>::from_typed(elem);
    }
  }

  elem_t typed_eval_path(size_t idx) const {
    const auto& e = col_.get_edge(idx);
    return ed_accessor_.at(e.label).template get_typed_data_from_ptr<T>(e.prop);
  }

  bool is_optional() const override { return col_.is_optional(); }

 private:
  const IEdgeColumn& col_;
  std::map<LabelTriplet, EdgeDataAccessor> ed_accessor_;
};
class EdgeLabelPathAccessor : public IAccessor {
 public:
  using elem_t = std::string_view;
  EdgeLabelPathAccessor(const Schema& schema, const Context& ctx, int tag)
      : col_(*std::dynamic_pointer_cast<IEdgeColumn>(ctx.get(tag))),
        schema_(schema) {}

  RTAny eval_path(size_t idx) const override {
    if (!col_.has_value(idx)) {
      return RTAny(DataType(DataTypeId::kNull));
    }
    const auto& e = col_.get_edge(idx);
    return RTAny::from_string(schema_.get_edge_label_name(e.label.edge_label));
  }

  elem_t typed_eval_path(size_t idx) const {
    const auto& e = col_.get_edge(idx);
    return schema_.get_edge_label_name(e.label.edge_label);
  }

 private:
  const IEdgeColumn& col_;
  const Schema& schema_;
};

template <typename T>
class EdgePropertyEdgeAccessor : public IAccessor {
 public:
  using elem_t = T;
  EdgePropertyEdgeAccessor(const StorageReadInterface& graph,
                           const std::string& name) {
    label_t edge_label_num = graph.schema().edge_label_num();
    label_t vertex_label_num = graph.schema().vertex_label_num();
    for (label_t src_label = 0; src_label < vertex_label_num; ++src_label) {
      for (label_t dst_label = 0; dst_label < vertex_label_num; ++dst_label) {
        for (label_t edge_label = 0; edge_label < edge_label_num;
             ++edge_label) {
          if (!graph.schema().exist(src_label, dst_label, edge_label)) {
            continue;
          }
          const std::vector<std::string>& names =
              graph.schema().get_edge_property_names(src_label, dst_label,
                                                     edge_label);
          const std::vector<DataTypeId>& types =
              graph.schema().get_edge_properties(src_label, dst_label,
                                                 edge_label);
          for (size_t i = 0; i < names.size(); ++i) {
            if (names[i] == name && types[i] == PropUtils<T>::prop_type()) {
              LabelTriplet label{src_label, dst_label, edge_label};
              ed_accessors_[label] = graph.GetEdgeDataAccessor(
                  src_label, dst_label, edge_label, i);
              break;
            }
          }
        }
      }
    }
  }

  elem_t typed_eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                         const void* ptr) const {
    return ed_accessors_.at(label).template get_typed_data_from_ptr<T>(ptr);
  }

  RTAny eval_path(size_t idx) const override {
    LOG(FATAL) << "not supposed to reach here...";
    return RTAny();
  }

  RTAny eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    return TypedConverter<T>::from_typed(
        typed_eval_edge(label, src, dst, data_ptr));
  }

 private:
  std::map<LabelTriplet, EdgeDataAccessor> ed_accessors_;
};

template <typename T>
class ParamAccessor : public IAccessor {
 public:
  using elem_t = T;
  ParamAccessor(const std::map<std::string, std::string>& params,
                const std::string& key) {
    val_ = TypedConverter<T>::typed_from_string(params.at(key));
  }

  T typed_eval_path(size_t) const { return val_; }
  T typed_eval_vertex(label_t, vid_t) const { return val_; }
  T typed_eval_edge(const LabelTriplet&, vid_t, vid_t, const Property&) const {
    return val_;
  }

  RTAny eval_path(size_t) const override {
    return TypedConverter<T>::from_typed(val_);
  }
  RTAny eval_vertex(label_t, vid_t) const override {
    return TypedConverter<T>::from_typed(val_);
  }
  RTAny eval_edge(const LabelTriplet&, vid_t, vid_t,
                  const void*) const override {
    return TypedConverter<T>::from_typed(val_);
  }

 private:
  T val_;
};

class PathWeightPathAccessor : public IAccessor {
 public:
  using elem_t = double;
  PathWeightPathAccessor(const Context& ctx, int tag)
      : path_col_(*std::dynamic_pointer_cast<IPathColumn>(ctx.get(tag))) {}

  elem_t typed_eval_path(size_t idx) const {
    return path_col_.get_path(idx).get_weight();
  }

  RTAny eval_path(size_t idx) const override {
    return RTAny::from_double(typed_eval_path(idx));
  }

 private:
  const IPathColumn& path_col_;
};
class PathIdPathAccessor : public IAccessor {
 public:
  using elem_t = Path;
  PathIdPathAccessor(const Context& ctx, int tag)
      : path_col_(*std::dynamic_pointer_cast<IPathColumn>(ctx.get(tag))) {}

  elem_t typed_eval_path(size_t idx) const { return path_col_.get_path(idx); }

  RTAny eval_path(size_t idx) const override { return path_col_.get_elem(idx); }

 private:
  const IPathColumn& path_col_;
};

class PathLenPathAccessor : public IAccessor {
 public:
  using elem_t = int64_t;
  PathLenPathAccessor(const Context& ctx, int tag)
      : path_col_(*std::dynamic_pointer_cast<IPathColumn>(ctx.get(tag))) {}

  elem_t typed_eval_path(size_t idx) const {
    return path_col_.get_path_length(idx);
  }

  RTAny eval_path(size_t idx) const override {
    return RTAny::from_int64(static_cast<int64_t>(typed_eval_path(idx)));
  }

 private:
  const IPathColumn& path_col_;
};

template <typename T>
class ConstAccessor : public IAccessor {
 public:
  using elem_t = T;
  explicit ConstAccessor(const T& val) : val_(val) {}

  T typed_eval_path(size_t) const { return val_; }
  T typed_eval_vertex(label_t, vid_t, size_t) const { return val_; }
  T typed_eval_edge(const LabelTriplet&, vid_t, vid_t, const Property&,
                    size_t) const {
    return val_;
  }

  RTAny eval_path(size_t) const override {
    return TypedConverter<T>::from_typed(val_);
  }

  RTAny eval_vertex(label_t, vid_t) const override {
    return TypedConverter<T>::from_typed(val_);
  }

  RTAny eval_edge(const LabelTriplet&, vid_t, vid_t,
                  const void* data_ptr) const override {
    return TypedConverter<T>::from_typed(val_);
  }

 private:
  T val_;
};

std::shared_ptr<IAccessor> create_context_value_accessor(const Context& ctx,
                                                         int tag,
                                                         const DataType& type);

std::shared_ptr<IAccessor> create_vertex_property_path_accessor(
    const StorageReadInterface& graph, const Context& ctx, int tag,
    const DataType& type, const std::string& prop_name);

std::shared_ptr<IAccessor> create_vertex_property_vertex_accessor(
    const StorageReadInterface& graph, const DataType& type,
    const std::string& prop_name);

std::shared_ptr<IAccessor> create_vertex_label_path_accessor(
    const Schema& schema, const Context& ctx, int tag);

std::shared_ptr<IAccessor> create_edge_property_path_accessor(
    const StorageReadInterface& graph, const std::string& prop_name,
    const Context& ctx, int tag, const DataType& type);

std::shared_ptr<IAccessor> create_edge_label_path_accessor(const Schema& schema,
                                                           const Context& ctx,
                                                           int tag);

std::shared_ptr<IAccessor> create_edge_property_edge_accessor(
    const StorageReadInterface& graph, const std::string& prop_name,
    const DataType& type);

}  // namespace runtime

}  // namespace gs
