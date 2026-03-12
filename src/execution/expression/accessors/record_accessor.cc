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

#include "neug/execution/expression/accessors/record_accessor.h"
#include "neug/execution/common/columns/i_context_column.h"

namespace neug {
namespace execution {

class BindedRecordAccessor : public RecordExprBase {
 public:
  BindedRecordAccessor(int tag, const DataType& type)
      : tag_(tag), type_(type) {}

  Value eval_record(const Context& ctx, size_t idx) const override {
    return ctx.get(tag_)->get_elem(idx);
  }

  const DataType& type() const override { return type_; }

 private:
  int tag_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> RecordAccessor::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  return std::make_unique<BindedRecordAccessor>(tag_, type_);
}

class BindedRecordVertexPropertyExpr : public RecordExprBase {
 public:
  BindedRecordVertexPropertyExpr(int tag, const IStorageInterface& storage,
                                 const std::string& property_name,
                                 const DataType& type)
      : tag_(tag), property_name_(property_name), type_(type) {
    const auto& storage_interface =
        dynamic_cast<const StorageReadInterface&>(storage);
    property_columns_.reserve(storage.schema().vertex_label_frontier());
    for (label_t label = 0; label < storage.schema().vertex_label_frontier();
         ++label) {
      if (!storage.schema().vertex_label_valid(label)) {
        continue;
      }
      property_columns_.emplace_back(
          storage_interface.GetVertexPropColumn(label, property_name_));
    }
  }

  Value eval_record(const Context& ctx, size_t idx) const override {
    const auto& vertex_val = ctx.get(tag_)->get_elem(idx);
    if (vertex_val.IsNull()) {
      return Value(type_);
    }
    vertex_t vertex = vertex_val.GetValue<vertex_t>();
    vid_t vid = static_cast<vid_t>(vertex.vid());
    label_t vlabel = static_cast<label_t>(vertex.label());

    auto column = property_columns_[vlabel];
    if (column == nullptr) {
      return Value(type_);  // return null value
    }

    return property_to_value(column->get(vid));
  }

  const DataType& type() const override { return type_; }

 private:
  int tag_;
  std::string property_name_;
  DataType type_;
  std::vector<std::shared_ptr<RefColumnBase>> property_columns_;
};

class BindedRecordVertexLabelExpr : public RecordExprBase {
 public:
  BindedRecordVertexLabelExpr(int tag, const Schema& schema)
      : tag_(tag), schema_(schema), type_(DataTypeId::kVarchar) {}
  Value eval_record(const Context& ctx, size_t idx) const override {
    Value vertex_val = ctx.get(tag_)->get_elem(idx);
    if (vertex_val.IsNull()) {
      return Value(type_);
    }
    vertex_t vertex = vertex_val.GetValue<vertex_t>();
    return Value::STRING(schema_.get_vertex_label_name(vertex.label()));
  }

  const DataType& type() const override { return type_; }

 private:
  int tag_;
  const Schema& schema_;
  DataType type_;
};

class BindedRecordVertexGIdExpr : public RecordExprBase {
 public:
  BindedRecordVertexGIdExpr(int tag) : tag_(tag), type_(DataTypeId::kInt64) {}
  Value eval_record(const Context& ctx, size_t idx) const override {
    Value vertex_val = ctx.get(tag_)->get_elem(idx);
    if (vertex_val.IsNull()) {
      return Value(type_);
    }
    vertex_t vertex = vertex_val.GetValue<vertex_t>();
    int64_t gid = encode_unique_vertex_id(static_cast<label_t>(vertex.label()),
                                          static_cast<vid_t>(vertex.vid()));
    return Value::CreateValue<int64_t>(gid);
  }

  const DataType& type() const override { return type_; }

 private:
  int tag_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> RecordVertexAccessor::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  switch (access_type_) {
  case GraphAccessType::kProperty:
    return std::make_unique<BindedRecordVertexPropertyExpr>(
        tag_, *storage, property_name_, data_type_);
  case GraphAccessType::kLabel:
    return std::make_unique<BindedRecordVertexLabelExpr>(tag_,
                                                         storage->schema());
  case GraphAccessType::kGid:
    return std::make_unique<BindedRecordVertexGIdExpr>(tag_);
  default:
    LOG(FATAL) << "Unknown RecordVertexAccessor GraphAccessType: "
               << static_cast<int>(access_type_);
    break;
  }
  return nullptr;
}

class BindedEdgeRecordPropertyExpr : public RecordExprBase {
 public:
  BindedEdgeRecordPropertyExpr(int tag, const IStorageInterface& storage,
                               const std::string& prop_name,
                               const DataType& type)
      : tag_(tag), type_(type) {
    const auto& graph = dynamic_cast<const StorageReadInterface&>(storage);
    label_t edge_label_num = graph.schema().edge_label_frontier();
    label_t vertex_label_num = graph.schema().vertex_label_frontier();
    for (label_t src_label = 0; src_label < vertex_label_num; ++src_label) {
      if (!graph.schema().vertex_label_valid(src_label)) {
        continue;
      }
      for (label_t dst_label = 0; dst_label < vertex_label_num; ++dst_label) {
        if (!graph.schema().vertex_label_valid(dst_label)) {
          continue;
        }
        for (label_t edge_label = 0; edge_label < edge_label_num;
             ++edge_label) {
          if (!graph.schema().exist(src_label, dst_label, edge_label)) {
            continue;
          }
          const std::vector<std::string>& names =
              graph.schema().get_edge_property_names(src_label, dst_label,
                                                     edge_label);
          for (size_t i = 0; i < names.size(); ++i) {
            if (names[i] == prop_name) {
              LabelTriplet label{src_label, dst_label, edge_label};
              edge_accessors_[label] = graph.GetEdgeDataAccessor(
                  src_label, dst_label, edge_label, i);
              break;
            }
          }
        }
      }
    }
  }
  Value eval_record(const Context& ctx, size_t idx) const override {
    const auto& edge_val = ctx.get(tag_)->get_elem(idx);
    if (edge_val.IsNull()) {
      return Value(type_);
    }
    edge_t edge = edge_val.GetValue<edge_t>();
    LabelTriplet label = edge.label;
    auto it = edge_accessors_.find(label);
    if (it == edge_accessors_.end()) {
      return Value(type_);  // return null value
    }
    auto accessor = it->second;
    auto prop = accessor.get_data_from_ptr(edge.prop);
    return property_to_value(prop);
  }

  const DataType& type() const override { return type_; }

 private:
  int tag_;
  DataType type_;
  std::map<LabelTriplet, EdgeDataAccessor> edge_accessors_;
};

class BindedEdgeRecordLabelExpr : public RecordExprBase {
 public:
  BindedEdgeRecordLabelExpr(int tag, const Schema& schema)
      : tag_(tag), schema_(schema), type_(DataTypeId::kVarchar) {}
  Value eval_record(const Context& ctx, size_t idx) const override {
    Value edge_val = ctx.get(tag_)->get_elem(idx);
    if (edge_val.IsNull()) {
      return Value(type_);
    }
    edge_t edge = edge_val.GetValue<edge_t>();
    return Value::STRING(schema_.get_edge_label_name(edge.label.edge_label));
  }

  const DataType& type() const override { return type_; }

 private:
  int tag_;
  const Schema& schema_;
  DataType type_;
};

class BindedEdgeRecordGIdExpr : public RecordExprBase {
 public:
  BindedEdgeRecordGIdExpr(int tag) : tag_(tag), type_(DataTypeId::kInt64) {}
  Value eval_record(const Context& ctx, size_t idx) const override {
    const auto& edge_val = ctx.get(tag_)->get_elem(idx);
    if (edge_val.IsNull()) {
      return Value(type_);
    }
    edge_t edge = edge_val.GetValue<edge_t>();
    auto label = generate_edge_label_id(
        edge.label.src_label, edge.label.dst_label, edge.label.edge_label);
    int64_t gid = encode_unique_edge_id(label, static_cast<vid_t>(edge.src),
                                        static_cast<vid_t>(edge.dst));
    return Value::CreateValue<int64_t>(gid);
  }

  const DataType& type() const override { return type_; }

 private:
  int tag_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> RecordEdgeAccessor::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  switch (access_type_) {
  case GraphAccessType::kProperty:
    return std::make_unique<BindedEdgeRecordPropertyExpr>(
        tag_, *storage, property_name_, data_type_);
  case GraphAccessType::kLabel:
    return std::make_unique<BindedEdgeRecordLabelExpr>(tag_, storage->schema());
  case GraphAccessType::kGid:
    return std::make_unique<BindedEdgeRecordGIdExpr>(tag_);
  default:
    LOG(FATAL) << "Unknown RecordEdgeAccessor GraphAccessType: "
               << static_cast<int>(access_type_);
    break;
  }
  return nullptr;
}

class BindedRecordPathLengthExpr : public RecordExprBase {
 public:
  BindedRecordPathLengthExpr(int tag) : tag_(tag), type_(DataTypeId::kInt64) {}
  Value eval_record(const Context& ctx, size_t idx) const override {
    Value path_val = ctx.get(tag_)->get_elem(idx);
    if (path_val.IsNull()) {
      return Value(type_);
    }
    const auto& path = PathValue::Get(path_val);
    return Value::CreateValue<int64_t>(static_cast<int64_t>(path.length()));
  }

  const DataType& type() const override { return type_; }

 private:
  int tag_;
  DataType type_;
};

class BindedPathWeightExpr : public RecordExprBase {
 public:
  BindedPathWeightExpr(int tag) : tag_(tag), type_(DataTypeId::kDouble) {}
  Value eval_record(const Context& ctx, size_t idx) const override {
    Value path_val = ctx.get(tag_)->get_elem(idx);
    if (path_val.IsNull()) {
      return Value(type_);
    }
    const auto& path = PathValue::Get(path_val);
    return Value::CreateValue<double>(path.get_weight());
  }

  const DataType& type() const override { return type_; }

 private:
  int tag_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> RecordPathAccessor::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  if (property_ == "length") {
    return std::make_unique<BindedRecordPathLengthExpr>(tag_);
  } else if (property_ == "cost") {
    return std::make_unique<BindedPathWeightExpr>(tag_);
  }
  LOG(FATAL) << "Unknown RecordPathAccessor property: " << property_;
  return nullptr;
}

}  // namespace execution
}  // namespace neug