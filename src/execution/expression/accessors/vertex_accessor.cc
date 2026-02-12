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

#include "neug/execution/expression/accessors/vertex_accessor.h"

namespace neug {
namespace execution {

class BindedVertexPropertyAccessor : public VertexExprBase {
 public:
  BindedVertexPropertyAccessor(const StorageReadInterface& graph,
                               const std::string& prop_name,
                               const DataType& type)
      : type_(type) {
    int32_t label_num = graph.schema().vertex_label_frontier();
    for (label_t label = 0; label < label_num; ++label) {
      if (!graph.schema().vertex_label_valid(label)) {
        continue;
      }
      property_columns_.emplace_back(
          graph.GetVertexPropColumn(static_cast<label_t>(label), prop_name));
    }
  }

  Value eval_vertex(label_t v_label, vid_t v) const override {
    if (property_columns_[v_label] == nullptr) {
      return Value(type_);  // return null value
    }
    auto val = property_columns_[v_label]->get(v);
    return property_to_value(val);
  }

  const DataType& type() const override { return type_; }

 private:
  DataType type_;
  std::vector<std::shared_ptr<RefColumnBase>> property_columns_;
};

class BindedVertexLabelAccessor : public VertexExprBase {
 public:
  explicit BindedVertexLabelAccessor(const IStorageInterface& graph)
      : schema_(graph.schema()) {}

  std::string typed_eval_vertex(label_t v_label, vid_t v_id) const {
    return schema_.get_vertex_label_name(v_label);
  }

  Value eval_vertex(label_t v_label, vid_t v_id) const override {
    return Value::STRING(schema_.get_vertex_label_name(v_label));
  }
  const DataType& type() const override { return type_; }

 private:
  DataType type_;
  const Schema& schema_;
};

class VertexGIdVertexAccessor : public VertexExprBase {
 public:
  explicit VertexGIdVertexAccessor() { type_ = DataType(DataTypeId::kInt64); }

  int64_t typed_eval_vertex(label_t v_label, vid_t v_id) const {
    return encode_unique_vertex_id(v_label, v_id);
  }

  Value eval_vertex(label_t v_label, vid_t v_id) const override {
    return Value::CreateValue<int64_t>(encode_unique_vertex_id(v_label, v_id));
  }
  const DataType& type() const override { return type_; }

 private:
  DataType type_;
};

std::unique_ptr<BindedExprBase> VertexAccessor::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  switch (access_type_) {
  case GraphAccessType::kProperty: {
    return std::make_unique<BindedVertexPropertyAccessor>(
        dynamic_cast<const StorageReadInterface&>(*storage), property_name_,
        type_);
  }
  case GraphAccessType::kLabel: {
    return std::make_unique<BindedVertexLabelAccessor>(*storage);
  }
  case GraphAccessType::kGid: {
    return std::make_unique<VertexGIdVertexAccessor>();
  }
  default:
    LOG(FATAL) << "Unknown GraphAccessType: " << static_cast<int>(access_type_);
    break;
  }
  return nullptr;
}

}  // namespace execution
}  // namespace neug