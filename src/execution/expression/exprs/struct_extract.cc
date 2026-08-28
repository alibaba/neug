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

#include "neug/execution/expression/exprs/struct_extract.h"

#include "neug/common/types/value.h"
#include "neug/utils/property/struct_property_column.h"

namespace neug {
namespace execution {
namespace {

// Narrows per-label struct ref columns to their `field_idx` child columns,
// preserving null columns (a label without the property). Returns an empty
// vector when any present column is not a struct ref column, meaning pushdown
// does not apply and the caller falls back to whole-struct evaluation.
std::vector<std::shared_ptr<RefColumnBase>> narrow_to_field(
    const std::vector<std::shared_ptr<RefColumnBase>>& parent_columns,
    size_t field_idx) {
  std::vector<std::shared_ptr<RefColumnBase>> field_columns;
  field_columns.reserve(parent_columns.size());
  for (const auto& column : parent_columns) {
    if (column == nullptr) {
      field_columns.push_back(nullptr);
      continue;
    }
    auto* struct_column =
        dynamic_cast<const StructPropertyRefColumn*>(column.get());
    if (struct_column == nullptr) {
      return {};
    }
    field_columns.push_back(struct_column->field_ref_ptr(field_idx));
  }
  return field_columns;
}

Value read_field(const std::vector<std::shared_ptr<RefColumnBase>>& columns,
                 size_t label, vid_t vid, const DataType& type) {
  if (label >= columns.size() || columns[label] == nullptr) {
    return Value(type);  // the label has no such property
  }
  return columns[label]->get_any(vid);
}

// Reads one struct field column per vertex label. Nested struct fields recurse
// through bind_struct_field, so a multi-level access only scans the leaf
// column.
class BindedVertexStructFieldExpr : public VertexExprBase {
 public:
  BindedVertexStructFieldExpr(
      std::vector<std::shared_ptr<RefColumnBase>> field_columns,
      const DataType& type)
      : field_columns_(std::move(field_columns)), type_(type) {}

  Value eval_vertex(label_t v_label, vid_t v) const override {
    return read_field(field_columns_, v_label, v, type_);
  }
  const DataType& type() const override { return type_; }
  std::unique_ptr<BindedExprBase> bind_struct_field(
      size_t field_idx, const DataType& field_type) const override {
    return bind_vertex_struct_field(field_columns_, field_idx, field_type);
  }

 private:
  std::vector<std::shared_ptr<RefColumnBase>> field_columns_;
  DataType type_;
};

// Record-context counterpart: resolves the vertex from the chunk, then reads
// its field column.
class BindedRecordVertexStructFieldExpr : public RecordExprBase {
 public:
  BindedRecordVertexStructFieldExpr(
      int tag, std::vector<std::shared_ptr<RefColumnBase>> field_columns,
      const DataType& type)
      : tag_(tag), field_columns_(std::move(field_columns)), type_(type) {}

  Value eval_record(const DataChunk& chunk, size_t idx) const override {
    const auto& vertex_val = chunk.get(tag_)->get_elem(idx);
    if (vertex_val.IsNull()) {
      return Value(type_);
    }
    vertex_t vertex = vertex_val.GetValue<vertex_t>();
    return read_field(field_columns_, vertex.label(), vertex.vid(), type_);
  }
  const DataType& type() const override { return type_; }
  std::unique_ptr<BindedExprBase> bind_struct_field(
      size_t field_idx, const DataType& field_type) const override {
    return bind_record_vertex_struct_field(tag_, field_columns_, field_idx,
                                           field_type);
  }

 private:
  int tag_;
  std::vector<std::shared_ptr<RefColumnBase>> field_columns_;
  DataType type_;
};

// Default path used when the child is not a plain struct column read (e.g. an
// edge property or a computed struct): evaluate the struct value, then pick
// the field.
class BindedStructExtractExpr : public VertexExprBase,
                                public EdgeExprBase,
                                public RecordExprBase {
 public:
  BindedStructExtractExpr(std::unique_ptr<BindedExprBase> child,
                          size_t field_idx, const DataType& type)
      : child_(std::move(child)), field_idx_(field_idx), type_(type) {}

  const DataType& type() const override { return type_; }
  Value eval_vertex(label_t v_label, vid_t v) const override {
    return extract(child_->Cast<VertexExprBase>().eval_vertex(v_label, v));
  }
  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    return extract(
        child_->Cast<EdgeExprBase>().eval_edge(label, src, dst, data_ptr));
  }
  Value eval_record(const DataChunk& chunk, size_t idx) const override {
    return extract(child_->Cast<RecordExprBase>().eval_record(chunk, idx));
  }

 private:
  Value extract(const Value& struct_val) const {
    if (struct_val.IsNull()) {
      return Value(type_);
    }
    return StructValue::GetChildren(struct_val)[field_idx_];
  }

  std::unique_ptr<BindedExprBase> child_;
  size_t field_idx_;
  DataType type_;
};

}  // namespace

StructExtractExpr::StructExtractExpr(std::unique_ptr<ExprBase> child,
                                     size_t field_idx, DataType field_type)
    : child_(std::move(child)),
      field_idx_(field_idx),
      type_(std::move(field_type)) {}

std::unique_ptr<BindedExprBase> StructExtractExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  auto bound_child = child_->bind(storage, params);
  if (auto pushed_down = bound_child->bind_struct_field(field_idx_, type_)) {
    return pushed_down;
  }
  return std::make_unique<BindedStructExtractExpr>(std::move(bound_child),
                                                   field_idx_, type_);
}

std::unique_ptr<BindedExprBase> bind_vertex_struct_field(
    const std::vector<std::shared_ptr<RefColumnBase>>& parent_columns,
    size_t field_idx, const DataType& field_type) {
  auto field_columns = narrow_to_field(parent_columns, field_idx);
  if (field_columns.empty()) {
    return nullptr;
  }
  return std::make_unique<BindedVertexStructFieldExpr>(std::move(field_columns),
                                                       field_type);
}

std::unique_ptr<BindedExprBase> bind_record_vertex_struct_field(
    int tag, const std::vector<std::shared_ptr<RefColumnBase>>& parent_columns,
    size_t field_idx, const DataType& field_type) {
  auto field_columns = narrow_to_field(parent_columns, field_idx);
  if (field_columns.empty()) {
    return nullptr;
  }
  return std::make_unique<BindedRecordVertexStructFieldExpr>(
      tag, std::move(field_columns), field_type);
}

}  // namespace execution
}  // namespace neug
