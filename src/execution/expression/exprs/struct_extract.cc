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

#include <map>

#include "neug/common/types/value.h"
#include "neug/utils/property/struct_property_column.h"

namespace neug {
namespace execution {
namespace {

// Shared field selection for vertex and edge refs. Missing vertex properties
// remain null; mismatched layouts cause the caller to fall back.
bool select_struct_field(const std::shared_ptr<RefColumnBase>& column,
                         const DataType& struct_type, size_t field_idx,
                         std::shared_ptr<RefColumnBase>& field) {
  if (field_idx >= StructType::GetNumFields(struct_type)) {
    return false;
  }
  if (!column) {
    field.reset();
    return true;
  }
  auto* ref = dynamic_cast<const StructPropertyRefColumn*>(column.get());
  if (!ref || ref->struct_type() != struct_type) {
    return false;
  }
  field = ref->field_ref_ptr(field_idx);
  return true;
}

// Narrows per-label struct ref columns to their `field_idx` child columns,
// preserving null columns (a label without the property). Returns an empty
// vector when any present column has a different struct layout, meaning
// pushdown does not apply and the caller falls back to whole-struct evaluation.
std::vector<std::shared_ptr<RefColumnBase>> narrow_to_field(
    const std::vector<std::shared_ptr<RefColumnBase>>& parent_columns,
    const DataType& struct_type, size_t field_idx) {
  std::vector<std::shared_ptr<RefColumnBase>> field_columns;
  if (field_idx >= StructType::GetNumFields(struct_type)) {
    return field_columns;
  }
  field_columns.reserve(parent_columns.size());
  for (const auto& column : parent_columns) {
    std::shared_ptr<RefColumnBase> field;
    if (!select_struct_field(column, struct_type, field_idx, field)) {
      return {};
    }
    field_columns.push_back(std::move(field));
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
    return bind_vertex_struct_field(field_columns_, type_, field_idx,
                                    field_type);
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
    return bind_record_vertex_struct_field(tag_, field_columns_, type_,
                                           field_idx, field_type);
  }

 private:
  int tag_;
  std::vector<std::shared_ptr<RefColumnBase>> field_columns_;
  DataType type_;
};

using EdgeColumns = std::map<LabelTriplet, std::shared_ptr<RefColumnBase>>;

EdgeColumns narrow_edge_to_field(const EdgeColumns& columns,
                                 const DataType& struct_type,
                                 size_t field_idx) {
  EdgeColumns fields;
  for (const auto& [label, column] : columns) {
    std::shared_ptr<RefColumnBase> field;
    // A null edge ref denotes bundled storage, which cannot be narrowed.
    if (!column ||
        !select_struct_field(column, struct_type, field_idx, field)) {
      return {};
    }
    fields.emplace(label, std::move(field));
  }
  return fields;
}

// Edge-context counterpart of BindedVertexStructFieldExpr: reads only the
// field's child column through the narrowed per-triplet accessor.
class BindedEdgeStructFieldExpr : public EdgeExprBase {
 public:
  BindedEdgeStructFieldExpr(EdgeColumns field_accessors, const DataType& type)
      : field_accessors_(std::move(field_accessors)), type_(type) {}

  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    auto it = field_accessors_.find(label);
    if (it == field_accessors_.end()) {
      return Value(type_);  // the triplet has no such property
    }
    return it->second->get_any(*static_cast<const size_t*>(data_ptr));
  }
  const DataType& type() const override { return type_; }
  std::unique_ptr<BindedExprBase> bind_struct_field(
      size_t field_idx, const DataType& field_type) const override {
    auto fields = narrow_edge_to_field(field_accessors_, type_, field_idx);
    if (fields.empty()) {
      return nullptr;
    }
    return std::make_unique<BindedEdgeStructFieldExpr>(std::move(fields),
                                                       field_type);
  }

 private:
  EdgeColumns field_accessors_;
  DataType type_;
};

// Record-context counterpart: resolves the edge from the chunk, then reads
// its field column.
class BindedRecordEdgeStructFieldExpr : public RecordExprBase {
 public:
  BindedRecordEdgeStructFieldExpr(int tag, EdgeColumns field_accessors,
                                  const DataType& type)
      : tag_(tag), field_accessors_(std::move(field_accessors)), type_(type) {}

  Value eval_record(const DataChunk& chunk, size_t idx) const override {
    const auto& edge_val = chunk.get(tag_)->get_elem(idx);
    if (edge_val.IsNull()) {
      return Value(type_);
    }
    edge_t edge = edge_val.GetValue<edge_t>();
    auto it = field_accessors_.find(edge.label);
    if (it == field_accessors_.end()) {
      return Value(type_);
    }
    return it->second->get_any(*static_cast<const size_t*>(edge.prop));
  }
  const DataType& type() const override { return type_; }
  std::unique_ptr<BindedExprBase> bind_struct_field(
      size_t field_idx, const DataType& field_type) const override {
    auto fields = narrow_edge_to_field(field_accessors_, type_, field_idx);
    if (fields.empty()) {
      return nullptr;
    }
    return std::make_unique<BindedRecordEdgeStructFieldExpr>(
        tag_, std::move(fields), field_type);
  }

 private:
  int tag_;
  EdgeColumns field_accessors_;
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
    const DataType& struct_type, size_t field_idx, const DataType& field_type) {
  auto field_columns = narrow_to_field(parent_columns, struct_type, field_idx);
  if (field_columns.empty()) {
    return nullptr;
  }
  return std::make_unique<BindedVertexStructFieldExpr>(std::move(field_columns),
                                                       field_type);
}

std::unique_ptr<BindedExprBase> bind_record_vertex_struct_field(
    int tag, const std::vector<std::shared_ptr<RefColumnBase>>& parent_columns,
    const DataType& struct_type, size_t field_idx, const DataType& field_type) {
  auto field_columns = narrow_to_field(parent_columns, struct_type, field_idx);
  if (field_columns.empty()) {
    return nullptr;
  }
  return std::make_unique<BindedRecordVertexStructFieldExpr>(
      tag, std::move(field_columns), field_type);
}

std::unique_ptr<BindedExprBase> bind_edge_struct_field(
    const std::map<LabelTriplet, std::shared_ptr<RefColumnBase>>& accessors,
    const DataType& struct_type, size_t field_idx, const DataType& field_type) {
  auto field_accessors =
      narrow_edge_to_field(accessors, struct_type, field_idx);
  if (field_accessors.empty()) {
    return nullptr;
  }
  return std::make_unique<BindedEdgeStructFieldExpr>(std::move(field_accessors),
                                                     field_type);
}

std::unique_ptr<BindedExprBase> bind_record_edge_struct_field(
    int tag,
    const std::map<LabelTriplet, std::shared_ptr<RefColumnBase>>& accessors,
    const DataType& struct_type, size_t field_idx, const DataType& field_type) {
  auto field_accessors =
      narrow_edge_to_field(accessors, struct_type, field_idx);
  if (field_accessors.empty()) {
    return nullptr;
  }
  return std::make_unique<BindedRecordEdgeStructFieldExpr>(
      tag, std::move(field_accessors), field_type);
}

}  // namespace execution
}  // namespace neug
