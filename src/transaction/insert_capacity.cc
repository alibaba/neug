/** Copyright 2026 Alibaba Group Holding Limited.
 * Licensed under the Apache License, Version 2.0.
 */
#include "neug/transaction/insert_capacity.h"
#include <algorithm>
#include <limits>
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/index/storage_index_manager.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/property/array_column.h"
#include "neug/utils/property/default_value.h"
#include "neug/utils/property/list_property_column.h"
#include "neug/utils/property/struct_property_column.h"
#include "neug/utils/property/vec_column.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {
#ifdef NEUG_BUILD_TEST
namespace {
thread_local int reservation_failures = 0;
}
void SetInsertReservationFailuresForTest(int count) {
  reservation_failures = count;
}
#endif

namespace {
size_t AddSize(size_t a, size_t b) {
  if (b > std::numeric_limits<size_t>::max() - a)
    THROW_STORAGE_EXCEPTION("Insert capacity overflow");
  return a + b;
}
// Reserve 25% headroom over the complete requirement, with a minimum
// capacity of 4096. Call only when the existing capacity is insufficient.
size_t Target(size_t required) {
  return std::max(size_t{4096}, AddSize(required, required / 4));
}
void Check(const Status& status) {
  if (!status.ok())
    THROW_STORAGE_EXCEPTION(status.ToString());
}
void Validate(const Table& table, const InsertCapacityRequest::Need& need) {
  if (table.col_num() != need.types.size())
    THROW_STORAGE_EXCEPTION("Schema changed during insert capacity retry");
  for (size_t i = 0; i < need.types.size(); ++i) {
    if (table.get_column_by_id(i)->type() != need.types[i].id())
      THROW_STORAGE_EXCEPTION("Property type changed during capacity retry");
  }
}
}  // namespace

void InsertCapacityRequest::AddValue(ColumnNeed& n, const Value& value) {
  n.count = AddSize(n.count, 1);
  Value fallback;
  if (value.IsNull())
    fallback = get_default_value(value.type());
  const Value& normalized = value.IsNull() ? fallback : value;
  switch (normalized.type().id()) {
  case DataTypeId::kVarchar:
    n.bytes = AddSize(n.bytes, normalized.GetValue<std::string_view>().size());
    break;
  case DataTypeId::kList:
  case DataTypeId::kArray: {
    n.children.resize(1);
    const auto& children = normalized.type().id() == DataTypeId::kList
                               ? ListValue::GetChildren(normalized)
                               : ArrayValue::GetChildren(normalized);
    for (const auto& child : children)
      AddValue(n.children[0], child);
    break;
  }
  case DataTypeId::kStruct: {
    const auto& children = StructValue::GetChildren(normalized);
    n.children.resize(children.size());
    for (size_t i = 0; i < children.size(); ++i)
      AddValue(n.children[i], children[i]);
    break;
  }
  default:
    break;
  }
}
void InsertCapacityRequest::MergeColumn(ColumnNeed& n,
                                        const ColumnNeed& other) {
  n.count = AddSize(n.count, other.count);
  n.bytes = AddSize(n.bytes, other.bytes);
  n.children.resize(std::max(n.children.size(), other.children.size()));
  for (size_t i = 0; i < other.children.size(); ++i)
    MergeColumn(n.children[i], other.children[i]);
}
void InsertCapacityRequest::SubtractColumn(ColumnNeed& n,
                                           const ColumnNeed& other) noexcept {
  CHECK_GE(n.count, other.count);
  CHECK_GE(n.bytes, other.bytes);
  n.count -= other.count;
  n.bytes -= other.bytes;
  for (size_t i = 0; i < other.children.size(); ++i)
    SubtractColumn(n.children[i], other.children[i]);
}
void InsertCapacityRequest::Add(Need& n, const std::vector<Value>& props) {
  n.rows = AddSize(n.rows, 1);
  if (n.types.empty()) {
    for (const auto& p : props)
      n.types.push_back(p.type());
    n.columns.resize(props.size());
  }
  for (size_t i = 0; i < props.size(); ++i)
    AddValue(n.columns[i], props[i]);
}
void InsertCapacityRequest::AddVertex(const std::string& label,
                                      const Value& key,
                                      const std::vector<Value>& props) {
  auto& n = vertices_[label];
  Add(n, props);
  if (key.type().id() == DataTypeId::kVarchar)
    n.key_bytes = AddSize(n.key_bytes, key.GetValue<std::string_view>().size());
}
void InsertCapacityRequest::AddEdge(const std::string& src,
                                    const std::string& dst,
                                    const std::string& edge,
                                    const std::vector<Value>& props) {
  Add(edges_[{src, dst, edge}], props);
}
InsertCapacityRequest InsertCapacityRequest::FromWal(char* data,
                                                     size_t length) {
  InsertCapacityRequest request;
  OutArchive arc;
  arc.SetSlice(data, length);
  while (!arc.Empty()) {
    OpType op;
    arc >> op;
    if (op == OpType::kInsertVertex) {
      InsertVertexRedo r;
      arc >> r;
      request.AddVertex(r.vertex_type, r.oid, r.props);
    } else if (op == OpType::kInsertEdge) {
      InsertEdgeRedo r;
      arc >> r;
      request.AddEdge(r.src_type, r.dst_type, r.edge_type, r.properties);
    } else {
      THROW_STORAGE_EXCEPTION("Invalid insert WAL operation");
    }
  }
  return request;
}
bool InsertCapacityRequest::ColumnFits(ColumnBase& col, size_t rows,
                                       const ColumnNeed& need) {
  if (auto* vec = dynamic_cast<VecColumn*>(&col)) {
    return rows <= vec->size() &&
           AddSize(vec->get_offset_accessor()->GetNextIndexID(), need.count) <=
               vec->buffer_size();
  }
  if (rows > col.size())
    return false;
  if (auto* str = dynamic_cast<StringColumn*>(&col))
    return need.bytes <= str->available_space();
  const ColumnNeed empty;
  const auto& child = need.children.empty() ? empty : need.children[0];
  if (auto* list = dynamic_cast<ListPropertyColumn*>(&col)) {
    return ColumnFits(
        *list->elements_,
        AddSize(list->elements_tail_.load(std::memory_order_relaxed),
                child.count),
        child);
  }
  if (auto* array = dynamic_cast<ArrayColumn*>(&col)) {
    if (array->array_size_ &&
        rows > std::numeric_limits<size_t>::max() / array->array_size_)
      THROW_STORAGE_EXCEPTION("Insert array capacity overflow");
    return ColumnFits(*array->child_column_, rows * array->array_size_, child);
  }
  if (auto* tuple = dynamic_cast<StructPropertyColumn*>(&col)) {
    for (size_t i = 0; i < tuple->fields_.size(); ++i)
      if (!ColumnFits(*tuple->fields_[i], rows,
                      i < need.children.size() ? need.children[i] : empty))
        return false;
  }
  return true;
}
void InsertCapacityRequest::GrowColumn(ColumnBase& col, size_t rows,
                                       const ColumnNeed& need) {
  if (ColumnFits(col, rows, need))
    return;
  if (dynamic_cast<VecColumn*>(&col))
    THROW_STORAGE_EXCEPTION(
        "Insert-only VecColumn growth is not supported; use update mode");
  const auto target_rows = rows > col.size() ? Target(rows) : col.size();
  const ColumnNeed empty;
  const auto& child = need.children.empty() ? empty : need.children[0];
  if (auto* str = dynamic_cast<StringColumn*>(&col)) {
    auto append_bytes = need.bytes;
    const auto available = str->available_space();
    if (need.bytes > available) {
      const auto capacity = str->data_capacity();
      const auto used = capacity - available;
      // Grow total payload capacity geometrically, then translate back to
      // StringColumn::resize's additional-bytes argument.
      append_bytes = Target(AddSize(used, need.bytes)) - used;
    }
    str->resize(target_rows, append_bytes);
  } else if (auto* list = dynamic_cast<ListPropertyColumn*>(&col)) {
    if (target_rows > list->size())
      list->resize(target_rows);
    GrowColumn(*list->elements_,
               AddSize(list->elements_tail_.load(std::memory_order_relaxed),
                       child.count),
               child);
  } else if (auto* array = dynamic_cast<ArrayColumn*>(&col)) {
    if (array->array_size_ &&
        target_rows > std::numeric_limits<size_t>::max() / array->array_size_)
      THROW_STORAGE_EXCEPTION("Insert array capacity overflow");
    GrowColumn(*array->child_column_, target_rows * array->array_size_, child);
    array->size_ = target_rows;
  } else if (auto* tuple = dynamic_cast<StructPropertyColumn*>(&col)) {
    for (size_t i = 0; i < tuple->fields_.size(); ++i)
      GrowColumn(*tuple->fields_[i], target_rows,
                 i < need.children.size() ? need.children[i] : empty);
  } else if (rows > col.size()) {
    col.resize(target_rows);
  }
}
void InsertCapacityRequest::ValidateGrowthSupport(PropertyGraph& graph) const {
  auto check_columns = [](Table& table, size_t rows, size_t count,
                          bool table_grows) {
    for (size_t i = 0; i < table.col_num(); ++i) {
      auto* vec = dynamic_cast<VecColumn*>(table.get_column_by_id(i));
      if (vec &&
          (table_grows || !ColumnFits(*vec, rows, ColumnNeed{count, 0, {}})))
        THROW_STORAGE_EXCEPTION(
            "Insert-only VecColumn growth is not supported; use update mode");
    }
  };
  const auto& schema = graph.schema();
  for (const auto& [name, n] : vertices_) {
    const auto label = schema.get_vertex_label_id(name);
    auto& vt = graph.get_vertex_table(label);
    const auto rows = AddSize(vt.Size(), n.rows);
    check_columns(vt.get_table(), rows, n.rows, rows > vt.Capacity());
    if (rows > vt.Capacity()) {
      const auto& manager = graph.index_manager();
      bool indexed = manager.HasPendingIndex(label);
      auto indexes = manager.GetAllIndexes();
      if (!indexes)
        THROW_STORAGE_EXCEPTION(indexes.error().error_message());
      for (const auto* index : indexes.value())
        indexed |= index->GetMeta().schema.label_id == label;
      if (indexed)
        THROW_STORAGE_EXCEPTION(
            "Insert-only indexed table growth is not supported; use update "
            "mode");
    }
  }
  for (const auto& [names, n] : edges_) {
    const auto& [src, dst, edge] = names;
    auto& et = graph.get_edge_table(schema.get_vertex_label_id(src),
                                    schema.get_vertex_label_id(dst),
                                    schema.get_edge_label_id(edge));
    if (!et.meta()->is_bundled()) {
      const auto rows = AddSize(et.PropTableSize(), n.rows);
      check_columns(*et.table(), rows, n.rows, rows > et.Capacity());
    }
  }
}

bool InsertCapacityRequest::Fits(PropertyGraph& graph) const {
  ValidateGrowthSupport(graph);
  const auto& schema = graph.schema();
  for (const auto& [name, n] : vertices_) {
    auto& vt = graph.get_vertex_table(schema.get_vertex_label_id(name));
    auto& table = vt.get_table();
    Validate(table, n);
    auto rows = AddSize(vt.Size(), n.rows);
    if (rows > vt.Capacity())
      return false;
    auto& key = const_cast<ColumnBase&>(vt.get_indexer().get_keys());
    if (!ColumnFits(key, rows, ColumnNeed{n.rows, n.key_bytes, {}}))
      return false;
    for (size_t i = 0; i < n.columns.size(); ++i)
      if (!ColumnFits(*table.get_column_by_id(i), rows, n.columns[i]))
        return false;
  }
  for (const auto& [names, n] : edges_) {
    const auto& [src, dst, edge] = names;
    auto& et = graph.get_edge_table(schema.get_vertex_label_id(src),
                                    schema.get_vertex_label_id(dst),
                                    schema.get_edge_label_id(edge));
    if (et.meta()->is_bundled())
      continue;
    Validate(*et.table(), n);
    auto rows = AddSize(et.PropTableSize(), n.rows);
    if (rows > et.Capacity())
      return false;
    for (size_t i = 0; i < n.columns.size(); ++i)
      if (!ColumnFits(*et.table()->get_column_by_id(i), rows, n.columns[i]))
        return false;
  }
  return true;
}
void InsertCapacityRequest::Grow(PropertyGraph& graph) const {
  // Reject unsupported requests before mutating any table or column.
  ValidateGrowthSupport(graph);
  const auto& schema = graph.schema();
  for (const auto& [name, n] : vertices_) {
    auto label = schema.get_vertex_label_id(name);
    auto& vt = graph.get_vertex_table(label);
    Validate(vt.get_table(), n);
    auto rows = AddSize(vt.Size(), n.rows);
    if (rows > std::numeric_limits<vid_t>::max())
      THROW_STORAGE_EXCEPTION("VID capacity overflow");
    if (rows > vt.Capacity())
      Check(graph.EnsureCapacity(label, Target(rows)));
    GrowColumn(const_cast<ColumnBase&>(vt.get_indexer().get_keys()), rows,
               ColumnNeed{n.rows, n.key_bytes, {}});
    for (size_t i = 0; i < n.columns.size(); ++i)
      GrowColumn(*vt.get_table().get_column_by_id(i), rows, n.columns[i]);
    graph.MarkVertexTableDirty(label);
  }
  for (const auto& [names, n] : edges_) {
    const auto& [src, dst, edge] = names;
    auto sl = schema.get_vertex_label_id(src),
         dl = schema.get_vertex_label_id(dst),
         el = schema.get_edge_label_id(edge);
    auto& et = graph.get_edge_table(sl, dl, el);
    if (et.meta()->is_bundled())
      continue;
    Validate(*et.table(), n);
    auto rows = AddSize(et.PropTableSize(), n.rows);
    if (rows > et.Capacity())
      Check(graph.EnsureCapacity(sl, dl, el, Target(rows)));
    for (size_t i = 0; i < n.columns.size(); ++i)
      GrowColumn(*et.table()->get_column_by_id(i), rows, n.columns[i]);
    graph.MarkEdgeTableDirty(sl, dl, el);
  }
}
void InsertCapacityRequest::Merge(const InsertCapacityRequest& other) {
  auto merge = [](auto& destination, const auto& source) {
    for (const auto& [key, need] : source) {
      auto it = destination.find(key);
      if (it == destination.end()) {
        destination.emplace(key, need);
        continue;
      }
      auto& n = it->second;
      CHECK(n.types == need.types);
      n.rows = AddSize(n.rows, need.rows);
      n.key_bytes = AddSize(n.key_bytes, need.key_bytes);
      for (size_t i = 0; i < n.columns.size(); ++i)
        MergeColumn(n.columns[i], need.columns[i]);
    }
  };
  merge(vertices_, other.vertices_);
  merge(edges_, other.edges_);
}
void InsertCapacityRequest::Subtract(
    const InsertCapacityRequest& other) noexcept {
  auto subtract = [](auto& destination, const auto& source) {
    for (const auto& [key, need] : source) {
      auto it = destination.find(key);
      CHECK(it != destination.end());
      auto& n = it->second;
      CHECK_GE(n.rows, need.rows);
      CHECK_GE(n.key_bytes, need.key_bytes);
      n.rows -= need.rows;
      n.key_bytes -= need.key_bytes;
      for (size_t i = 0; i < n.columns.size(); ++i) {
        SubtractColumn(n.columns[i], need.columns[i]);
      }
      if (n.rows == 0)
        destination.erase(it);
    }
  };
  subtract(vertices_, other.vertices_);
  subtract(edges_, other.edges_);
}
bool InsertCapacityReservations::TryReserve(
    PropertyGraph& graph, const InsertCapacityRequest& request,
    InsertCapacityRequest& growth_request) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto combined = outstanding_;
  combined.Merge(request);
#ifdef NEUG_BUILD_TEST
  if (reservation_failures > 0) {
    --reservation_failures;
    growth_request = std::move(combined);
    return false;
  }
#endif
  if (!combined.Fits(graph)) {
    growth_request = std::move(combined);
    return false;
  }
  outstanding_ = std::move(combined);
  return true;
}
void InsertCapacityReservations::Release(
    const InsertCapacityRequest& request) noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  outstanding_.Subtract(request);
}
}  // namespace neug
