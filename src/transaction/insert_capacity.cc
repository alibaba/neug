/** Copyright 2026 Alibaba Group Holding Limited.
 * Licensed under the Apache License, Version 2.0.
 */
#include "neug/transaction/insert_capacity.h"
#include <algorithm>
#include <limits>
#include "neug/storages/graph/property_graph.h"
#include "neug/storages/index/storage_index_manager.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/property/vec_column.h"
#include "neug/utils/serialization/out_archive.h"

namespace neug {
namespace {
size_t AddSize(size_t a, size_t b) {
  if (b > std::numeric_limits<size_t>::max() - a)
    THROW_STORAGE_EXCEPTION("Insert capacity overflow");
  return a + b;
}
size_t Target(size_t current, size_t required) {
  return std::max(required,
                  AddSize(current, std::max(size_t{4096}, current / 4)));
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

void InsertCapacityRequest::Add(Need& n, const std::vector<Value>& props) {
  n.rows = AddSize(n.rows, 1);
  if (n.types.empty()) {
    for (const auto& p : props)
      n.types.push_back(p.type());
    n.bytes.resize(props.size());
  }
  for (size_t i = 0; i < props.size(); ++i) {
    if (!props[i].IsNull() && props[i].type().id() == DataTypeId::kVarchar)
      n.bytes[i] =
          AddSize(n.bytes[i], props[i].GetValue<std::string_view>().size());
  }
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
                                       size_t count, size_t bytes) {
  if (auto* vec = dynamic_cast<VecColumn*>(&col)) {
    return rows <= vec->size() &&
           AddSize(vec->get_offset_accessor()->GetNextIndexID(), count) <=
               vec->buffer_size();
  }
  if (rows > col.size())
    return false;
  if (auto* str = dynamic_cast<StringColumn*>(&col))
    return bytes <= str->available_space();
  return true;
}
void InsertCapacityRequest::GrowColumn(ColumnBase& col, size_t rows,
                                       size_t count, size_t bytes) {
  if (dynamic_cast<VecColumn*>(&col)) {
    if (!ColumnFits(col, rows, count, bytes))
      THROW_STORAGE_EXCEPTION(
          "Insert-only VecColumn growth is not supported; use update mode");
    return;
  }
  const auto target_rows =
      rows > col.size() ? Target(col.size(), rows) : col.size();
  if (auto* str = dynamic_cast<StringColumn*>(&col)) {
    // The second argument is additional append bytes, not total capacity.
    const auto append_bytes = bytes > str->available_space()
                                  ? Target(str->available_space(), bytes)
                                  : bytes;
    str->resize(target_rows, append_bytes);
  } else if (rows > col.size()) {
    col.resize(target_rows);
  }
}
void InsertCapacityRequest::ValidateGrowthSupport(PropertyGraph& graph) const {
  auto check_columns = [](Table& table, size_t rows, size_t count,
                          bool table_grows) {
    for (size_t i = 0; i < table.col_num(); ++i) {
      auto* vec = dynamic_cast<VecColumn*>(table.get_column_by_id(i));
      if (vec && (table_grows || !ColumnFits(*vec, rows, count, 0)))
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
    if (!ColumnFits(key, rows, n.rows, n.key_bytes))
      return false;
    for (size_t i = 0; i < n.bytes.size(); ++i)
      if (!ColumnFits(*table.get_column_by_id(i), rows, n.rows, n.bytes[i]))
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
    for (size_t i = 0; i < n.bytes.size(); ++i)
      if (!ColumnFits(*et.table()->get_column_by_id(i), rows, n.rows,
                      n.bytes[i]))
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
      Check(graph.EnsureCapacity(label, Target(vt.Capacity(), rows)));
    GrowColumn(const_cast<ColumnBase&>(vt.get_indexer().get_keys()), rows,
               n.rows, n.key_bytes);
    for (size_t i = 0; i < n.bytes.size(); ++i)
      GrowColumn(*vt.get_table().get_column_by_id(i), rows, n.rows, n.bytes[i]);
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
      Check(graph.EnsureCapacity(sl, dl, el, Target(et.Capacity(), rows)));
    for (size_t i = 0; i < n.bytes.size(); ++i)
      GrowColumn(*et.table()->get_column_by_id(i), rows, n.rows, n.bytes[i]);
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
      for (size_t i = 0; i < n.bytes.size(); ++i)
        n.bytes[i] = AddSize(n.bytes[i], need.bytes[i]);
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
      for (size_t i = 0; i < n.bytes.size(); ++i) {
        CHECK_GE(n.bytes[i], need.bytes[i]);
        n.bytes[i] -= need.bytes[i];
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
