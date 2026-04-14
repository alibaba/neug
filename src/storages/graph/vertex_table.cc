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

#include "neug/storages/graph/vertex_table.h"

#include "neug/storages/module/module_factory.h"
#include "neug/storages/module_descriptor.h"
#include "neug/storages/workspace.h"
#include "neug/utils/file_utils.h"
#include "neug/utils/likely.h"

namespace neug {

// ---------------------------------------------------------------------------
// Key helpers for flat ModuleStore layout
// ---------------------------------------------------------------------------

// Vertex indexer: "vidx:{label}"
static inline std::string vidx_key(label_t label) {
  return "vidx:" + std::to_string(label);
}
// Vertex column: "vcol:{label}:{col_id}"
static inline std::string vcol_key(label_t label, int32_t col_id) {
  return "vcol:" + std::to_string(label) + ":" + std::to_string(col_id);
}
// Vertex timestamp: "vts:{label}"
static inline std::string vts_key(label_t label) {
  return "vts:" + std::to_string(label);
}

void VertexTable::Open(Checkpoint& ckp, const ModuleDescriptor& descriptor,
                       MemoryLevel memory_level) {
  memory_level_ = memory_level;

  const auto& indexer_desc = descriptor.get_sub_module("indexer");
  indexer_->Open(ckp, indexer_desc, memory_level);
  const auto& table_desc = descriptor.get_sub_module("property_table");
  table_->Open(ckp, table_desc, memory_level, vertex_schema_->property_names,
               vertex_schema_->property_types);
  const auto& vts_desc = descriptor.get_sub_module("vertex_timestamp");
  v_ts_.Open(ckp, vts_desc, memory_level);
}

std::unique_ptr<VertexTable> VertexTable::Fork(Checkpoint& ckp,
                                               MemoryLevel level) {
  if (!vertex_schema_) {
    THROW_INTERNAL_EXCEPTION("VertexTable::Fork requires a valid schema");
  }
  auto forked = std::make_unique<VertexTable>(vertex_schema_);
  forked->memory_level_ = level;

  // Clone indexer through module Dump/Open to ensure the underlying key
  // column buffers are fully initialized.
  auto forked_indexer = indexer_->Fork(ckp, level);
  auto forked_indexer_ptr =
      dynamic_cast<IndexerType*>(forked_indexer.release());
  forked->indexer_->swap(*forked_indexer_ptr);
  delete forked_indexer_ptr;

  if (table_) {
    forked->table_ = table_->Fork(ckp, level);
  } else {
    forked->table_.reset();
  }

  auto vts_module = v_ts_.Fork(ckp, level);
  auto* vts = dynamic_cast<VertexTimestamp*>(vts_module.release());
  if (vts == nullptr) {
    THROW_INTERNAL_EXCEPTION("VertexTable::Fork failed to fork timestamp");
  }
  forked->v_ts_.Swap(*vts);
  delete vts;

  return forked;
}

void VertexTable::RebaseFromLive(const VertexTable& live, vid_t base_lid_num) {
  auto live_size = static_cast<vid_t>(live.indexer_->size());
  if (live_size <= base_lid_num) {
    // No vertices inserted into live since fork was taken.
    return;
  }
  append_vertex_range_from(live, base_lid_num, live_size);
}

void VertexTable::append_vertex_range_from(const VertexTable& live,
                                           vid_t begin_lid, vid_t end_lid) {
  if (begin_lid >= end_lid) {
    return;
  }
  CHECK_LE(end_lid, static_cast<vid_t>(live.indexer_->size()));

  // Grow indexer and table to accommodate lids [0, end_lid).
  if (end_lid > indexer_->capacity()) {
    size_t new_cap =
        std::max(static_cast<size_t>(end_lid), indexer_->capacity() * 2);
    EnsureCapacity(new_cap);
  }

  for (vid_t lid = begin_lid; lid < end_lid; ++lid) {
    // Copy primary key — indexer must assign the same lid since we insert
    // in monotone order and the fork was taken from the same live graph.
    Property oid = live.indexer_->get_key(lid);
    vid_t assigned = indexer_->insert(oid);
    if (assigned != lid) {
      THROW_INTERNAL_EXCEPTION(
          "VertexTable::append_vertex_range_from: lid mismatch. Expected " +
          std::to_string(lid) + ", got " + std::to_string(assigned));
    }

    // Copy property columns.
    if (table_ && live.table_) {
      size_t num_cols = table_->col_num();
      for (size_t col = 0; col < num_cols; ++col) {
        auto live_col = live.table_->get_column_by_id(col);
        auto this_col = table_->get_column_by_id(col);
        if (!live_col || !this_col) {
          continue;
        }
        this_col->set_any(lid, live_col->get_prop(lid));
      }
    }

    // Copy vertex timestamp from live.
    auto insert_ts = live.v_ts_.GetRawTimestamp(lid);
    if (insert_ts != VertexTimestamp::DELETED_TIMESTAMP) {
      v_ts_.InsertVertex(lid, insert_ts);
    }
    // If DELETED_TIMESTAMP (vertex was deleted in live after insert) we skip
    // marking it valid; the slot stays DELETED_TIMESTAMP in our fork too.
  }
}

void VertexTable::insert_vertices(
    std::shared_ptr<IRecordBatchSupplier> supplier) {
  auto pk_type_id = pk_type_.id();
  if (pk_type_id == DataTypeId::kInt64) {
    insert_vertices_impl<int64_t>(supplier);
  } else if (pk_type_id == DataTypeId::kInt32) {
    insert_vertices_impl<int32_t>(supplier);
  } else if (pk_type_id == DataTypeId::kUInt32) {
    insert_vertices_impl<uint32_t>(supplier);
  } else if (pk_type_id == DataTypeId::kUInt64) {
    insert_vertices_impl<uint64_t>(supplier);
  } else if (pk_type_id == DataTypeId::kVarchar) {
    insert_vertices_impl<std::string_view>(supplier);
  } else {
    LOG(FATAL) << "Unsupported primary key type for vertex, type: "
               << pk_type_.ToString()
               << ", label: " << vertex_schema_->label_name;
  }
}

ModuleDescriptor VertexTable::Dump(Checkpoint& ckp) {
  ModuleDescriptor descriptor;
  descriptor.module_type = ModuleTypeName();
  descriptor.set_sub_module("indexer", indexer_->Dump(ckp));
  descriptor.set_sub_module("property_table", table_->Dump(ckp));
  descriptor.set_sub_module("vertex_timestamp", v_ts_.Dump(ckp));
  return descriptor;
}

void VertexTable::From(ModuleStore& store, label_t label) {
  // ── Indexer ───────────────────────────────────────────────────────────────
  indexer_->From(store, vidx_key(label));

  // ── Property columns ─────────────────────────────────────────────────────
  if (vertex_schema_) {
    table_ = std::make_unique<Table>();
    table_->initColumns(vertex_schema_->property_names,
                        vertex_schema_->property_types);
    auto& cols = table_->columns();
    for (size_t col = 0; col < cols.size(); ++col) {
      auto col_mod = store.get(vcol_key(label, static_cast<int32_t>(col)));
      if (col_mod) {
        auto* column = dynamic_cast<ColumnBase*>(col_mod.get());
        if (column) {
          cols[col] = std::shared_ptr<ColumnBase>(col_mod, column);
        }
      }
    }
    table_->buildColumnPtrs();
  }

  // ── Vertex timestamp ─────────────────────────────────────────────────────
  auto vts_mod = store.get(vts_key(label));
  if (vts_mod) {
    auto* vts = dynamic_cast<VertexTimestamp*>(vts_mod.get());
    if (vts) {
      v_ts_.Swap(*vts);
    }
  }
}

void VertexTable::MoveTo(ModuleStore& store, label_t label) {
  // ── Indexer ───────────────────────────────────────────────────────────────
  indexer_->MoveTo(store, vidx_key(label));

  // ── Property columns ─────────────────────────────────────────────────────
  if (table_) {
    for (size_t col = 0; col < table_->col_num(); ++col) {
      auto col_ptr = table_->get_column_by_id(static_cast<int32_t>(col));
      store.set(vcol_key(label, static_cast<int32_t>(col)), col_ptr);
    }
    table_.reset();
  }

  // ── Vertex timestamp ─────────────────────────────────────────────────────
  auto vts_mod = std::make_shared<VertexTimestamp>();
  vts_mod->Swap(v_ts_);
  store.set(vts_key(label), vts_mod);
}

void VertexTable::Close() {
  indexer_->Close();
  table_->close();
  v_ts_.Clear();
}

void VertexTable::SetVertexSchema(
    std::shared_ptr<const VertexSchema> vertex_schema) {
  // First ensure the primary key is same with the existing one
  if (vertex_schema->primary_keys.size() != 1) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Vertex schema must have exactly one primary key.");
  }
  if (!VertexSchema::is_pk_same(*vertex_schema_, *vertex_schema)) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "New vertex schema's primary key is different from the existing "
        "one.");
  }

  vertex_schema_ = vertex_schema;
}

bool VertexTable::get_index(const Property& oid, vid_t& lid,
                            timestamp_t ts) const {
  auto res = indexer_->get_index(oid, lid);
  if (NEUG_UNLIKELY(res && !v_ts_.IsVertexValid(lid, ts))) {
    return false;
  }
  return res;
}

size_t VertexTable::VertexNum(timestamp_t ts) const {
  return v_ts_.ValidVertexNum(ts, indexer_->size());
}

size_t VertexTable::LidNum() const { return indexer_->size(); }

bool VertexTable::AddVertex(const Property& id,
                            const std::vector<Property>& props, vid_t& vid,
                            timestamp_t ts, bool insert_safe) {
  if (indexer_->capacity() <= indexer_->size()) {
    return false;
  }
  vid = insert_vertex_pk(id, ts, insert_safe);
  assert([&]() {
    if (table_->col_num() > 0) {
      return vid < table_->get_column_by_id(0)->size();
    } else {
      return true;
    }
  }());
  table_->insert(vid, props, insert_safe);
  return true;
}

bool VertexTable::UpdateProperty(vid_t vid, int32_t prop_id,
                                 const Property& value, timestamp_t ts) {
  if (NEUG_UNLIKELY(vid >= indexer_->size())) {
    LOG(ERROR) << "Lid " << vid << " is out of range.";
    return false;
  }
  if (NEUG_UNLIKELY(!v_ts_.IsVertexValid(vid, ts))) {
    LOG(ERROR) << "Vertex with lid " << vid << " is not valid at timestamp "
               << ts << ".";
    return false;
  }
  if (prop_id < 0 || prop_id >= static_cast<int32_t>(table_->col_num())) {
    LOG(ERROR) << "Property id " << prop_id << " is out of range.";
    return false;
  }
  table_->get_column_by_id(prop_id)->set_any(vid, value);
  return true;
}

Property VertexTable::GetOid(vid_t lid, timestamp_t ts) const {
  if (NEUG_UNLIKELY(lid >= indexer_->size())) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Lid " + std::to_string(lid) +
                                     " is out of range.");
  }
  if (NEUG_UNLIKELY(!v_ts_.IsVertexValid(lid, ts))) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Lid " + std::to_string(lid) +
                                     " has been deleted.");
  }
  return indexer_->get_key(lid);
}

bool VertexTable::IsValidLid(vid_t lid, timestamp_t ts) const {
  return lid < indexer_->size() && v_ts_.IsVertexValid(lid, ts);
}

size_t VertexTable::EnsureCapacity(size_t capacity) {
  if (capacity <= indexer_->capacity()) {
    return indexer_->capacity();
  }
  capacity = std::max(capacity, 4096UL);
  if (capacity > indexer_->capacity()) {
    indexer_->reserve(capacity);
  }
  if (table_ && table_->size() < capacity) {
    table_->resize(capacity, vertex_schema_->default_property_values);
  }
  v_ts_.Reserve(capacity);
  return indexer_->capacity();
}

void VertexTable::BatchDeleteVertices(const std::vector<vid_t>& vids) {
  size_t delete_cnt = 0;
  for (auto v : vids) {
    if (v < indexer_->size() && v_ts_.IsVertexValid(v, MAX_TIMESTAMP)) {
      v_ts_.RemoveVertex(v);
      delete_cnt++;
    }
  }
  VLOG(10) << "Deleted " << delete_cnt << " vertices in batch.";
}

void VertexTable::DeleteVertex(const Property& id, timestamp_t ts) {
  vid_t vid;
  if (!get_index(id, vid, ts)) {
    LOG(WARNING) << "Vertex with id " << id.to_string() << " not found.";
    return;
  }
  return DeleteVertex(vid, ts);
}

void VertexTable::DeleteVertex(vid_t lid, timestamp_t ts) {
  if (lid >= indexer_->size()) {
    LOG(WARNING) << "Lid " << lid << " is out of range.";
    return;
  }
  if (v_ts_.IsVertexValid(lid, ts)) {
    v_ts_.RemoveVertex(lid);
  } else {
    LOG(WARNING) << "Vertex with lid " << lid << " has been deleted.";
  }
}

void VertexTable::RevertDeleteVertex(vid_t lid, timestamp_t ts) {
  assert(lid < indexer_->size());
  if (v_ts_.IsRemoved(lid)) {
    v_ts_.RevertRemoveVertex(lid, ts);
  } else {
    LOG(WARNING) << "Vertex with lid " << lid << " is not deleted.";
  }
}

void VertexTable::DeleteProperties(const std::vector<std::string>& properties) {
  for (const auto& prop : properties) {
    table_->delete_column(prop);
  }
}

void VertexTable::AddProperties(Checkpoint& ckp,
                                const std::vector<std::string>& properties,
                                const std::vector<DataType>& types,
                                const std::vector<Property>& default_values) {
  table_->add_columns(ckp, properties, types, default_values,
                      indexer_->capacity(), memory_level_);
}

void VertexTable::Drop() {
  indexer_->drop();
  table_->drop();
  v_ts_.Clear();
  table_.reset();
  // TODO(zhanglei): reset the indexer.
  // indexer_ = IndexerType();
}

void VertexTable::RenameProperties(const std::vector<std::string>& old_names,
                                   const std::vector<std::string>& new_names) {
  CHECK(old_names.size() == new_names.size());
  for (size_t i = 0; i < old_names.size(); ++i) {
    table_->rename_column(old_names[i], new_names[i]);
  }
}

void VertexTable::Compact(timestamp_t ts) {
  v_ts_.Compact();
  // TODO(zhanglei): Support compact unused lid in indexer_ and table
}

vid_t VertexTable::insert_vertex_pk(const Property& id, timestamp_t ts,
                                    bool insert_safe) {
  vid_t vid;
  if (NEUG_UNLIKELY(indexer_->get_index(id, vid))) {
    if (NEUG_UNLIKELY(v_ts_.IsVertexValid(vid, ts))) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Vertex with id " + id.to_string() +
                                       " already exists with lid " +
                                       std::to_string(vid));
    }
  } else {
    vid = indexer_->insert(id, insert_safe);
  }
  v_ts_.InsertVertex(vid, ts);
  return vid;
}

}  // namespace neug