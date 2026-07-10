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

#include "neug/storages/checkpoint_manifest.h"
#include "neug/storages/module/module_broker.h"
#include "neug/storages/module/module_factory.h"
#include "neug/storages/module_descriptor.h"
#include "neug/utils/io/file/file_utils.h"
#include "neug/utils/likely.h"

namespace neug {

void VertexTable::Init(std::shared_ptr<Checkpoint> ckp, MemoryLevel level) {
  CHECK(vertex_schema_ != nullptr) << "VertexTable::Init requires schema";
  CHECK(indexer_ != nullptr) << "VertexTable::Init requires indexer slot";
  CHECK(v_ts_ != nullptr) << "VertexTable::Init requires vertex_timestamp slot";
  CHECK(pk_type_.id() != DataTypeId::kUnknown)
      << "VertexTable::Init: pk_type must be set; was the schema-aware "
         "constructor used?";
  ckp_ = std::move(ckp);
  memory_level_ = level;
  auto keys = CreateColumn(pk_type_);
  keys->Open(*ckp_, ModuleDescriptor{}, level);
  auto indices = std::make_unique<TypedColumn<vid_t>>();
  indices->Open(*ckp_, ModuleDescriptor{}, level);
  indexer_->Open(*ckp_, ModuleDescriptor{}, level, std::move(keys),
                 std::move(indices));
  table_ = std::make_unique<Table>(vertex_schema_->property_names,
                                   vertex_schema_->property_types);
  table_->Init(*ckp_, level);
  v_ts_->Open(*ckp_, ModuleDescriptor{}, level);
}

void VertexTable::insert_vertices(
    std::shared_ptr<IDataChunkSupplier> supplier) {
  auto row_nums = supplier->RowNum();
  if (row_nums < 0) {
    VLOG(1) << "Row number from supplier is unknown, skip pre-reserve.";
    row_nums = 0;
  }
  size_t new_size = indexer_->size() + row_nums;
  if (new_size > indexer_->capacity()) {
    size_t cap = indexer_->capacity();
    while (new_size >= cap) {
      cap = cap < 4096 ? 4096 : cap + cap / 4;
    }
    EnsureCapacity(cap);
  }
  while (true) {
    auto chunk = supplier->GetNextChunk();
    if (chunk == nullptr) {
      break;
    }
    auto& columns = chunk->columns;
    const auto& property_names = vertex_schema_->property_names;
    CHECK(columns.size() == property_names.size() + 1)
        << "Number of columns in the chunk (" << columns.size()
        << ") does not match the number of properties ("
        << property_names.size() + 1 << ").";
    auto ind = std::get<2>(vertex_schema_->primary_keys[0]);
    auto pk_col = columns[ind];

    // Build a list of property columns excluding the PK column.
    std::vector<std::shared_ptr<IContextColumn>> prop_cols;
    prop_cols.reserve(columns.size() - 1);
    for (size_t i = 0; i < columns.size(); ++i) {
      if (static_cast<int>(i) != ind) {
        prop_cols.push_back(columns[i]);
      }
    }

    // Capacity check for actual batch size.
    size_t chunk_rows = chunk->row_num();
    size_t new_size = indexer_->size() + chunk_rows;
    if (new_size > indexer_->capacity()) {
      size_t cap = indexer_->capacity();
      while (new_size >= cap) {
        cap = cap < 4096 ? 4096 : cap + cap / 4;
      }
      EnsureCapacity(cap);
    }

    auto vids = insert_primary_keys(pk_col);

    for (size_t i = 0; i < prop_cols.size(); ++i) {
      auto col = table_->get_column_by_id(i);
      set_properties_from_context_column(col, prop_cols[i], vids);
    }
    VLOG(10) << "Inserted " << chunk_rows
             << " vertices, current vertex num: " << VertexNum();
  }
}

void VertexTable::Close() {
  indexer_.reset();
  if (table_) {
    table_->close();
  }
  v_ts_.reset();
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

bool VertexTable::get_index(const Value& oid, vid_t& lid,
                            timestamp_t ts) const {
  auto res = indexer_->get_index(oid, lid);
  if (NEUG_UNLIKELY(res && !v_ts_->IsVertexValid(lid, ts))) {
    return false;
  }
  return res;
}

size_t VertexTable::VertexNum(timestamp_t ts) const {
  return v_ts_->ValidVertexNum(ts, indexer_->size());
}

size_t VertexTable::LidNum() const { return indexer_->size(); }

vid_t internal::insert_vertex_pk_internal(IndexerType& indexer,
                                          VertexTimestamp& v_ts,
                                          const Value& id, timestamp_t ts,
                                          bool insert_safe) {
  vid_t vid;
  if (NEUG_UNLIKELY(indexer.get_index(id, vid))) {
    if (NEUG_UNLIKELY(v_ts.IsVertexValid(vid, ts))) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Vertex with id " + id.to_string() +
                                       " already exists with lid " +
                                       std::to_string(vid));
    }
  } else {
    vid = indexer.insert(id, insert_safe);
  }
  v_ts.InsertVertex(vid, ts);
  return vid;
}

bool VertexTable::AddVertex(const Value& id, const std::vector<Value>& props,
                            vid_t& vid, timestamp_t ts, bool insert_safe) {
  if (indexer_->capacity() <= indexer_->size()) {
    return false;
  }
  vid = internal::insert_vertex_pk_internal(*indexer_, *v_ts_, id, ts,
                                            insert_safe);
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

bool VertexTable::UpdateProperty(vid_t vid, int32_t prop_id, const Value& value,
                                 timestamp_t ts) {
  if (NEUG_UNLIKELY(vid >= indexer_->size())) {
    LOG(ERROR) << "Lid " << vid << " is out of range.";
    return false;
  }
  if (NEUG_UNLIKELY(!v_ts_->IsVertexValid(vid, ts))) {
    LOG(ERROR) << "Vertex with lid " << vid << " is not valid at timestamp "
               << ts << ".";
    return false;
  }
  if (prop_id < 0 || prop_id >= static_cast<int32_t>(table_->col_num())) {
    LOG(ERROR) << "Property id " << prop_id << " is out of range.";
    return false;
  }
  table_->get_column_by_id(prop_id)->set_any(vid, value, true);
  return true;
}

Value VertexTable::GetOid(vid_t lid, timestamp_t ts) const {
  if (NEUG_UNLIKELY(lid >= indexer_->size())) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Lid " + std::to_string(lid) +
                                     " is out of range.");
  }
  if (NEUG_UNLIKELY(!v_ts_->IsVertexValid(lid, ts))) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Lid " + std::to_string(lid) +
                                     " has been deleted.");
  }
  return indexer_->get_key(lid);
}

bool VertexTable::IsValidLid(vid_t lid, timestamp_t ts) const {
  return lid < indexer_->size() && v_ts_->IsVertexValid(lid, ts);
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
    table_->resize(capacity, vertex_schema_->get_default_property_values());
  }
  v_ts_->Reserve(capacity);
  return indexer_->capacity();
}

void VertexTable::BatchDeleteVertices(const std::vector<vid_t>& vids) {
  size_t delete_cnt = 0;
  for (auto v : vids) {
    if (v < indexer_->size() && v_ts_->IsVertexValid(v, MAX_TIMESTAMP)) {
      v_ts_->RemoveVertex(v);
      delete_cnt++;
    }
  }
  VLOG(10) << "Deleted " << delete_cnt << " vertices in batch.";
}

void VertexTable::DeleteVertex(const Value& id, timestamp_t ts) {
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
  if (v_ts_->IsVertexValid(lid, ts)) {
    v_ts_->RemoveVertex(lid);
  } else {
    LOG(WARNING) << "Vertex with lid " << lid << " has been deleted.";
  }
}

void VertexTable::RevertDeleteVertex(vid_t lid, timestamp_t ts) {
  assert(lid < indexer_->size());
  if (v_ts_->IsRemoved(lid)) {
    v_ts_->RevertRemoveVertex(lid, ts);
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
                                const std::vector<Value>& default_values) {
  table_->add_columns(ckp, properties, types, default_values,
                      indexer_->capacity(), memory_level_);
}

void VertexTable::RenameProperties(const std::vector<std::string>& old_names,
                                   const std::vector<std::string>& new_names) {
  CHECK(old_names.size() == new_names.size());
  for (size_t i = 0; i < old_names.size(); ++i) {
    table_->rename_column(old_names[i], new_names[i]);
  }
}

void VertexTable::Compact(timestamp_t ts) {
  v_ts_->Compact();
  // TODO(zhanglei): Support compact unused lid in indexer_ and table
}

vid_t VertexTable::insert_vertex_pk(const Value& id, timestamp_t ts,
                                    bool insert_safe) {
  return internal::insert_vertex_pk_internal(*indexer_, *v_ts_, id, ts,
                                             insert_safe);
}

// --- Static key builders ---

std::string VertexTable::KeyKeys(const std::string& label) {
  return "vertex_" + label + "_keys";
}

std::string VertexTable::KeyIndices(const std::string& label) {
  return "vertex_" + label + "_indices";
}

std::string VertexTable::KeyIndexer(const std::string& label) {
  return "vertex_" + label + "_indexer";
}

std::string VertexTable::KeyVertexTimestamp(const std::string& label) {
  return "vertex_" + label + "_v_ts";
}

std::string VertexTable::KeyProperty(const std::string& label, size_t index) {
  return "vertex_" + label + "_prop_" + std::to_string(index);
}

// --- Snapshot orchestration ---

VertexTable VertexTable::OpenFrom(std::shared_ptr<Checkpoint> ckp,
                                  std::shared_ptr<const VertexSchema> vs,
                                  ModuleBroker& store,
                                  const CheckpointManifest& meta,
                                  MemoryLevel level) {
  VertexTable vt(vs);
  vt.ckp_ = ckp;
  vt.SetMemoryLevel(level);
  const auto& lbl = vs->label_name;

  if (!store.Contains(KeyKeys(lbl))) {
    vt.Init(ckp, level);
    return vt;
  }

  // Restore indexer via LFIndexer::Open
  auto& idx = vt.get_indexer();
  auto indexer_desc = meta.module(KeyIndexer(lbl));
  CHECK(indexer_desc.has_value())
      << "missing indexer meta entry for vertex " << lbl;
  idx.Open(*ckp, indexer_desc.value(), level,
           store.TakeModule<ColumnBase>(KeyKeys(lbl)),
           store.TakeModule<TypedColumn<vid_t>>(KeyIndices(lbl)));

  auto table = std::make_unique<Table>(vs->property_names, vs->property_types);
  for (size_t i = 0; i < vs->property_types.size(); ++i) {
    table->SetColumn(static_cast<int>(i),
                     store.TakeModule<ColumnBase>(KeyProperty(lbl, i)));
  }
  vt.SetTable(std::move(table));
  vt.SetVertexTimestamp(
      store.TakeModule<VertexTimestamp>(KeyVertexTimestamp(lbl)));
  return vt;
}

void VertexTable::DisassembleTo(ModuleBroker& store, CheckpointManifest& meta,
                                Checkpoint& ckp) {
  const auto& lbl = vertex_schema_->label_name;
  auto& idx = get_indexer();

  // Persist indexer via LFIndexer::Dump.  The returned descriptor carries the
  // indexer's three scalars; store it under KeyIndexer so store.Dump's later
  // pass (which writes the columns' own descriptors to KeyKeys / KeyIndices)
  // does not clobber it.
  std::unique_ptr<ColumnBase> keys_out;
  std::unique_ptr<TypedColumn<vid_t>> indices_out;
  meta.set_module(KeyIndexer(lbl), idx.Dump(ckp, keys_out, indices_out));
  store.SetModule(KeyKeys(lbl), std::move(keys_out));
  store.SetModule(KeyIndices(lbl), std::move(indices_out));

  auto table = TakeTable();
  for (size_t i = 0; i < table->col_num(); ++i) {
    table->get_column_by_id(i)->Dump(ckp, meta, KeyProperty(lbl, i));
  }
  store.SetModule(KeyVertexTimestamp(lbl), TakeVertexTimestamp());
}

VertexTable VertexTable::Clone() const {
  CHECK(ckp_ != nullptr) << "VertexTable::Clone requires a valid checkpoint";
  VertexTable cow_clone;
  cow_clone.ckp_ = ckp_;
  cow_clone.indexer_ = indexer_->Clone();
  cow_clone.table_ = table_->Clone();
  cow_clone.vertex_schema_ = vertex_schema_;
  cow_clone.v_ts_ = std::unique_ptr<VertexTimestamp>(
      dynamic_cast<VertexTimestamp*>(v_ts_->Clone().release()));
  cow_clone.pk_type_ = pk_type_;
  cow_clone.memory_level_ = memory_level_;
  return cow_clone;
}

void VertexTable::DetachIndexer() {
  CHECK(ckp_ != nullptr) << "Checkpoint is null, cannot detach indexer";
  indexer_->Detach(*ckp_, memory_level_);
}

void VertexTable::DetachVertexTimestamp() {
  CHECK(ckp_ != nullptr)
      << "Checkpoint is null, cannot detach vertex timestamp";
  v_ts_->Detach(*ckp_, memory_level_);
}

}  // namespace neug
