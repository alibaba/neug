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
#include <filesystem>
#include "neug/utils/file_utils.h"
#include "neug/utils/likely.h"

namespace neug {

namespace {

// Remove any tmp_dir files belonging to a previously dropped vertex label of
// the given name. tmp filenames key on the label string (not the numeric
// label_t), and Schema reuses the same numeric id when a same-name label is
// re-created, so without this sweep an in-process DROP+CREATE would re-mmap
// the prior label's leftover data.
void cleanup_vertex_label_tmp_files(const std::string& work_dir,
                                    const std::string& label_name) {
  std::string tmp_dir_path = tmp_dir(work_dir);
  if (!std::filesystem::exists(tmp_dir_path)) {
    return;
  }
  const std::string prefixes[] = {
      vertex_table_prefix(label_name),
      vertex_map_prefix(label_name),
      IndexerType::prefix() + "_" + vertex_map_prefix(label_name),
      vertex_tracker_file(label_name),
  };
  for (const auto& entry : std::filesystem::directory_iterator(tmp_dir_path)) {
    if (!entry.is_regular_file()) {
      continue;
    }
    const std::string fname = entry.path().filename().string();
    for (const auto& p : prefixes) {
      if (filename_matches_label_prefix(fname, p)) {
        std::error_code ec;
        std::filesystem::remove(entry.path(), ec);
        if (ec) {
          LOG(WARNING) << "Failed to remove stale tmp file " << entry.path()
                       << ": " << ec.message();
        }
        break;
      }
    }
  }
}

}  // namespace

void VertexTable::Open(const std::string& work_dir, MemoryLevel memory_level) {
  openImpl(work_dir, memory_level, checkpoint_dir(work_dir));
}

// Initialize releases nothing on the filesystem itself: it only sweeps stale
// tmp files left behind by a prior in-process DROP of the same label and then
// opens fresh containers. Persistent checkpoint state is never touched here —
// only the next CHECKPOINT (PropertyGraph::Dump) advances it.
void VertexTable::Initialize(const std::string& work_dir,
                             MemoryLevel memory_level) {
  if (vertex_schema_) {
    cleanup_vertex_label_tmp_files(work_dir, vertex_schema_->label_name);
  }
  openImpl(work_dir, memory_level, "");
}

void VertexTable::openImpl(const std::string& work_dir,
                           MemoryLevel memory_level,
                           const std::string& checkpoint_dir_path) {
  memory_level_ = memory_level;
  work_dir_ = work_dir;

  const auto& label_name = vertex_schema_->label_name;
  std::string vertex_tracker_filename =
      checkpoint_dir_path.empty()
          ? ""
          : checkpoint_dir_path + "/" + vertex_tracker_file(label_name);
  auto indexer_filename =
      IndexerType::prefix() + "_" + vertex_map_prefix(label_name);
  if (memory_level_ == MemoryLevel::kSyncToFile) {
    indexer_->open(indexer_filename, checkpoint_dir_path, work_dir_);
    table_->open(vertex_table_prefix(label_name), work_dir_,
                 vertex_schema_->property_names,
                 vertex_schema_->property_types);

  } else if (memory_level_ == MemoryLevel::kInMemory) {
    indexer_->open_in_memory(checkpoint_dir_path.empty()
                                 ? ""
                                 : checkpoint_dir_path + "/" +
                                       indexer_filename);
    table_->open_in_memory(vertex_table_prefix(label_name), work_dir_,
                           vertex_schema_->property_names,
                           vertex_schema_->property_types);

  } else if (memory_level_ == MemoryLevel::kHugePagePreferred) {
    indexer_->open_with_hugepages(checkpoint_dir_path.empty()
                                      ? ""
                                      : checkpoint_dir_path + "/" +
                                            indexer_filename);
    table_->open_with_hugepages(vertex_table_prefix(label_name), work_dir_,
                                vertex_schema_->property_names,
                                vertex_schema_->property_types);
  } else {
    THROW_INVALID_ARGUMENT_EXCEPTION("Invalid memory level: " +
                                     std::to_string(memory_level_));
  }
  v_ts_->Open(vertex_tracker_filename);
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
    THROW_NOT_SUPPORTED_EXCEPTION(
        "Unsupported primary key type for vertex, type: " +
        pk_type_.ToString() + ", label: " + vertex_schema_->label_name);
  }
}

void VertexTable::Dump(const std::string& target_dir) {
  const auto& label_name = vertex_schema_->label_name;
  indexer_->dump(IndexerType::prefix() + "_" + vertex_map_prefix(label_name),
                 target_dir);
  table_->dump(vertex_table_prefix(label_name), target_dir);
  v_ts_->Dump(target_dir + "/" + vertex_tracker_file(label_name));
}

void VertexTable::Close() {
  indexer_->close();
  table_->close();
  v_ts_->Clear();
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
  if (NEUG_UNLIKELY(res && !v_ts_->IsVertexValid(lid, ts))) {
    return false;
  }
  return res;
}

size_t VertexTable::VertexNum(timestamp_t ts) const {
  return v_ts_->ValidVertexNum(ts, indexer_->size());
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

Property VertexTable::GetOid(vid_t lid, timestamp_t ts) const {
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
    table_->resize(capacity, vertex_schema_->get_default_properties());
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

void VertexTable::AddProperties(const std::vector<std::string>& properties,
                                const std::vector<DataType>& types,
                                const std::vector<Property>& default_values) {
  table_->add_columns(properties, types, default_values, indexer_->capacity(),
                      memory_level_);
}

void VertexTable::Drop() {
  indexer_->drop();
  table_->drop();
  v_ts_->Clear();
  indexer_.reset();
  table_.reset();
  v_ts_.reset();
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

vid_t VertexTable::insert_vertex_pk(const Property& id, timestamp_t ts,
                                    bool insert_safe) {
  vid_t vid;
  if (NEUG_UNLIKELY(indexer_->get_index(id, vid))) {
    if (NEUG_UNLIKELY(v_ts_->IsVertexValid(vid, ts))) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Vertex with id " + id.to_string() +
                                       " already exists with lid " +
                                       std::to_string(vid));
    }
  } else {
    vid = indexer_->insert(id, insert_safe);
  }
  v_ts_->InsertVertex(vid, ts);
  return vid;
}

}  // namespace neug