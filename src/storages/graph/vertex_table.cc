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
#include "neug/utils/file_utils.h"
#include "neug/utils/likely.h"

namespace neug {

void VertexTable::Open(const std::string& work_dir, int memory_level) {
  memory_level_ = memory_level;
  work_dir_ = work_dir;
  std::string tmp_dir_path = tmp_dir(work_dir_);
  std::string checkpoint_dir_path = checkpoint_dir(work_dir_);

  const auto& label_name = vertex_schema_->label_name;
  std::string vertex_tracker_filename =
      checkpoint_dir_path + "/" + vertex_tracker_file(label_name);
  auto indexer_filename =
      IndexerType::prefix() + "_" + vertex_map_prefix(label_name);
  if (memory_level_ == 0) {
    indexer_.open(indexer_filename, checkpoint_dir_path, work_dir_);
    table_->open(vertex_table_prefix(label_name), work_dir_,
                 vertex_schema_->property_names, vertex_schema_->property_types,
                 vertex_schema_->default_property_values,
                 vertex_schema_->storage_strategies);

  } else if (memory_level_ == 1) {
    indexer_.open_in_memory(checkpoint_dir_path + "/" + indexer_filename);
    table_->open_in_memory(vertex_table_prefix(label_name), work_dir_,
                           vertex_schema_->property_names,
                           vertex_schema_->property_types,
                           vertex_schema_->default_property_values,
                           vertex_schema_->storage_strategies);

  } else if (memory_level_ >= 2) {
    indexer_.open_with_hugepages(checkpoint_dir_path + "/" + indexer_filename,
                                 (memory_level_ > 2));
    table_->open_with_hugepages(
        vertex_table_prefix(label_name), work_dir_,
        vertex_schema_->property_names, vertex_schema_->property_types,
        vertex_schema_->default_property_values,
        vertex_schema_->storage_strategies, (memory_level_ > 2));
  } else {
    THROW_INTERNAL_EXCEPTION("Invalid memory level: " +
                             std::to_string(memory_level_));
  }
  v_ts_.Open(vertex_tracker_filename);
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

void VertexTable::Dump(const std::string& target_dir) {
  const auto& label_name = vertex_schema_->label_name;
  indexer_.dump(IndexerType::prefix() + "_" + vertex_map_prefix(label_name),
                target_dir);
  table_->dump(vertex_table_prefix(label_name), target_dir);
  v_ts_.Dump(target_dir + "/" + vertex_tracker_file(label_name));
}

void VertexTable::Close() {
  indexer_.close();
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
  auto res = indexer_.get_index(oid, lid);
  if (NEUG_UNLIKELY(res && !v_ts_.IsVertexValid(lid, ts))) {
    return false;
  }
  return res;
}

size_t VertexTable::VertexNum(timestamp_t ts) const {
  return v_ts_.ValidVertexNum(ts, indexer_.size());
}

size_t VertexTable::LidNum() const { return indexer_.size(); }

bool VertexTable::AddVertex(const Property& id,
                            const std::vector<Property>& props, vid_t& vid,
                            timestamp_t ts, bool insert_safe) {
  indexer_.ensure_writable(work_dir_);
  if (indexer_.capacity() <= indexer_.size()) {
    return false;
  }
  vid = insert_vertex_pk(id, ts);
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
  if (NEUG_UNLIKELY(vid >= indexer_.size())) {
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
  if (NEUG_UNLIKELY(lid >= indexer_.size())) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Lid " + std::to_string(lid) +
                                     " is out of range.");
  }
  if (NEUG_UNLIKELY(!v_ts_.IsVertexValid(lid, ts))) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Lid " + std::to_string(lid) +
                                     " has been deleted.");
  }
  return indexer_.get_key(lid);
}

bool VertexTable::IsValidLid(vid_t lid, timestamp_t ts) const {
  return lid < indexer_.size() && v_ts_.IsVertexValid(lid, ts);
}

size_t VertexTable::EnsureCapacity(size_t capacity) {
  if (capacity <= indexer_.capacity()) {
    return indexer_.capacity();
  }
  capacity = std::max(capacity, 4096UL);
  if (capacity > indexer_.capacity()) {
    indexer_.reserve(capacity);
  }
  if (table_ && table_->size() < capacity) {
    table_->resize(capacity);
  }
  v_ts_.Reserve(capacity);
  return indexer_.capacity();
}

void VertexTable::BatchDeleteVertices(const std::vector<vid_t>& vids) {
  indexer_.ensure_writable(work_dir_);
  size_t delete_cnt = 0;
  for (auto v : vids) {
    if (v < indexer_.size() && v_ts_.IsVertexValid(v, MAX_TIMESTAMP)) {
      v_ts_.RemoveVertex(v);
      delete_cnt++;
    }
  }
  VLOG(10) << "Deleted " << delete_cnt << " vertices in batch.";
}

void VertexTable::DeleteVertex(const Property& id, timestamp_t ts) {
  indexer_.ensure_writable(work_dir_);
  vid_t vid;
  if (!get_index(id, vid, ts)) {
    LOG(WARNING) << "Vertex with id " << id.to_string() << " not found.";
    return;
  }
  return DeleteVertex(vid, ts);
}

void VertexTable::DeleteVertex(vid_t lid, timestamp_t ts) {
  indexer_.ensure_writable(work_dir_);
  if (lid >= indexer_.size()) {
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
  indexer_.ensure_writable(work_dir_);
  assert(lid < indexer_.size());
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

void VertexTable::AddProperties(
    const std::vector<std::string>& properties,
    const std::vector<DataType>& types,
    const std::vector<Property>& default_values,
    const std::vector<StorageStrategy>& strategies) {
  table_->add_columns(properties, types, default_values, indexer_.capacity(),
                      strategies, memory_level_);
}

void VertexTable::Drop() {
  indexer_.drop();
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

vid_t VertexTable::insert_vertex_pk(const Property& id, timestamp_t ts) {
  vid_t vid;
  if (NEUG_UNLIKELY(indexer_.get_index(id, vid))) {
    if (NEUG_UNLIKELY(v_ts_.IsVertexValid(vid, ts))) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Vertex with id " + id.to_string() +
                                       " already exists with lid " +
                                       std::to_string(vid));
    }
  } else {
    vid = indexer_.insert(id);
  }
  v_ts_.InsertVertex(vid, ts);
  return vid;
}

}  // namespace neug