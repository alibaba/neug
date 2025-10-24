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
#include "neug/utils/likely.h"

namespace gs {

void VertexTable::Open(const std::string& work_dir, int memory_level,
                       bool build_empty_graph) {
  memory_level_ = memory_level;
  work_dir_ = work_dir;
  std::string tmp_dir_path = tmp_dir(work_dir_);
  std::string checkpoint_dir_path = checkpoint_dir(work_dir_);

  std::string vertex_ts_filename =
      checkpoint_dir_path + "/" + vertex_timestamp_file(v_label_name_);
  auto indexer_filename =
      IndexerType::prefix() + "_" + vertex_map_prefix(v_label_name_);
  if (memory_level_ == 0) {
    indexer_.open(indexer_filename, checkpoint_dir_path, work_dir_);
    table_->open(vertex_table_prefix(v_label_name_), work_dir_, property_names,
                 property_types, storage_strategies);
    vertex_ts_.open(vertex_ts_filename, true);
  } else if (memory_level_ == 1) {
    indexer_.open_in_memory(checkpoint_dir_path + "/" + indexer_filename);
    table_->open_in_memory(vertex_table_prefix(v_label_name_), work_dir_,
                           property_names, property_types, storage_strategies);
    vertex_ts_.open(vertex_ts_filename, false);
  } else if (memory_level_ >= 2) {
    indexer_.open_with_hugepages(checkpoint_dir_path + "/" + indexer_filename,
                                 (memory_level_ > 2));
    table_->open_with_hugepages(vertex_table_prefix(v_label_name_), work_dir_,
                                property_names, property_types,
                                storage_strategies, (memory_level_ > 2));
    vertex_ts_.open_with_hugepages(vertex_ts_filename);
  } else {
    THROW_INTERNAL_EXCEPTION("Invalid memory level: " +
                             std::to_string(memory_level_));
  }
  // TODO(zhanglei): We need to a better way to shrink the vertex_ts_ with
  // checkpoint.
  for (size_t i = 0; i < vertex_ts_.size(); ++i) {
    if (vertex_ts_.get(i) != 0) {
      is_vertex_table_modified_ = true;
      break;
    }
  }
}

void VertexTable::insert_vertices(
    std::shared_ptr<IRecordBatchSupplier> supplier) {
  if (pk_type_ == PropertyType::kInt64) {
    insert_vertices_impl<int64_t>(supplier);
  } else if (pk_type_ == PropertyType::kInt32) {
    insert_vertices_impl<int32_t>(supplier);
  } else if (pk_type_ == PropertyType::kUInt32) {
    insert_vertices_impl<uint32_t>(supplier);
  } else if (pk_type_ == PropertyType::kUInt64) {
    insert_vertices_impl<uint64_t>(supplier);
  } else if (pk_type_.type_enum == impl::PropertyTypeImpl::kVarChar ||
             pk_type_.type_enum == impl::PropertyTypeImpl::kStringView) {
    insert_vertices_impl<std::string_view>(supplier);
  } else {
    LOG(FATAL) << "Unsupported primary key type for vertex, type: " << pk_type_
               << ", label: " << v_label_name_;
  }
}

void VertexTable::Dump(const std::string& target_dir) {
  indexer_.dump(IndexerType::prefix() + "_" + vertex_map_prefix(v_label_name_),
                target_dir);
  table_->resize(indexer_.size());
  table_->dump(vertex_table_prefix(v_label_name_), target_dir);
  vertex_ts_.resize(indexer_.size());
  // When dumping, we need to reset the existing vertex's timestamp to 0.
  for (size_t i = 0; i < vertex_ts_.size(); ++i) {
    if (vertex_ts_.get(i) != INVALID_TIMESTAMP) {
      vertex_ts_.set(i, 0);
    }
  }
  vertex_ts_.dump(target_dir + "/" + vertex_timestamp_file(v_label_name_));
  VLOG(1) << "Dump vertex table " << v_label_name_ << " done, size "
          << indexer_.size();
}

void VertexTable::Close() {
  indexer_.close();
  table_->close();
  vertex_ts_.reset();
}

bool VertexTable::get_index(const Property& oid, vid_t& lid,
                            timestamp_t ts) const {
  auto res = indexer_.get_index(oid, lid);
  if (NEUG_UNLIKELY(res && is_vertex_table_modified_)) {
    if (!is_valid_lid(lid, ts)) {
      LOG(WARNING) << "Lid " << lid << " has been deleted.";
      return false;
    }
  }
  return res;
}

size_t VertexTable::vertex_num(timestamp_t ts) const {
  if (!is_vertex_table_modified_) {
    return indexer_.size();
  } else {
    size_t count = 0;
    for (vid_t lid = 0; lid < indexer_.size(); ++lid) {
      if (vertex_ts_.get(lid) <= ts) {
        count++;
      }
    }
    return count;
  }
}

size_t VertexTable::lid_num() const { return indexer_.size(); }

vid_t VertexTable::add_vertex(const Property& id, timestamp_t ts) {
  indexer_.ensure_writable(work_dir_);
  vid_t vid = indexer_.insert(id);
  vertex_ts_.set(vid, ts);
  if (ts > 0) {
    is_vertex_table_modified_ = true;
  }
  return vid;
}

vid_t VertexTable::add_vertex_safe(const Property& id, timestamp_t ts) {
  auto lid = indexer_.insert_safe(id);
  if (lid >= vertex_ts_.size()) {
    vertex_ts_.resize(vertex_ts_.size() + (vertex_ts_.size() >> 2));
  }
  vertex_ts_.set(lid, ts);
  if (ts > 0) {  // Only mark update ops when ts > 0
    is_vertex_table_modified_ = true;
  }
  return lid;
}

Property VertexTable::get_oid(vid_t lid, timestamp_t ts) const {
  if (NEUG_UNLIKELY(is_vertex_table_modified_)) {
    if (!is_valid_lid(lid, ts)) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Lid " + std::to_string(lid) +
                                       " has been deleted.");
    }
  }
  return indexer_.get_key(lid);
}

bool VertexTable::is_valid_lid(vid_t lid, timestamp_t ts) const {
  // We use numeric_limits<timestamp_t>::max() to denote a deleted vertex.
  // But we ts is passed as a timestamp limit, we take it as a normal timestamp.
  if (NEUG_LIKELY(!is_vertex_table_modified_)) {
    return lid < indexer_.size();
  }
  return lid < indexer_.size() && vertex_ts_.get(lid) <= ts;
}

void VertexTable::Reserve(size_t cap) {
  if (cap > indexer_.capacity()) {
    indexer_.reserve(cap);
  }
  table_->resize(cap);
  vertex_ts_.resize(cap);
}

void VertexTable::BatchAddVertices(std::vector<Property>&& ids,
                                   std::unique_ptr<Table> table,
                                   timestamp_t ts) {
  size_t new_row_num = table_->row_num() + ids.size();
  Reserve(new_row_num);
  vid_t vid;
  size_t cur_idx = 0;
  for (const auto& id : ids) {
    if (indexer_.get_index(id, vid)) {
      cur_idx++;
      continue;
    }
    vid = add_vertex(id, ts);
    table_->insert(vid, table->get_row(cur_idx++));
    vertex_ts_.set(vid, ts);
    if (ts > 0) {  // Only mark update ops when ts > 0
      is_vertex_table_modified_ = true;
    }
  }
}

void VertexTable::BatchDeleteVertices(const std::vector<vid_t>& vids) {
  indexer_.ensure_writable(work_dir_);
  size_t delete_cnt = 0;
  for (auto v : vids) {
    if (v < indexer_.size() && vertex_ts_.get(v) != INVALID_TIMESTAMP) {
      auto oid = indexer_.get_key(v);
      indexer_.remove(oid);
      vertex_ts_.set(v, INVALID_TIMESTAMP);
      delete_cnt++;
    }
  }
  if (delete_cnt > 0) {
    is_vertex_table_modified_ = true;
  }
}

void VertexTable::DeleteProperties(const std::vector<std::string>& properties) {
  for (size_t i = property_names.size(); i-- > 0;) {
    if (std::find(properties.begin(), properties.end(), property_names[i]) !=
        properties.end()) {
      table_->delete_column(property_names[i]);
      property_names.erase(property_names.begin() + i);
      property_types.erase(property_types.begin() + i);
      storage_strategies.erase(storage_strategies.begin() + i);
    }
  }
}

void VertexTable::AddProperties(
    const std::vector<std::string>& properties,
    const std::vector<PropertyType>& types,
    const std::vector<StorageStrategy>& strategies) {
  table_->add_columns(properties, types, memory_level_);
  for (size_t i = 0; i < properties.size(); ++i) {
    property_names.push_back(properties[i]);
    property_types.push_back(types[i]);
    if (i >= strategies.size()) {
      storage_strategies.push_back(StorageStrategy::kMem);
    } else {
      storage_strategies.push_back(strategies[i]);
    }
  }
}

void VertexTable::Drop() {
  indexer_.drop();
  table_->drop();
  vertex_ts_.reset();
  table_.reset();
  // TODO(zhanglei): reset the indexer.
  // indexer_ = IndexerType();
  property_names.clear();
  property_types.clear();
  storage_strategies.clear();
}

void VertexTable::RenameProperties(const std::vector<std::string>& old_names,
                                   const std::vector<std::string>& new_names) {
  for (size_t i = 0; i < old_names.size(); ++i) {
    auto it =
        std::find(property_names.begin(), property_names.end(), old_names[i]);
    if (it == property_names.end()) {
      LOG(ERROR) << "Column " << old_names[i] << " does not exist.";
      return;
    }
    if (std::find(property_names.begin(), property_names.end(), new_names[i]) !=
        property_names.end()) {
      LOG(ERROR) << "Column " << new_names[i] << " already exists.";
      return;
    }
    size_t index = std::distance(property_names.begin(), it);
    property_names[index] = new_names[i];
    table_->rename_column(old_names[i], new_names[i]);
  }
}

void VertexTable::Compact(bool reset_timestamp, timestamp_t ts) {
  if (reset_timestamp) {
    for (size_t i = 0; i < indexer_.size(); i++) {
      if (vertex_ts_.get(i) != INVALID_TIMESTAMP) {
        vertex_ts_.set(i, 0);
      } else {
        VLOG(1) << "Skipping deleted vertex with lid " << i;
      }
    }
  }
  // TODO(zhanglei): Support compact unused lid in indexer_ and table
}

}  // namespace gs